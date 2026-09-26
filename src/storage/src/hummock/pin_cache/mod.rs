// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use parking_lot::{Mutex, RwLock};
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::{HummockSstableObjectId, HummockVersionId};
use risingwave_object_store::object::{
    MonitoredStreamingReader, ObjectError, ObjectRangeBounds, ObjectResult, ObjectStoreRef,
};
use tokio::sync::Notify;
#[cfg(test)]
use tokio::sync::Semaphore;

mod gc;
mod recovery;
#[cfg(test)]
mod tests;

use gc::PinCacheGc;
use recovery::RecoveryState;

use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

fn metric_bytes(bytes: u64) -> i64 {
    bytes.min(i64::MAX as u64) as i64
}

struct PinCacheEntry {
    path: String,
    size: u64,
}

// All lifecycle state for an object lives in one shard.
#[derive(Default)]
struct PinCacheState {
    // `None` means the initial pin-policy/version snapshot has not arrived yet.
    desired: Option<HashMap<HummockSstableObjectId, u64>>,
    // Only complete objects participate in read routing.
    published: HashMap<HummockSstableObjectId, Arc<PinCacheEntry>>,
    // Arc identity is the active download token; an obsolete guard cannot remove its replacement.
    inflight: HashMap<HummockSstableObjectId, Arc<PinCacheEntry>>,
    // Startup inventory waiting for the initial desired snapshot.
    recovered_files: Vec<(HummockSstableObjectId, Arc<PinCacheEntry>)>,
    // Size and the version whose application allows the old route to be reclaimed.
    retired: HashMap<HummockSstableObjectId, (u64, HummockVersionId)>,
    // Admission tokens for queued refills, including those that have not started downloading.
    generations: HashMap<HummockSstableObjectId, u64>,
    // Never reset on removal: reintroducing an object must not admit an old queued refill.
    next_generation: u64,
}

impl PinCacheState {
    fn needed_size(&self, id: HummockSstableObjectId) -> Option<u64> {
        self.desired
            .as_ref()
            .and_then(|desired| desired.get(&id))
            .copied()
            .or_else(|| self.retired.get(&id).map(|(size, _)| *size))
    }

    fn publish(&mut self, object_id: HummockSstableObjectId, entry: Arc<PinCacheEntry>) {
        assert!(!self.published.contains_key(&object_id));
        GLOBAL_PIN_CACHE_METRICS.published_objects.inc();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .add(metric_bytes(entry.size));
        self.published.insert(object_id, entry);
    }

    fn remove_published(
        &mut self,
        object_id: HummockSstableObjectId,
    ) -> Option<Arc<PinCacheEntry>> {
        let entry = self.published.remove(&object_id)?;
        GLOBAL_PIN_CACHE_METRICS.published_objects.dec();
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .sub(metric_bytes(entry.size));
        Some(entry)
    }

    fn apply_desired_object_delta(
        &mut self,
        version: HummockVersionId,
        removed: impl IntoIterator<Item = HummockSstableObjectId>,
        inserted: HashMap<HummockSstableObjectId, u64>,
    ) {
        let desired = self
            .desired
            .as_mut()
            .expect("pin-cache object delta requires an initial desired snapshot");
        for object_id in removed {
            // An insertion of the same immutable object in this update keeps it desired.
            if !inserted.contains_key(&object_id)
                && let Some(size) = desired.remove(&object_id)
            {
                self.retired.insert(object_id, (size, version));
            }
        }
        for (object_id, size) in inserted {
            self.retired.remove(&object_id);
            if let Some(existing_size) = desired.insert(object_id, size) {
                assert_eq!(
                    existing_size, size,
                    "one object must have one physical size"
                );
            }
        }
    }

    fn replace_desired_objects(
        &mut self,
        desired: HashMap<HummockSstableObjectId, u64>,
    ) -> Vec<Arc<PinCacheEntry>> {
        // A policy replacement revokes retired routes as well as current membership.
        self.retired.clear();
        self.generations.retain(|id, _| desired.contains_key(id));
        self.inflight
            .retain(|id, entry| desired.get(id) == Some(&entry.size));
        let removed = self
            .published
            .iter()
            .filter(|(id, entry)| desired.get(*id) != Some(&entry.size))
            .map(|(&id, _)| id)
            .collect::<Vec<_>>();
        let mut stale = removed
            .into_iter()
            .filter_map(|id| self.remove_published(id))
            .collect::<Vec<_>>();
        self.desired = Some(desired);
        stale.extend(self.reconcile_recovered_files());
        stale
    }
}

// Fixed independently of the disk budget: capacity is shared by all shards.
const PIN_CACHE_SHARDS: usize = 64;

/// Membership and the published route observed under the same shard read lock.
pub(crate) struct PinCacheLookup {
    pub(crate) desired: bool,
    pub(crate) route: Option<PinCacheReadHandle>,
}

/// A local whole-SST cache. The remote object store remains authoritative.
pub(crate) struct PinCache {
    store: ObjectStoreRef,
    shards: [RwLock<PinCacheState>; PIN_CACHE_SHARDS],
    // Serializes batch membership updates and recovery, never acquired by foreground lookups.
    // A batch becomes visible shard by shard; each object's transition remains atomic.
    // Lock order: membership_update -> one shard -> GC. Never hold two shard locks together.
    membership_update: Mutex<()>,
    recovery_state: RwLock<RecoveryState>,
    recovery_notify: Notify,
    gc: Arc<PinCacheGc>,
    next_path_id: AtomicU64,
    #[cfg(test)]
    refill_gate: Mutex<Option<Arc<Semaphore>>>,
}

/// Describes the work performed or skipped by one refill attempt.
#[derive(Debug, Eq, PartialEq)]
pub(crate) enum PinCacheRefillOutcome {
    /// This attempt copied and validated the SST, then published its local read route.
    Published,
    /// A local read route already existed, so this attempt skipped the download.
    AlreadyPublished,
    /// Another download for this object is still in flight; this attempt skipped the download.
    InProgress,
    /// This attempt could not reserve enough local capacity to start downloading.
    CapacityRejected,
    /// This attempt is no longer eligible to publish, for example after its generation is revoked.
    Obsolete,
}

enum PinCacheDownloadStart {
    Download(PinCacheDownloadGuard),
    Complete(PinCacheRefillOutcome),
}

/// Cleanup responsibility retained by a download until its local route is published.
enum CleanupAction {
    /// Deleting the final path is sufficient to reclaim this reservation.
    Reclaim,
    /// Backend-owned temporary files may remain; retain capacity until startup recovery.
    RetainReservation,
    /// The published route now owns the file.
    KeepPublished,
}

struct PinCacheDownloadGuard {
    pin_cache: Arc<PinCache>,
    object_id: HummockSstableObjectId,
    entry: Arc<PinCacheEntry>,
    cleanup: CleanupAction,
}

impl PinCacheDownloadGuard {
    fn record_io_failure(&self, phase: &'static str) {
        GLOBAL_PIN_CACHE_METRICS
            .io_failures
            .with_label_values(&[phase])
            .inc();
    }

    /// Copies an existing SST stream into the reserved local path using the object-store uploader.
    /// After finishing the upload, checks both the copied byte count and local file size before
    /// attempting publication. Publication still requires the current download token and membership.
    /// On error or cancellation, `Drop` reclaims the path or retains its capacity reservation if
    /// backend-owned temporary files may remain.
    async fn write(
        mut self,
        mut reader: MonitoredStreamingReader,
    ) -> ObjectResult<PinCacheRefillOutcome> {
        // Set this before opening the writer: cancellation can occur during its creation too.
        self.cleanup = CleanupAction::RetainReservation;
        let mut writer = self
            .pin_cache
            .store
            .streaming_upload(&self.entry.path)
            .await
            .inspect_err(|_| self.record_io_failure("local_upload_init"))?;
        let mut written = 0_u64;
        while let Some(chunk) = reader.read_bytes().await {
            let chunk = chunk.inspect_err(|_| self.record_io_failure("remote_read"))?;
            written = written.saturating_add(chunk.len() as u64);
            if written > self.entry.size {
                self.record_io_failure("size_validation");
                return Err(ObjectError::internal(
                    "pinned SST is larger than its version metadata",
                ));
            }
            writer
                .write_bytes(chunk)
                .await
                .inspect_err(|_| self.record_io_failure("local_upload_write"))?;
        }
        writer
            .finish()
            .await
            .inspect_err(|_| self.record_io_failure("local_upload_finish"))?;
        self.cleanup = CleanupAction::Reclaim;
        let local_size = self
            .pin_cache
            .store
            .metadata(&self.entry.path)
            .await
            .inspect_err(|_| self.record_io_failure("local_metadata"))?
            .total_size as u64;
        if written != self.entry.size || local_size != self.entry.size {
            self.record_io_failure("size_validation");
            return Err(ObjectError::internal(
                "pinned SST size does not match its version metadata",
            ));
        }
        Ok(self.publish())
    }

    /// Removes only this guard's download token, leaving any replacement download intact.
    /// Returns whether this guard still owned the token.
    fn remove_inflight(&self, state: &mut PinCacheState) -> bool {
        if state
            .inflight
            .get(&self.object_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
        {
            state.inflight.remove(&self.object_id);
            true
        } else {
            false
        }
    }

    /// Publishes a completed, validated file if this download is still needed and owns its token.
    /// Consumes the guard: success transfers cleanup to the published route; rejection leaves
    /// cleanup to `Drop` after the state lock is released.
    fn publish(mut self) -> PinCacheRefillOutcome {
        let mut state = self.pin_cache.shard(self.object_id).write();
        if state.needed_size(self.object_id) != Some(self.entry.size) {
            return PinCacheRefillOutcome::Obsolete;
        }
        if !self.remove_inflight(&mut state) {
            return PinCacheRefillOutcome::Obsolete;
        }

        // Downloads cannot start for published objects, and recovery skips in-flight objects.
        state.publish(self.object_id, self.entry.clone());
        self.cleanup = CleanupAction::KeepPublished;
        PinCacheRefillOutcome::Published
    }
}

impl Drop for PinCacheDownloadGuard {
    fn drop(&mut self) {
        let retain_reservation = match self.cleanup {
            CleanupAction::Reclaim => false,
            CleanupAction::RetainReservation => true,
            CleanupAction::KeepPublished => return,
        };

        self.remove_inflight(&mut self.pin_cache.shard(self.object_id).write());

        if retain_reservation {
            // Keep the full reservation until startup recovery inventories the actual files.
            // Do not let final-path deletion release capacity while hidden temporary bytes remain.
            self.pin_cache.gc.mark_uncertain(&self.entry);
            tracing::warn!(
                object_id = self.object_id.as_raw_id(),
                path = %self.entry.path,
                reserved_bytes = self.entry.size,
                "unfinished pin cache upload; retaining capacity until recovery"
            );
            return;
        }
        self.pin_cache.gc.reclaim([self.entry.clone()]);
    }
}

/// A snapshot of a published route. Reads never look up the object a second time.
#[derive(Clone)]
pub(crate) struct PinCacheReadHandle {
    pin_cache: Arc<PinCache>,
    object_id: HummockSstableObjectId,
    entry: Arc<PinCacheEntry>,
}

impl PinCacheReadHandle {
    pub(crate) fn invalidate(&self) {
        let entry = {
            let mut state = self.pin_cache.shard(self.object_id).write();
            // A late failure must not invalidate a newer publication of the same object.
            state
                .published
                .get(&self.object_id)
                .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
                .then(|| state.remove_published(self.object_id).unwrap())
        };
        if let Some(entry) = entry {
            self.pin_cache.gc.reclaim([entry]);
        }
    }

    /// Reads the selected publication through the local object store. On failure, invalidates only
    /// this publication and returns the error so the caller can fall back to its normal read path.
    pub(crate) async fn read(&self, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let result = self.pin_cache.store.read(&self.entry.path, range).await;
        if result.is_err() {
            self.invalidate();
        }
        result
    }
}

impl PinCache {
    pub(crate) fn new(store: ObjectStoreRef, capacity: u64) -> Arc<Self> {
        let gc = PinCacheGc::new(store.clone(), capacity);
        let pin_cache = Arc::new(Self {
            store,
            shards: std::array::from_fn(|_| RwLock::new(PinCacheState::default())),
            membership_update: Mutex::new(()),
            recovery_state: RwLock::new(RecoveryState::Pending),
            recovery_notify: Notify::new(),
            gc,
            next_path_id: AtomicU64::new(rand::random()),
            #[cfg(test)]
            refill_gate: Mutex::new(None),
        });
        GLOBAL_PIN_CACHE_METRICS.published_objects.set(0);
        GLOBAL_PIN_CACHE_METRICS.published_bytes.set(0);
        GLOBAL_PIN_CACHE_METRICS.recovery_ready.set(0);
        let recovery = pin_cache.clone();
        tokio::spawn(async move {
            let objects = recovery.store.list("", None, None).await;
            recovery.recover_local_files(objects).await;
        });
        pin_cache
    }

    /// Inputs stay routable until replacement application. Older snapshots may fall back later.
    pub(crate) fn release_retired(self: &Arc<Self>, applied: HummockVersionId) {
        self.revoke_retired_if(|retired_at| applied >= *retired_at);
    }

    /// A policy or ownership change takes precedence over pending version handoff.
    pub(crate) fn revoke_retired(self: &Arc<Self>) {
        self.revoke_retired_if(|_| true);
    }

    fn revoke_retired_if(
        self: &Arc<Self>,
        mut should_revoke: impl FnMut(&HummockVersionId) -> bool,
    ) {
        let stale = {
            let _update = self.membership_update.lock();
            let mut stale = Vec::new();
            for shard in &self.shards {
                let mut state = shard.write();
                let removed = state
                    .retired
                    .extract_if(|_, (_, retired_at)| should_revoke(retired_at))
                    .map(|(id, _)| id)
                    .collect::<Vec<_>>();
                for id in removed {
                    state.generations.remove(&id);
                    state.inflight.remove(&id);
                    stale.extend(state.remove_published(id));
                }
            }
            stale
        };
        self.gc.reclaim(stale);
    }

    fn shard_index(object_id: HummockSstableObjectId) -> usize {
        xxhash_rust::xxh64::xxh64(&object_id.as_raw_id().to_le_bytes(), 0) as usize
            % PIN_CACHE_SHARDS
    }

    fn shard(&self, object_id: HummockSstableObjectId) -> &RwLock<PinCacheState> {
        &self.shards[Self::shard_index(object_id)]
    }

    fn new_object_path(&self, object_id: HummockSstableObjectId) -> String {
        let path_id = self.next_path_id.fetch_add(1, Ordering::Relaxed);
        format!("{}-{path_id}.sst", object_id.as_raw_id())
    }

    pub(crate) fn get(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
    ) -> Option<PinCacheReadHandle> {
        self.lookup(object_id).route
    }

    pub(crate) fn lookup(self: &Arc<Self>, object_id: HummockSstableObjectId) -> PinCacheLookup {
        let state = self.shard(object_id).read();
        PinCacheLookup {
            desired: state
                .desired
                .as_ref()
                .is_some_and(|desired| desired.contains_key(&object_id)),
            route: state
                .published
                .get(&object_id)
                .map(|entry| PinCacheReadHandle {
                    pin_cache: self.clone(),
                    object_id,
                    entry: entry.clone(),
                }),
        }
    }

    fn partition_objects(
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) -> [HashMap<HummockSstableObjectId, u64>; PIN_CACHE_SHARDS] {
        let mut shards = std::array::from_fn(|_| HashMap::new());
        for (id, size) in objects {
            if let Some(previous) = shards[Self::shard_index(id)].insert(id, size) {
                assert_eq!(previous, size, "one object must have one physical size");
            }
        }
        shards
    }

    /// Replaces policy/ownership membership immediately, revoking removed routes and refills.
    /// Unlike a version update, this does not preserve retired objects for handoff.
    pub(crate) fn replace_desired_objects(
        self: &Arc<Self>,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) {
        let desired = Self::partition_objects(objects);
        let stale = {
            let _update = self.membership_update.lock();
            let mut stale = Vec::new();
            for (shard, desired) in self.shards.iter().zip_eq_fast(desired) {
                stale.extend(shard.write().replace_desired_objects(desired));
            }
            stale
        };
        self.gc.reclaim(stale);
    }

    /// Installs a version snapshot, retaining removed objects until `release_retired(version)`.
    /// Also accepts the initial snapshot, before any membership has been installed.
    pub(crate) fn replace_version_objects(
        self: &Arc<Self>,
        version: HummockVersionId,
        objects: HashMap<HummockSstableObjectId, u64>,
    ) {
        let desired = Self::partition_objects(objects);
        let stale = {
            let _update = self.membership_update.lock();
            let mut stale = Vec::new();
            for (shard, desired) in self.shards.iter().zip_eq_fast(desired) {
                let mut state = shard.write();
                if let Some(previous) = &state.desired {
                    let removed = previous.keys().copied().collect::<Vec<_>>();
                    state.apply_desired_object_delta(version, removed, desired);
                } else {
                    stale.extend(state.replace_desired_objects(desired));
                }
            }
            stale
        };
        self.gc.reclaim(stale);
    }

    /// Applies a version delta after initialization, with the same handoff rule as a snapshot.
    pub(crate) fn apply_desired_object_delta(
        self: &Arc<Self>,
        version: HummockVersionId,
        removed: impl IntoIterator<Item = HummockSstableObjectId>,
        inserted: HashMap<HummockSstableObjectId, u64>,
    ) {
        let mut removed_by_shard: [Vec<_>; PIN_CACHE_SHARDS] = std::array::from_fn(|_| Vec::new());
        for id in removed {
            removed_by_shard[Self::shard_index(id)].push(id);
        }
        let inserted_by_shard = Self::partition_objects(inserted);
        let _update = self.membership_update.lock();
        for ((shard, removed), inserted) in self
            .shards
            .iter()
            .zip_eq_fast(removed_by_shard)
            .zip_eq_fast(inserted_by_shard)
        {
            if !removed.is_empty() || !inserted.is_empty() {
                shard
                    .write()
                    .apply_desired_object_delta(version, removed, inserted);
            }
        }
    }

    pub(crate) fn is_needed(&self, object_id: HummockSstableObjectId) -> bool {
        self.shard(object_id)
            .read()
            .needed_size(object_id)
            .is_some()
    }

    /// Returns the admission token to capture when queuing a refill for a needed object.
    pub(crate) fn refill_generation(&self, object: HummockSstableObjectId) -> Option<u64> {
        let mut state = self.shard(object).write();
        state.needed_size(object)?;
        if let Some(generation) = state.generations.get(&object) {
            return Some(*generation);
        }
        state.next_generation += 1;
        let generation = state.next_generation;
        state.generations.insert(object, generation);
        Some(generation)
    }

    /// Revokes queued and active refills without withdrawing an already published route.
    /// Running I/O may finish, but its guard can no longer publish.
    pub(crate) fn revoke_inflight(&self, object: HummockSstableObjectId) {
        let mut state = self.shard(object).write();
        state.inflight.remove(&object);
        if state.needed_size(object).is_none() {
            state.generations.remove(&object);
            return;
        }
        state.next_generation += 1;
        let generation = state.next_generation;
        state.generations.insert(object, generation);
    }

    pub(crate) fn is_desired(&self, object_id: HummockSstableObjectId) -> bool {
        self.shard(object_id)
            .read()
            .desired
            .as_ref()
            .is_some_and(|desired| desired.contains_key(&object_id))
    }

    fn start_download_at_generation(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
        generation: u64,
    ) -> ObjectResult<PinCacheDownloadStart> {
        let recovery_failed = *self.recovery_state.read() == RecoveryState::Failed;
        let mut state = self.shard(object_id).write();
        if state.generations.get(&object_id) != Some(&generation) {
            return Ok(PinCacheDownloadStart::Complete(
                PinCacheRefillOutcome::Obsolete,
            ));
        }
        if recovery_failed {
            return Err(ObjectError::internal(
                "pin cache recovery failed; refusing new local writes",
            ));
        }
        let Some(size) = state.needed_size(object_id) else {
            return Ok(PinCacheDownloadStart::Complete(
                PinCacheRefillOutcome::Obsolete,
            ));
        };
        if state.inflight.contains_key(&object_id) {
            return Ok(PinCacheDownloadStart::Complete(
                PinCacheRefillOutcome::InProgress,
            ));
        }
        if state.published.contains_key(&object_id) {
            return Ok(PinCacheDownloadStart::Complete(
                PinCacheRefillOutcome::AlreadyPublished,
            ));
        }
        let entry = Arc::new(PinCacheEntry {
            path: self.new_object_path(object_id),
            size,
        });
        if let Err(accounted_bytes) = self.gc.try_reserve(&entry) {
            tracing::warn!(
                object_id = object_id.as_raw_id(),
                object_size = size,
                accounted_bytes,
                capacity = self.gc.capacity(),
                "skipping pin cache refill because local capacity is exhausted"
            );
            return Ok(PinCacheDownloadStart::Complete(
                PinCacheRefillOutcome::CapacityRejected,
            ));
        }
        state.inflight.insert(object_id, entry.clone());
        Ok(PinCacheDownloadStart::Download(PinCacheDownloadGuard {
            pin_cache: self.clone(),
            object_id,
            entry,
            cleanup: CleanupAction::Reclaim,
        }))
    }

    #[cfg(test)]
    pub(crate) fn set_refill_gate_for_test(&self, gate: Arc<Semaphore>) {
        *self.refill_gate.lock() = Some(gate);
    }

    #[cfg(test)]
    pub(crate) async fn pin_sst(
        self: &Arc<Self>,
        remote_store: ObjectStoreRef,
        remote_path: String,
        object_id: HummockSstableObjectId,
    ) -> ObjectResult<PinCacheRefillOutcome> {
        let Some(generation) = self.refill_generation(object_id) else {
            return Ok(PinCacheRefillOutcome::Obsolete);
        };
        self.pin_sst_at_generation(remote_store, remote_path, object_id, generation)
            .await
    }

    pub(crate) async fn pin_sst_at_generation(
        self: &Arc<Self>,
        remote_store: ObjectStoreRef,
        remote_path: String,
        object_id: HummockSstableObjectId,
        generation: u64,
    ) -> ObjectResult<PinCacheRefillOutcome> {
        self.wait_for_recovery().await;
        #[cfg(test)]
        let refill_gate = { self.refill_gate.lock().clone() };
        #[cfg(test)]
        if let Some(gate) = refill_gate {
            gate.acquire()
                .await
                .expect("test refill gate must stay open")
                .forget();
        }
        let download = match self.start_download_at_generation(object_id, generation)? {
            PinCacheDownloadStart::Download(download) => download,
            PinCacheDownloadStart::Complete(outcome) => return Ok(outcome),
        };
        let reader = match remote_store.streaming_read(&remote_path, ..).await {
            Ok(reader) => reader,
            Err(error) => {
                download.record_io_failure("remote_read_init");
                return Err(error);
            }
        };
        download.write(reader).await
    }
}
