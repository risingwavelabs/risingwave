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
#[cfg(test)]
use parking_lot::Mutex;
use parking_lot::RwLock;
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

#[derive(Default)]
struct PinCacheState {
    // `None` means the initial pin-policy/version snapshot has not arrived yet.
    desired: Option<HashMap<HummockSstableObjectId, u64>>,
    // Only complete objects participate in read routing.
    published: HashMap<HummockSstableObjectId, Arc<PinCacheEntry>>,
    published_bytes: u64,
    inflight: HashMap<HummockSstableObjectId, Arc<PinCacheEntry>>,
    recovered_files: Vec<(HummockSstableObjectId, Arc<PinCacheEntry>)>,
    recovery_state: RecoveryState,
    retired: HashMap<HummockSstableObjectId, (u64, HummockVersionId)>,
    generations: HashMap<HummockSstableObjectId, u64>,
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

    fn report_published(&self) {
        GLOBAL_PIN_CACHE_METRICS
            .published_objects
            .set(metric_bytes(self.published.len() as u64));
        GLOBAL_PIN_CACHE_METRICS
            .published_bytes
            .set(metric_bytes(self.published_bytes));
    }
}

/// A local whole-SST cache. The remote object store remains authoritative.
pub(crate) struct PinCache {
    store: ObjectStoreRef,
    state: RwLock<PinCacheState>,
    recovery_notify: Notify,
    gc: Arc<PinCacheGc>,
    next_path_id: AtomicU64,
    #[cfg(test)]
    refill_gate: Mutex<Option<Arc<Semaphore>>>,
}

/// Describes the work performed or skipped by one refill attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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

struct PinCacheDownloadGuard {
    pin_cache: Arc<PinCache>,
    object_id: HummockSstableObjectId,
    entry: Arc<PinCacheEntry>,
    published: bool,
    // OpenDAL may leave a backend-owned atomic-write file when an upload fails or is cancelled.
    // Until finish succeeds, deleting the final path cannot prove that its bytes were reclaimed.
    may_have_temporary_file: bool,
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
        self.may_have_temporary_file = true;
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
        self.may_have_temporary_file = false;
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
        Ok(if self.publish() {
            PinCacheRefillOutcome::Published
        } else {
            PinCacheRefillOutcome::Obsolete
        })
    }

    fn publish(mut self) -> bool {
        let mut state = self.pin_cache.state.write();
        if state
            .inflight
            .get(&self.object_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
            && state.needed_size(self.object_id) == Some(self.entry.size)
        {
            state.inflight.remove(&self.object_id);
            if let Some(previous) = state.published.insert(self.object_id, self.entry.clone()) {
                state.published_bytes -= previous.size;
            }
            state.published_bytes += self.entry.size;
            state.report_published();
            self.published = true;
        }
        self.published
    }
}

impl Drop for PinCacheDownloadGuard {
    fn drop(&mut self) {
        if self.published {
            return;
        }

        let mut state = self.pin_cache.state.write();
        if state
            .inflight
            .get(&self.object_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
        {
            state.inflight.remove(&self.object_id);
        }
        drop(state);

        if self.may_have_temporary_file {
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
            let mut state = self.pin_cache.state.write();
            // A late failure must not invalidate a newer publication of the same object.
            let removed = state
                .published
                .get(&self.object_id)
                .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
                .then(|| state.published.remove(&self.object_id).unwrap());
            if let Some(entry) = &removed {
                state.published_bytes -= entry.size;
                state.report_published();
            }
            removed
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
            state: RwLock::new(PinCacheState::default()),
            recovery_notify: Notify::new(),
            gc,
            next_path_id: AtomicU64::new(rand::random()),
            #[cfg(test)]
            refill_gate: Mutex::new(None),
        });
        pin_cache.state.read().report_published();
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
            let mut state = self.state.write();
            let removed = state
                .retired
                .extract_if(|_, (_, retired_at)| should_revoke(retired_at))
                .map(|(id, _)| id)
                .collect::<Vec<_>>();
            let stale = removed
                .into_iter()
                .filter_map(|id| {
                    state.generations.remove(&id);
                    state.inflight.remove(&id);
                    state.published.remove(&id)
                })
                .collect::<Vec<_>>();
            state.published_bytes -= stale.iter().map(|entry| entry.size).sum::<u64>();
            state.report_published();
            stale
        };
        self.gc.reclaim(stale);
    }

    fn new_object_path(&self, object_id: HummockSstableObjectId) -> String {
        let path_id = self.next_path_id.fetch_add(1, Ordering::Relaxed);
        format!("{}-{path_id}.sst", object_id.as_raw_id())
    }

    pub(crate) fn get(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
    ) -> Option<PinCacheReadHandle> {
        let entry = self.state.read().published.get(&object_id)?.clone();
        Some(PinCacheReadHandle {
            pin_cache: self.clone(),
            object_id,
            entry,
        })
    }

    pub(crate) fn replace_desired_objects(
        self: &Arc<Self>,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) {
        let mut desired = HashMap::new();
        for (object_id, size) in objects {
            desired
                .entry(object_id)
                .and_modify(|existing_size| {
                    assert_eq!(
                        *existing_size, size,
                        "one object must have one physical size"
                    );
                })
                .or_insert(size);
        }

        let stale_objects = {
            let mut state = self.state.write();
            // A policy replacement revokes retired routes as well as current membership.
            state.retired.clear();
            state.desired = Some(desired);
            let PinCacheState {
                desired,
                published,
                inflight,
                generations,
                ..
            } = &mut *state;
            let desired = desired.as_ref().unwrap();
            let needed_size = |object_id: &HummockSstableObjectId| desired.get(object_id).copied();
            generations.retain(|id, _| needed_size(id).is_some());
            // Revoking the token also prevents an old queued or in-flight task from publishing.
            inflight.retain(|object_id, entry| {
                needed_size(object_id).is_some_and(|desired_size| desired_size == entry.size)
            });
            let mut stale = published
                .extract_if(|object_id, entry| {
                    !needed_size(object_id).is_some_and(|desired_size| desired_size == entry.size)
                })
                .map(|(_, entry)| entry)
                .collect::<Vec<_>>();
            state.published_bytes -= stale.iter().map(|entry| entry.size).sum::<u64>();
            if state.recovery_state != RecoveryState::Pending {
                stale.extend(Self::reconcile_recovered_files(&mut state));
            }
            state.report_published();
            stale
        };
        self.gc.reclaim(stale_objects);
    }

    pub(crate) fn replace_version_objects(
        self: &Arc<Self>,
        version: HummockVersionId,
        objects: HashMap<HummockSstableObjectId, u64>,
    ) {
        let previous = self
            .state
            .read()
            .desired
            .as_ref()
            .map(|desired| desired.keys().copied().collect::<Vec<_>>());
        if let Some(previous) = previous {
            self.apply_desired_object_delta(version, previous, objects);
        } else {
            self.replace_desired_objects(objects);
        }
    }

    pub(crate) fn apply_desired_object_delta(
        self: &Arc<Self>,
        version: HummockVersionId,
        removed: impl IntoIterator<Item = HummockSstableObjectId>,
        inserted: HashMap<HummockSstableObjectId, u64>,
    ) {
        let mut state = self.state.write();
        let PinCacheState {
            desired, retired, ..
        } = &mut *state;
        let desired = desired
            .as_mut()
            .expect("pin-cache object delta requires an initial desired snapshot");
        for object_id in removed {
            // An insertion of the same immutable object in this update keeps it desired.
            if !inserted.contains_key(&object_id)
                && let Some(size) = desired.remove(&object_id)
            {
                retired.insert(object_id, (size, version));
            }
        }
        for (object_id, size) in inserted {
            retired.remove(&object_id);
            if let Some(existing_size) = desired.insert(object_id, size) {
                assert_eq!(
                    existing_size, size,
                    "one object must have one physical size"
                );
            }
        }
    }

    pub(crate) fn is_needed(&self, object_id: HummockSstableObjectId) -> bool {
        self.state.read().needed_size(object_id).is_some()
    }

    pub(crate) fn refill_generation(&self, object: HummockSstableObjectId) -> Option<u64> {
        let mut state = self.state.write();
        state.needed_size(object)?;
        if let Some(generation) = state.generations.get(&object) {
            return Some(*generation);
        }
        state.next_generation += 1;
        let generation = state.next_generation;
        state.generations.insert(object, generation);
        Some(generation)
    }

    pub(crate) fn revoke_inflight(&self, object: HummockSstableObjectId) {
        let mut state = self.state.write();
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
        self.state
            .read()
            .desired
            .as_ref()
            .is_some_and(|desired| desired.contains_key(&object_id))
    }

    #[cfg(test)]
    fn start_download(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
    ) -> ObjectResult<PinCacheDownloadStart> {
        self.start_download_at_generation(object_id, None)
    }

    fn start_download_at_generation(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
        generation: Option<u64>,
    ) -> ObjectResult<PinCacheDownloadStart> {
        let mut state = self.state.write();
        if generation.is_some_and(|expected| state.generations.get(&object_id) != Some(&expected)) {
            return Ok(PinCacheDownloadStart::Complete(
                PinCacheRefillOutcome::Obsolete,
            ));
        }
        if state.recovery_state == RecoveryState::Failed {
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
            published: false,
            may_have_temporary_file: false,
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
        let download = match self.start_download_at_generation(object_id, Some(generation))? {
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
