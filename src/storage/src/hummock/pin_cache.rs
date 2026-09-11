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
use futures::StreamExt;
use parking_lot::{Mutex, RwLock};
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    MonitoredStreamingReader, ObjectError, ObjectRangeBounds, ObjectResult, ObjectStoreRef,
};
use thiserror_ext::AsReport;
use tokio::sync::{Notify, Semaphore};

struct PinCacheEntry {
    path: String,
    size: u64,
}

#[derive(Default)]
struct PinCacheGcState {
    accounted_paths: HashMap<String, u64>,
    accounted_bytes: u64,
}

/// Owns physical-path accounting and reclamation. Callers must withdraw routes first.
struct PinCacheGc {
    store: ObjectStoreRef,
    capacity: u64,
    state: Mutex<PinCacheGcState>,
    concurrency: Semaphore,
}

impl PinCacheGc {
    fn new(store: ObjectStoreRef, capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            store,
            capacity,
            state: Mutex::new(PinCacheGcState::default()),
            concurrency: Semaphore::new(2),
        })
    }

    fn account_existing(&self, entry: &PinCacheEntry) {
        let mut state = self.state.lock();
        if state
            .accounted_paths
            .insert(entry.path.clone(), entry.size)
            .is_none()
        {
            state.accounted_bytes = state.accounted_bytes.saturating_add(entry.size);
        }
    }

    fn try_reserve(&self, entry: &PinCacheEntry) -> Result<(), u64> {
        let mut state = self.state.lock();
        let Some(accounted_bytes) = state.accounted_bytes.checked_add(entry.size) else {
            return Err(state.accounted_bytes);
        };
        if accounted_bytes > self.capacity {
            return Err(state.accounted_bytes);
        }
        let old = state.accounted_paths.insert(entry.path.clone(), entry.size);
        debug_assert!(old.is_none(), "pin-cache paths must be unique");
        state.accounted_bytes = accounted_bytes;
        Ok(())
    }

    fn reclaim(self: &Arc<Self>, entries: impl IntoIterator<Item = Arc<PinCacheEntry>>) {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.is_empty() {
            return;
        }
        let gc = self.clone();
        tokio::spawn(async move {
            let _permit = gc.concurrency.acquire().await.unwrap();
            let paths = entries
                .iter()
                .map(|entry| entry.path.clone())
                .collect::<Vec<_>>();
            match gc.store.delete_objects(&paths).await {
                Ok(()) => gc.finish_delete(&paths),
                Err(error) => {
                    tracing::warn!(
                        object_count = paths.len(),
                        error = %error.as_report(),
                        "failed to reclaim pinned SSTs; keeping them accounted until recovery"
                    );
                }
            }
        });
    }

    fn finish_delete(&self, paths: &[String]) {
        let mut state = self.state.lock();
        for path in paths {
            if let Some(size) = state.accounted_paths.remove(path) {
                state.accounted_bytes = state.accounted_bytes.saturating_sub(size);
            }
        }
    }
}

#[derive(Default, PartialEq, Eq)]
enum RecoveryState {
    #[default]
    Pending,
    Ready,
    Failed,
}

#[derive(Default)]
struct PinCacheState {
    // `None` means the initial pin-policy/version snapshot has not arrived yet.
    desired: Option<HashMap<HummockSstableObjectId, u64>>,
    // Only complete objects participate in read routing.
    published: HashMap<HummockSstableObjectId, Arc<PinCacheEntry>>,
    inflight: HashMap<HummockSstableObjectId, Arc<PinCacheEntry>>,
    recovered_files: Vec<(HummockSstableObjectId, Arc<PinCacheEntry>)>,
    recovery_state: RecoveryState,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PinCacheRefillOutcome {
    Published,
    Skipped,
    CapacityRejected,
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
            .await?;
        let mut written = 0_u64;
        while let Some(chunk) = reader.read_bytes().await {
            let chunk = chunk?;
            written = written.saturating_add(chunk.len() as u64);
            if written > self.entry.size {
                return Err(ObjectError::internal(
                    "pinned SST is larger than its version metadata",
                ));
            }
            writer.write_bytes(chunk).await?;
        }
        writer.finish().await?;
        self.may_have_temporary_file = false;
        let local_size = self
            .pin_cache
            .store
            .metadata(&self.entry.path)
            .await?
            .total_size as u64;
        if written != self.entry.size || local_size != self.entry.size {
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
            && state
                .desired
                .as_ref()
                .and_then(|desired| desired.get(&self.object_id))
                .is_some_and(|desired_size| *desired_size == self.entry.size)
        {
            state.inflight.remove(&self.object_id);
            state.published.insert(self.object_id, self.entry.clone());
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
            state
                .published
                .get(&self.object_id)
                .is_some_and(|entry| Arc::ptr_eq(entry, &self.entry))
                .then(|| state.published.remove(&self.object_id).unwrap())
        };
        if let Some(entry) = entry {
            self.pin_cache.gc.reclaim([entry]);
        }
    }

    pub(crate) async fn read(&self, range: impl ObjectRangeBounds) -> ObjectResult<Bytes> {
        let result = self.pin_cache.store.read(&self.entry.path, range).await;
        if result.is_err() {
            self.invalidate();
        }
        result
    }

    pub(crate) async fn streaming_read(
        &self,
        range: impl ObjectRangeBounds,
    ) -> ObjectResult<MonitoredStreamingReader> {
        let result = self
            .pin_cache
            .store
            .streaming_read(&self.entry.path, range)
            .await;
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
        let recovery = pin_cache.clone();
        tokio::spawn(async move { recovery.recover_local_files().await });
        pin_cache
    }

    fn new_object_path(&self, object_id: HummockSstableObjectId) -> String {
        let path_id = self.next_path_id.fetch_add(1, Ordering::Relaxed);
        format!("{}-{path_id}.sst", object_id.as_raw_id())
    }

    fn parse_object_id(path: &str) -> Option<HummockSstableObjectId> {
        if path.contains('/') {
            return None;
        }
        let (object_id, path_id) = path.strip_suffix(".sst")?.split_once('-')?;
        path_id.parse::<u64>().ok()?;
        object_id.parse::<u64>().ok().map(Into::into)
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
            state.desired = Some(desired);
            let PinCacheState {
                desired,
                published,
                inflight,
                ..
            } = &mut *state;
            let desired = desired.as_ref().unwrap();
            // Revoking the token also prevents an old queued or in-flight task from publishing.
            inflight.retain(|object_id, entry| {
                desired
                    .get(object_id)
                    .is_some_and(|desired_size| *desired_size == entry.size)
            });
            let mut stale = published
                .extract_if(|object_id, entry| {
                    !desired
                        .get(object_id)
                        .is_some_and(|desired_size| *desired_size == entry.size)
                })
                .map(|(_, entry)| entry)
                .collect::<Vec<_>>();
            if state.recovery_state != RecoveryState::Pending {
                stale.extend(Self::reconcile_recovered_files(&mut state));
            }
            stale
        };
        self.gc.reclaim(stale_objects);
    }

    pub(crate) fn apply_desired_object_delta(
        self: &Arc<Self>,
        removed: impl IntoIterator<Item = HummockSstableObjectId>,
        inserted: HashMap<HummockSstableObjectId, u64>,
    ) {
        let stale_objects = {
            let mut state = self.state.write();
            let PinCacheState {
                desired,
                published,
                inflight,
                ..
            } = &mut *state;
            let desired = desired
                .as_mut()
                .expect("pin-cache object delta requires an initial desired snapshot");
            let mut stale = Vec::new();
            for object_id in removed {
                // An insertion of the same immutable object in this update keeps it desired.
                if !inserted.contains_key(&object_id) && desired.remove(&object_id).is_some() {
                    // The guard owns cleanup once the active download stops; revoking its token
                    // only prevents publication here.
                    inflight.remove(&object_id);
                    if let Some(entry) = published.remove(&object_id) {
                        stale.push(entry);
                    }
                }
            }
            for (object_id, size) in inserted {
                if let Some(existing_size) = desired.insert(object_id, size) {
                    assert_eq!(
                        existing_size, size,
                        "one object must have one physical size"
                    );
                }
            }
            stale
        };
        self.gc.reclaim(stale_objects);
    }

    pub(crate) fn is_desired(&self, object_id: HummockSstableObjectId) -> bool {
        self.state
            .read()
            .desired
            .as_ref()
            .is_some_and(|desired| desired.contains_key(&object_id))
    }

    fn reconcile_recovered_files(state: &mut PinCacheState) -> Vec<Arc<PinCacheEntry>> {
        let PinCacheState {
            desired,
            published,
            inflight,
            recovered_files,
            ..
        } = state;
        let Some(desired) = desired.as_ref() else {
            return vec![];
        };
        let mut stale = Vec::new();
        for (object_id, entry) in std::mem::take(recovered_files) {
            if desired
                .get(&object_id)
                .is_some_and(|desired_size| *desired_size == entry.size)
                && !published.contains_key(&object_id)
                && !inflight.contains_key(&object_id)
            {
                published.insert(object_id, entry);
            } else {
                stale.push(entry);
            }
        }
        stale
    }

    async fn wait_for_recovery(&self) {
        loop {
            let notified = self.recovery_notify.notified();
            if self.state.read().recovery_state != RecoveryState::Pending {
                return;
            }
            notified.await;
        }
    }

    async fn recover_local_files(self: Arc<Self>) {
        let mut recovered_files = Vec::new();
        let mut stale_objects = Vec::new();
        let mut recovery_failed = false;
        match self.store.list("", None, None).await {
            Ok(mut objects) => {
                while let Some(result) = objects.next().await {
                    match result {
                        Ok(metadata) => {
                            if metadata.key.is_empty() || metadata.key.ends_with('/') {
                                continue;
                            }
                            let entry = Arc::new(PinCacheEntry {
                                path: metadata.key,
                                size: metadata.total_size as u64,
                            });
                            self.gc.account_existing(&entry);
                            if let Some(object_id) = Self::parse_object_id(&entry.path) {
                                recovered_files.push((object_id, entry));
                            } else {
                                stale_objects.push(entry);
                            }
                        }
                        Err(error) => {
                            recovery_failed = true;
                            tracing::warn!(
                                error = %error.as_report(),
                                "failed to inspect an object while recovering pin cache"
                            );
                        }
                    }
                }
            }
            Err(error) => {
                recovery_failed = true;
                tracing::warn!(
                    error = %error.as_report(),
                    "failed to list pin cache while recovering"
                );
            }
        }

        let reconciled_stale_objects = {
            let mut state = self.state.write();
            state.recovered_files = recovered_files;
            state.recovery_state = if recovery_failed {
                RecoveryState::Failed
            } else {
                RecoveryState::Ready
            };
            Self::reconcile_recovered_files(&mut state)
        };
        stale_objects.extend(reconciled_stale_objects);
        self.recovery_notify.notify_waiters();
        self.gc.reclaim(stale_objects);
    }

    fn start_download(
        self: &Arc<Self>,
        object_id: HummockSstableObjectId,
    ) -> ObjectResult<PinCacheDownloadStart> {
        let mut state = self.state.write();
        if state.recovery_state == RecoveryState::Failed {
            return Err(ObjectError::internal(
                "pin cache recovery failed; refusing new local writes",
            ));
        }
        let Some(size) = state
            .desired
            .as_ref()
            .and_then(|desired| desired.get(&object_id))
            .copied()
        else {
            return Ok(PinCacheDownloadStart::Complete(
                PinCacheRefillOutcome::Skipped,
            ));
        };
        if state.published.contains_key(&object_id) || state.inflight.contains_key(&object_id) {
            return Ok(PinCacheDownloadStart::Complete(
                PinCacheRefillOutcome::Skipped,
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
                capacity = self.gc.capacity,
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

    pub(crate) async fn pin_sst(
        self: &Arc<Self>,
        remote_store: ObjectStoreRef,
        remote_path: String,
        object_id: HummockSstableObjectId,
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
        let download = match self.start_download(object_id)? {
            PinCacheDownloadStart::Download(download) => download,
            PinCacheDownloadStart::Complete(outcome) => return Ok(outcome),
        };
        let reader = remote_store.streaming_read(&remote_path, ..).await?;
        download.write(reader).await
    }

    #[cfg(test)]
    pub(crate) fn set_refill_gate_for_test(&self, gate: Arc<Semaphore>) {
        *self.refill_gate.lock() = Some(gate);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt, stream};
    use risingwave_common::config::ObjectStoreConfig;
    use risingwave_hummock_sdk::HummockSstableObjectId;
    use risingwave_object_store::object::{
        InMemObjectStore, MonitoredStreamingReader, ObjectError, ObjectStore, ObjectStoreImpl,
        ObjectStoreRef, build_remote_object_store,
    };

    use super::{
        PinCache, PinCacheDownloadGuard, PinCacheDownloadStart, PinCacheRefillOutcome,
        RecoveryState,
    };
    use crate::monitor::ObjectStoreMetrics;

    fn in_memory_object_store() -> ObjectStoreRef {
        Arc::new(ObjectStoreImpl::InMem(
            InMemObjectStore::for_test().monitored(
                Arc::new(ObjectStoreMetrics::unused()),
                Arc::new(ObjectStoreConfig::default()),
            ),
        ))
    }

    async fn local_object_store() -> (tempfile::TempDir, ObjectStoreRef) {
        let dir = tempfile::tempdir().unwrap();
        let mut config = ObjectStoreConfig {
            upload_part_size: 1,
            ..Default::default()
        };
        config.set_atomic_write_dir();
        let store = Arc::new(
            build_remote_object_store(
                &format!("fs://{}", dir.path().display()),
                Arc::new(ObjectStoreMetrics::unused()),
                "test pin cache",
                Arc::new(config),
            )
            .await,
        );
        (dir, store)
    }

    async fn wait_for_reclaim(pin_cache: &PinCache) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while pin_cache.gc.state.lock().accounted_bytes != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    fn start_download(
        pin_cache: &Arc<PinCache>,
        object_id: HummockSstableObjectId,
    ) -> PinCacheDownloadGuard {
        match pin_cache.start_download(object_id).unwrap() {
            PinCacheDownloadStart::Download(download) => download,
            PinCacheDownloadStart::Complete(outcome) => {
                panic!("expected download, got {outcome:?}")
            }
        }
    }

    fn completed_download_outcome(
        pin_cache: &Arc<PinCache>,
        object_id: HummockSstableObjectId,
    ) -> PinCacheRefillOutcome {
        match pin_cache.start_download(object_id).unwrap() {
            PinCacheDownloadStart::Download(_) => panic!("expected download to be skipped"),
            PinCacheDownloadStart::Complete(outcome) => outcome,
        }
    }

    #[test]
    fn test_parse_finalized_object_path() {
        assert_eq!(
            PinCache::parse_object_id("1001-42.sst"),
            Some(HummockSstableObjectId::from(1001))
        );
        assert_eq!(PinCache::parse_object_id("1001-recovered.sst"), None);
        assert_eq!(PinCache::parse_object_id("1001-42.tmp"), None);
        assert_eq!(PinCache::parse_object_id("nested/1001-42.sst"), None);
    }

    #[tokio::test]
    async fn test_pin_read_and_unpin_lifecycle() {
        let remote_store = in_memory_object_store();
        let (_dir, local_store) = local_object_store().await;
        let pin_cache = PinCache::new(local_store, u64::MAX);
        let object_id = HummockSstableObjectId::from(1001);
        let remote_path = "remote.sst";
        let original = Bytes::from_static(b"complete sst");
        remote_store
            .upload(remote_path, original.clone())
            .await
            .unwrap();

        pin_cache.replace_desired_objects(HashMap::from([(object_id, original.len() as u64)]));
        pin_cache
            .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
            .await
            .unwrap();
        assert!(pin_cache.get(object_id).is_some());
        assert_eq!(
            pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
            original
        );
        let mut reader = pin_cache
            .get(object_id)
            .unwrap()
            .streaming_read(..)
            .await
            .unwrap();
        assert_eq!(reader.read_bytes().await.unwrap().unwrap(), original);
        assert!(reader.read_bytes().await.is_none());

        remote_store
            .upload(remote_path, Bytes::from_static(b"changed remote"))
            .await
            .unwrap();
        pin_cache
            .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
            .await
            .unwrap();
        assert_eq!(
            pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
            original
        );

        pin_cache.replace_desired_objects(HashMap::new());
        assert!(pin_cache.get(object_id).is_none());

        let changed = Bytes::from_static(b"changed remote");
        pin_cache.replace_desired_objects(HashMap::from([(object_id, changed.len() as u64)]));
        pin_cache
            .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
            .await
            .unwrap();
        assert_eq!(
            pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
            changed
        );

        pin_cache.replace_desired_objects(HashMap::new());
        pin_cache
            .pin_sst(remote_store, remote_path.to_owned(), object_id)
            .await
            .unwrap();
        assert!(pin_cache.get(object_id).is_none());
    }

    #[tokio::test]
    async fn test_same_delta_replacement_keeps_route() {
        let remote_store = in_memory_object_store();
        let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX);
        let object_id = HummockSstableObjectId::from(1001);
        remote_store
            .upload("sst", Bytes::from_static(b"12345678"))
            .await
            .unwrap();
        pin_cache.replace_desired_objects([(object_id, 8)]);
        pin_cache
            .pin_sst(remote_store, "sst".into(), object_id)
            .await
            .unwrap();

        pin_cache.apply_desired_object_delta([object_id], HashMap::from([(object_id, 8)]));
        assert!(pin_cache.get(object_id).is_some());

        pin_cache.apply_desired_object_delta([object_id], HashMap::new());
        assert!(pin_cache.get(object_id).is_none());
    }

    #[tokio::test]
    async fn test_inflight_is_not_routable_and_cancellation_releases_token() {
        let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX);
        let object_id = HummockSstableObjectId::from(1001);
        pin_cache.replace_desired_objects(HashMap::from([(object_id, 8)]));

        let download = start_download(&pin_cache, object_id);
        assert!(pin_cache.get(object_id).is_none());
        assert_eq!(
            completed_download_outcome(&pin_cache, object_id),
            PinCacheRefillOutcome::Skipped
        );
        drop(download);
        assert!(pin_cache.state.read().inflight.is_empty());

        let retry = start_download(&pin_cache, object_id);
        pin_cache
            .store
            .upload(&retry.entry.path, Bytes::from_static(b"complete"))
            .await
            .unwrap();
        retry.publish();
        assert!(pin_cache.state.read().inflight.is_empty());
        assert!(pin_cache.get(object_id).is_some());
        assert_eq!(
            completed_download_outcome(&pin_cache, object_id),
            PinCacheRefillOutcome::Skipped
        );
    }

    #[tokio::test]
    async fn test_revoked_download_cannot_publish_or_remove_replacement() {
        for revoke_by_unpin in [false, true] {
            let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX);
            let object_id = HummockSstableObjectId::from(1001);
            let desired = HashMap::from([(object_id, 11)]);
            pin_cache.replace_desired_objects(desired.clone());
            let old = start_download(&pin_cache, object_id);

            if revoke_by_unpin {
                pin_cache.replace_desired_objects(HashMap::new());
            } else {
                pin_cache.replace_desired_objects(HashMap::from([(object_id, 12)]));
            }
            pin_cache.replace_desired_objects(desired);
            let replacement = start_download(&pin_cache, object_id);
            assert_ne!(old.entry.path, replacement.entry.path);

            old.publish();
            assert!(pin_cache.get(object_id).is_none());
            assert!(Arc::ptr_eq(
                &pin_cache.state.read().inflight[&object_id],
                &replacement.entry,
            ));
            pin_cache
                .store
                .upload(&replacement.entry.path, Bytes::from_static(b"replacement"))
                .await
                .unwrap();
            replacement.publish();
            assert_eq!(
                pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
                Bytes::from_static(b"replacement")
            );
        }
    }

    #[tokio::test]
    async fn test_failed_download_can_be_retried() {
        let remote_store = in_memory_object_store();
        let pin_cache = PinCache::new(in_memory_object_store(), 8);
        let object_id = HummockSstableObjectId::from(1001);
        pin_cache.replace_desired_objects(HashMap::from([(object_id, 8)]));
        assert!(
            pin_cache
                .pin_sst(remote_store.clone(), "sst".into(), object_id)
                .await
                .is_err()
        );
        assert!(pin_cache.get(object_id).is_none());
        assert!(pin_cache.state.read().inflight.is_empty());
        wait_for_reclaim(&pin_cache).await;

        remote_store
            .upload("sst", Bytes::from_static(b"complete"))
            .await
            .unwrap();
        pin_cache
            .pin_sst(remote_store, "sst".into(), object_id)
            .await
            .unwrap();
        assert!(pin_cache.get(object_id).is_some());
    }

    #[tokio::test]
    async fn test_interrupted_fs_upload_keeps_capacity_until_recovery() {
        for cancel in [false, true] {
            let (_dir, local_store) = local_object_store().await;
            let pin_cache = PinCache::new(local_store.clone(), 8);
            let object_id = HummockSstableObjectId::from(1001);
            pin_cache.replace_desired_objects([(object_id, 8)]);
            pin_cache.wait_for_recovery().await;
            let download = start_download(&pin_cache, object_id);
            let final_path = download.entry.path.clone();
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let (fail_tx, fail_rx) = tokio::sync::oneshot::channel();
            let reader = MonitoredStreamingReader::new(
                "test",
                Box::pin(
                    stream::iter([
                        Ok(Bytes::from_static(b"half")),
                        Ok(Bytes::from_static(b"x")),
                    ])
                    .chain(stream::once(async move {
                        // Two writes flush the FS position writer's one-chunk buffer.
                        started_tx.send(()).unwrap();
                        let _ = fail_rx.await;
                        Err(ObjectError::internal("injected remote read failure"))
                    })),
                ),
                Arc::new(ObjectStoreMetrics::unused()),
                None,
            );
            let task = tokio::spawn(download.write(reader));
            tokio::time::timeout(Duration::from_secs(5), started_rx)
                .await
                .unwrap()
                .unwrap();
            // Wait for Tokio's buffered file write to reach the filesystem before cancellation.
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let files: Vec<_> = local_store
                        .list("", None, None)
                        .await
                        .unwrap()
                        .try_collect()
                        .await
                        .unwrap();
                    if files.iter().any(|file| file.total_size == 4) {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            if cancel {
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());
            } else {
                fail_tx.send(()).unwrap();
                assert!(task.await.unwrap().is_err());
            }

            assert!(pin_cache.get(object_id).is_none());
            assert!(pin_cache.state.read().inflight.is_empty());
            assert!(
                local_store
                    .metadata(&final_path)
                    .await
                    .unwrap_err()
                    .is_object_not_found_error()
            );
            assert_eq!(pin_cache.gc.state.lock().accounted_bytes, 8);
            assert_eq!(
                completed_download_outcome(&pin_cache, object_id),
                PinCacheRefillOutcome::CapacityRejected
            );
            // Unpin must not release the reservation for the backend-owned temporary file either.
            pin_cache.replace_desired_objects([]);
            assert_eq!(pin_cache.gc.state.lock().accounted_bytes, 8);
            drop(pin_cache);

            let recovered = PinCache::new(local_store.clone(), 8);
            recovered.replace_desired_objects([(object_id, 8)]);
            recovered.wait_for_recovery().await;
            wait_for_reclaim(&recovered).await;
            assert!(recovered.get(object_id).is_none());
            let files: Vec<_> = local_store
                .list("", None, None)
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            assert!(files.iter().all(|file| file.key.ends_with('/')));
            let remote_store = in_memory_object_store();
            remote_store
                .upload("sst", Bytes::from_static(b"complete"))
                .await
                .unwrap();
            recovered
                .pin_sst(remote_store, "sst".into(), object_id)
                .await
                .unwrap();
            assert!(recovered.get(object_id).is_some());
        }
    }

    #[tokio::test]
    async fn test_completed_invalid_fs_upload_reclaims_capacity() {
        let (_dir, local_store) = local_object_store().await;
        let pin_cache = PinCache::new(local_store.clone(), 8);
        let remote_store = in_memory_object_store();
        remote_store
            .upload("sst", Bytes::from_static(b"half"))
            .await
            .unwrap();
        let object_id = HummockSstableObjectId::from(1001);
        pin_cache.replace_desired_objects([(object_id, 8)]);
        assert!(
            pin_cache
                .pin_sst(remote_store, "sst".into(), object_id)
                .await
                .is_err()
        );
        assert!(pin_cache.get(object_id).is_none());
        wait_for_reclaim(&pin_cache).await;
        let files: Vec<_> = local_store
            .list("", None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(files.iter().all(|file| file.key.ends_with('/')));
    }

    #[tokio::test]
    async fn test_failed_recovery_refuses_new_local_writes() {
        let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX);
        pin_cache.wait_for_recovery().await;
        let object_id = HummockSstableObjectId::from(1001);
        pin_cache.replace_desired_objects(HashMap::from([(object_id, 8)]));
        pin_cache.state.write().recovery_state = RecoveryState::Failed;

        let Err(error) = pin_cache.start_download(object_id) else {
            panic!("failed recovery must reject new downloads");
        };
        assert!(error.to_string().contains("recovery failed"));
        assert!(pin_cache.state.read().inflight.is_empty());
    }

    #[tokio::test]
    async fn test_read_failure_only_invalidates_selected_publication() {
        for streaming in [false, true] {
            let remote_store = in_memory_object_store();
            let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX);
            let object_id = HummockSstableObjectId::from(1001);
            pin_cache.replace_desired_objects(HashMap::from([(object_id, 8)]));
            remote_store
                .upload("sst", Bytes::from_static(b"complete"))
                .await
                .unwrap();
            pin_cache
                .pin_sst(remote_store.clone(), "sst".into(), object_id)
                .await
                .unwrap();
            let old = pin_cache.get(object_id).unwrap();
            pin_cache.store.delete(&old.entry.path).await.unwrap();
            if streaming {
                assert!(old.streaming_read(..).await.is_err());
            } else {
                assert!(old.read(..).await.is_err());
            }
            assert!(pin_cache.get(object_id).is_none());

            pin_cache
                .pin_sst(remote_store, "sst".into(), object_id)
                .await
                .unwrap();
            // This handle stays on the old path and must not remove the new route.
            if streaming {
                assert!(old.streaming_read(..).await.is_err());
            } else {
                assert!(old.read(..).await.is_err());
            }
            assert_eq!(
                pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
                Bytes::from_static(b"complete")
            );
        }
    }

    #[tokio::test]
    async fn test_pending_deletion_keeps_capacity_reserved() {
        let local_store = in_memory_object_store();
        let remote_store = in_memory_object_store();
        let pin_cache = PinCache::new(local_store, 4);
        let old_object = HummockSstableObjectId::from(1001);
        let new_object = HummockSstableObjectId::from(1002);
        remote_store
            .upload("old", Bytes::from_static(b"old!"))
            .await
            .unwrap();
        remote_store
            .upload("new", Bytes::from_static(b"new!"))
            .await
            .unwrap();

        pin_cache.replace_desired_objects(HashMap::from([(old_object, 4), (new_object, 4)]));
        pin_cache
            .pin_sst(remote_store.clone(), "old".into(), old_object)
            .await
            .unwrap();
        let gc_permits = pin_cache.gc.concurrency.acquire_many(2).await.unwrap();
        pin_cache.replace_desired_objects(HashMap::from([(new_object, 4)]));
        pin_cache
            .pin_sst(remote_store.clone(), "new".into(), new_object)
            .await
            .unwrap();
        assert!(pin_cache.get(new_object).is_none());

        drop(gc_permits);
        while pin_cache.gc.state.lock().accounted_bytes != 0 {
            tokio::task::yield_now().await;
        }
        pin_cache
            .pin_sst(remote_store, "new".into(), new_object)
            .await
            .unwrap();
        assert!(pin_cache.get(new_object).is_some());
    }

    #[tokio::test]
    async fn test_recovery_publishes_only_desired_complete_file() {
        let remote_store = in_memory_object_store();
        let (_dir, local_store) = local_object_store().await;
        let object_id = HummockSstableObjectId::from(1001);
        let data = Bytes::from_static(b"complete");
        remote_store.upload("sst", data.clone()).await.unwrap();

        let first = PinCache::new(local_store.clone(), 1024);
        first.replace_desired_objects(HashMap::from([(object_id, data.len() as u64)]));
        first
            .pin_sst(remote_store, "sst".into(), object_id)
            .await
            .unwrap();
        assert!(first.get(object_id).is_some());
        drop(first);

        let recovered = PinCache::new(local_store, 1024);
        recovered.replace_desired_objects(HashMap::from([(object_id, data.len() as u64)]));
        recovered.wait_for_recovery().await;
        assert_eq!(
            recovered.get(object_id).unwrap().read(..).await.unwrap(),
            data
        );
    }

    #[tokio::test]
    async fn test_recovery_waits_for_initial_desired_snapshot_before_cleanup() {
        use risingwave_hummock_sdk::version::HummockVersion;
        use risingwave_pb::hummock::PbHummockVersion;

        use crate::hummock::iterator::test_utils::mock_sstable_store;
        use crate::hummock::local_version::pinned_version::PinnedVersion;
        use crate::hummock::pin_cache_refill::PinCacheRefillController;

        let local_store = in_memory_object_store();
        let path = "1001-42.sst";
        local_store
            .upload(path, Bytes::from_static(b"stale"))
            .await
            .unwrap();
        local_store
            .upload("unfinished.tmp", Bytes::from_static(b"partial"))
            .await
            .unwrap();
        let pin_cache = PinCache::new(local_store.clone(), 1024);
        pin_cache.wait_for_recovery().await;

        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while local_store.metadata("unfinished.tmp").await.is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert!(local_store.metadata(path).await.is_ok());
        let sstable_store = mock_sstable_store().await;
        sstable_store.set_pin_cache(pin_cache.clone());
        let version = PinnedVersion::new(
            HummockVersion::from(PbHummockVersion::default()),
            tokio::sync::mpsc::unbounded_channel().0,
        );
        let mut controller = PinCacheRefillController::new(sstable_store, version);
        // The first empty policy snapshot must initialize membership, not take the no-op path.
        controller.replace_policies(&HashMap::new());
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while local_store.metadata(path).await.is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pin_cache.gc.state.lock().accounted_bytes, 0);
    }
}
