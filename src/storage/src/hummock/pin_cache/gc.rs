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
use std::collections::hash_map::Entry;
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_object_store::object::ObjectStoreRef;
use thiserror_ext::AsReport;
use tokio::sync::Semaphore;

use super::{PinCacheEntry, metric_bytes};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

struct AccountedPath {
    size: u64,
    // Whether backend-owned temporary files may remain after deleting the final path.
    uncertain: bool,
}

#[derive(Default)]
struct PinCacheGcState {
    accounted_paths: HashMap<String, AccountedPath>,
    accounted_bytes: u64,
    // A subset of accounted_bytes, retained until a new cache inventories the actual files.
    uncertain_bytes: u64,
}

/// Owns physical-path accounting and reclamation. Callers must withdraw routes first.
pub(super) struct PinCacheGc {
    store: ObjectStoreRef,
    capacity: u64,
    state: Mutex<PinCacheGcState>,
    concurrency: Semaphore,
}

impl PinCacheGc {
    pub(super) fn capacity(&self) -> u64 {
        self.capacity
    }

    #[cfg(test)]
    pub(super) fn accounted_bytes(&self) -> u64 {
        self.state.lock().accounted_bytes
    }

    pub(super) fn new(store: ObjectStoreRef, capacity: u64) -> Arc<Self> {
        GLOBAL_PIN_CACHE_METRICS
            .capacity_bytes
            .set(metric_bytes(capacity));
        GLOBAL_PIN_CACHE_METRICS.accounted_bytes.set(0);
        GLOBAL_PIN_CACHE_METRICS.uncertain_bytes.set(0);
        Arc::new(Self {
            store,
            capacity,
            state: Mutex::new(PinCacheGcState::default()),
            concurrency: Semaphore::new(2),
        })
    }

    pub(super) fn account_existing(&self, entry: &PinCacheEntry) {
        let mut state = self.state.lock();
        if let Entry::Vacant(slot) = state.accounted_paths.entry(entry.path.clone()) {
            slot.insert(AccountedPath {
                size: entry.size,
                uncertain: false,
            });
            state.accounted_bytes = state.accounted_bytes.saturating_add(entry.size);
            GLOBAL_PIN_CACHE_METRICS
                .accounted_bytes
                .set(metric_bytes(state.accounted_bytes));
        }
    }

    pub(super) fn try_reserve(&self, entry: &PinCacheEntry) -> Result<(), u64> {
        let mut state = self.state.lock();
        let Some(accounted_bytes) = state.accounted_bytes.checked_add(entry.size) else {
            return Err(state.accounted_bytes);
        };
        if accounted_bytes > self.capacity {
            return Err(state.accounted_bytes);
        }
        let old = state.accounted_paths.insert(
            entry.path.clone(),
            AccountedPath {
                size: entry.size,
                uncertain: false,
            },
        );
        debug_assert!(old.is_none(), "pin-cache paths must be unique");
        state.accounted_bytes = accounted_bytes;
        GLOBAL_PIN_CACHE_METRICS
            .accounted_bytes
            .set(metric_bytes(state.accounted_bytes));
        Ok(())
    }

    pub(super) fn mark_uncertain(&self, entry: &PinCacheEntry) {
        let mut state = self.state.lock();
        if let Some(path) = state.accounted_paths.get_mut(&entry.path)
            && !path.uncertain
        {
            path.uncertain = true;
            let size = path.size;
            state.uncertain_bytes = state.uncertain_bytes.saturating_add(size);
            GLOBAL_PIN_CACHE_METRICS
                .uncertain_bytes
                .set(metric_bytes(state.uncertain_bytes));
        }
    }

    pub(super) fn reclaim(self: &Arc<Self>, entries: impl IntoIterator<Item = Arc<PinCacheEntry>>) {
        let entries = entries.into_iter().collect::<Vec<_>>();
        if entries.is_empty() {
            return;
        }
        let gc = self.clone();
        tokio::spawn(async move { gc.reclaim_batch(entries).await });
    }

    async fn reclaim_batch(&self, entries: Vec<Arc<PinCacheEntry>>) {
        let _permit = self.concurrency.acquire().await.unwrap();
        let paths = entries
            .iter()
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>();
        match self.store.delete_objects(&paths).await {
            Ok(()) => self.finish_delete(&paths),
            Err(error) => {
                GLOBAL_PIN_CACHE_METRICS.gc_failures.inc();
                tracing::warn!(
                    object_count = paths.len(),
                    error = %error.as_report(),
                    "failed to reclaim pinned SSTs; keeping them accounted until recovery"
                );
            }
        }
    }

    fn finish_delete(&self, paths: &[String]) {
        let mut state = self.state.lock();
        for path in paths {
            if state
                .accounted_paths
                .get(path)
                .is_some_and(|entry| entry.uncertain)
            {
                continue;
            }
            if let Some(entry) = state.accounted_paths.remove(path) {
                state.accounted_bytes = state.accounted_bytes.saturating_sub(entry.size);
            }
        }
        GLOBAL_PIN_CACHE_METRICS
            .accounted_bytes
            .set(metric_bytes(state.accounted_bytes));
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_hummock_sdk::HummockSstableObjectId;

    use super::super::PinCache;
    use super::super::tests::{in_memory_object_store, local_object_store};
    use super::{PinCacheEntry, PinCacheGc};

    #[tokio::test]
    async fn test_failed_deletion_keeps_capacity_reserved() {
        let (dir, local_store) = local_object_store().await;
        let entry = Arc::new(PinCacheEntry {
            path: "1001-42.sst".into(),
            size: 8,
        });
        // A nonempty directory at the exact object path makes FS deletion fail,
        // including when the test runs as root (unlike permission-based failures).
        let path = dir.path().join(&entry.path);
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("child"), b"complete").unwrap();
        let gc = PinCacheGc::new(local_store, 8);
        gc.try_reserve(&entry).unwrap();
        gc.reclaim_batch(vec![entry.clone()]).await;

        assert!(path.join("child").exists());
        {
            let state = gc.state.lock();
            assert_eq!(state.accounted_bytes, 8);
            let path = state.accounted_paths.get(&entry.path).unwrap();
            assert_eq!(path.size, 8);
            assert!(!path.uncertain);
            assert_eq!(state.uncertain_bytes, 0);
        }
        let replacement = PinCacheEntry {
            path: "1002-43.sst".into(),
            size: 8,
        };
        assert_eq!(gc.try_reserve(&replacement), Err(8));

        // A later successful deletion is the only event that may release this reservation.
        std::fs::remove_file(path.join("child")).unwrap();
        std::fs::remove_dir(&path).unwrap();
        gc.reclaim_batch(vec![entry]).await;
        assert_eq!(gc.state.lock().accounted_bytes, 0);
        assert_eq!(gc.state.lock().uncertain_bytes, 0);
        gc.try_reserve(&replacement).unwrap();
    }

    #[tokio::test]
    async fn test_final_path_deletion_keeps_uncertain_capacity_reserved() {
        for final_path_exists in [false, true] {
            let (dir, local_store) = local_object_store().await;
            let entry = Arc::new(PinCacheEntry {
                path: "1001-42.sst".into(),
                size: 8,
            });
            if final_path_exists {
                local_store
                    .upload(&entry.path, Bytes::from_static(b"complete"))
                    .await
                    .unwrap();
            }
            // Model an orphan whose path is not known to the download guard.
            let temporary_path = dir.path().join("orphan.tmp");
            std::fs::write(&temporary_path, b"half").unwrap();
            let gc = PinCacheGc::new(local_store.clone(), 8);
            gc.try_reserve(&entry).unwrap();
            gc.mark_uncertain(&entry);
            gc.mark_uncertain(&entry);

            for _ in 0..2 {
                gc.reclaim_batch(vec![entry.clone()]).await;
                assert!(
                    local_store
                        .metadata(&entry.path)
                        .await
                        .unwrap_err()
                        .is_object_not_found_error()
                );
                assert!(temporary_path.exists());
                let state = gc.state.lock();
                assert_eq!(state.accounted_bytes, 8);
                assert_eq!(state.uncertain_bytes, 8);
                assert!(state.accounted_paths[&entry.path].uncertain);
            }
            let replacement = PinCacheEntry {
                path: "1002-43.sst".into(),
                size: 8,
            };
            assert_eq!(gc.try_reserve(&replacement), Err(8));
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
}
