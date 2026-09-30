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
use tokio::sync::mpsc;

use super::{Ordering, PinCacheFile, metric_bytes};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

// Match the maximum delete batch size used by Hummock GC.
const DELETE_BATCH_SIZE: usize = 1000;

#[derive(Default)]
struct PinCacheGcState {
    // Reservations that can be released by deleting their final paths.
    accounted_paths: HashMap<String, u64>,
    // Total of path reservations and uncertain bytes.
    accounted_bytes: u64,
    // A subset of accounted_bytes, retained until a new cache inventories the actual files.
    uncertain_bytes: u64,
}

/// Owns physical-path accounting and reclamation. Callers must withdraw routes first.
pub(super) struct PinCacheGc {
    capacity: u64,
    // Leaf lock: accounting must not call back into PinCache or the executor, or perform I/O.
    state: Arc<Mutex<PinCacheGcState>>,
    // Pending paths, not tasks. Their reservations remain charged until deletion succeeds.
    reclaim_tx: mpsc::UnboundedSender<String>,
}

impl PinCacheGc {
    pub(super) fn capacity(&self) -> u64 {
        self.capacity
    }

    pub(super) fn new(store: ObjectStoreRef, capacity: u64) -> Self {
        GLOBAL_PIN_CACHE_METRICS
            .capacity_bytes
            .set(metric_bytes(capacity));
        GLOBAL_PIN_CACHE_METRICS.accounted_bytes.set(0);
        GLOBAL_PIN_CACHE_METRICS.uncertain_bytes.set(0);
        let state = Arc::new(Mutex::new(PinCacheGcState::default()));
        let (reclaim_tx, reclaim_rx) = mpsc::unbounded_channel();
        // The worker owns neither PinCacheGc nor a sender. Dropping the cache closes the
        // channel, allowing the worker to drain pending paths and then exit.
        tokio::spawn(Self::run(store, state.clone(), reclaim_rx));
        Self {
            capacity,
            state,
            reclaim_tx,
        }
    }

    pub(super) fn account_existing(&self, entry: &PinCacheFile) {
        let mut state = self.state.lock();
        if let Entry::Vacant(slot) = state.accounted_paths.entry(entry.path.clone()) {
            slot.insert(entry.size);
            state.accounted_bytes = state.accounted_bytes.saturating_add(entry.size);
            GLOBAL_PIN_CACHE_METRICS
                .accounted_bytes
                .set(metric_bytes(state.accounted_bytes));
        }
    }

    pub(super) fn try_reserve(&self, entry: &PinCacheFile) -> Result<(), u64> {
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
        GLOBAL_PIN_CACHE_METRICS
            .accounted_bytes
            .set(metric_bytes(state.accounted_bytes));
        Ok(())
    }

    pub(super) fn mark_uncertain(&self, entry: &PinCacheFile) {
        let mut state = self.state.lock();
        // Detach the reservation from its final path without releasing capacity. Deleting that
        // path cannot reclaim backend-owned temporary files; only startup can inventory them.
        if let Some(size) = state.accounted_paths.remove(&entry.path) {
            state.uncertain_bytes = state.uncertain_bytes.saturating_add(size);
            GLOBAL_PIN_CACHE_METRICS
                .uncertain_bytes
                .set(metric_bytes(state.uncertain_bytes));
        }
    }

    pub(super) fn reclaim(&self, files: impl IntoIterator<Item = Arc<PinCacheFile>>) {
        for file in files {
            // The final file reference submits deletion; outstanding readers keep it on disk.
            file.retired.store(true, Ordering::Relaxed);
        }
    }

    pub(super) fn enqueue(&self, path: String) {
        if self.reclaim_tx.send(path).is_err() {
            GLOBAL_PIN_CACHE_METRICS.gc_failures.inc();
            tracing::warn!("pin cache GC worker stopped; keeping files accounted until recovery");
        }
    }

    async fn run(
        store: ObjectStoreRef,
        state: Arc<Mutex<PinCacheGcState>>,
        mut receiver: mpsc::UnboundedReceiver<String>,
    ) {
        let mut paths = Vec::with_capacity(DELETE_BATCH_SIZE);
        while receiver.recv_many(&mut paths, DELETE_BATCH_SIZE).await != 0 {
            Self::reclaim_batch(&store, &state, &paths).await;
            paths.clear();
        }
    }

    async fn reclaim_batch(
        store: &ObjectStoreRef,
        state: &Mutex<PinCacheGcState>,
        paths: &[String],
    ) {
        match store.delete_objects(paths).await {
            Ok(()) => Self::finish_delete(state, paths),
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

    fn finish_delete(state: &Mutex<PinCacheGcState>, paths: &[String]) {
        for path in paths {
            // Release accounting between paths so capacity reservations can proceed during
            // a large deletion batch. Refills reserve capacity without holding a shard lock.
            let mut state = state.lock();
            if let Some(size) = state.accounted_paths.remove(path) {
                state.accounted_bytes = state.accounted_bytes.saturating_sub(size);
            }
        }
        let state = state.lock();
        GLOBAL_PIN_CACHE_METRICS
            .accounted_bytes
            .set(metric_bytes(state.accounted_bytes));
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use parking_lot::Mutex;
    use tokio::sync::mpsc;

    use super::super::PinCache;
    use super::super::test_utils::{in_memory_object_store, local_object_store};
    use super::{DELETE_BATCH_SIZE, PinCacheFile, PinCacheGc, PinCacheGcState};

    pub(in crate::hummock::pin_cache) fn accounted_bytes(gc: &PinCacheGc) -> u64 {
        gc.state.lock().accounted_bytes
    }

    pub(in crate::hummock::pin_cache) async fn wait_for_reclaim(pin_cache: &PinCache) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while accounted_bytes(&pin_cache.gc) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_failed_deletion_keeps_capacity_reserved() {
        let (dir, local_store) = local_object_store().await;
        let gc = Arc::new(PinCacheGc::new(local_store.clone(), 8));
        let entry = Arc::new(PinCacheFile::new("1001-42.sst".into(), 8, Arc::clone(&gc)));
        // A nonempty directory at the exact object path makes FS deletion fail,
        // including when the test runs as root (unlike permission-based failures).
        let path = dir.path().join(&entry.path);
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("child"), b"complete").unwrap();
        gc.try_reserve(&entry).unwrap();
        PinCacheGc::reclaim_batch(&local_store, &gc.state, std::slice::from_ref(&entry.path)).await;

        assert!(path.join("child").exists());
        {
            let state = gc.state.lock();
            assert_eq!(state.accounted_bytes, 8);
            assert_eq!(state.accounted_paths[&entry.path], 8);
            assert_eq!(state.uncertain_bytes, 0);
        }
        let replacement = PinCacheFile::new("1002-43.sst".into(), 8, Arc::clone(&gc));
        assert_eq!(gc.try_reserve(&replacement), Err(8));

        // A later successful deletion is the only event that may release this reservation.
        std::fs::remove_file(path.join("child")).unwrap();
        std::fs::remove_dir(&path).unwrap();
        PinCacheGc::reclaim_batch(&local_store, &gc.state, std::slice::from_ref(&entry.path)).await;
        assert_eq!(gc.state.lock().accounted_bytes, 0);
        assert_eq!(gc.state.lock().uncertain_bytes, 0);
        gc.try_reserve(&replacement).unwrap();
    }

    #[tokio::test]
    async fn test_final_path_deletion_keeps_uncertain_capacity_reserved() {
        for final_path_exists in [false, true] {
            let (dir, local_store) = local_object_store().await;
            let gc = Arc::new(PinCacheGc::new(local_store.clone(), 16));
            let entry = Arc::new(PinCacheFile::new("1001-42.sst".into(), 8, Arc::clone(&gc)));
            if final_path_exists {
                local_store
                    .upload(&entry.path, Bytes::from_static(b"complete"))
                    .await
                    .unwrap();
            }
            // Model an orphan whose path is not known to the download guard.
            let temporary_path = dir.path().join("orphan.tmp");
            std::fs::write(&temporary_path, b"half").unwrap();
            gc.try_reserve(&entry).unwrap();
            gc.mark_uncertain(&entry);
            gc.mark_uncertain(&entry);

            // Normal reclamation must release only its own reservation, leaving uncertain debt.
            let complete = Arc::new(PinCacheFile::new("1002-43.sst".into(), 8, Arc::clone(&gc)));
            gc.try_reserve(&complete).unwrap();
            local_store
                .upload(&complete.path, Bytes::from_static(b"complete"))
                .await
                .unwrap();
            PinCacheGc::reclaim_batch(
                &local_store,
                &gc.state,
                std::slice::from_ref(&complete.path),
            )
            .await;

            for _ in 0..2 {
                PinCacheGc::reclaim_batch(
                    &local_store,
                    &gc.state,
                    std::slice::from_ref(&entry.path),
                )
                .await;
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
                assert!(state.accounted_paths.is_empty());
            }
            let replacement = PinCacheFile::new("1003-44.sst".into(), 9, Arc::clone(&gc));
            assert_eq!(gc.try_reserve(&replacement), Err(8));
        }
    }

    #[tokio::test]
    async fn test_worker_drains_pending_deletions_on_shutdown() {
        let local_store = in_memory_object_store();
        let state = Arc::new(Mutex::new(PinCacheGcState::default()));
        let (reclaim_tx, reclaim_rx) = mpsc::unbounded_channel();
        let gc = Arc::new(PinCacheGc {
            capacity: (DELETE_BATCH_SIZE + 1) as u64,
            state: state.clone(),
            reclaim_tx,
        });
        // Queue more than one batch before starting the worker. Pending files must still
        // consume capacity, and closing the queue must not discard the final partial batch.
        for id in 0..=DELETE_BATCH_SIZE {
            let entry = Arc::new(PinCacheFile::new(format!("{id}-1.sst"), 1, Arc::clone(&gc)));
            local_store
                .upload(&entry.path, Bytes::from_static(b"x"))
                .await
                .unwrap();
            gc.try_reserve(&entry).unwrap();
            gc.reclaim([entry]);
        }
        let replacement = PinCacheFile::new("replacement".into(), 1, Arc::clone(&gc));
        assert_eq!(gc.try_reserve(&replacement), Err(gc.capacity));
        drop(replacement);
        drop(gc);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            PinCacheGc::run(local_store.clone(), state.clone(), reclaim_rx),
        )
        .await
        .unwrap();
        assert_eq!(state.lock().accounted_bytes, 0);
        assert!(state.lock().accounted_paths.is_empty());
        for id in 0..=DELETE_BATCH_SIZE {
            assert!(
                local_store
                    .metadata(&format!("{id}-1.sst"))
                    .await
                    .unwrap_err()
                    .is_object_not_found_error()
            );
        }
    }
}
