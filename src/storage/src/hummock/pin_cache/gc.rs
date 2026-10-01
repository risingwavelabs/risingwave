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

use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_object_store::object::ObjectStoreRef;
use thiserror_ext::AsReport;
use tokio::sync::mpsc;

use super::metric_bytes;
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

// Match the maximum delete batch size used by Hummock GC.
const DELETE_BATCH_SIZE: usize = 1000;

#[derive(Default)]
struct PinCacheGcState {
    // Total of path reservations and uncertain bytes.
    accounted_bytes: u64,
    // A subset of accounted_bytes, retained until a new cache inventories the actual files.
    uncertain_bytes: u64,
}

impl PinCacheGcState {
    fn release(&mut self, bytes: u64) {
        self.accounted_bytes = self.accounted_bytes.saturating_sub(bytes);
        GLOBAL_PIN_CACHE_METRICS
            .accounted_bytes
            .set(metric_bytes(self.accounted_bytes));
    }
}

/// Owns physical-path accounting and reclamation. Callers must withdraw routes first.
pub(super) struct PinCacheGc {
    capacity: u64,
    // Leaf lock: accounting must not call back into PinCache or the executor, or perform I/O.
    state: Arc<Mutex<PinCacheGcState>>,
    // Each retired file's final owner submits its path and reservation exactly once.
    // Pending reservations remain charged until deletion succeeds.
    reclaim_tx: mpsc::UnboundedSender<(String, u64)>,
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

    /// Accounts one file from the startup inventory, which lists each physical path once.
    pub(super) fn account_existing(&self, size: u64) {
        let mut state = self.state.lock();
        state.accounted_bytes = state.accounted_bytes.saturating_add(size);
        GLOBAL_PIN_CACHE_METRICS
            .accounted_bytes
            .set(metric_bytes(state.accounted_bytes));
    }

    pub(super) fn try_reserve(&self, size: u64) -> Result<(), u64> {
        let mut state = self.state.lock();
        let Some(accounted_bytes) = state.accounted_bytes.checked_add(size) else {
            return Err(state.accounted_bytes);
        };
        if accounted_bytes > self.capacity {
            return Err(state.accounted_bytes);
        }
        state.accounted_bytes = accounted_bytes;
        GLOBAL_PIN_CACHE_METRICS
            .accounted_bytes
            .set(metric_bytes(state.accounted_bytes));
        Ok(())
    }

    /// Returns a reservation only when no local upload has started.
    pub(super) fn release_unused(&self, size: u64) {
        self.state.lock().release(size);
    }

    /// Called once by an unfinished download; its reservation must not also be enqueued.
    pub(super) fn mark_uncertain(&self, size: u64) {
        let mut state = self.state.lock();
        // The unfinished download relinquishes its reservation without releasing capacity.
        // Its final path cannot reclaim backend-owned temporary files; startup inventories them.
        state.uncertain_bytes = state.uncertain_bytes.saturating_add(size);
        GLOBAL_PIN_CACHE_METRICS
            .uncertain_bytes
            .set(metric_bytes(state.uncertain_bytes));
    }

    pub(super) fn enqueue(&self, path: String, size: u64) {
        if self.reclaim_tx.send((path, size)).is_err() {
            GLOBAL_PIN_CACHE_METRICS.gc_failures.inc();
            tracing::warn!("pin cache GC worker stopped; keeping files accounted until recovery");
        }
    }

    async fn run(
        store: ObjectStoreRef,
        state: Arc<Mutex<PinCacheGcState>>,
        mut receiver: mpsc::UnboundedReceiver<(String, u64)>,
    ) {
        let mut files = Vec::with_capacity(DELETE_BATCH_SIZE);
        let mut paths = Vec::with_capacity(DELETE_BATCH_SIZE);
        while receiver.recv_many(&mut files, DELETE_BATCH_SIZE).await != 0 {
            let mut bytes = 0_u64;
            for (path, size) in files.drain(..) {
                paths.push(path);
                bytes = bytes.saturating_add(size);
            }
            Self::reclaim_batch(&store, &state, &paths, bytes).await;
            paths.clear();
        }
    }

    async fn reclaim_batch(
        store: &ObjectStoreRef,
        state: &Mutex<PinCacheGcState>,
        paths: &[String],
        bytes: u64,
    ) {
        match store.delete_objects(paths).await {
            Ok(()) => {
                state.lock().release(bytes);
            }
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
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use parking_lot::Mutex;
    use tokio::sync::mpsc;

    use super::super::test_utils::{in_memory_object_store, local_object_store};
    use super::super::{PinCache, PinCacheFile};
    use super::{DELETE_BATCH_SIZE, PinCacheGc, PinCacheGcState};

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
        let gc = PinCacheGc::new(local_store.clone(), 16);
        let failed_path = "1001-42.sst".to_owned();
        // A nonempty directory at the exact object path makes FS deletion fail,
        // including when the test runs as root (unlike permission-based failures).
        let path = dir.path().join(&failed_path);
        std::fs::create_dir(&path).unwrap();
        std::fs::write(path.join("child"), b"complete").unwrap();
        gc.try_reserve(8).unwrap();
        PinCacheGc::reclaim_batch(&local_store, &gc.state, &[failed_path], 8).await;

        assert!(path.join("child").exists());
        {
            let state = gc.state.lock();
            assert_eq!(state.accounted_bytes, 8);
            assert_eq!(state.uncertain_bytes, 0);
        }
        // A successful later batch releases only its own reservation, not the failed batch's.
        let complete_path = "1002-43.sst".to_owned();
        gc.try_reserve(8).unwrap();
        local_store
            .upload(&complete_path, Bytes::from_static(b"complete"))
            .await
            .unwrap();
        PinCacheGc::reclaim_batch(&local_store, &gc.state, &[complete_path], 8).await;
        assert!(path.join("child").exists());
        assert_eq!(gc.state.lock().accounted_bytes, 8);
        assert_eq!(gc.state.lock().uncertain_bytes, 0);
        assert_eq!(gc.try_reserve(9), Err(8));
    }

    #[tokio::test]
    async fn test_reclamation_keeps_uncertain_capacity_reserved() {
        let local_store = in_memory_object_store();
        let gc = PinCacheGc::new(local_store.clone(), 16);
        gc.try_reserve(8).unwrap();
        gc.mark_uncertain(8);

        // Normal reclamation releases only its own reservation, leaving uncertain debt.
        gc.try_reserve(8).unwrap();
        let path = "1002-43.sst".to_owned();
        local_store
            .upload(&path, Bytes::from_static(b"complete"))
            .await
            .unwrap();
        PinCacheGc::reclaim_batch(&local_store, &gc.state, &[path], 8).await;

        {
            let state = gc.state.lock();
            assert_eq!(state.accounted_bytes, 8);
            assert_eq!(state.uncertain_bytes, 8);
        }
        assert_eq!(gc.try_reserve(9), Err(8));
    }

    #[tokio::test]
    async fn test_worker_drains_pending_deletions_on_shutdown() {
        let local_store = in_memory_object_store();
        let state = Arc::new(Mutex::new(PinCacheGcState::default()));
        let (reclaim_tx, reclaim_rx) = mpsc::unbounded_channel();
        let gc = Arc::new(PinCacheGc {
            capacity: (0..=DELETE_BATCH_SIZE)
                .map(|id| (id % 7 + 1) as u64)
                .sum::<u64>()
                + 7,
            state: state.clone(),
            reclaim_tx,
        });
        // Keep a reservation outside the queue to detect an over-release across batches.
        gc.try_reserve(7).unwrap();
        // Queue more than one batch before starting the worker. Pending files must still
        // consume capacity, and closing the queue must not discard the final partial batch.
        for id in 0..=DELETE_BATCH_SIZE {
            let size = id % 7 + 1;
            let entry = PinCacheFile::new(format!("{id}-1.sst"), size as u64, Arc::clone(&gc));
            local_store
                .upload(&entry.path, Bytes::from(vec![b'x'; size]))
                .await
                .unwrap();
            gc.try_reserve(entry.size).unwrap();
            entry.retire();
            // Repeated retirement still submits exactly one deletion when the owner drops.
            entry.retire();
        }
        assert_eq!(gc.try_reserve(1), Err(gc.capacity));
        drop(gc);
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            PinCacheGc::run(local_store.clone(), state.clone(), reclaim_rx),
        )
        .await
        .unwrap();
        assert_eq!(state.lock().accounted_bytes, 7);
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
