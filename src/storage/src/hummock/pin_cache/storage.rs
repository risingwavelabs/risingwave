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

//! Physical file ownership for the local cache.
//!
//! Files stay owned here until confirmed absent or deleted, even after publication is
//! withdrawn. Publications, readers and GC selections hold external file leases.
//! Quiescent filesystem samples calibrate an atomic data-byte budget; file and upload
//! charges then remain owned until confirmed physical cleanup.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use super::{PinCache, PinCacheFile};

#[derive(Default)]
pub(super) struct PinCacheStorageState {
    pub(super) files: HashMap<String, Arc<PinCacheFile>>,
    // Protect live copies and their backend-generated temporary paths from GC.
    pub(super) uploads: HashMap<String, u64>,
    // Full upload reservations remain here until actual I/O and known failure cleanup finish.
    pub(super) failed_uploads: HashSet<String>,
    // Calibrated only without unsettled uploads, never from free minus full reservations.
    pub(super) capacity_limit: Option<u64>,
    // A subset of files: exact paths explicitly discarded, including reader-held files.
    pub(super) pending_deletes: HashSet<String>,
}

impl PinCacheStorageState {
    /// Forget a confirmed absent file and its deletion intent at most once.
    pub(super) fn remove_file(&mut self, path: &str) {
        // A probe may already have observed an unacknowledged physical deletion.
        // Do not credit those bytes twice within an unsettled upload window. A later
        // quiescent probe restores the budget from current physical free space.
        if let Some(file) = self.files.remove(path)
            && let Some(limit) = &mut self.capacity_limit
        {
            *limit = limit.saturating_sub(file.size);
        }
        self.pending_deletes.remove(path);
    }
}

impl PinCache {
    /// Atomically admit bytes for a new physical target. Probe ordering is protected by
    /// the gate; an active upload completing during the probe never recalibrates the window.
    pub(super) async fn admit_upload(
        &self,
        file: &PinCacheFile,
    ) -> risingwave_object_store::object::ObjectResult<bool> {
        let _gate = self.capacity_gate.lock().await;
        let quiescent = self.storage.lock().uploads.is_empty();
        let available = self.available_space().await?;
        let mut state = self.storage.lock();
        let resident: u64 = state.files.values().map(|file| file.size).sum();
        let reserved: u64 = state.uploads.values().sum();
        if let Some(available) = available {
            if quiescent || state.capacity_limit.is_none() {
                state.capacity_limit = Some(available.saturating_add(resident));
            }
            let used = resident.saturating_add(reserved);
            if available < file.size
                || used.saturating_add(file.size) > state.capacity_limit.unwrap()
            {
                return Ok(false);
            }
        }
        assert!(!state.files.contains_key(&file.path));
        assert!(state.uploads.insert(file.path.clone(), file.size).is_none());
        Ok(true)
    }

    /// Release a preparation that has not started physical I/O.
    pub(super) fn finish_upload(&self, path: &str) {
        self.storage.lock().uploads.remove(path);
    }

    /// Track one physical path, retaining its file until readers and GC release it.
    pub(super) fn track_file(&self, path: String, size: u64) -> Arc<PinCacheFile> {
        let mut state = self.storage.lock();
        assert!(
            !state.files.contains_key(&path),
            "each path is registered once"
        );
        let file = Arc::new(PinCacheFile {
            path: path.clone(),
            size,
        });
        state.uploads.remove(&path);
        state.files.insert(path, file.clone());
        file
    }

    /// Record deletion intent after releasing its shard lock; no GC pass starts here.
    pub(super) fn enqueue_delete(&self, file: Arc<PinCacheFile>) {
        let mut state = self.storage.lock();
        debug_assert!(
            state
                .files
                .get(&file.path)
                .is_some_and(|entry| Arc::ptr_eq(entry, &file))
        );
        state.pending_deletes.insert(file.path.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use crate::hummock::pin_cache::test_utils::in_memory_object_store;
    use crate::hummock::pin_cache::{PinCache, PinCacheDownloadError};

    #[tokio::test]
    async fn test_concurrent_byte_admission_and_partial_allocation_do_not_oversell_or_double_count()
    {
        let cache = PinCache::new(in_memory_object_store(), 1, [])
            .await
            .unwrap();
        let free = Arc::new(AtomicU64::new(100));
        cache.set_available_space_for_test({
            let free = free.clone();
            move || Ok(Some(free.load(Ordering::SeqCst)))
        });
        let mut probes = cache.watch_space_probes_for_test();
        let (polled, mut initial_polls) = tokio::sync::mpsc::unbounded_channel();
        let (finished, mut completions) = tokio::sync::mpsc::unbounded_channel();
        let tasks: Vec<_> = (1..=20)
            .map(|id| {
                let cache = cache.clone();
                let free = free.clone();
                let polled = polled.clone();
                let finished = finished.clone();
                tokio::spawn(async move {
                    let preparation = cache.prepare_download(id.into(), 25);
                    tokio::pin!(preparation);
                    assert!(futures::poll!(preparation.as_mut()).is_pending());
                    polled.send(()).unwrap();
                    let result = preparation.await.map(|preparation| {
                        // Model completed physical writes between probes. Without serialization,
                        // already captured quiescent samples would create extra credit.
                        let path = cache.storage.lock().uploads.keys().next().unwrap().clone();
                        let file = cache.track_file(path, 25);
                        let available = free.fetch_sub(25, Ordering::SeqCst);
                        assert!(available >= 25, "admission oversold physical bytes");
                        drop(preparation);
                        file
                    });
                    finished.send(()).unwrap();
                    result
                })
            })
            .collect();
        drop(polled);
        drop(finished);
        tokio::time::timeout(Duration::from_secs(1), async {
            for _ in 0..20 {
                initial_polls.recv().await.unwrap();
            }
        })
        .await
        .unwrap();
        for _ in 0..20 {
            let resume = tokio::time::timeout(Duration::from_secs(1), probes.recv())
                .await
                .unwrap()
                .unwrap();
            resume.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(1), completions.recv())
                .await
                .unwrap()
                .unwrap();
        }
        let bids = futures::future::join_all(tasks).await;
        let admitted: Vec<_> = bids
            .into_iter()
            .filter_map(|task| task.unwrap().ok())
            .collect();
        assert_eq!(admitted.len(), 4);
        assert_eq!(admitted.iter().map(|file| file.size).sum::<u64>(), 100);
        assert_eq!(free.load(Ordering::SeqCst), 0);
        assert!(cache.storage.lock().uploads.is_empty());
        for file in admitted {
            cache.enqueue_delete(file);
        }
        cache.select_minor().delete().await.unwrap();
        drop(probes);

        let free = Arc::new(AtomicU64::new(200));
        cache.set_available_space_for_test({
            let free = free.clone();
            move || Ok(Some(free.load(Ordering::SeqCst)))
        });
        let first = cache.prepare_download(100.into(), 100).await.unwrap();
        // Model 50 bytes of the first reservation already allocated on disk. Subtracting
        // its full100 from fresh150 would incorrectly reject this second100-byte copy.
        free.store(150, Ordering::SeqCst);
        let second = cache.prepare_download(101.into(), 100).await.unwrap();
        assert_eq!(cache.storage.lock().uploads.values().sum::<u64>(), 200);
        assert!(matches!(
            cache.prepare_download(102.into(), 1).await,
            Err(PinCacheDownloadError::CapacityRejected)
        ));
        drop((first, second));
        assert!(cache.storage.lock().uploads.is_empty());
        // An external writer can reduce current free space even when our window has room.
        free.store(200, Ordering::SeqCst);
        let first = cache.prepare_download(103.into(), 50).await.unwrap();
        free.store(10, Ordering::SeqCst);
        assert!(matches!(
            cache.prepare_download(104.into(), 20).await,
            Err(PinCacheDownloadError::CapacityRejected)
        ));
        drop(first);
    }

    #[tokio::test]
    async fn test_completion_during_a_stale_space_probe_does_not_create_extra_budget() {
        let cache = PinCache::new(in_memory_object_store(), 1, [])
            .await
            .unwrap();
        let free = Arc::new(AtomicU64::new(200));
        cache.set_available_space_for_test({
            let free = free.clone();
            move || Ok(Some(free.load(Ordering::SeqCst)))
        });
        let first = cache.prepare_download(100.into(), 100).await.unwrap();
        let mut probes = cache.watch_space_probes_for_test();
        let pending = tokio::spawn({
            let cache = cache.clone();
            async move { cache.prepare_download(101.into(), 150).await }
        });
        let resume = tokio::time::timeout(Duration::from_secs(1), probes.recv())
            .await
            .unwrap()
            .unwrap();
        // The sampled200 is now stale: first finishes100 while B's probe is suspended.
        // Moving reservation -> resident must not be mistaken for an empty initial window.
        let path = cache.storage.lock().uploads.keys().next().unwrap().clone();
        let file = cache.track_file(path, 100);
        free.store(100, Ordering::SeqCst);
        drop(first);
        resume.send(()).unwrap();
        assert!(matches!(
            pending.await.unwrap(),
            Err(PinCacheDownloadError::CapacityRejected)
        ));
        *cache.test_hooks.space_pause.lock() = None;
        let last = cache.prepare_download(102.into(), 100).await.unwrap();
        {
            let state = cache.storage.lock();
            assert_eq!(
                state.files.values().map(|f| f.size).sum::<u64>()
                    + state.uploads.values().sum::<u64>(),
                200
            );
        }
        drop((last, file));
    }
    #[tokio::test]
    async fn test_missing_file_ack_after_space_probe_does_not_credit_bytes_twice() {
        use std::time::SystemTime;

        use bytes::Bytes;

        let store = in_memory_object_store();
        let cache = PinCache::new(store.clone(), 1, []).await.unwrap();
        let old = cache.track_file("1001-1.sst".into(), 100);
        store
            .upload(&old.path, Bytes::from(vec![1; 100]))
            .await
            .unwrap();
        // A delete may have physically completed after its waiter timed out, while its
        // keeper charge remains unacknowledged. The next probe already sees those bytes.
        store.delete(&old.path).await.unwrap();
        drop(old);
        cache.set_available_space_for_test(|| Ok(Some(200)));
        let first = cache.prepare_download(1002.into(), 100).await.unwrap();
        cache
            .select_full(SystemTime::now())
            .await
            .unwrap()
            .delete()
            .await
            .unwrap();
        let second = cache.prepare_download(1003.into(), 100).await.unwrap();
        assert!(
            matches!(
                cache.prepare_download(1004.into(), 100).await,
                Err(PinCacheDownloadError::CapacityRejected)
            ),
            "acknowledging already-observed free space must not admit 300 bytes into 200"
        );
        drop((first, second));
    }
}
