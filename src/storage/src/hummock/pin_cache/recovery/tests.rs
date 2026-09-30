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

use futures::stream;

use super::*;
use crate::hummock::pin_cache::test_utils::in_memory_object_store;

fn metadata(id: HummockSstableObjectId, path_id: usize, size: usize) -> ObjectMetadata {
    ObjectMetadata {
        key: format!("{}-{path_id}.sst", id.as_raw_id()),
        last_modified: 0.0,
        total_size: size,
    }
}

fn object_in_shard(shard: usize, shard_num: usize) -> HummockSstableObjectId {
    (1..)
        .map(HummockSstableObjectId::from)
        .find(|&id| PinCache::shard_index(id, shard_num) == shard)
        .unwrap()
}

async fn cache(shard_num: usize, ids: &[HummockSstableObjectId]) -> PinCache {
    Arc::try_unwrap(
        PinCache::new(
            in_memory_object_store(),
            shard_num,
            2,
            ids.iter().map(|&id| (id, 8)),
        )
        .await
        .unwrap(),
    )
    .ok()
    .unwrap()
}

#[tokio::test]
async fn test_recovery_keeps_first_valid_file_across_batches() {
    let id = HummockSstableObjectId::from(1001);
    let mut cache = cache(1, &[id]).await;
    let files = stream::iter((0..RECOVERY_BATCH_SIZE + 1).map(move |path_id| {
        // The first candidate is invalid; the next must win both within and across batches.
        Ok(metadata(id, path_id, if path_id == 0 { 4 } else { 8 }))
    }));
    let stats = cache
        .recover_local_files(Ok(files.boxed()), 2)
        .await
        .unwrap();
    assert_eq!(stats.objects, 1);
    assert_eq!(stats.bytes, 8);
    let object = &cache.shards[0].get_mut().objects[&id];
    assert_eq!(object.published().unwrap().path, "1001-1.sst");
}

#[tokio::test]
async fn test_recovery_scan_failure_after_completed_batch() {
    let ids = [object_in_shard(0, 3), object_in_shard(2, 3)];
    let mut cache = cache(3, &ids).await;
    let files = stream::iter(
        (0..RECOVERY_BATCH_SIZE)
            .map(move |i| Ok(metadata(ids[i % ids.len()], i, 8)))
            .chain([Err(ObjectError::internal("injected inventory failure"))]),
    );
    let error = cache
        .recover_local_files(Ok(files.boxed()), 2)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected inventory failure"));
    // The failure occurred after real index work, not merely after skipped non-members.
    // Construction receives Err instead of recovery stats, so it cannot publish this cache.
    for id in ids {
        assert!(cache.shard(id).read().objects[&id].published().is_some());
    }
}

// Blocking tasks really run in parallel only on native Tokio, not the simulator.
#[cfg(not(madsim))]
mod parallel {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::time::Duration;

    use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

    use super::*;

    #[derive(Default)]
    struct Gate {
        open: Mutex<bool>,
        changed: Condvar,
    }

    impl Gate {
        fn wait(&self) {
            let (open, _) = self
                .changed
                .wait_timeout_while(self.open.lock().unwrap(), Duration::from_secs(10), |open| {
                    !*open
                })
                .unwrap();
            let released = *open;
            drop(open);
            assert!(released, "recovery test worker was not released");
        }

        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.changed.notify_all();
        }
    }

    struct ReleaseOnDrop(Arc<Gate>);

    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    #[derive(Clone)]
    struct Probe {
        gate: Arc<Gate>,
        active: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        started: UnboundedSender<()>,
        completed: UnboundedSender<()>,
    }

    impl Probe {
        fn new() -> (
            Self,
            ReleaseOnDrop,
            UnboundedReceiver<()>,
            UnboundedReceiver<()>,
        ) {
            let (started, starts) = unbounded_channel();
            let (completed, completions) = unbounded_channel();
            let gate = Arc::new(Gate::default());
            (
                Self {
                    gate: gate.clone(),
                    active: Arc::default(),
                    peak: Arc::default(),
                    started,
                    completed,
                },
                ReleaseOnDrop(gate),
                starts,
                completions,
            )
        }

        fn recover(&self, shards: Vec<ShardRecovery>) -> (Vec<ShardRecovery>, RecoveryStats) {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.started.send(()).unwrap();
            self.gate.wait();
            let result = recover_group(shards);
            self.active.fetch_sub(1, Ordering::SeqCst);
            let _ = self.completed.send(());
            result
        }
    }

    async fn receive(receiver: &mut UnboundedReceiver<()>) {
        tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .expect("recovery worker timed out")
            .expect("recovery worker exited early");
    }

    #[tokio::test]
    async fn test_recovery_parallelism_is_bounded_and_handles_sparse_shards() {
        // These shard indices would all collide if workers were chosen by shard_index % K.
        for concurrency in [1_usize, 2, 8, 32] {
            for occupied in [vec![0, 8, 16], (0..17).collect()] {
                let ids: Vec<_> = occupied.iter().map(|&i| object_in_shard(i, 17)).collect();
                let mut cache = cache(17, &ids).await;
                for round in 0..2 {
                    let (probe, release, mut starts, _) = Probe::new();
                    let observer = probe.clone();
                    let batch = ids.iter().map(|&id| metadata(id, round, 8)).collect();
                    let expected = concurrency.min(ids.len());
                    let mut task = tokio::spawn(async move {
                        let stats = cache
                            .recover_batch(batch, concurrency, move |group| probe.recover(group))
                            .await;
                        (cache, stats)
                    });
                    for _ in 0..expected {
                        receive(&mut starts).await;
                    }
                    assert_eq!(observer.peak.load(Ordering::SeqCst), expected);
                    assert!(futures::poll!(&mut task).is_pending());
                    drop(release);
                    let (restored, stats) = task.await.unwrap();
                    assert_eq!(
                        stats.unwrap().objects,
                        if round == 0 { ids.len() as u64 } else { 0 }
                    );
                    assert_eq!(observer.active.load(Ordering::SeqCst), 0);
                    cache = restored;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_recovery_joins_other_workers_after_panic() {
        let ids = [object_in_shard(0, 2), object_in_shard(1, 2)];
        let mut cache = cache(2, &ids).await;
        let (probe, release, mut starts, _) = Probe::new();
        let observer = probe.clone();
        let batch = ids.iter().map(|&id| metadata(id, 0, 8)).collect();
        let mut task = tokio::spawn(async move {
            cache
                .recover_batch(batch, 2, move |group| {
                    assert_ne!(group[0].index, 0, "injected recovery worker panic");
                    probe.recover(group)
                })
                .await
        });
        receive(&mut starts).await;
        assert!(futures::poll!(&mut task).is_pending());
        drop(release);
        let error = task.await.unwrap().unwrap_err();
        assert!(error.to_string().contains("pin cache recovery task failed"));
        assert_eq!(observer.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_recovery_cancellation_leaves_only_private_worker_state() {
        let ids = [object_in_shard(0, 2), object_in_shard(1, 2)];
        let mut cache = cache(2, &ids).await;
        let (probe, release, mut starts, mut completions) = Probe::new();
        let observer = probe.clone();
        let batch = ids.iter().map(|&id| metadata(id, 0, 8)).collect();
        let task = tokio::spawn(async move {
            cache
                .recover_batch(batch, 2, move |group| probe.recover(group))
                .await
        });
        for _ in ids {
            receive(&mut starts).await;
        }
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(release);
        for _ in ids {
            receive(&mut completions).await;
        }
        assert_eq!(observer.active.load(Ordering::SeqCst), 0);
        // Workers return only private shards/stats; the aborted caller cannot install them
        // or commit publication metrics. Avoid assertions on process-global test metrics.
    }
}
