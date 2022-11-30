// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use risingwave_common::util::epoch::INVALID_EPOCH;
use risingwave_pb::hummock::HummockSnapshot;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::{channel as once_channel, Sender as Callback};
use tracing::error;

use crate::meta_client::FrontendMetaClient;
use crate::scheduler::plan_fragmenter::QueryId;
use crate::scheduler::{SchedulerError, SchedulerResult};

const UNPIN_INTERVAL_SECS: u64 = 10;

pub type HummockSnapshotManagerRef = Arc<HummockSnapshotManager>;
pub type PinnedHummockSnapshot = HummockSnapshotGuard;

type SnapshotRef = Arc<ArcSwap<HummockSnapshot>>;

/// Cache of hummock snapshot in meta.
pub struct HummockSnapshotManager {
    /// Send epoch-related operations to `HummockSnapshotManagerCore` for async batch handling.
    sender: Option<UnboundedSender<EpochOperation>>,

    /// The latest snapshot synced from the meta service.
    ///
    /// The `max_committed_epoch` and `max_current_epoch` are pushed from meta node to reduce rpc
    /// number.
    ///
    /// We have two epoch(committed and current), We only use `committed_epoch` to pin or unpin,
    /// because `committed_epoch` always less or equal `current_epoch`, and the data with
    /// `current_epoch` is always in the shared buffer, so it will never be gc before the data
    /// of `committed_epoch`.
    latest_snapshot: SnapshotRef,

    /// Epoch for fragment mapping updates
    align_epoch: AtomicU64,

    /// Save max snapshots that are not updated (align_epoch -> max_snapshot).
    /// After align_epoch synchronization, the snapshot will be updated using max_snapshot.
    align_epoch_snapshot_map: Mutex<BTreeMap<u64, HummockSnapshot>>,

    notify_sender: tokio::sync::watch::Sender<()>,
}
#[derive(Debug)]
enum EpochOperation {
    RequestEpoch {
        query_id: QueryId,
        sender: Callback<SchedulerResult<HummockSnapshot>>,
    },
    ReleaseEpoch {
        query_id: QueryId,
        epoch: u64,
    },
    Tick,
}

pub struct HummockSnapshotGuard {
    snapshot: HummockSnapshot,
    query_id: QueryId,
    unpin_snapshot_sender: UnboundedSender<EpochOperation>,
}

impl HummockSnapshotGuard {
    pub fn get_committed_epoch(&self) -> u64 {
        self.snapshot.committed_epoch
    }

    pub fn get_current_epoch(&self) -> u64 {
        self.snapshot.current_epoch
    }
}

impl Drop for HummockSnapshotGuard {
    fn drop(&mut self) {
        let _ = self
            .unpin_snapshot_sender
            .send(EpochOperation::ReleaseEpoch {
                query_id: self.query_id.clone(),
                epoch: self.snapshot.committed_epoch,
            })
            .inspect_err(|err| {
                error!("failed to send release epoch: {}", err);
            });
    }
}

impl HummockSnapshotManager {
    pub fn mock() -> Self {
        let (notify_sender, _) = tokio::sync::watch::channel(());
        Self {
            sender: None,
            latest_snapshot: Arc::new(ArcSwap::from_pointee(HummockSnapshot {
                committed_epoch: INVALID_EPOCH,
                current_epoch: INVALID_EPOCH,
                need_align: false,
            })),
            align_epoch: AtomicU64::new(INVALID_EPOCH),
            align_epoch_snapshot_map: Mutex::new(BTreeMap::default()),
            notify_sender,
        }
    }

    pub fn new(meta_client: Arc<dyn FrontendMetaClient>) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let (notify_sender, _) = tokio::sync::watch::channel(());

        let latest_snapshot = Arc::new(ArcSwap::from_pointee(HummockSnapshot {
            committed_epoch: INVALID_EPOCH,
            current_epoch: INVALID_EPOCH,
            need_align: false,
        }));
        let latest_snapshot_cloned = latest_snapshot.clone();

        tokio::spawn(async move {
            let mut manager = HummockSnapshotManagerCore::new(meta_client, latest_snapshot_cloned);
            let mut unpin_batches = vec![];
            let mut pin_batches = vec![];
            let mut unpin_interval =
                tokio::time::interval(Duration::from_secs(UNPIN_INTERVAL_SECS));
            let mut last_unpin_time = Instant::now();
            loop {
                let msg = tokio::select! {
                    _ = unpin_interval.tick() => {
                        Some(EpochOperation::Tick)
                    }
                    m = receiver.recv() => m
                };
                match msg {
                    Some(EpochOperation::RequestEpoch { query_id, sender }) => {
                        pin_batches.push((query_id, sender));
                    }
                    Some(EpochOperation::ReleaseEpoch { query_id, epoch }) => {
                        unpin_batches.push((query_id, epoch));
                    }
                    Some(EpochOperation::Tick) => {}
                    None => return,
                }
                while let Ok(msg) = receiver.try_recv() {
                    match msg {
                        EpochOperation::RequestEpoch { query_id, sender } => {
                            pin_batches.push((query_id, sender));
                        }
                        EpochOperation::ReleaseEpoch { query_id, epoch } => {
                            unpin_batches.push((query_id, epoch));
                        }
                        EpochOperation::Tick => unreachable!(),
                    }
                }
                if !unpin_batches.is_empty() {
                    manager.release_epoch(&mut unpin_batches);
                }

                let need_unpin = last_unpin_time.elapsed().as_secs() >= UNPIN_INTERVAL_SECS;
                if !pin_batches.is_empty() || need_unpin {
                    // Note: If we want stronger consistency, we should use
                    // `get_epoch_for_query_from_rpc`.
                    let epoch = manager
                        .get_epoch_for_query_from_push(&mut pin_batches)
                        .committed_epoch;
                    if need_unpin && epoch > 0 {
                        manager.unpin_snapshot_before(epoch);
                        last_unpin_time = Instant::now();
                    }
                }
            }
        });
        Self {
            sender: Some(sender),
            latest_snapshot,
            align_epoch: AtomicU64::new(INVALID_EPOCH),
            align_epoch_snapshot_map: Mutex::new(BTreeMap::default()),
            notify_sender,
        }
    }

    pub async fn acquire(&self, query_id: &QueryId) -> SchedulerResult<PinnedHummockSnapshot> {
        let (sender, rc) = once_channel();
        let msg = EpochOperation::RequestEpoch {
            query_id: query_id.clone(),
            sender,
        };
        self.sender.as_ref().unwrap().send(msg).map_err(|_| {
            SchedulerError::Internal(anyhow!("Failed to get epoch for query: {:?}", query_id,))
        })?;
        let snapshot = rc.await.unwrap_or_else(|e| {
            Err(SchedulerError::Internal(anyhow!(
                "Failed to get epoch for query: {:?}, the rpc thread may panic: {:?}",
                query_id,
                e
            )))
        })?;
        Ok(HummockSnapshotGuard {
            snapshot,
            query_id: query_id.clone(),
            unpin_snapshot_sender: self.sender.as_ref().unwrap().clone(),
        })
    }

    /// The `snapshot` update schedule must be later than the `fragment_mapping` update schedule.
    /// So we will update the snapshot directly only in the following cases.
    /// 1. Snapshot don't need align, and there are no other snapshots in the map that need to be
    /// updated. 2. Snapshot need align, but `align_epoch` has been updated.
    pub fn update_snapshot(&self, snapshot: HummockSnapshot) {
        if !snapshot.need_align {
            match self.align_epoch_snapshot_map.lock().last_entry() {
                Some(mut entry) => *entry.get_mut() = snapshot,
                None => self.update_snapshot_epoch(snapshot),
            }
        } else if self.align_epoch.load(Relaxed).ge(&snapshot.committed_epoch) {
            self.update_snapshot_epoch(snapshot);
        } else {
            *self
                .align_epoch_snapshot_map
                .lock()
                .entry(snapshot.committed_epoch)
                .or_default() = snapshot.clone();
        }
    }

    /// If the snapshot is larger than the `latest_snapshot`, it will wait for the `latest_snapshot`
    /// to be updated
    pub async fn wait_align_epoch(&self, snapshot: &HummockSnapshot) {
        let mut rx = self.notify_sender.subscribe();
        loop {
            if snapshot.current_epoch <= self.latest_snapshot.load().current_epoch {
                break;
            }
            rx.changed().await.unwrap();
        }
    }

    pub fn update_snapshot_epoch(&self, snapshot: HummockSnapshot) {
        // Note: currently the snapshot is not only updated from the observer, so we need to take
        // the `max` here instead of directly replace the snapshot.
        self.latest_snapshot.rcu(|prev| HummockSnapshot {
            committed_epoch: std::cmp::max(prev.committed_epoch, snapshot.committed_epoch),
            current_epoch: std::cmp::max(prev.current_epoch, snapshot.current_epoch),
            // Not used here
            need_align: false,
        });
        if self.notify_sender.receiver_count() != 0 {
            self.notify_sender.send(()).unwrap();
        }
    }

    /// Modify `align_epoch`, Only `worker_node_manager` can change the `align_epoch`.
    /// After synchronization, we will update snapshot.
    pub fn update_align_epoch(&self, epoch: u64) {
        self.align_epoch.store(epoch, Relaxed);
        let mut evens: BTreeMap<_, _> = self
            .align_epoch_snapshot_map
            .lock()
            .drain_filter(|key, _| key <= &self.align_epoch.load(Relaxed))
            .collect();

        if let Some((_, value)) = evens.pop_last() {
            self.update_snapshot_epoch(value);
        }
    }
}

struct HummockSnapshotManagerCore {
    /// Record the query ids that pin each snapshot.
    /// Send an `unpin_snapshot` RPC when a snapshot is not pinned any more.
    epoch_to_query_ids: BTreeMap<u64, HashSet<QueryId>>,
    meta_client: Arc<dyn FrontendMetaClient>,
    last_unpin_snapshot: Arc<AtomicU64>,
    latest_snapshot: SnapshotRef,
}

impl HummockSnapshotManagerCore {
    fn new(meta_client: Arc<dyn FrontendMetaClient>, latest_snapshot: SnapshotRef) -> Self {
        Self {
            // Initialize by setting `is_outdated` to `true`.
            meta_client,
            epoch_to_query_ids: BTreeMap::default(),
            last_unpin_snapshot: Arc::new(AtomicU64::new(INVALID_EPOCH)),
            latest_snapshot,
        }
    }

    /// Retrieve max committed epoch from meta with an rpc. This method provides
    /// better epoch freshness.
    async fn get_epoch_for_query_from_rpc(
        &mut self,
        batches: &mut Vec<(QueryId, Callback<SchedulerResult<HummockSnapshot>>)>,
    ) -> HummockSnapshot {
        let ret = self.meta_client.get_epoch().await;
        match ret {
            Ok(snapshot) => {
                self.notify_epoch_assigned_for_queries(&snapshot, batches);
                snapshot
            }
            Err(e) => {
                for (id, cb) in batches.drain(..) {
                    let _ = cb.send(Err(SchedulerError::Internal(anyhow!(
                        "Failed to get epoch for query: {:?} because of RPC Error: {:?}",
                        id,
                        e
                    ))));
                }
                HummockSnapshot {
                    committed_epoch: INVALID_EPOCH,
                    current_epoch: INVALID_EPOCH,
                    need_align: false,
                }
            }
        }
    }

    /// Retrieve max committed epoch and max current epoch from locally cached value, which is
    /// maintained by meta's notification service.
    fn get_epoch_for_query_from_push(
        &mut self,
        batches: &mut Vec<(QueryId, Callback<SchedulerResult<HummockSnapshot>>)>,
    ) -> HummockSnapshot {
        let snapshot = HummockSnapshot::clone(&self.latest_snapshot.load());
        self.notify_epoch_assigned_for_queries(&snapshot, batches);
        snapshot
    }

    /// Add committed epoch in `epoch_to_query_ids`, notify queries with committed epoch and current
    /// epoch
    fn notify_epoch_assigned_for_queries(
        &mut self,
        snapshot: &HummockSnapshot,
        batches: &mut Vec<(QueryId, Callback<SchedulerResult<HummockSnapshot>>)>,
    ) {
        if batches.is_empty() {
            return;
        }
        let committed_epoch = snapshot.committed_epoch;
        let queries = match self.epoch_to_query_ids.get_mut(&committed_epoch) {
            None => {
                self.epoch_to_query_ids
                    .insert(committed_epoch, HashSet::default());
                self.epoch_to_query_ids.get_mut(&committed_epoch).unwrap()
            }
            Some(queries) => queries,
        };
        for (id, cb) in batches.drain(..) {
            queries.insert(id);
            let _ = cb.send(Ok(snapshot.clone()));
        }
    }

    pub fn release_epoch(&mut self, queries: &mut Vec<(QueryId, u64)>) {
        for (query_id, epoch) in queries.drain(..) {
            let query_ids = self.epoch_to_query_ids.get_mut(&epoch);
            if let Some(query_ids) = query_ids {
                query_ids.remove(&query_id);
                if query_ids.is_empty() {
                    self.epoch_to_query_ids.remove(&epoch);
                }
            }
        }
    }

    pub fn unpin_snapshot_before(&mut self, last_committed_epoch: u64) {
        // Check the min epoch which still exists running query. If there is no running query,
        // we shall unpin snapshot with the last committed epoch.
        let min_epoch = match self.epoch_to_query_ids.first_key_value() {
            Some((epoch, query_ids)) => {
                assert!(
                    !query_ids.is_empty(),
                    "No query is associated with epoch {}",
                    epoch
                );
                *epoch
            }
            None => last_committed_epoch,
        };

        let last_unpin_snapshot = self.last_unpin_snapshot.load(Ordering::Acquire);
        tracing::trace!(
            "Try unpin snapshot: min_epoch {}, last_unpin_snapshot: {}",
            min_epoch,
            last_unpin_snapshot
        );

        if min_epoch <= last_unpin_snapshot {
            return;
        }

        let last_unpin_epoch = self.last_unpin_snapshot.clone();
        let meta_client = self.meta_client.clone();
        // do not block other requests getting epoch.
        tokio::spawn(async move {
            tracing::info!("Unpin epoch {:?} with RPC", min_epoch);
            match meta_client.unpin_snapshot_before(min_epoch).await {
                Ok(()) => last_unpin_epoch.store(min_epoch, Ordering::Release),
                Err(e) => error!("Request meta to unpin snapshot failed {:?}!", e),
            }
        });
    }
}
#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use risingwave_pb::hummock::HummockSnapshot;

    use crate::scheduler::distributed::tests::create_query;
    use crate::scheduler::HummockSnapshotManager;
    use crate::test_utils::MockFrontendMetaClient;

    #[tokio::test]
    async fn test_update_align_epoch_before_snapshot() {
        let hummock_snapshot_manager =
            HummockSnapshotManager::new(Arc::new(MockFrontendMetaClient {}));
        let align_snapshot1 = HummockSnapshot {
            committed_epoch: 0,
            current_epoch: 0,
            need_align: true,
        };
        let non_align_snapshot1 = HummockSnapshot {
            committed_epoch: 0,
            current_epoch: 1,
            need_align: false,
        };
        let non_align_snapshot2 = HummockSnapshot {
            committed_epoch: 2,
            current_epoch: 2,
            need_align: false,
        };
        // The fragment mapping is updated before the snapshot
        hummock_snapshot_manager.update_align_epoch(0);
        hummock_snapshot_manager.update_snapshot(align_snapshot1);
        let query = create_query().await;
        let query_id = query.query_id().clone();
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 0);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 0);
        hummock_snapshot_manager.update_snapshot(non_align_snapshot1);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 1);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 0);
        hummock_snapshot_manager.update_snapshot(non_align_snapshot2);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 2);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 2);
        hummock_snapshot_manager.update_align_epoch(3);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 2);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 2);
    }

    #[tokio::test]
    async fn test_update_align_epoch_after_snapshot() {
        let hummock_snapshot_manager =
            HummockSnapshotManager::new(Arc::new(MockFrontendMetaClient {}));
        let align_snapshot1 = HummockSnapshot {
            committed_epoch: 0,
            current_epoch: 0,
            need_align: true,
        };
        let align_snapshot2 = HummockSnapshot {
            committed_epoch: 1,
            current_epoch: 1,
            need_align: true,
        };

        let query = create_query().await;
        let query_id = query.query_id().clone();

        hummock_snapshot_manager.update_align_epoch(0);
        hummock_snapshot_manager.update_snapshot(align_snapshot1);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 0);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 0);

        hummock_snapshot_manager.update_snapshot(align_snapshot2);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 0);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 0);

        hummock_snapshot_manager.update_align_epoch(1);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 1);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 1);
    }

    #[tokio::test]
    async fn test_update_align_epoch_after_many_snapshot() {
        let hummock_snapshot_manager =
            HummockSnapshotManager::new(Arc::new(MockFrontendMetaClient {}));
        let align_snapshot1 = HummockSnapshot {
            committed_epoch: 0,
            current_epoch: 0,
            need_align: true,
        };
        let align_snapshot2 = HummockSnapshot {
            committed_epoch: 1,
            current_epoch: 1,
            need_align: true,
        };
        let non_align_snapshot1 = HummockSnapshot {
            committed_epoch: 1,
            current_epoch: 2,
            need_align: false,
        };
        let align_snapshot3 = HummockSnapshot {
            committed_epoch: 3,
            current_epoch: 3,
            need_align: true,
        };

        let non_align_snapshot2 = HummockSnapshot {
            committed_epoch: 3,
            current_epoch: 4,
            need_align: false,
        };

        let query = create_query().await;
        let query_id = query.query_id().clone();

        hummock_snapshot_manager.update_align_epoch(0);
        hummock_snapshot_manager.update_snapshot(align_snapshot1);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 0);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 0);

        hummock_snapshot_manager.update_snapshot(align_snapshot2);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 0);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 0);

        hummock_snapshot_manager.update_snapshot(non_align_snapshot1);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 0);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 0);

        hummock_snapshot_manager.update_snapshot(align_snapshot3);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 0);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 0);

        hummock_snapshot_manager.update_align_epoch(1);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 2);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 1);

        hummock_snapshot_manager.update_align_epoch(3);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 3);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 3);

        hummock_snapshot_manager.update_snapshot(non_align_snapshot2);
        let pinned_snapshot = hummock_snapshot_manager.acquire(&query_id).await.unwrap();
        assert_eq!(pinned_snapshot.get_current_epoch(), 4);
        assert_eq!(pinned_snapshot.get_committed_epoch(), 3);
    }
}
