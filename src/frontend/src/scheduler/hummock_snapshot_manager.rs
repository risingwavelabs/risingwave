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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use arc_swap::ArcSwap;
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
    sender: UnboundedSender<EpochOperation>,

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
        self.unpin_snapshot_sender
            .send(EpochOperation::ReleaseEpoch {
                query_id: self.query_id.clone(),
                epoch: self.snapshot.committed_epoch,
            })
            .expect("Unpin channel should never closed");
    }
}

impl HummockSnapshotManager {
    pub fn new(meta_client: Arc<dyn FrontendMetaClient>) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        let latest_snapshot = Arc::new(ArcSwap::from_pointee(HummockSnapshot {
            committed_epoch: INVALID_EPOCH,
            current_epoch: INVALID_EPOCH,
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
            sender,
            latest_snapshot,
        }
    }

    pub async fn acquire(&self, query_id: &QueryId) -> SchedulerResult<PinnedHummockSnapshot> {
        let (sender, rc) = once_channel();
        let msg = EpochOperation::RequestEpoch {
            query_id: query_id.clone(),
            sender,
        };
        self.sender.send(msg).map_err(|_| {
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
            unpin_snapshot_sender: self.sender.clone(),
        })
    }

    pub fn update_epoch(&self, snapshot: HummockSnapshot) {
        let snapshot = Arc::new(snapshot);
        let prev = self.latest_snapshot.swap(snapshot.clone());
        assert!(prev.committed_epoch <= snapshot.committed_epoch);
        assert!(prev.current_epoch < snapshot.current_epoch);
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
