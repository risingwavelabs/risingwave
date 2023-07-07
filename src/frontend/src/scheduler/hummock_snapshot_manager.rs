// Copyright 2023 RisingWave Labs
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

use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use arc_swap::ArcSwap;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_pb::common::{batch_query_epoch, BatchQueryEpoch};
use risingwave_pb::hummock::HummockSnapshot;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::{channel as once_channel, Sender as Callback};
use tracing::error;

use crate::meta_client::FrontendMetaClient;
use crate::scheduler::{SchedulerError, SchedulerResult};
use crate::session::transaction::Id as TxnId;

const UNPIN_INTERVAL_SECS: u64 = 10;

pub type HummockSnapshotManagerRef = Arc<HummockSnapshotManager>;

pub enum PinnedHummockSnapshot {
    FrontendPinned(
        HummockSnapshotGuard,
        // `is_barrier_read`.
        // It's embedded here because we always use it together with snapshot.
        bool,
    ),
    /// Other arbitrary epoch, e.g. user specified.
    /// Availability and consistency of underlying data should be guaranteed accordingly.
    /// Currently it's only used for querying meta snapshot backup.
    Other(Epoch),
}

pub type PinnedHummockSnapshotRef = Arc<PinnedHummockSnapshot>;

impl PinnedHummockSnapshot {
    pub fn get_batch_query_epoch(&self) -> BatchQueryEpoch {
        match self {
            PinnedHummockSnapshot::FrontendPinned(s, is_barrier_read) => {
                s.get_batch_query_epoch(*is_barrier_read)
            }
            PinnedHummockSnapshot::Other(e) => BatchQueryEpoch {
                epoch: Some(batch_query_epoch::Epoch::Backup(e.0)),
            },
        }
    }

    pub fn support_barrier_read(&self) -> bool {
        match self {
            PinnedHummockSnapshot::FrontendPinned(_, is_barrier_read) => *is_barrier_read,
            PinnedHummockSnapshot::Other(_) => false,
        }
    }
}

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
        txn_id: TxnId,
        sender: Callback<SchedulerResult<HummockSnapshot>>,
    },
    ReleaseEpoch {
        txn_id: TxnId,
        epoch: u64,
    },
    Tick,
}

pub struct HummockSnapshotGuard {
    snapshot: HummockSnapshot,
    txn_id: TxnId,
    unpin_snapshot_sender: UnboundedSender<EpochOperation>,
}

impl HummockSnapshotGuard {
    pub fn get_batch_query_epoch(&self, is_barrier_read: bool) -> BatchQueryEpoch {
        let epoch = if is_barrier_read {
            batch_query_epoch::Epoch::Current(self.snapshot.current_epoch)
        } else {
            batch_query_epoch::Epoch::Committed(self.snapshot.committed_epoch)
        };
        BatchQueryEpoch { epoch: Some(epoch) }
    }
}

impl Drop for HummockSnapshotGuard {
    fn drop(&mut self) {
        let _ = self
            .unpin_snapshot_sender
            .send(EpochOperation::ReleaseEpoch {
                txn_id: self.txn_id,
                epoch: self.snapshot.committed_epoch,
            })
            .inspect_err(|err| {
                error!("failed to send release epoch: {}", err);
            });
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
                    Some(EpochOperation::RequestEpoch { txn_id, sender }) => {
                        pin_batches.push((txn_id, sender));
                    }
                    Some(EpochOperation::ReleaseEpoch { txn_id, epoch }) => {
                        unpin_batches.push((txn_id, epoch));
                    }
                    Some(EpochOperation::Tick) => {}
                    None => return,
                }
                while let Ok(msg) = receiver.try_recv() {
                    match msg {
                        EpochOperation::RequestEpoch { txn_id, sender } => {
                            pin_batches.push((txn_id, sender));
                        }
                        EpochOperation::ReleaseEpoch { txn_id, epoch } => {
                            unpin_batches.push((txn_id, epoch));
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
                    // `get_epoch_for_txn_from_rpc`.
                    let epoch = manager
                        .get_epoch_for_txn_from_push(&mut pin_batches)
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

    pub async fn acquire(&self, txn_id: TxnId) -> SchedulerResult<HummockSnapshotGuard> {
        let (sender, rc) = once_channel();
        let msg = EpochOperation::RequestEpoch { txn_id, sender };
        self.sender.send(msg).map_err(|_| {
            SchedulerError::Internal(anyhow!("Failed to get epoch for transaction: {:?}", txn_id))
        })?;
        let snapshot = rc.await.unwrap_or_else(|e| {
            Err(SchedulerError::Internal(anyhow!(
                "Failed to get epoch for transaction: {:?}, the rpc thread may panic: {:?}",
                txn_id,
                e
            )))
        })?;
        Ok(HummockSnapshotGuard {
            snapshot,
            txn_id,
            unpin_snapshot_sender: self.sender.clone(),
        })
    }

    pub fn update_epoch(&self, snapshot: HummockSnapshot) {
        // Note: currently the snapshot is not only updated from the observer, so we need to take
        // the `max` here instead of directly replace the snapshot.
        self.latest_snapshot.rcu(|prev| HummockSnapshot {
            committed_epoch: std::cmp::max(prev.committed_epoch, snapshot.committed_epoch),
            current_epoch: std::cmp::max(prev.current_epoch, snapshot.current_epoch),
        });
    }

    pub fn latest_snapshot_current_epoch(&self) -> Epoch {
        self.latest_snapshot.load().current_epoch.into()
    }
}

struct HummockSnapshotManagerCore {
    /// Record the transaction ids that pin each snapshot.
    /// Send an `unpin_snapshot` RPC when a snapshot is not pinned any more.
    epoch_to_txn_ids: BTreeMap<u64, HashSet<TxnId>>,
    meta_client: Arc<dyn FrontendMetaClient>,
    last_unpin_snapshot: Arc<AtomicU64>,
    latest_snapshot: SnapshotRef,
}

impl HummockSnapshotManagerCore {
    fn new(meta_client: Arc<dyn FrontendMetaClient>, latest_snapshot: SnapshotRef) -> Self {
        Self {
            // Initialize by setting `is_outdated` to `true`.
            meta_client,
            epoch_to_txn_ids: BTreeMap::default(),
            last_unpin_snapshot: Arc::new(AtomicU64::new(INVALID_EPOCH)),
            latest_snapshot,
        }
    }

    /// Retrieve max committed epoch and max current epoch from locally cached value, which is
    /// maintained by meta's notification service.
    fn get_epoch_for_txn_from_push(
        &mut self,
        batches: &mut Vec<(TxnId, Callback<SchedulerResult<HummockSnapshot>>)>,
    ) -> HummockSnapshot {
        let snapshot = HummockSnapshot::clone(&self.latest_snapshot.load());
        self.notify_epoch_assigned_for_txns(&snapshot, batches);
        snapshot
    }

    /// Add committed epoch in `epoch_to_txn_ids`, notify transactions with committed epoch and
    /// current epoch.
    fn notify_epoch_assigned_for_txns(
        &mut self,
        snapshot: &HummockSnapshot,
        batches: &mut Vec<(TxnId, Callback<SchedulerResult<HummockSnapshot>>)>,
    ) {
        if batches.is_empty() {
            return;
        }
        let committed_epoch = snapshot.committed_epoch;
        let txns = match self.epoch_to_txn_ids.get_mut(&committed_epoch) {
            None => {
                self.epoch_to_txn_ids
                    .insert(committed_epoch, HashSet::default());
                self.epoch_to_txn_ids.get_mut(&committed_epoch).unwrap()
            }
            Some(txns) => txns,
        };
        for (id, cb) in batches.drain(..) {
            txns.insert(id);
            let _ = cb.send(Ok(snapshot.clone()));
        }
    }

    pub fn release_epoch(&mut self, txns: &mut Vec<(TxnId, u64)>) {
        for (txn_id, epoch) in txns.drain(..) {
            let txn_ids = self.epoch_to_txn_ids.get_mut(&epoch);
            if let Some(txn_ids) = txn_ids {
                txn_ids.remove(&txn_id);
                if txn_ids.is_empty() {
                    self.epoch_to_txn_ids.remove(&epoch);
                }
            }
        }
    }

    pub fn unpin_snapshot_before(&mut self, last_committed_epoch: u64) {
        // Check the min epoch which still exists running transaction. If there is no running
        // transaction, we shall unpin snapshot with the last committed epoch.
        let min_epoch = match self.epoch_to_txn_ids.first_key_value() {
            Some((epoch, txn_ids)) => {
                assert!(
                    !txn_ids.is_empty(),
                    "No transaction is associated with epoch {}",
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
