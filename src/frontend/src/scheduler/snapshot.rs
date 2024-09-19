// Copyright 2024 RisingWave Labs
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

use std::assert_matches::assert_matches;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use risingwave_common::catalog::TableId;
use risingwave_common::must_match;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
use risingwave_hummock_sdk::{
    FrontendHummockVersion, FrontendHummockVersionDelta, HummockVersionId, INVALID_VERSION_ID,
};
use risingwave_pb::common::{batch_query_epoch, BatchQueryEpoch};
use risingwave_pb::hummock::{HummockVersionDeltas, PbHummockSnapshot, StateTableInfoDelta};
use thiserror_ext::AsReport;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;

use crate::expr::InlineNowProcTime;
use crate::meta_client::FrontendMetaClient;
use crate::scheduler::SchedulerError;

/// The interval between two unpin batches.
const UNPIN_INTERVAL_SECS: u64 = 10;

/// The storage snapshot to read from in a query, which can be freely cloned.
#[derive(Clone)]
pub enum ReadSnapshot {
    /// A frontend-pinned snapshot.
    FrontendPinned {
        snapshot: PinnedSnapshotRef,
    },

    BarrierRead,

    /// Other arbitrary epoch, e.g. user specified.
    /// Availability and consistency of underlying data should be guaranteed accordingly.
    /// Currently it's only used for querying meta snapshot backup.
    Other(Epoch),
}

pub struct QuerySnapshot {
    snapshot: ReadSnapshot,
    scan_tables: HashSet<TableId>,
}

impl QuerySnapshot {
    pub fn new(snapshot: ReadSnapshot, scan_tables: HashSet<TableId>) -> Self {
        Self {
            snapshot,
            scan_tables,
        }
    }

    /// Get the [`BatchQueryEpoch`] for this snapshot.
    pub fn batch_query_epoch(&self) -> Result<BatchQueryEpoch, SchedulerError> {
        Ok(match &self.snapshot {
            ReadSnapshot::FrontendPinned { snapshot } => BatchQueryEpoch {
                epoch: Some(batch_query_epoch::Epoch::Committed(
                    snapshot.batch_query_epoch(&self.scan_tables)?.0,
                )),
            },
            ReadSnapshot::BarrierRead => BatchQueryEpoch {
                epoch: Some(batch_query_epoch::Epoch::Current(u64::MAX)),
            },
            ReadSnapshot::Other(e) => BatchQueryEpoch {
                epoch: Some(batch_query_epoch::Epoch::Backup(e.0)),
            },
        })
    }

    pub fn inline_now_proc_time(&self) -> Result<InlineNowProcTime, SchedulerError> {
        let epoch = match &self.snapshot {
            ReadSnapshot::FrontendPinned { snapshot, .. } => {
                snapshot.batch_query_epoch(&self.scan_tables)?
            }
            ReadSnapshot::Other(epoch) => *epoch,
            ReadSnapshot::BarrierRead => Epoch::now(),
        };
        Ok(InlineNowProcTime::new(epoch))
    }

    /// Returns true if this snapshot is a barrier read.
    pub fn support_barrier_read(&self) -> bool {
        matches!(&self.snapshot, ReadSnapshot::BarrierRead)
    }
}

/// A frontend-pinned snapshot that notifies the [`UnpinWorker`] when it's dropped.
// DO NOT implement `Clone` for `PinnedSnapshot` because it's a "resource" that should always be a
// singleton for each snapshot. Use `PinnedSnapshotRef` instead.
pub struct PinnedSnapshot {
    value: FrontendHummockVersion,
    unpin_sender: UnboundedSender<Operation>,
}

impl std::fmt::Debug for PinnedSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

/// A reference to a frontend-pinned snapshot.
pub type PinnedSnapshotRef = Arc<PinnedSnapshot>;

impl PinnedSnapshot {
    fn batch_query_epoch(&self, scan_tables: &HashSet<TableId>) -> Result<Epoch, SchedulerError> {
        // use the min committed epoch of tables involved in the scan
        let epoch = scan_tables
            .iter()
            .map(|table_id| {
                self.value
                    .state_table_info
                    .info()
                    .get(table_id)
                    .map(|info| Epoch(info.committed_epoch))
                    .ok_or_else(|| anyhow!("table id {table_id} may have been dropped"))
            })
            .try_fold(None, |prev_min_committed_epoch, committed_epoch| {
                committed_epoch.map(|committed_epoch| {
                    if let Some(prev_min_committed_epoch) = prev_min_committed_epoch
                        && prev_min_committed_epoch >= committed_epoch
                    {
                        Some(prev_min_committed_epoch)
                    } else {
                        Some(committed_epoch)
                    }
                })
            })?
            .unwrap_or_else(Epoch::now); // When no table is involved, use current timestamp as epoch
        Ok(epoch)
    }

    pub fn version(&self) -> &FrontendHummockVersion {
        &self.value
    }
}

impl Drop for PinnedSnapshot {
    fn drop(&mut self) {
        let _ = self.unpin_sender.send(Operation::Unpin(self.value.id));
    }
}

/// Returns an invalid snapshot, used for initial values.
fn invalid_snapshot() -> FrontendHummockVersion {
    FrontendHummockVersion {
        id: INVALID_VERSION_ID,
        max_committed_epoch: 0,
        state_table_info: HummockVersionStateTableInfo::from_protobuf(&HashMap::new()),
        table_change_log: Default::default(),
    }
}

/// Cache of hummock snapshot in meta.
pub struct HummockSnapshotManager {
    /// Send epoch-related operations to [`UnpinWorker`] for managing the pinned snapshots and
    /// unpin them in a batch through RPC.
    worker_sender: UnboundedSender<Operation>,

    /// The latest snapshot synced from the meta service.
    ///
    /// The `max_committed_epoch` and `max_current_epoch` are pushed from meta node to reduce rpc
    /// number.
    ///
    /// We have two epoch(committed and current), We only use `committed_epoch` to pin or unpin,
    /// because `committed_epoch` always less or equal `current_epoch`, and the data with
    /// `current_epoch` is always in the shared buffer, so it will never be gc before the data
    /// of `committed_epoch`.
    latest_snapshot: watch::Sender<PinnedSnapshotRef>,
}

pub type HummockSnapshotManagerRef = Arc<HummockSnapshotManager>;

impl HummockSnapshotManager {
    pub fn new(meta_client: Arc<dyn FrontendMetaClient>) -> Self {
        let (worker_sender, worker_receiver) = tokio::sync::mpsc::unbounded_channel();

        tokio::spawn(UnpinWorker::new(meta_client, worker_receiver).run());

        let latest_snapshot = Arc::new(PinnedSnapshot {
            value: invalid_snapshot(),
            unpin_sender: worker_sender.clone(),
        });

        let (latest_snapshot, _) = watch::channel(latest_snapshot);

        Self {
            worker_sender,
            latest_snapshot,
        }
    }

    /// Acquire the latest snapshot by increasing its reference count.
    pub fn acquire(&self) -> PinnedSnapshotRef {
        self.latest_snapshot.borrow().clone()
    }

    pub fn init(&self, version: FrontendHummockVersion) {
        self.update_inner(|_| Some(version));
    }

    /// Update the latest snapshot.
    ///
    /// Should only be called by the observer manager.
    pub fn update(&self, deltas: HummockVersionDeltas) {
        self.update_inner(|old_snapshot| {
            if deltas.version_deltas.is_empty() {
                return None;
            }
            let mut snapshot = old_snapshot.clone();
            for delta in deltas.version_deltas {
                snapshot.apply_delta(FrontendHummockVersionDelta::from_protobuf(delta));
            }
            Some(snapshot)
        })
    }

    pub fn add_table_for_test(&self, table_id: TableId) {
        self.update_inner(|version| {
            let mut version = version.clone();
            version.id = version.id.next();
            version.state_table_info.apply_delta(
                &HashMap::from_iter([(
                    table_id,
                    StateTableInfoDelta {
                        committed_epoch: INVALID_EPOCH,
                        safe_epoch: INVALID_EPOCH,
                        compaction_group_id: 0,
                    },
                )]),
                &HashSet::new(),
            );
            Some(version)
        });
    }

    fn update_inner(
        &self,
        get_new_snapshot: impl FnOnce(&FrontendHummockVersion) -> Option<FrontendHummockVersion>,
    ) {
        self.latest_snapshot.send_if_modified(move |old_snapshot| {
            let new_snapshot = get_new_snapshot(&old_snapshot.value);
            let Some(snapshot) = new_snapshot else {
                return false;
            };
            if snapshot.id <= old_snapshot.value.id {
                assert_eq!(
                    snapshot.id, old_snapshot.value.id,
                    "receive stale frontend version"
                );
                return false;
            }
            // First tell the worker that a new snapshot is going to be pinned.
            self.worker_sender
                .send(Operation::Pin(snapshot.id, snapshot.max_committed_epoch))
                .unwrap();
            // Then set the latest snapshot.
            *old_snapshot = Arc::new(PinnedSnapshot {
                value: snapshot,
                unpin_sender: self.worker_sender.clone(),
            });

            true
        });
    }

    /// Wait until the latest snapshot is newer than the given one.
    pub async fn wait(&self, snapshot: PbHummockSnapshot) {
        let mut rx = self.latest_snapshot.subscribe();
        while rx.borrow_and_update().value.max_committed_epoch < snapshot.committed_epoch {
            rx.changed().await.unwrap();
        }
    }
}

/// The pin state of a snapshot.
#[derive(Debug)]
enum PinState {
    /// The snapshot is currently pinned by some sessions in this frontend.
    Pinned(u64),

    /// The snapshot is no longer pinned by any session in this frontend, but it's still considered
    /// to be pinned by the meta service. It will be unpinned by the [`UnpinWorker`] in the next
    /// unpin batch through RPC, and the entry will be removed then.
    Unpinned,
}

/// The operation handled by the [`UnpinWorker`].
#[derive(Debug)]
enum Operation {
    /// Mark the snapshot as pinned, sent when a new snapshot is pinned with `update`.
    Pin(HummockVersionId, u64),

    /// Mark the snapshot as unpinned, sent when all references to a [`PinnedSnapshot`] is dropped.
    Unpin(HummockVersionId),
}

impl Operation {
    /// Returns whether the operation is for an invalid snapshot, which should be ignored.
    fn is_invalid(&self) -> bool {
        *match self {
            Operation::Pin(id, _) | Operation::Unpin(id) => id,
        } == INVALID_VERSION_ID
    }
}

/// The key for the states map in [`UnpinWorker`].
///
/// The snapshot will be first sorted by `committed_epoch`, then by `current_epoch`.
#[derive(Debug, PartialEq, Clone)]
struct SnapshotKey(HummockVersionId);

impl Eq for SnapshotKey {}

impl Ord for SnapshotKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.to_u64().cmp(&other.0.to_u64())
    }
}

impl PartialOrd for SnapshotKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// The worker that manages the pin states of snapshots and unpins them periodically in a batch
/// through RPC to the meta service.
struct UnpinWorker {
    meta_client: Arc<dyn FrontendMetaClient>,

    /// The receiver of operations from snapshot updating in [`HummockSnapshotManager`] and
    /// dropping of [`PinnedSnapshot`].
    receiver: UnboundedReceiver<Operation>,

    /// The pin states of existing snapshots in this frontend.
    ///
    /// All snapshots in this map are considered to be pinned by the meta service, those with
    /// [`PinState::Unpinned`] will be unpinned in the next unpin batch through RPC.
    states: BTreeMap<SnapshotKey, PinState>,
}

impl UnpinWorker {
    fn new(
        meta_client: Arc<dyn FrontendMetaClient>,
        receiver: UnboundedReceiver<Operation>,
    ) -> Self {
        Self {
            meta_client,
            receiver,
            states: Default::default(),
        }
    }

    /// Run the loop of handling operations and unpinning snapshots.
    async fn run(mut self) {
        let mut ticker = tokio::time::interval(Duration::from_secs(UNPIN_INTERVAL_SECS));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                operation = self.receiver.recv() => {
                    let Some(operation) = operation else { return }; // manager dropped
                    self.handle_operation(operation);
                }

                _ = ticker.tick() => {
                    self.unpin_batch().await;
                }
            }
        }
    }

    /// Handle an operation and manipulate the states.
    fn handle_operation(&mut self, operation: Operation) {
        if operation.is_invalid() {
            return;
        }

        match operation {
            Operation::Pin(version_id, committed_epoch) => {
                self.states
                    .try_insert(SnapshotKey(version_id), PinState::Pinned(committed_epoch))
                    .unwrap();
            }
            Operation::Unpin(snapshot) => match self.states.entry(SnapshotKey(snapshot)) {
                Entry::Vacant(_v) => unreachable!("unpin a snapshot that is not pinned"),
                Entry::Occupied(o) => {
                    assert_matches!(o.get(), PinState::Pinned(_));
                    *o.into_mut() = PinState::Unpinned;
                }
            },
        }
    }

    /// Try to unpin all continuous snapshots with [`PinState::Unpinned`] in a batch through RPC,
    /// and clean up their entries.
    async fn unpin_batch(&mut self) {
        // Find the minimum snapshot that is pinned. Unpin all snapshots before it.
        if let Some((min_snapshot, min_committed_epoch)) = self
            .states
            .iter()
            .find(|(_, s)| matches!(s, PinState::Pinned(_)))
            .map(|(k, s)| {
                (
                    k.clone(),
                    must_match!(s, PinState::Pinned(committed_epoch) => *committed_epoch),
                )
            })
        {
            if &min_snapshot == self.states.first_key_value().unwrap().0 {
                // Nothing to unpin.
                return;
            }

            let min_epoch = min_committed_epoch;

            match self.meta_client.unpin_snapshot_before(min_epoch).await {
                Ok(()) => {
                    // Remove all snapshots before this one.
                    self.states = self.states.split_off(&min_snapshot);
                }
                Err(e) => {
                    tracing::error!(error = %e.as_report(), min_epoch, "unpin snapshot failed")
                }
            }
        }
    }
}
