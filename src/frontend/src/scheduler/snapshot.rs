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

use std::assert_matches::assert_matches;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_pb::common::{batch_query_epoch, BatchQueryEpoch};
use risingwave_pb::hummock::PbHummockSnapshot;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;

use crate::meta_client::FrontendMetaClient;

/// The interval between two unpin batches.
const UNPIN_INTERVAL_SECS: u64 = 10;

/// The storage snapshot to read from in a query, which can be freely cloned.
#[derive(Clone)]
pub enum ReadSnapshot {
    /// A frontend-pinned snapshot.
    FrontendPinned {
        snapshot: PinnedSnapshotRef,
        // It's embedded here because we always use it together with snapshot.
        is_barrier_read: bool,
    },

    /// Other arbitrary epoch, e.g. user specified.
    /// Availability and consistency of underlying data should be guaranteed accordingly.
    /// Currently it's only used for querying meta snapshot backup.
    Other(Epoch),
}

impl ReadSnapshot {
    /// Get the [`BatchQueryEpoch`] for this snapshot.
    pub fn batch_query_epoch(&self) -> BatchQueryEpoch {
        match self {
            ReadSnapshot::FrontendPinned {
                snapshot,
                is_barrier_read,
            } => snapshot.batch_query_epoch(*is_barrier_read),
            ReadSnapshot::Other(e) => BatchQueryEpoch {
                epoch: Some(batch_query_epoch::Epoch::Backup(e.0)),
            },
        }
    }

    /// Get the [`Epoch`] value for this snapshot.
    pub fn epoch(&self) -> Epoch {
        match self.batch_query_epoch().epoch.unwrap() {
            batch_query_epoch::Epoch::Committed(epoch)
            | batch_query_epoch::Epoch::Current(epoch)
            | batch_query_epoch::Epoch::Backup(epoch) => epoch.into(),
        }
    }

    /// Returns true if this snapshot is a barrier read.
    pub fn support_barrier_read(&self) -> bool {
        match self {
            ReadSnapshot::FrontendPinned {
                snapshot: _,
                is_barrier_read,
            } => *is_barrier_read,
            ReadSnapshot::Other(_) => false,
        }
    }
}

/// A frontend-pinned snapshot that notifies the [`UnpinWorker`] when it's dropped.
// DO NOT implement `Clone` for `PinnedSnapshot` because it's a "resource" that should always be a
// singleton for each snapshot. Use `PinnedSnapshotRef` instead.
pub struct PinnedSnapshot {
    value: PbHummockSnapshot,
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
    fn batch_query_epoch(&self, is_barrier_read: bool) -> BatchQueryEpoch {
        let epoch = if is_barrier_read {
            batch_query_epoch::Epoch::Current(self.value.current_epoch)
        } else {
            batch_query_epoch::Epoch::Committed(self.value.committed_epoch)
        };
        BatchQueryEpoch { epoch: Some(epoch) }
    }
}

impl Drop for PinnedSnapshot {
    fn drop(&mut self) {
        let _ = self.unpin_sender.send(Operation::Unpin(self.value.clone()));
    }
}

/// Returns an invalid snapshot, used for initial values.
fn invalid_snapshot() -> PbHummockSnapshot {
    PbHummockSnapshot {
        committed_epoch: INVALID_EPOCH,
        current_epoch: INVALID_EPOCH,
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

    /// Update the latest snapshot.
    ///
    /// Should only be called by the observer manager.
    pub fn update(&self, snapshot: PbHummockSnapshot) {
        self.latest_snapshot.send_if_modified(move |old_snapshot| {
            // Note(bugen): theoretically, the snapshots from the observer should always be
            // monotonically increasing, so there's no need to `max` them or check whether they are
            // the same. But we still do it here to be safe.
            // TODO: turn this into an assertion.
            let snapshot = PbHummockSnapshot {
                committed_epoch: std::cmp::max(
                    old_snapshot.value.committed_epoch,
                    snapshot.committed_epoch,
                ),
                current_epoch: std::cmp::max(
                    old_snapshot.value.current_epoch,
                    snapshot.current_epoch,
                ),
            };

            if old_snapshot.value == snapshot {
                // Ignore the same snapshot
                false
            } else {
                // First tell the worker that a new snapshot is going to be pinned.
                self.worker_sender
                    .send(Operation::Pin(snapshot.clone()))
                    .unwrap();
                // Then set the latest snapshot.
                *old_snapshot = Arc::new(PinnedSnapshot {
                    value: snapshot,
                    unpin_sender: self.worker_sender.clone(),
                });

                true
            }
        });
    }

    /// Wait until the latest snapshot is newer than the given one.
    pub async fn wait(&self, snapshot: PbHummockSnapshot) {
        let mut rx = self.latest_snapshot.subscribe();
        while rx.borrow_and_update().value.committed_epoch < snapshot.committed_epoch {
            rx.changed().await.unwrap();
        }
    }
}

/// The pin state of a snapshot.
#[derive(Debug)]
enum PinState {
    /// The snapshot is currently pinned by some sessions in this frontend.
    Pinned,

    /// The snapshot is no longer pinned by any session in this frontend, but it's still considered
    /// to be pinned by the meta service. It will be unpinned by the [`UnpinWorker`] in the next
    /// unpin batch through RPC, and the entry will be removed then.
    Unpinned,
}

/// The operation handled by the [`UnpinWorker`].
#[derive(Debug)]
enum Operation {
    /// Mark the snapshot as pinned, sent when a new snapshot is pinned with `update`.
    Pin(PbHummockSnapshot),

    /// Mark the snapshot as unpinned, sent when all references to a [`PinnedSnapshot`] is dropped.
    Unpin(PbHummockSnapshot),
}

impl Operation {
    /// Returns whether the operation is for an invalid snapshot, which should be ignored.
    fn is_invalid(&self) -> bool {
        match self {
            Operation::Pin(s) | Operation::Unpin(s) => s,
        }
        .current_epoch
            == INVALID_EPOCH
    }
}

/// The key for the states map in [`UnpinWorker`].
///
/// The snapshot will be first sorted by `committed_epoch`, then by `current_epoch`.
#[derive(Debug, PartialEq, Clone)]
struct SnapshotKey(PbHummockSnapshot);

impl Eq for SnapshotKey {}

impl Ord for SnapshotKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.0.committed_epoch, self.0.current_epoch)
            .cmp(&(other.0.committed_epoch, other.0.current_epoch))
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
            Operation::Pin(snapshot) => {
                self.states
                    .try_insert(SnapshotKey(snapshot), PinState::Pinned)
                    .unwrap();
            }
            Operation::Unpin(snapshot) => match self.states.entry(SnapshotKey(snapshot)) {
                Entry::Vacant(_v) => unreachable!("unpin a snapshot that is not pinned"),
                Entry::Occupied(o) => {
                    assert_matches!(o.get(), PinState::Pinned);
                    *o.into_mut() = PinState::Unpinned;
                }
            },
        }
    }

    /// Try to unpin all continuous snapshots with [`PinState::Unpinned`] in a batch through RPC,
    /// and clean up their entries.
    async fn unpin_batch(&mut self) {
        // Find the minimum snapshot that is pinned. Unpin all snapshots before it.
        if let Some(min_snapshot) = self
            .states
            .iter()
            .find(|(_, s)| matches!(s, PinState::Pinned))
            .map(|(k, _)| k.clone())
        {
            if &min_snapshot == self.states.first_key_value().unwrap().0 {
                // Nothing to unpin.
                return;
            }

            let min_epoch = min_snapshot.0.committed_epoch;
            tracing::info!(min_epoch, "unpin snapshot with RPC");

            match self.meta_client.unpin_snapshot_before(min_epoch).await {
                Ok(()) => {
                    // Remove all snapshots before this one.
                    self.states = self.states.split_off(&min_snapshot);
                }
                Err(e) => tracing::error!(%e, min_epoch, "unpin snapshot failed"),
            }
        }
    }
}
