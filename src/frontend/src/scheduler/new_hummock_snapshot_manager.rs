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
use risingwave_pb::hummock::{HummockSnapshot, PbHummockSnapshot};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::watch;

use crate::meta_client::FrontendMetaClient;

const UNPIN_INTERVAL_SECS: u64 = 10;

#[derive(Clone)]
pub enum ReadSnapshot {
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

    pub fn batch_query_epoch_value(&self) -> Epoch {
        match self.batch_query_epoch().epoch.unwrap() {
            batch_query_epoch::Epoch::Committed(epoch)
            | batch_query_epoch::Epoch::Current(epoch)
            | batch_query_epoch::Epoch::Backup(epoch) => epoch.into(),
        }
    }

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

pub struct PinnedSnapshot {
    value: PbHummockSnapshot,
    unpin_sender: UnboundedSender<Operation>,
}

pub type PinnedSnapshotRef = Arc<PinnedSnapshot>;

impl PinnedSnapshot {
    pub fn batch_query_epoch(&self, is_barrier_read: bool) -> BatchQueryEpoch {
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
        let _ = self.unpin_sender.send(Operation::Unpin {
            commited_epoch: self.value.committed_epoch,
        });
    }
}

/// Cache of hummock snapshot in meta.
pub struct HummockSnapshotManager {
    /// Send epoch-related operations to `HummockSnapshotManagerCore` for async batch handling.
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
            value: HummockSnapshot {
                committed_epoch: INVALID_EPOCH,
                current_epoch: INVALID_EPOCH,
            },
            unpin_sender: worker_sender.clone(),
        });

        let (latest_snapshot, _) = watch::channel(latest_snapshot);

        Self {
            worker_sender,
            latest_snapshot,
        }
    }

    pub fn acquire(&self) -> PinnedSnapshotRef {
        self.latest_snapshot.borrow().clone()
    }

    pub fn update(&self, snapshot: PbHummockSnapshot) {
        self.worker_sender
            .send(Operation::Pin {
                commited_epoch: snapshot.committed_epoch,
            })
            .unwrap();

        let snapshot = Arc::new(PinnedSnapshot {
            value: snapshot,
            unpin_sender: self.worker_sender.clone(),
        });
        let _ = self.latest_snapshot.send_replace(snapshot);
    }

    pub async fn wait(&self, snapshot: PbHummockSnapshot) {
        let mut rx = self.latest_snapshot.subscribe();
        while rx.borrow_and_update().value.committed_epoch < snapshot.committed_epoch {
            rx.changed().await.unwrap();
        }
    }
}

#[derive(Debug)]
enum PinState {
    Pinned,
    Unpinned,
}

#[derive(Debug)]
enum Operation {
    Pin { commited_epoch: u64 },
    Unpin { commited_epoch: u64 },
}

struct UnpinWorker {
    meta_client: Arc<dyn FrontendMetaClient>,

    receiver: UnboundedReceiver<Operation>,

    states: BTreeMap<u64, PinState>,
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

    async fn run(mut self) {
        let mut ticker = tokio::time::interval(Duration::from_secs(UNPIN_INTERVAL_SECS));

        tokio::select! {
            operation = self.receiver.recv() => {
                let Some(operation) = operation else { return };
                self.handle_operation(operation);
            }

            _ = ticker.tick() => {
                self.unpin_batch().await;
            }
        }
    }

    fn handle_operation(&mut self, operation: Operation) {
        match operation {
            Operation::Pin { commited_epoch } => {
                self.states
                    .try_insert(commited_epoch, PinState::Pinned)
                    .unwrap();
            }
            Operation::Unpin { commited_epoch } => match self.states.entry(commited_epoch) {
                Entry::Vacant(_) => assert_eq!(commited_epoch, INVALID_EPOCH),
                Entry::Occupied(o) => {
                    assert_matches!(o.get(), PinState::Pinned);
                    *o.into_mut() = PinState::Unpinned;
                }
            },
        }
    }

    async fn unpin_batch(&mut self) {
        if let Some((&min_epoch, _)) = self
            .states
            .iter()
            .find(|(_, s)| matches!(s, PinState::Unpinned))
        {
            tracing::info!("Unpin epoch {:?} with RPC", min_epoch);

            match self.meta_client.unpin_snapshot_before(min_epoch).await {
                Ok(()) => self.states = self.states.split_off(&min_epoch),
                Err(e) => tracing::error!("Request meta to unpin snapshot failed {:?}!", e),
            }
        }
    }
}
