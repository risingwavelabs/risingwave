// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::version::HummockVersionStateTableInfo;
use risingwave_hummock_sdk::{
    FrontendHummockVersion, FrontendHummockVersionDelta, HummockVersionId, INVALID_VERSION_ID,
};
use risingwave_pb::common::{BatchQueryCommittedEpoch, BatchQueryEpoch, batch_query_epoch};
use risingwave_pb::hummock::{HummockVersionDeltas, StateTableInfoDelta};
use tokio::sync::watch;

use crate::error::{ErrorCode, RwError};
use crate::expr::InlineNowProcTime;
use crate::meta_client::FrontendMetaClient;
use crate::scheduler::SchedulerError;

/// The storage snapshot to read from in a query, which can be freely cloned.
#[derive(Clone)]
pub enum ReadSnapshot {
    /// A frontend-pinned snapshot.
    FrontendPinned {
        snapshot: PinnedSnapshotRef,
    },

    ReadUncommitted,

    /// Other arbitrary epoch, e.g. user specified.
    /// Availability and consistency of underlying data should be guaranteed accordingly.
    /// Currently it's only used for querying meta snapshot backup.
    Other(Epoch),
}

impl ReadSnapshot {
    /// Get the [`BatchQueryEpoch`] for this snapshot.
    pub fn batch_query_epoch(
        &self,
        read_storage_tables: &HashSet<TableId>,
    ) -> Result<BatchQueryEpoch, SchedulerError> {
        Ok(match self {
            ReadSnapshot::FrontendPinned { snapshot } => BatchQueryEpoch {
                epoch: Some(batch_query_epoch::Epoch::Committed(
                    BatchQueryCommittedEpoch {
                        epoch: snapshot.batch_query_epoch(read_storage_tables)?.0,
                        hummock_version_id: snapshot.value.id.to_u64(),
                    },
                )),
            },
            ReadSnapshot::ReadUncommitted => BatchQueryEpoch {
                epoch: Some(batch_query_epoch::Epoch::Current(u64::MAX)),
            },
            ReadSnapshot::Other(e) => BatchQueryEpoch {
                epoch: Some(batch_query_epoch::Epoch::Backup(e.0)),
            },
        })
    }

    pub fn inline_now_proc_time(&self) -> InlineNowProcTime {
        let epoch = match self {
            ReadSnapshot::FrontendPinned { snapshot } => snapshot
                .value
                .state_table_info
                .max_table_committed_epoch()
                .map(Epoch)
                .unwrap_or_else(Epoch::now),
            ReadSnapshot::ReadUncommitted => Epoch::now(),
            ReadSnapshot::Other(epoch) => *epoch,
        };
        InlineNowProcTime::new(epoch)
    }

    /// Returns true if this snapshot is a barrier read.
    pub fn support_barrier_read(&self) -> bool {
        matches!(self, ReadSnapshot::ReadUncommitted)
    }
}

// DO NOT implement `Clone` for `PinnedSnapshot` because it's a "resource" that should always be a
// singleton for each snapshot. Use `PinnedSnapshotRef` instead.
pub struct PinnedSnapshot {
    value: FrontendHummockVersion,
}

impl std::fmt::Debug for PinnedSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

/// A reference to a frontend-pinned snapshot.
pub type PinnedSnapshotRef = Arc<PinnedSnapshot>;

impl PinnedSnapshot {
    fn batch_query_epoch(
        &self,
        read_storage_tables: &HashSet<TableId>,
    ) -> Result<Epoch, SchedulerError> {
        // use the min committed epoch of tables involved in the scan
        let epoch = read_storage_tables
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
                        && prev_min_committed_epoch <= committed_epoch
                    {
                        Some(prev_min_committed_epoch)
                    } else {
                        Some(committed_epoch)
                    }
                })
            })?
            .unwrap_or_else(Epoch::now);
        Ok(epoch)
    }

    pub fn version(&self) -> &FrontendHummockVersion {
        &self.value
    }

    pub fn list_change_log_epochs(
        &self,
        table_id: u32,
        min_epoch: u64,
        max_count: u32,
    ) -> Vec<u64> {
        if let Some(table_change_log) = self.value.table_change_log.get(&TableId::new(table_id)) {
            let table_change_log = table_change_log.clone();
            table_change_log.get_non_empty_epochs(min_epoch, max_count as usize)
        } else {
            vec![]
        }
    }
}

/// Returns an invalid snapshot, used for initial values.
fn invalid_snapshot() -> FrontendHummockVersion {
    FrontendHummockVersion {
        id: INVALID_VERSION_ID,
        state_table_info: HummockVersionStateTableInfo::from_protobuf(&HashMap::new()),
        table_change_log: Default::default(),
    }
}

/// Cache of hummock snapshot in meta.
pub struct HummockSnapshotManager {
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

    table_change_log_notification_sender: watch::Sender<TableChangeLogNotificationMsg>,
}

#[derive(Default)]
struct TableChangeLogNotificationMsg {
    updated_change_log_table_ids: HashSet<u32>,
    deleted_table_ids: HashSet<u32>,
}

pub type HummockSnapshotManagerRef = Arc<HummockSnapshotManager>;

impl HummockSnapshotManager {
    pub fn new(_meta_client: Arc<dyn FrontendMetaClient>) -> Self {
        let latest_snapshot = Arc::new(PinnedSnapshot {
            value: invalid_snapshot(),
        });

        let (latest_snapshot, _) = watch::channel(latest_snapshot);

        let (table_change_log_notification_sender, _) =
            watch::channel(TableChangeLogNotificationMsg::default());

        Self {
            latest_snapshot,
            table_change_log_notification_sender,
        }
    }

    /// Acquire the latest snapshot by increasing its reference count.
    pub fn acquire(&self) -> PinnedSnapshotRef {
        self.latest_snapshot.borrow().clone()
    }

    pub fn init(&self, version: FrontendHummockVersion) {
        let updated_change_log_table_ids: HashSet<_> = version
            .table_change_log
            .iter()
            .filter_map(|(table_id, change_log)| {
                if change_log.get_non_empty_epochs(0, usize::MAX).is_empty() {
                    None
                } else {
                    Some(table_id.table_id())
                }
            })
            .collect();
        self.table_change_log_notification_sender
            .send(TableChangeLogNotificationMsg {
                updated_change_log_table_ids,
                deleted_table_ids: Default::default(),
            })
            .ok();

        self.update_inner(|_| Some(version));
    }

    /// Update the latest snapshot.
    ///
    /// Should only be called by the observer manager.
    pub fn update(&self, deltas: HummockVersionDeltas) {
        let updated_change_log_table_ids: HashSet<_> = deltas
            .version_deltas
            .iter()
            .flat_map(|version_deltas| &version_deltas.change_log_delta)
            .filter_map(|(table_id, change_log)| match change_log.new_log.as_ref() {
                Some(new_log) => {
                    let new_value_empty = new_log.new_value.is_empty();
                    let old_value_empty = new_log.old_value.is_empty();
                    if !new_value_empty || !old_value_empty {
                        Some(*table_id)
                    } else {
                        None
                    }
                }
                None => None,
            })
            .collect();
        let deleted_table_ids: HashSet<_> = deltas
            .version_deltas
            .iter()
            .flat_map(|version_deltas| version_deltas.removed_table_ids.clone())
            .collect();
        self.table_change_log_notification_sender
            .send(TableChangeLogNotificationMsg {
                updated_change_log_table_ids,
                deleted_table_ids,
            })
            .ok();

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
            *old_snapshot = Arc::new(PinnedSnapshot { value: snapshot });

            true
        });
    }

    /// Wait until the latest snapshot is newer than the given one.
    pub async fn wait(&self, version_id: HummockVersionId) {
        let mut rx = self.latest_snapshot.subscribe();
        while rx.borrow_and_update().value.id < version_id {
            rx.changed().await.unwrap();
        }
    }

    pub async fn wait_table_change_log_notification(&self, table_id: u32) -> Result<(), RwError> {
        let mut rx = self.table_change_log_notification_sender.subscribe();
        loop {
            rx.changed()
                .await
                .map_err(|_| ErrorCode::InternalError("cursor notify channel is closed.".into()))?;
            let table_change_log_notification_msg = rx.borrow_and_update();
            if table_change_log_notification_msg
                .deleted_table_ids
                .contains(&table_id)
            {
                return Err(ErrorCode::InternalError(format!(
                    "Cursor dependent table deleted: table_id is {:?}",
                    table_id
                ))
                .into());
            }
            if table_change_log_notification_msg
                .updated_change_log_table_ids
                .contains(&table_id)
            {
                break;
            }
        }
        Ok(())
    }
}
