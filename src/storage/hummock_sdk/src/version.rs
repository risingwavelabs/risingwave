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

use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;

use prost::Message;
use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::hummock_version::Levels as PbLevels;
use risingwave_pb::hummock::hummock_version_delta::{ChangeLogDelta, GroupDeltas as PbGroupDeltas};
use risingwave_pb::hummock::{
    snapshot_group_delta, HummockVersion as PbHummockVersion,
    HummockVersionDelta as PbHummockVersionDelta, SnapshotGroup as PbSnapshotGroup,
    SnapshotGroupDelta as PbSnapshotGroupDelta, SstableInfo,
};

use crate::change_log::TableChangeLog;
use crate::table_watermark::TableWatermarks;
use crate::{CompactionGroupId, HummockSstableObjectId};

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotGroup {
    pub table_fragments_id: TableId,
    pub committed_epoch: u64,
    pub safe_epoch: u64,
    pub member_table_ids: HashSet<TableId>,
}

impl SnapshotGroup {
    pub fn to_protobuf(&self) -> PbSnapshotGroup {
        PbSnapshotGroup {
            table_fragments_id: self.table_fragments_id.into(),
            committed_epoch: self.committed_epoch,
            safe_epoch: self.safe_epoch,
            member_table_ids: self
                .member_table_ids
                .iter()
                .map(|table_id| table_id.table_id)
                .collect(),
        }
    }

    pub fn from_protobuf(group: &PbSnapshotGroup) -> Self {
        SnapshotGroup {
            table_fragments_id: TableId::new(group.table_fragments_id),
            committed_epoch: group.committed_epoch,
            safe_epoch: group.safe_epoch,
            member_table_ids: group
                .member_table_ids
                .iter()
                .map(|table_id| TableId::new(*table_id))
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HummockVersion {
    pub id: u64,
    pub levels: HashMap<CompactionGroupId, PbLevels>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
    pub table_watermarks: HashMap<TableId, Arc<TableWatermarks>>,
    pub table_change_log: HashMap<TableId, TableChangeLog>,
    pub snapshot_groups: HashMap<TableId, SnapshotGroup>,
}

impl Default for HummockVersion {
    fn default() -> Self {
        HummockVersion::from_protobuf_inner(&PbHummockVersion::default())
    }
}

impl HummockVersion {
    /// Convert the `PbHummockVersion` received from rpc to `HummockVersion`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(pb_version: &PbHummockVersion) -> Self {
        Self::from_protobuf_inner(pb_version)
    }

    /// Convert the `PbHummockVersion` deserialized from persisted state to `HummockVersion`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(pb_version: &PbHummockVersion) -> Self {
        Self::from_protobuf_inner(pb_version)
    }

    fn from_protobuf_inner(pb_version: &PbHummockVersion) -> Self {
        Self {
            id: pb_version.id,
            levels: pb_version
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as CompactionGroupId, levels.clone()))
                .collect(),
            max_committed_epoch: pb_version.max_committed_epoch,
            safe_epoch: pb_version.safe_epoch,
            table_watermarks: pb_version
                .table_watermarks
                .iter()
                .map(|(table_id, table_watermark)| {
                    (
                        TableId::new(*table_id),
                        Arc::new(TableWatermarks::from_protobuf(table_watermark)),
                    )
                })
                .collect(),
            table_change_log: pb_version
                .table_change_logs
                .iter()
                .map(|(table_id, change_log)| {
                    (
                        TableId::new(*table_id),
                        TableChangeLog::from_protobuf(change_log),
                    )
                })
                .collect(),
            snapshot_groups: pb_version
                .snapshot_groups
                .iter()
                .map(|(table_fragments_id, group)| {
                    (
                        TableId::new(*table_fragments_id),
                        SnapshotGroup::from_protobuf(group),
                    )
                })
                .collect(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersion {
        PbHummockVersion {
            id: self.id,
            levels: self
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as _, levels.clone()))
                .collect(),
            max_committed_epoch: self.max_committed_epoch,
            safe_epoch: self.safe_epoch,
            table_watermarks: self
                .table_watermarks
                .iter()
                .map(|(table_id, watermark)| (table_id.table_id, watermark.to_protobuf()))
                .collect(),
            table_change_logs: self
                .table_change_log
                .iter()
                .map(|(table_id, change_log)| (table_id.table_id, change_log.to_protobuf()))
                .collect(),
            snapshot_groups: self
                .snapshot_groups
                .iter()
                .map(|(group_id, group)| ((*group_id).into(), group.to_protobuf()))
                .collect(),
        }
    }

    pub fn estimated_encode_len(&self) -> usize {
        self.levels.len() * size_of::<CompactionGroupId>()
            + self
                .levels
                .values()
                .map(|level| level.encoded_len())
                .sum::<usize>()
            + self.table_watermarks.len() * size_of::<u32>()
            + self
                .table_watermarks
                .values()
                .map(|table_watermark| table_watermark.estimated_encode_len())
                .sum::<usize>()
    }

    fn need_fill_backward_compatibility_snapshot_group(&self) -> bool {
        let has_prev_table = self
            .levels
            .values()
            .any(|group| !group.member_table_ids.is_empty());
        has_prev_table && self.snapshot_groups.is_empty()
    }

    pub fn gen_fill_backward_compatibility_snapshot_group_delta(
        &self,
        existing_table_fragment_state_tables: &HashMap<u32, HashSet<u32>>,
    ) -> Option<HummockVersionDelta> {
        if existing_table_fragment_state_tables.is_empty()
            || !self.need_fill_backward_compatibility_snapshot_group()
        {
            return None;
        }
        let snapshot_group_delta = existing_table_fragment_state_tables
            .iter()
            .map(|(table_fragments_id, state_table_ids)| {
                (
                    TableId::new(*table_fragments_id),
                    SnapshotGroupDelta::NewSnapshotGroup {
                        member_table_ids: state_table_ids
                            .iter()
                            .map(|table_id| TableId::new(*table_id))
                            .collect(),
                        committed_epoch: self.max_committed_epoch,
                        safe_epoch: self.safe_epoch,
                    },
                )
            })
            .collect();
        let delta = HummockVersionDelta {
            id: self.id + 1,
            prev_id: self.id,
            max_committed_epoch: self.max_committed_epoch,
            safe_epoch: self.safe_epoch,
            snapshot_group_delta,
            ..Default::default()
        };
        Some(delta)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum SnapshotGroupDelta {
    NewSnapshotGroup {
        member_table_ids: HashSet<TableId>,
        committed_epoch: u64,
        safe_epoch: u64,
    },
    NewCommittedEpoch(u64),
    NewSafeEpoch(u64),
    Destroy,
}

impl SnapshotGroupDelta {
    pub fn to_protobuf(&self) -> PbSnapshotGroupDelta {
        let delta = match self {
            SnapshotGroupDelta::NewSnapshotGroup {
                member_table_ids,
                committed_epoch,
                safe_epoch,
            } => snapshot_group_delta::Delta::NewSnapshotGroup(
                snapshot_group_delta::NewSnapshotGroup {
                    member_table_ids: member_table_ids
                        .iter()
                        .map(|table_id| table_id.table_id)
                        .collect(),
                    committed_epoch: *committed_epoch,
                    safe_epoch: *safe_epoch,
                },
            ),
            SnapshotGroupDelta::NewCommittedEpoch(epoch) => {
                snapshot_group_delta::Delta::NewCommittedEpoch(*epoch)
            }
            SnapshotGroupDelta::NewSafeEpoch(epoch) => {
                snapshot_group_delta::Delta::NewSafeEpoch(*epoch)
            }
            SnapshotGroupDelta::Destroy => {
                snapshot_group_delta::Delta::Destroy(snapshot_group_delta::DestroySnapshotGroup {})
            }
        };
        PbSnapshotGroupDelta { delta: Some(delta) }
    }

    pub fn from_protobuf(delta: &PbSnapshotGroupDelta) -> Self {
        match delta.delta.as_ref().unwrap() {
            snapshot_group_delta::Delta::NewSnapshotGroup(group) => Self::NewSnapshotGroup {
                member_table_ids: HashSet::from_iter(
                    group.member_table_ids.iter().cloned().map(TableId::new),
                ),
                committed_epoch: group.committed_epoch,
                safe_epoch: group.safe_epoch,
            },
            snapshot_group_delta::Delta::NewCommittedEpoch(epoch) => {
                Self::NewCommittedEpoch(*epoch)
            }
            snapshot_group_delta::Delta::NewSafeEpoch(epoch) => Self::NewSafeEpoch(*epoch),
            snapshot_group_delta::Delta::Destroy(_) => Self::Destroy,
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct HummockVersionDelta {
    pub id: u64,
    pub prev_id: u64,
    pub group_deltas: HashMap<CompactionGroupId, PbGroupDeltas>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
    pub trivial_move: bool,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub removed_table_ids: Vec<TableId>,
    pub change_log_delta: HashMap<TableId, ChangeLogDelta>,
    pub snapshot_group_delta: HashMap<TableId, SnapshotGroupDelta>,
}

impl Default for HummockVersionDelta {
    fn default() -> Self {
        HummockVersionDelta::from_protobuf_inner(&PbHummockVersionDelta::default())
    }
}

impl HummockVersionDelta {
    /// Convert the `PbHummockVersionDelta` deserialized from persisted state to `HummockVersionDelta`.
    /// We should maintain backward compatibility.
    pub fn from_persisted_protobuf(delta: &PbHummockVersionDelta) -> Self {
        Self::from_protobuf_inner(delta)
    }

    /// Convert the `PbHummockVersionDelta` received from rpc to `HummockVersionDelta`. No need to
    /// maintain backward compatibility.
    pub fn from_rpc_protobuf(delta: &PbHummockVersionDelta) -> Self {
        Self::from_protobuf_inner(delta)
    }

    fn from_protobuf_inner(delta: &PbHummockVersionDelta) -> Self {
        Self {
            id: delta.id,
            prev_id: delta.prev_id,
            group_deltas: delta.group_deltas.clone(),
            max_committed_epoch: delta.max_committed_epoch,
            safe_epoch: delta.safe_epoch,
            trivial_move: delta.trivial_move,
            new_table_watermarks: delta
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| {
                    (
                        TableId::new(*table_id),
                        TableWatermarks::from_protobuf(watermarks),
                    )
                })
                .collect(),
            removed_table_ids: delta
                .removed_table_ids
                .iter()
                .map(|table_id| TableId::new(*table_id))
                .collect(),
            change_log_delta: delta
                .change_log_delta
                .iter()
                .map(|(table_id, log_delta)| {
                    (
                        TableId::new(*table_id),
                        ChangeLogDelta {
                            new_log: log_delta.new_log.clone(),
                            truncate_epoch: log_delta.truncate_epoch,
                        },
                    )
                })
                .collect(),
            snapshot_group_delta: delta
                .snapshot_group_delta
                .iter()
                .map(|(table_fragments_id, delta)| {
                    (
                        TableId::new(*table_fragments_id),
                        SnapshotGroupDelta::from_protobuf(delta),
                    )
                })
                .collect(),
        }
    }

    pub fn to_protobuf(&self) -> PbHummockVersionDelta {
        PbHummockVersionDelta {
            id: self.id,
            prev_id: self.prev_id,
            group_deltas: self.group_deltas.clone(),
            max_committed_epoch: self.max_committed_epoch,
            safe_epoch: self.safe_epoch,
            trivial_move: self.trivial_move,
            new_table_watermarks: self
                .new_table_watermarks
                .iter()
                .map(|(table_id, watermarks)| (table_id.table_id, watermarks.to_protobuf()))
                .collect(),
            removed_table_ids: self
                .removed_table_ids
                .iter()
                .map(|table_id| table_id.table_id)
                .collect(),
            change_log_delta: self
                .change_log_delta
                .iter()
                .map(|(table_id, log_delta)| (table_id.table_id, log_delta.clone()))
                .collect(),
            snapshot_group_delta: self
                .snapshot_group_delta
                .iter()
                .map(|(group_id, delta)| ((*group_id).into(), delta.to_protobuf()))
                .collect(),
        }
    }
}

impl HummockVersionDelta {
    /// Get the newly added object ids from the version delta.
    ///
    /// Note: the result can be false positive because we only collect the set of sst object ids in the `inserted_table_infos`,
    /// but it is possible that the object is moved or split from other compaction groups or levels.
    pub fn newly_added_object_ids(&self) -> HashSet<HummockSstableObjectId> {
        self.group_deltas
            .values()
            .flat_map(|group_deltas| {
                group_deltas.group_deltas.iter().flat_map(|group_delta| {
                    group_delta.delta_type.iter().flat_map(|delta_type| {
                        static EMPTY_VEC: Vec<SstableInfo> = Vec::new();
                        let sst_slice = match delta_type {
                            DeltaType::IntraLevel(level_delta) => &level_delta.inserted_table_infos,
                            DeltaType::GroupConstruct(_)
                            | DeltaType::GroupDestroy(_)
                            | DeltaType::GroupMetaChange(_)
                            | DeltaType::GroupTableChange(_) => &EMPTY_VEC,
                        };
                        sst_slice.iter().map(|sst| sst.object_id)
                    })
                })
            })
            .chain(self.change_log_delta.values().flat_map(|delta| {
                let new_log = delta.new_log.as_ref().unwrap();
                new_log
                    .new_value
                    .iter()
                    .map(|sst| sst.object_id)
                    .chain(new_log.old_value.iter().map(|sst| sst.object_id))
            }))
            .collect()
    }
}
