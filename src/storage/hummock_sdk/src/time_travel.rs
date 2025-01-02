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
use std::sync::Arc;

use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::hummock_version_delta::PbGroupDeltas;
use risingwave_pb::hummock::{PbHummockVersion, PbHummockVersionDelta, PbStateTableInfoDelta};

use crate::change_log::{ChangeLogDelta, EpochNewChangeLog, TableChangeLog};
use crate::level::{Level, Levels, OverlappingLevel};
use crate::sstable_info::{SstableInfo, SstableInfoInner};
use crate::table_watermark::TableWatermarks;
use crate::version::{
    GroupDelta, GroupDeltas, HummockVersion, HummockVersionDelta, HummockVersionStateTableInfo,
    IntraLevelDelta,
};
use crate::{CompactionGroupId, HummockSstableId, HummockVersionId};

/// [`IncompleteHummockVersion`] is incomplete because `SSTableInfo` only has the `sst_id` set in the following fields:
/// - `PbLevels`
/// - `TableChangeLog`
#[derive(Debug, Clone, PartialEq)]
pub struct IncompleteHummockVersion {
    pub id: HummockVersionId,
    pub levels: HashMap<CompactionGroupId, Levels>,
    max_committed_epoch: u64,
    safe_epoch: u64,
    pub table_watermarks: HashMap<TableId, Arc<TableWatermarks>>,
    pub table_change_log: HashMap<TableId, TableChangeLog>,
    pub state_table_info: HummockVersionStateTableInfo,
}

/// Clone from an `SstableInfo`, but only set the `sst_id` for the target, leaving other fields as default.
/// The goal is to reduce the size of pb object generated afterward.
fn stripped_sstable_info(origin: &SstableInfo) -> SstableInfo {
    SstableInfoInner {
        object_id: Default::default(),
        sst_id: origin.sst_id,
        key_range: Default::default(),
        file_size: Default::default(),
        table_ids: Default::default(),
        meta_offset: Default::default(),
        stale_key_count: Default::default(),
        total_key_count: Default::default(),
        min_epoch: Default::default(),
        max_epoch: Default::default(),
        uncompressed_file_size: Default::default(),
        range_tombstone_count: Default::default(),
        bloom_filter_kind: Default::default(),
    }
    .into()
}

fn stripped_epoch_new_change_log(origin: &EpochNewChangeLog) -> EpochNewChangeLog {
    EpochNewChangeLog {
        old_value: origin.old_value.iter().map(stripped_sstable_info).collect(),
        new_value: origin.new_value.iter().map(stripped_sstable_info).collect(),
        epochs: origin.epochs.clone(),
    }
}

fn stripped_change_log_delta(origin: &ChangeLogDelta) -> ChangeLogDelta {
    ChangeLogDelta {
        new_log: origin.new_log.as_ref().map(stripped_epoch_new_change_log),
        truncate_epoch: origin.truncate_epoch,
    }
}

fn stripped_level(origin: &Level) -> Level {
    Level {
        level_idx: origin.level_idx,
        level_type: origin.level_type,
        table_infos: origin
            .table_infos
            .iter()
            .map(stripped_sstable_info)
            .collect(),
        total_file_size: origin.total_file_size,
        sub_level_id: origin.sub_level_id,
        uncompressed_file_size: origin.uncompressed_file_size,
        vnode_partition_count: origin.vnode_partition_count,
    }
}

pub fn refill_version(
    version: &mut HummockVersion,
    sst_id_to_info: &HashMap<HummockSstableId, SstableInfo>,
) {
    for level in version.levels.values_mut().flat_map(|level| {
        level
            .l0
            .sub_levels
            .iter_mut()
            .rev()
            .chain(level.levels.iter_mut())
    }) {
        refill_level(level, sst_id_to_info);
    }

    for t in version.table_change_log.values_mut() {
        refill_table_change_log(t, sst_id_to_info);
    }
}

fn refill_level(level: &mut Level, sst_id_to_info: &HashMap<HummockSstableId, SstableInfo>) {
    for s in &mut level.table_infos {
        refill_sstable_info(s, sst_id_to_info);
    }
}

fn refill_table_change_log(
    table_change_log: &mut TableChangeLog,
    sst_id_to_info: &HashMap<HummockSstableId, SstableInfo>,
) {
    for c in table_change_log.iter_mut() {
        for s in &mut c.old_value {
            refill_sstable_info(s, sst_id_to_info);
        }
        for s in &mut c.new_value {
            refill_sstable_info(s, sst_id_to_info);
        }
    }
}

/// Caller should ensure `sst_id_to_info` includes an entry corresponding to `sstable_info`.
fn refill_sstable_info(
    sstable_info: &mut SstableInfo,
    sst_id_to_info: &HashMap<HummockSstableId, SstableInfo>,
) {
    *sstable_info = sst_id_to_info
        .get(&sstable_info.sst_id)
        .unwrap_or_else(|| panic!("SstableInfo should exist"))
        .clone();
}

fn stripped_l0(origin: &OverlappingLevel) -> OverlappingLevel {
    OverlappingLevel {
        sub_levels: origin.sub_levels.iter().map(stripped_level).collect(),
        total_file_size: origin.total_file_size,
        uncompressed_file_size: origin.uncompressed_file_size,
    }
}

#[allow(deprecated)]
fn stripped_levels(origin: &Levels) -> Levels {
    Levels {
        levels: origin.levels.iter().map(stripped_level).collect(),
        l0: stripped_l0(&origin.l0),
        group_id: origin.group_id,
        parent_group_id: origin.parent_group_id,
        member_table_ids: Default::default(),
    }
}

fn stripped_intra_level_delta(origin: &IntraLevelDelta) -> IntraLevelDelta {
    IntraLevelDelta {
        level_idx: origin.level_idx,
        l0_sub_level_id: origin.l0_sub_level_id,
        removed_table_ids: origin.removed_table_ids.clone(),
        inserted_table_infos: origin
            .inserted_table_infos
            .iter()
            .map(stripped_sstable_info)
            .collect(),
        vnode_partition_count: origin.vnode_partition_count,
    }
}

fn stripped_group_delta(origin: &GroupDelta) -> GroupDelta {
    match origin {
        GroupDelta::IntraLevel(l) => GroupDelta::IntraLevel(stripped_intra_level_delta(l)),
        _ => panic!("time travel expects DeltaType::IntraLevel only"),
    }
}

fn stripped_group_deltas(origin: &GroupDeltas) -> GroupDeltas {
    let group_deltas = origin
        .group_deltas
        .iter()
        .map(stripped_group_delta)
        .collect();
    GroupDeltas { group_deltas }
}

/// `SStableInfo` will be stripped.
impl From<(&HummockVersion, &HashSet<CompactionGroupId>)> for IncompleteHummockVersion {
    fn from(p: (&HummockVersion, &HashSet<CompactionGroupId>)) -> Self {
        let (version, select_group) = p;
        Self {
            id: version.id,
            levels: version
                .levels
                .iter()
                .filter_map(|(group_id, levels)| {
                    if select_group.contains(group_id) {
                        Some((*group_id as CompactionGroupId, stripped_levels(levels)))
                    } else {
                        None
                    }
                })
                .collect(),
            max_committed_epoch: version.visible_table_committed_epoch(),
            safe_epoch: version.visible_table_safe_epoch(),
            table_watermarks: version.table_watermarks.clone(),
            // TODO: optimization: strip table change log
            table_change_log: version
                .table_change_log
                .iter()
                .map(|(table_id, change_log)| {
                    let incomplete_table_change_log = change_log
                        .iter()
                        .map(stripped_epoch_new_change_log)
                        ;
                    (*table_id, TableChangeLog::new(incomplete_table_change_log))
                })
                .collect(),
            state_table_info: version.state_table_info.clone(),
        }
    }
}

impl IncompleteHummockVersion {
    /// Resulted `SStableInfo` is incompelte.
    pub fn to_protobuf(&self) -> PbHummockVersion {
        PbHummockVersion {
            id: self.id.0,
            levels: self
                .levels
                .iter()
                .map(|(group_id, levels)| (*group_id as _, levels.to_protobuf()))
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
            state_table_info: self.state_table_info.to_protobuf(),
        }
    }
}

/// [`IncompleteHummockVersionDelta`] is incomplete because `SSTableInfo` only has the `sst_id` set in the following fields:
/// - `PbGroupDeltas`
/// - `ChangeLogDelta`
#[derive(Debug, PartialEq, Clone)]
pub struct IncompleteHummockVersionDelta {
    pub id: HummockVersionId,
    pub prev_id: HummockVersionId,
    pub group_deltas: HashMap<CompactionGroupId, PbGroupDeltas>,
    pub max_committed_epoch: u64,
    pub safe_epoch: u64,
    pub trivial_move: bool,
    pub new_table_watermarks: HashMap<TableId, TableWatermarks>,
    pub removed_table_ids: HashSet<TableId>,
    pub change_log_delta: HashMap<TableId, ChangeLogDelta>,
    pub state_table_info_delta: HashMap<TableId, PbStateTableInfoDelta>,
}

/// `SStableInfo` will be stripped.
impl From<(&HummockVersionDelta, &HashSet<CompactionGroupId>)> for IncompleteHummockVersionDelta {
    fn from(p: (&HummockVersionDelta, &HashSet<CompactionGroupId>)) -> Self {
        let (delta, select_group) = p;
        Self {
            id: delta.id,
            prev_id: delta.prev_id,
            group_deltas: delta
                .group_deltas
                .iter()
                .filter_map(|(cg_id, deltas)| {
                    if select_group.contains(cg_id) {
                        Some((*cg_id, stripped_group_deltas(deltas).to_protobuf()))
                    } else {
                        None
                    }
                })
                .collect(),
            max_committed_epoch: delta.visible_table_committed_epoch(),
            safe_epoch: delta.visible_table_safe_epoch(),
            trivial_move: delta.trivial_move,
            new_table_watermarks: delta.new_table_watermarks.clone(),
            removed_table_ids: delta.removed_table_ids.clone(),
            // TODO: optimization: strip table change log
            change_log_delta: delta
                .change_log_delta
                .iter()
                .map(|(table_id, log_delta)| (*table_id, stripped_change_log_delta(log_delta)))
                .collect(),
            state_table_info_delta: delta.state_table_info_delta.clone(),
        }
    }
}

impl IncompleteHummockVersionDelta {
    /// Resulted `SStableInfo` is incompelte.
    pub fn to_protobuf(&self) -> PbHummockVersionDelta {
        PbHummockVersionDelta {
            id: self.id.0,
            prev_id: self.prev_id.0,
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
                .map(|(table_id, log_delta)| (table_id.table_id, log_delta.into()))
                .collect(),
            state_table_info_delta: self
                .state_table_info_delta
                .iter()
                .map(|(table_id, delta)| (table_id.table_id, *delta))
                .collect(),
        }
    }
}
