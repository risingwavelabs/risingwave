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

use risingwave_pb::hummock::hummock_version::PbLevels;
use risingwave_pb::hummock::hummock_version_delta::{PbChangeLogDelta, PbGroupDeltas};
use risingwave_pb::hummock::{PbEpochNewChangeLog, PbSstableInfo};

use crate::change_log::{TableChangeLog, TableChangeLogCommon};
use crate::level::Level;
use crate::sstable_info::SstableInfo;
use crate::version::{
    HummockVersion, HummockVersionCommon, HummockVersionDelta, HummockVersionDeltaCommon,
    ObjectIdReader, SstableIdReader,
};
use crate::{CompactionGroupId, HummockSstableId, HummockSstableObjectId};

pub type IncompleteHummockVersion = HummockVersionCommon<SstableIdInVersion>;

/// Populates `SstableInfo` for `table_id`.
/// `SstableInfo` not associated with `table_id` is removed.
pub fn refill_version(
    version: &mut HummockVersion,
    sst_id_to_info: &HashMap<HummockSstableId, SstableInfo>,
    table_id: u32,
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
        level
            .table_infos
            .retain(|t| t.table_ids.contains(&table_id));
    }
    version
        .table_change_log
        .retain(|t, _| t.table_id == table_id);
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
    for c in &mut table_change_log.0 {
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

/// `SStableInfo` will be stripped.
impl From<(&HummockVersion, &HashSet<CompactionGroupId>)> for IncompleteHummockVersion {
    fn from(p: (&HummockVersion, &HashSet<CompactionGroupId>)) -> Self {
        let (version, select_group) = p;
        #[expect(deprecated)]
        Self {
            id: version.id,
            levels: version
                .levels
                .iter()
                .filter_map(|(group_id, levels)| {
                    if select_group.contains(group_id) {
                        Some((
                            *group_id as CompactionGroupId,
                            PbLevels::from(levels).into(),
                        ))
                    } else {
                        None
                    }
                })
                .collect(),
            max_committed_epoch: version.max_committed_epoch,
            table_watermarks: version.table_watermarks.clone(),
            // TODO: optimization: strip table change log based on select_group
            table_change_log: version
                .table_change_log
                .iter()
                .map(|(table_id, change_log)| {
                    let incomplete_table_change_log = change_log
                        .0
                        .iter()
                        .map(|e| PbEpochNewChangeLog::from(e).into())
                        .collect();
                    (*table_id, TableChangeLogCommon(incomplete_table_change_log))
                })
                .collect(),
            state_table_info: version.state_table_info.clone(),
        }
    }
}

/// [`IncompleteHummockVersionDelta`] is incomplete because `SSTableInfo` only has the `sst_id` set in the following fields:
/// - `PbGroupDeltas`
/// - `ChangeLogDelta`
pub type IncompleteHummockVersionDelta = HummockVersionDeltaCommon<SstableIdInVersion>;

/// `SStableInfo` will be stripped.
impl From<(&HummockVersionDelta, &HashSet<CompactionGroupId>)> for IncompleteHummockVersionDelta {
    fn from(p: (&HummockVersionDelta, &HashSet<CompactionGroupId>)) -> Self {
        let (delta, select_group) = p;
        #[expect(deprecated)]
        Self {
            id: delta.id,
            prev_id: delta.prev_id,
            group_deltas: delta
                .group_deltas
                .iter()
                .filter_map(|(cg_id, deltas)| {
                    if select_group.contains(cg_id) {
                        Some((*cg_id, PbGroupDeltas::from(deltas).into()))
                    } else {
                        None
                    }
                })
                .collect(),
            max_committed_epoch: delta.max_committed_epoch,
            trivial_move: delta.trivial_move,
            new_table_watermarks: delta.new_table_watermarks.clone(),
            removed_table_ids: delta.removed_table_ids.clone(),
            // TODO: optimization: strip table change log based on select_group
            change_log_delta: delta
                .change_log_delta
                .iter()
                .map(|(table_id, log_delta)| (*table_id, PbChangeLogDelta::from(log_delta).into()))
                .collect(),
            state_table_info_delta: delta.state_table_info_delta.clone(),
        }
    }
}

pub struct SstableIdInVersion {
    sst_id: HummockSstableId,
    object_id: HummockSstableObjectId,
}

impl SstableIdReader for SstableIdInVersion {
    fn sst_id(&self) -> HummockSstableId {
        self.sst_id
    }
}

impl ObjectIdReader for SstableIdInVersion {
    fn object_id(&self) -> HummockSstableObjectId {
        self.object_id
    }
}

impl From<&SstableIdInVersion> for PbSstableInfo {
    fn from(sst_id: &SstableIdInVersion) -> Self {
        Self {
            sst_id: sst_id.sst_id,
            object_id: sst_id.object_id,
            ..Default::default()
        }
    }
}

impl From<SstableIdInVersion> for PbSstableInfo {
    fn from(sst_id: SstableIdInVersion) -> Self {
        (&sst_id).into()
    }
}

impl From<&PbSstableInfo> for SstableIdInVersion {
    fn from(s: &PbSstableInfo) -> Self {
        SstableIdInVersion {
            sst_id: s.sst_id,
            object_id: s.object_id,
        }
    }
}

impl From<PbSstableInfo> for SstableIdInVersion {
    fn from(value: PbSstableInfo) -> Self {
        (&value).into()
    }
}
