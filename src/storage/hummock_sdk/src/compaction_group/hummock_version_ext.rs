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

use std::collections::{BTreeMap, HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::hummock_version_delta::GroupDeltas;
use risingwave_pb::hummock::{
    CompactionConfig, GroupConstruct, GroupDestroy, GroupMetaChange, HummockVersion,
    HummockVersionDelta, Level, LevelType, OverlappingLevel, SstableInfo,
};

use super::StateTableId;
use crate::compaction_group::StaticCompactionGroupId;
use crate::prost_key_range::KeyRangeExt;
use crate::{can_concat, CompactionGroupId, HummockSstableId};

pub struct GroupDeltasSummary {
    pub delete_sst_levels: Vec<u32>,
    pub delete_sst_ids_set: HashSet<u64>,
    pub insert_sst_level_id: u32,
    pub insert_sub_level_id: u64,
    pub insert_table_infos: Vec<SstableInfo>,
    pub group_construct: Option<GroupConstruct>,
    pub group_destroy: Option<GroupDestroy>,
    pub group_meta_changes: Vec<GroupMetaChange>,
}

pub fn summarize_group_deltas(group_deltas: &GroupDeltas) -> GroupDeltasSummary {
    let mut delete_sst_levels = Vec::with_capacity(group_deltas.group_deltas.len());
    let mut delete_sst_ids_set = HashSet::new();
    let mut insert_sst_level_id = u32::MAX;
    let mut insert_sub_level_id = u64::MAX;
    let mut insert_table_infos = vec![];
    let mut group_construct = None;
    let mut group_destroy = None;
    let mut group_meta_changes = vec![];
    for group_delta in &group_deltas.group_deltas {
        match group_delta.get_delta_type().unwrap() {
            DeltaType::IntraLevel(intra_level) => {
                if !intra_level.removed_table_ids.is_empty() {
                    delete_sst_levels.push(intra_level.level_idx);
                    delete_sst_ids_set.extend(intra_level.removed_table_ids.iter().clone());
                }
                if !intra_level.inserted_table_infos.is_empty() {
                    insert_sst_level_id = intra_level.level_idx;
                    insert_sub_level_id = intra_level.l0_sub_level_id;
                    insert_table_infos.extend(intra_level.inserted_table_infos.iter().cloned());
                }
            }
            DeltaType::GroupConstruct(construct_delta) => {
                assert!(group_construct.is_none());
                group_construct = Some(construct_delta.clone());
            }
            DeltaType::GroupDestroy(destroy_delta) => {
                assert!(group_destroy.is_none());
                group_destroy = Some(destroy_delta.clone());
            }
            DeltaType::GroupMetaChange(meta_delta) => {
                group_meta_changes.push(meta_delta.clone());
            }
        }
    }

    GroupDeltasSummary {
        delete_sst_levels,
        delete_sst_ids_set,
        insert_sst_level_id,
        insert_sub_level_id,
        insert_table_infos,
        group_construct,
        group_destroy,
        group_meta_changes,
    }
}

pub trait HummockVersionExt {
    /// Gets `compaction_group_id`'s levels
    fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Levels;
    /// Gets `compaction_group_id`'s levels
    fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Levels;
    /// Gets all levels.
    ///
    /// Levels belonging to the same compaction group retain their relative order.
    fn get_combined_levels(&self) -> Vec<&Level>;
    fn num_levels(&self, compaction_group_id: CompactionGroupId) -> usize;
    fn level_iter<F: FnMut(&Level) -> bool>(&self, compaction_group_id: CompactionGroupId, f: F);

    fn get_sst_ids(&self) -> Vec<u64>;
}

pub trait HummockVersionUpdateExt {
    fn init_with_parent_group(
        &mut self,
        parent_group_id: CompactionGroupId,
        group_id: CompactionGroupId,
        member_table_ids: &HashSet<StateTableId>,
    ) -> Vec<(HummockSstableId, u64, u32)>;
    fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta) -> Vec<SstSplitInfo>;

    fn build_compaction_group_info(&self) -> HashMap<TableId, CompactionGroupId>;
    fn build_branched_sst_info(
        &self,
    ) -> BTreeMap<HummockSstableId, HashMap<CompactionGroupId, u64>>;
}

impl HummockVersionExt for HummockVersion {
    fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Levels {
        self.levels
            .get(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} does not exist", compaction_group_id))
    }

    fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Levels {
        self.levels
            .get_mut(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} does not exist", compaction_group_id))
    }

    fn get_combined_levels(&self) -> Vec<&Level> {
        let mut combined_levels = vec![];
        for level in self.levels.values() {
            combined_levels.extend(level.l0.as_ref().unwrap().sub_levels.iter().rev());
            combined_levels.extend(level.levels.iter());
        }
        combined_levels
    }

    fn get_sst_ids(&self) -> Vec<u64> {
        self.get_combined_levels()
            .iter()
            .flat_map(|level| level.table_infos.iter().map(|table_info| table_info.id))
            .collect_vec()
    }

    fn level_iter<F: FnMut(&Level) -> bool>(
        &self,
        compaction_group_id: CompactionGroupId,
        mut f: F,
    ) {
        if let Some(levels) = self.levels.get(&compaction_group_id) {
            for sub_level in &levels.l0.as_ref().unwrap().sub_levels {
                if !f(sub_level) {
                    return;
                }
            }
            for level in &levels.levels {
                if !f(level) {
                    return;
                }
            }
        }
    }

    fn num_levels(&self, compaction_group_id: CompactionGroupId) -> usize {
        // l0 is currently separated from all levels
        self.levels
            .get(&compaction_group_id)
            .map(|group| group.levels.len() + 1)
            .unwrap_or(0)
    }
}

pub type SstSplitInfo = (
    HummockSstableId,
    // divide version
    u64,
    // level idx
    u32,
);

impl HummockVersionUpdateExt for HummockVersion {
    fn init_with_parent_group(
        &mut self,
        parent_group_id: CompactionGroupId,
        group_id: CompactionGroupId,
        member_table_ids: &HashSet<StateTableId>,
    ) -> Vec<SstSplitInfo> {
        let mut split_id_vers = vec![];
        if parent_group_id == StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId
            || !self.levels.contains_key(&parent_group_id)
        {
            return split_id_vers;
        }
        let [parent_levels, cur_levels] = self
            .levels
            .get_many_mut([&parent_group_id, &group_id])
            .unwrap();
        if let Some(ref mut l0) = parent_levels.l0 {
            for sub_level in &mut l0.sub_levels {
                let mut insert_table_infos = vec![];
                for sst_info in &mut sub_level.table_infos {
                    if sst_info
                        .get_table_ids()
                        .iter()
                        .any(|table_id| member_table_ids.contains(table_id))
                    {
                        sst_info.divide_version += 1;
                        split_id_vers.push((sst_info.get_id(), sst_info.get_divide_version(), 0));
                        let mut branch_table_info = sst_info.clone();
                        branch_table_info.table_ids = sst_info
                            .table_ids
                            .drain_filter(|table_id| member_table_ids.contains(table_id))
                            .collect_vec();
                        insert_table_infos.push(branch_table_info);
                    }
                }
                add_new_sub_level(
                    cur_levels.l0.as_mut().unwrap(),
                    sub_level.get_sub_level_id(),
                    sub_level.level_type(),
                    insert_table_infos,
                );
            }
        }
        for (z, level) in parent_levels.levels.iter_mut().enumerate() {
            let level_idx = level.get_level_idx();
            for sst_info in &mut level.table_infos {
                if sst_info
                    .get_table_ids()
                    .iter()
                    .any(|table_id| member_table_ids.contains(table_id))
                {
                    sst_info.divide_version += 1;
                    split_id_vers.push((
                        sst_info.get_id(),
                        sst_info.get_divide_version(),
                        level_idx,
                    ));
                    let mut branch_table_info = sst_info.clone();
                    branch_table_info.table_ids = sst_info
                        .table_ids
                        .drain_filter(|table_id| member_table_ids.contains(table_id))
                        .collect_vec();
                    cur_levels.levels[z].total_file_size += branch_table_info.file_size;
                    cur_levels.levels[z].uncompressed_file_size +=
                        branch_table_info.uncompressed_file_size;
                    cur_levels.levels[z].table_infos.push(branch_table_info);
                }
            }
        }
        split_id_vers
    }

    fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta) -> Vec<SstSplitInfo> {
        let mut sst_split_info = vec![];
        for (compaction_group_id, group_deltas) in &version_delta.group_deltas {
            let summary = summarize_group_deltas(group_deltas);
            if let Some(group_construct) = &summary.group_construct {
                let mut new_levels = build_initial_compaction_group_levels(
                    *compaction_group_id,
                    group_construct.get_group_config().unwrap(),
                );
                let parent_group_id = group_construct.parent_group_id;
                new_levels.parent_group_id = parent_group_id;
                new_levels.member_table_ids = group_construct.table_ids.clone();
                self.levels.insert(*compaction_group_id, new_levels);
                sst_split_info.extend(self.init_with_parent_group(
                    parent_group_id,
                    *compaction_group_id,
                    &HashSet::from_iter(group_construct.get_table_ids().iter().cloned()),
                ));
            }
            let has_destroy = summary.group_destroy.is_some();
            let levels = self
                .levels
                .get_mut(compaction_group_id)
                .expect("compaction group should exist");

            for group_meta_delta in &summary.group_meta_changes {
                levels
                    .member_table_ids
                    .extend(group_meta_delta.table_ids_add.clone());
                levels
                    .member_table_ids
                    .drain_filter(|t| group_meta_delta.table_ids_remove.contains(t));
            }

            assert!(
                self.max_committed_epoch <= version_delta.max_committed_epoch,
                "new max commit epoch {} is older than the current max commit epoch {}",
                version_delta.max_committed_epoch,
                self.max_committed_epoch
            );
            if self.max_committed_epoch < version_delta.max_committed_epoch {
                // `max_committed_epoch` increases. It must be a `commit_epoch`
                let GroupDeltasSummary {
                    delete_sst_levels,
                    delete_sst_ids_set,
                    insert_sst_level_id,
                    insert_sub_level_id,
                    insert_table_infos,
                    ..
                } = summary;
                assert!(
                    insert_sst_level_id == 0 || insert_table_infos.is_empty(),
                    "we should only add to L0 when we commit an epoch. Inserting into {} {:?}",
                    insert_sst_level_id,
                    insert_table_infos
                );
                assert!(
                    delete_sst_levels.is_empty() && delete_sst_ids_set.is_empty() || has_destroy,
                    "no sst should be deleted when committing an epoch"
                );
                add_new_sub_level(
                    levels.l0.as_mut().unwrap(),
                    insert_sub_level_id,
                    LevelType::Overlapping,
                    insert_table_infos,
                );
            } else {
                // `max_committed_epoch` is not changed. The delta is caused by compaction.
                levels.apply_compact_ssts(summary);
            }
            if has_destroy {
                self.levels.remove(compaction_group_id);
            }
        }
        self.id = version_delta.id;
        self.max_committed_epoch = version_delta.max_committed_epoch;
        self.safe_epoch = version_delta.safe_epoch;
        sst_split_info
    }

    fn build_compaction_group_info(&self) -> HashMap<TableId, CompactionGroupId> {
        let mut ret = HashMap::new();
        for (compaction_group_id, levels) in &self.levels {
            for table_id in &levels.member_table_ids {
                ret.insert(TableId::new(*table_id), *compaction_group_id);
            }
        }
        ret
    }

    fn build_branched_sst_info(
        &self,
    ) -> BTreeMap<HummockSstableId, HashMap<CompactionGroupId, u64>> {
        let mut ret: BTreeMap<_, HashMap<_, _>> = BTreeMap::new();
        for compaction_group_id in self.get_levels().keys() {
            self.level_iter(*compaction_group_id, |level| {
                for table_info in level.get_table_infos() {
                    let sst_id = table_info.get_id();
                    ret.entry(sst_id)
                        .or_default()
                        .insert(*compaction_group_id, table_info.get_divide_version());
                }
                true
            });
        }
        ret.retain(|_, v| v.len() != 1 || *v.values().next().unwrap() != 0);
        ret
    }
}

pub trait HummockLevelsExt {
    fn get_level0(&self) -> &OverlappingLevel;
    fn get_level(&self, idx: usize) -> &Level;
    fn get_level_mut(&mut self, idx: usize) -> &mut Level;
    fn apply_compact_ssts(&mut self, summary: GroupDeltasSummary);
}

impl HummockLevelsExt for Levels {
    fn get_level0(&self) -> &OverlappingLevel {
        self.l0.as_ref().unwrap()
    }

    fn get_level(&self, level_idx: usize) -> &Level {
        &self.levels[level_idx - 1]
    }

    fn get_level_mut(&mut self, level_idx: usize) -> &mut Level {
        &mut self.levels[level_idx - 1]
    }

    fn apply_compact_ssts(&mut self, summary: GroupDeltasSummary) {
        let GroupDeltasSummary {
            delete_sst_levels,
            delete_sst_ids_set,
            insert_sst_level_id,
            insert_sub_level_id,
            insert_table_infos,
            ..
        } = summary;
        let mut deleted = false;
        for level_idx in &delete_sst_levels {
            if *level_idx == 0 {
                for level in &mut self.l0.as_mut().unwrap().sub_levels {
                    deleted = level_delete_ssts(level, &delete_sst_ids_set) || deleted;
                }
            } else {
                let idx = *level_idx as usize - 1;
                deleted = level_delete_ssts(&mut self.levels[idx], &delete_sst_ids_set) || deleted;
            }
        }
        if !insert_table_infos.is_empty() {
            if insert_sst_level_id == 0 {
                let l0 = self.l0.as_mut().unwrap();
                let index = l0
                    .sub_levels
                    .partition_point(|level| level.sub_level_id < insert_sub_level_id);
                assert!(
                    index < l0.sub_levels.len() && l0.sub_levels[index].sub_level_id == insert_sub_level_id,
                    "should find the level to insert into when applying compaction generated delta. sub level idx: {}, sub level count: {}",
                    insert_sub_level_id, l0.sub_levels.len()
                );
                level_insert_ssts(&mut l0.sub_levels[index], insert_table_infos);
            } else {
                let idx = insert_sst_level_id as usize - 1;
                level_insert_ssts(&mut self.levels[idx], insert_table_infos);
            }
        }
        if delete_sst_levels.iter().any(|level_id| *level_id == 0) {
            self.l0
                .as_mut()
                .unwrap()
                .sub_levels
                .retain(|level| !level.table_infos.is_empty());
            self.l0.as_mut().unwrap().total_file_size = self
                .l0
                .as_mut()
                .unwrap()
                .sub_levels
                .iter()
                .map(|level| level.total_file_size)
                .sum::<u64>();
            self.l0.as_mut().unwrap().uncompressed_file_size = self
                .l0
                .as_mut()
                .unwrap()
                .sub_levels
                .iter()
                .map(|level| level.uncompressed_file_size)
                .sum::<u64>();
        }
    }
}

pub fn build_initial_compaction_group_levels(
    group_id: CompactionGroupId,
    compaction_config: &CompactionConfig,
) -> Levels {
    let mut levels = vec![];
    for l in 0..compaction_config.get_max_level() {
        levels.push(Level {
            level_idx: (l + 1) as u32,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: vec![],
            total_file_size: 0,
            sub_level_id: 0,
            uncompressed_file_size: 0,
        });
    }
    Levels {
        levels,
        l0: Some(OverlappingLevel {
            sub_levels: vec![],
            total_file_size: 0,
            uncompressed_file_size: 0,
        }),
        group_id,
        parent_group_id: StaticCompactionGroupId::NewCompactionGroup as _,
        member_table_ids: vec![],
    }
}

pub fn try_get_compaction_group_id_by_table_id(
    version: &HummockVersion,
    table_id: StateTableId,
) -> Option<CompactionGroupId> {
    for (group_id, levels) in &version.levels {
        if levels.member_table_ids.contains(&table_id) {
            return Some(*group_id);
        }
    }
    None
}

/// Gets all compaction group ids.
pub fn get_compaction_group_ids(version: &HummockVersion) -> Vec<CompactionGroupId> {
    version.levels.keys().cloned().collect()
}

/// Gets all member table ids.
pub fn get_member_table_ids(version: &HummockVersion) -> HashSet<StateTableId> {
    version
        .levels
        .iter()
        .flat_map(|(_, levels)| levels.member_table_ids.clone())
        .collect()
}

/// Gets all SST ids in `group_id`
pub fn get_compaction_group_sst_ids(
    version: &HummockVersion,
    group_id: CompactionGroupId,
) -> Vec<HummockSstableId> {
    let group_levels = version.get_compaction_group_levels(group_id);
    group_levels
        .l0
        .as_ref()
        .unwrap()
        .sub_levels
        .iter()
        .rev()
        .chain(group_levels.levels.iter())
        .flat_map(|level| level.table_infos.iter().map(|table_info| table_info.id))
        .collect_vec()
}

pub fn new_sub_level(
    sub_level_id: u64,
    level_type: LevelType,
    table_infos: Vec<SstableInfo>,
) -> Level {
    if level_type == LevelType::Nonoverlapping {
        debug_assert!(
            can_concat(&table_infos),
            "sst of non-overlapping level is not concat-able: {:?}",
            table_infos
        );
    }
    let total_file_size = table_infos.iter().map(|table| table.file_size).sum();
    let uncompressed_file_size = table_infos
        .iter()
        .map(|table| table.uncompressed_file_size)
        .sum();
    Level {
        level_idx: 0,
        level_type: level_type as i32,
        table_infos,
        total_file_size,
        sub_level_id,
        uncompressed_file_size,
    }
}

pub fn add_new_sub_level(
    l0: &mut OverlappingLevel,
    insert_sub_level_id: u64,
    level_type: LevelType,
    insert_table_infos: Vec<SstableInfo>,
) {
    if insert_sub_level_id == u64::MAX {
        return;
    }
    if let Some(newest_level) = l0.sub_levels.last() {
        assert!(
            newest_level.sub_level_id < insert_sub_level_id,
            "inserted new level is not the newest: prev newest: {}, insert: {}. L0: {:?}",
            newest_level.sub_level_id,
            insert_sub_level_id,
            l0,
        );
    }
    // All files will be committed in one new Overlapping sub-level and become
    // Nonoverlapping  after at least one compaction.
    let level = new_sub_level(insert_sub_level_id, level_type, insert_table_infos);
    l0.total_file_size += level.total_file_size;
    l0.uncompressed_file_size += level.uncompressed_file_size;
    l0.sub_levels.push(level);
}

pub fn build_version_delta_after_version(version: &HummockVersion) -> HummockVersionDelta {
    HummockVersionDelta {
        id: version.id + 1,
        prev_id: version.id,
        safe_epoch: version.safe_epoch,
        trivial_move: false,
        max_committed_epoch: version.max_committed_epoch,
        group_deltas: Default::default(),
        gc_sst_ids: vec![],
    }
}

/// Delete sstables if the table id is in the id set.
///
/// Return `true` if some sst is deleted, and `false` is the deletion is trivial
fn level_delete_ssts(operand: &mut Level, delete_sst_ids_superset: &HashSet<u64>) -> bool {
    let original_len = operand.table_infos.len();
    operand
        .table_infos
        .retain(|table| !delete_sst_ids_superset.contains(&table.id));
    operand.total_file_size = operand
        .table_infos
        .iter()
        .map(|table| table.file_size)
        .sum::<u64>();
    operand.uncompressed_file_size = operand
        .table_infos
        .iter()
        .map(|table| table.uncompressed_file_size)
        .sum::<u64>();
    original_len != operand.table_infos.len()
}

fn level_insert_ssts(operand: &mut Level, insert_table_infos: Vec<SstableInfo>) {
    operand.total_file_size += insert_table_infos
        .iter()
        .map(|sst| sst.file_size)
        .sum::<u64>();
    operand.uncompressed_file_size += insert_table_infos
        .iter()
        .map(|sst| sst.uncompressed_file_size)
        .sum::<u64>();
    operand.table_infos.extend(insert_table_infos);
    operand.table_infos.sort_by(|sst1, sst2| {
        let a = sst1.key_range.as_ref().unwrap();
        let b = sst2.key_range.as_ref().unwrap();
        a.compare(b)
    });
    if operand.level_type == LevelType::Overlapping as i32 {
        operand.level_type = LevelType::Nonoverlapping as i32;
    }
    debug_assert!(can_concat(&operand.table_infos));
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_pb::hummock::group_delta::DeltaType;
    use risingwave_pb::hummock::hummock_version::Levels;
    use risingwave_pb::hummock::hummock_version_delta::GroupDeltas;
    use risingwave_pb::hummock::{
        CompactionConfig, GroupConstruct, GroupDelta, GroupDestroy, HummockVersion,
        HummockVersionDelta, IntraLevelDelta, Level, LevelType, OverlappingLevel, SstableInfo,
    };

    use crate::compaction_group::hummock_version_ext::{
        build_initial_compaction_group_levels, HummockVersionExt, HummockVersionUpdateExt,
    };

    #[test]
    fn test_get_sst_ids() {
        let mut version = HummockVersion {
            id: 0,
            levels: HashMap::from_iter([(
                0,
                Levels {
                    levels: vec![],
                    l0: Some(OverlappingLevel {
                        sub_levels: vec![],
                        total_file_size: 0,
                        uncompressed_file_size: 0,
                    }),
                    ..Default::default()
                },
            )]),
            max_committed_epoch: 0,
            safe_epoch: 0,
        };
        assert_eq!(version.get_sst_ids().len(), 0);

        // Add to sub level
        version
            .levels
            .get_mut(&0)
            .unwrap()
            .l0
            .as_mut()
            .unwrap()
            .sub_levels
            .push(Level {
                table_infos: vec![SstableInfo {
                    id: 11,
                    ..Default::default()
                }],
                ..Default::default()
            });
        assert_eq!(version.get_sst_ids().len(), 1);

        // Add to non sub level
        version.levels.get_mut(&0).unwrap().levels.push(Level {
            table_infos: vec![SstableInfo {
                id: 22,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_eq!(version.get_sst_ids().len(), 2);
    }

    #[test]
    fn test_apply_version_delta() {
        let mut version = HummockVersion {
            id: 0,
            levels: HashMap::from_iter([
                (
                    0,
                    build_initial_compaction_group_levels(
                        0,
                        &CompactionConfig {
                            max_level: 6,
                            ..Default::default()
                        },
                    ),
                ),
                (
                    1,
                    build_initial_compaction_group_levels(
                        1,
                        &CompactionConfig {
                            max_level: 6,
                            ..Default::default()
                        },
                    ),
                ),
            ]),
            max_committed_epoch: 0,
            safe_epoch: 0,
        };
        let version_delta = HummockVersionDelta {
            id: 1,
            group_deltas: HashMap::from_iter([
                (
                    2,
                    GroupDeltas {
                        group_deltas: vec![GroupDelta {
                            delta_type: Some(DeltaType::GroupConstruct(GroupConstruct {
                                group_config: Some(CompactionConfig {
                                    max_level: 6,
                                    ..Default::default()
                                }),
                                ..Default::default()
                            })),
                        }],
                    },
                ),
                (
                    0,
                    GroupDeltas {
                        group_deltas: vec![GroupDelta {
                            delta_type: Some(DeltaType::GroupDestroy(GroupDestroy {})),
                        }],
                    },
                ),
                (
                    1,
                    GroupDeltas {
                        group_deltas: vec![GroupDelta {
                            delta_type: Some(DeltaType::IntraLevel(IntraLevelDelta {
                                level_idx: 1,
                                inserted_table_infos: vec![SstableInfo {
                                    id: 1,
                                    ..Default::default()
                                }],
                                ..Default::default()
                            })),
                        }],
                    },
                ),
            ]),
            ..Default::default()
        };
        version.apply_version_delta(&version_delta);
        let mut cg1 = build_initial_compaction_group_levels(
            1,
            &CompactionConfig {
                max_level: 6,
                ..Default::default()
            },
        );
        cg1.levels[0] = Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: vec![SstableInfo {
                id: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            version,
            HummockVersion {
                id: 1,
                levels: HashMap::from_iter([
                    (
                        2,
                        build_initial_compaction_group_levels(
                            2,
                            &CompactionConfig {
                                max_level: 6,
                                ..Default::default()
                            }
                        ),
                    ),
                    (1, cg1,),
                ]),
                max_committed_epoch: 0,
                safe_epoch: 0,
            }
        );
    }
}
