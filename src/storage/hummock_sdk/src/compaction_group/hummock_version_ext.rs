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

use std::collections::{BTreeMap, HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_pb::hummock::group_delta::DeltaType;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::hummock_version_delta::GroupDeltas;
use risingwave_pb::hummock::{
    CompactionConfig, GroupConstruct, GroupDestroy, HummockVersion, HummockVersionDelta, Level,
    LevelType, OverlappingLevel, SstableInfo,
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
}

pub fn summarize_group_deltas(group_deltas: &GroupDeltas) -> GroupDeltasSummary {
    let mut delete_sst_levels = Vec::with_capacity(group_deltas.group_deltas.len());
    let mut delete_sst_ids_set = HashSet::new();
    let mut insert_sst_level_id = u32::MAX;
    let mut insert_sub_level_id = u64::MAX;
    let mut insert_table_infos = vec![];
    let mut group_construct = None;
    let mut group_destroy = None;
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
    fn iter_tables<F: FnMut(&SstableInfo)>(
        &self,
        compaction_group_id: CompactionGroupId,
        level_idex: usize,
        f: F,
    );
    fn iter_group_tables<F: FnMut(&SstableInfo)>(
        &self,
        compaction_group_id: CompactionGroupId,
        f: F,
    );
    fn map_level<F: FnMut(&Level)>(
        &self,
        compaction_group_id: CompactionGroupId,
        level_idex: usize,
        f: F,
    );
    fn num_levels(&self, compaction_group_id: CompactionGroupId) -> usize;
    fn level_iter<F: FnMut(&Level) -> bool>(&self, compaction_group_id: CompactionGroupId, f: F);

    fn get_sst_ids(&self) -> Vec<u64>;
    fn init_with_father_group(
        &mut self,
        father_group_id: CompactionGroupId,
        group_id: CompactionGroupId,
        member_table_ids: &HashSet<StateTableId>,
    ) -> (bool, Vec<HummockSstableId>);
    fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta);

    fn build_compaction_group_info(&self) -> HashMap<TableId, CompactionGroupId>;
    fn build_branched_sst_info(&self) -> BTreeMap<HummockSstableId, HashSet<CompactionGroupId>>;
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

    fn iter_tables<F: FnMut(&SstableInfo)>(
        &self,
        compaction_group_id: CompactionGroupId,
        level_idx: usize,
        mut f: F,
    ) {
        if let Some(levels) = self.levels.get(&compaction_group_id) {
            if level_idx == 0 {
                for level in &levels.l0.as_ref().unwrap().sub_levels {
                    for table in &level.table_infos {
                        f(table);
                    }
                }
            } else {
                for table in &levels.levels[level_idx - 1].table_infos {
                    f(table);
                }
            }
        }
    }

    fn iter_group_tables<F: FnMut(&SstableInfo)>(
        &self,
        compaction_group_id: CompactionGroupId,
        mut f: F,
    ) {
        if let Some(levels) = self.levels.get(&compaction_group_id) {
            if let Some(ref l0) = levels.l0 {
                for sub_level in l0.get_sub_levels() {
                    for table_info in sub_level.get_table_infos() {
                        f(table_info);
                    }
                }
            }
            for level in levels.get_levels() {
                for table_info in level.get_table_infos() {
                    f(table_info);
                }
            }
        }
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

    fn map_level<F: FnMut(&Level)>(
        &self,
        compaction_group_id: CompactionGroupId,
        level_idx: usize,
        mut f: F,
    ) {
        if let Some(levels) = self.levels.get(&compaction_group_id) {
            if level_idx == 0 {
                for sub_level in &levels.l0.as_ref().unwrap().sub_levels {
                    f(sub_level);
                }
            } else {
                f(&levels.levels[level_idx - 1]);
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

    fn init_with_father_group(
        &mut self,
        father_group_id: CompactionGroupId,
        group_id: CompactionGroupId,
        member_table_ids: &HashSet<StateTableId>,
    ) -> (bool, Vec<HummockSstableId>) {
        let mut split_ids = vec![];
        if father_group_id == StaticCompactionGroupId::NewCompactionGroup as CompactionGroupId
            || !self.levels.contains_key(&father_group_id)
        {
            return (false, split_ids);
        }
        let (father_levels, cur_levels) = unsafe {
            let father_levels = self.levels.get_mut(&father_group_id).unwrap() as *mut Levels;
            let cur_levels = self.levels.get_mut(&group_id).unwrap() as *mut Levels;
            assert_ne!(father_levels, cur_levels);
            (&mut *father_levels, &mut *cur_levels)
        };
        if let Some(ref mut l0) = father_levels.l0 {
            let mut insert_table_infos = vec![];
            for sub_level in &mut l0.sub_levels {
                for table_info in &mut sub_level.table_infos {
                    if table_info
                        .get_table_ids()
                        .iter()
                        .any(|table_id| member_table_ids.contains(table_id))
                    {
                        table_info.divide_version += 1;
                        split_ids.push(table_info.get_id());
                        let mut branch_table_info = table_info.clone();
                        table_info
                            .table_ids
                            .retain(|table_id| !member_table_ids.contains(table_id));
                        branch_table_info
                            .table_ids
                            .retain(|table_id| member_table_ids.contains(table_id));
                        insert_table_infos.push(branch_table_info);
                    }
                }
            }
            add_new_sub_level(cur_levels.l0.as_mut().unwrap(), 0, insert_table_infos);
        }
        for (z, level) in father_levels.levels.iter_mut().enumerate() {
            for table_info in &mut level.table_infos {
                if table_info
                    .get_table_ids()
                    .iter()
                    .any(|table_id| member_table_ids.contains(table_id))
                {
                    table_info.divide_version += 1;
                    split_ids.push(table_info.get_id());
                    let mut branch_table_info = table_info.clone();
                    table_info
                        .table_ids
                        .retain(|table_id| !member_table_ids.contains(table_id));
                    branch_table_info
                        .table_ids
                        .retain(|table_id| member_table_ids.contains(table_id));
                    cur_levels.levels[z].total_file_size += branch_table_info.file_size;
                    cur_levels.levels[z].table_infos.push(branch_table_info);
                }
            }
        }
        (true, split_ids)
    }

    fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta) {
        for (compaction_group_id, group_deltas) in &version_delta.group_deltas {
            let summary = summarize_group_deltas(group_deltas);
            if let Some(group_construct) = &summary.group_construct {
                self.levels.insert(
                    *compaction_group_id,
                    <Levels as HummockLevelsExt>::build_initial_levels(
                        group_construct.get_group_config().unwrap(),
                    ),
                );
                let father_group_id = group_construct.get_father_group_id();
                self.init_with_father_group(
                    father_group_id,
                    *compaction_group_id,
                    &HashSet::from_iter(group_construct.get_table_ids().iter().cloned()),
                );
            }
            let has_destroy = summary.group_destroy.is_some();
            let levels = self
                .levels
                .get_mut(compaction_group_id)
                .expect("compaction group should exist");

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
                    insert_table_infos,
                );
            } else {
                // `max_committed_epoch` is not changed. The delta is caused by compaction.
                levels.apply_compact_ssts(summary, false);
            }
            if has_destroy {
                self.levels.remove(compaction_group_id);
            }
        }
        self.id = version_delta.id;
        self.max_committed_epoch = version_delta.max_committed_epoch;
        self.safe_epoch = version_delta.safe_epoch;
    }

    fn build_compaction_group_info(&self) -> HashMap<TableId, CompactionGroupId> {
        let mut ret = HashMap::new();
        for (compaction_group_id, levels) in &self.levels {
            if let Some(ref l0) = levels.l0 {
                for sub_level in l0.get_sub_levels() {
                    update_compaction_group_info(sub_level, *compaction_group_id, &mut ret);
                }
            }
            for level in levels.get_levels() {
                update_compaction_group_info(level, *compaction_group_id, &mut ret);
            }
        }
        ret
    }

    fn build_branched_sst_info(&self) -> BTreeMap<HummockSstableId, HashSet<CompactionGroupId>> {
        let mut ret: BTreeMap<_, HashSet<_>> = BTreeMap::new();
        for compaction_group_id in self.get_levels().keys() {
            self.iter_group_tables(*compaction_group_id, |table_info| {
                let sst_id = table_info.get_id();
                ret.entry(sst_id).or_default().insert(*compaction_group_id);
            });
        }
        ret.retain(|_, v| v.len() != 1);
        ret
    }
}

fn update_compaction_group_info(
    level: &Level,
    compaction_group_id: CompactionGroupId,
    compaction_group_info: &mut HashMap<TableId, CompactionGroupId>,
) {
    for table_info in level.get_table_infos() {
        table_info.get_table_ids().iter().for_each(|table_id| {
            compaction_group_info.insert(TableId::new(*table_id), compaction_group_id);
        });
    }
}

pub trait HummockLevelsExt {
    fn get_level0(&self) -> &OverlappingLevel;
    fn get_level(&self, idx: usize) -> &Level;
    fn get_level_mut(&mut self, idx: usize) -> &mut Level;
    fn apply_compact_ssts(&mut self, summary: GroupDeltasSummary, local_related_only: bool);
    fn build_initial_levels(compaction_config: &CompactionConfig) -> Levels;
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

    fn apply_compact_ssts(&mut self, summary: GroupDeltasSummary, local_related_only: bool) {
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
        if local_related_only && !delete_sst_ids_set.is_empty() && !deleted {
            // If no sst is deleted, the current delta will not be related to the local version.
            // Therefore, if we only care local related data, we can return without inserting the
            // ssts.
            return;
        }
        if !insert_table_infos.is_empty() {
            if insert_sst_level_id == 0 {
                let l0 = self.l0.as_mut().unwrap();
                let index = l0
                    .sub_levels
                    .partition_point(|level| level.sub_level_id < insert_sub_level_id);
                if local_related_only {
                    // Some sub level in the full hummock version may be empty in the local related
                    // pruned version, so it's possible the level to be inserted is not found
                    if index == l0.sub_levels.len()
                        || l0.sub_levels[index].sub_level_id > insert_sub_level_id
                    {
                        // level not found, insert a new level
                        let new_level = new_sub_level(
                            insert_sub_level_id,
                            LevelType::Nonoverlapping,
                            insert_table_infos,
                        );
                        l0.sub_levels.insert(index, new_level);
                    } else {
                        // level found, add to the level.
                        level_insert_ssts(&mut l0.sub_levels[index], insert_table_infos);
                    }
                } else {
                    assert!(
                        index < l0.sub_levels.len() && l0.sub_levels[index].sub_level_id == insert_sub_level_id,
                        "should find the level to insert into when applying compaction generated delta. sub level idx: {}",
                        insert_sub_level_id
                    );
                    level_insert_ssts(&mut l0.sub_levels[index], insert_table_infos);
                }
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
        }
    }

    fn build_initial_levels(compaction_config: &CompactionConfig) -> Levels {
        let mut levels = vec![];
        for l in 0..compaction_config.get_max_level() {
            levels.push(Level {
                level_idx: (l + 1) as u32,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![],
                total_file_size: 0,
                sub_level_id: 0,
            });
        }
        Levels {
            levels,
            l0: Some(OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
            }),
        }
    }
}

pub fn new_sub_level(
    sub_level_id: u64,
    level_type: LevelType,
    table_infos: Vec<SstableInfo>,
) -> Level {
    if level_type == LevelType::Nonoverlapping {
        debug_assert!(
            can_concat(&table_infos.iter().collect_vec()),
            "sst of non-overlapping level is not concat-able: {:?}",
            table_infos
        );
    }
    let total_file_size = table_infos.iter().map(|table| table.file_size).sum();
    Level {
        level_idx: 0,
        level_type: level_type as i32,
        table_infos,
        total_file_size,
        sub_level_id,
    }
}

pub fn add_new_sub_level(
    l0: &mut OverlappingLevel,
    insert_sub_level_id: u64,
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
    let level = new_sub_level(
        insert_sub_level_id,
        LevelType::Overlapping,
        insert_table_infos,
    );
    l0.total_file_size += level.total_file_size;
    l0.sub_levels.push(level);
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
    original_len != operand.table_infos.len()
}

fn level_insert_ssts(operand: &mut Level, insert_table_infos: Vec<SstableInfo>) {
    operand.total_file_size += insert_table_infos
        .iter()
        .map(|sst| sst.file_size)
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
    debug_assert!(can_concat(&operand.table_infos.iter().collect_vec()));
}

pub trait HummockVersionDeltaExt {
    fn get_removed_sst_ids(&self) -> Vec<HummockSstableId>;
    fn get_inserted_sst_ids(&self) -> Vec<HummockSstableId>;
}

impl HummockVersionDeltaExt for HummockVersionDelta {
    fn get_removed_sst_ids(&self) -> Vec<HummockSstableId> {
        let mut ret = vec![];
        for group_deltas in self.group_deltas.values() {
            for group_delta in &group_deltas.group_deltas {
                if let DeltaType::IntraLevel(intra_level) = group_delta.get_delta_type().unwrap() {
                    for sst_id in &intra_level.removed_table_ids {
                        ret.push(*sst_id);
                    }
                }
            }
        }
        ret
    }

    fn get_inserted_sst_ids(&self) -> Vec<HummockSstableId> {
        let mut ret = vec![];
        for group_deltas in self.group_deltas.values() {
            for group_delta in &group_deltas.group_deltas {
                if let DeltaType::IntraLevel(intra_level) = group_delta.get_delta_type().unwrap() {
                    for sst in &intra_level.inserted_table_infos {
                        ret.push(sst.id);
                    }
                }
            }
        }
        ret
    }
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

    use super::HummockLevelsExt;
    use crate::compaction_group::hummock_version_ext::HummockVersionExt;

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
                    }),
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
                    Levels::build_initial_levels(&CompactionConfig {
                        max_level: 6,
                        ..Default::default()
                    }),
                ),
                (
                    1,
                    Levels::build_initial_levels(&CompactionConfig {
                        max_level: 6,
                        ..Default::default()
                    }),
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
        let mut cg1 = Levels::build_initial_levels(&CompactionConfig {
            max_level: 6,
            ..Default::default()
        });
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
                        Levels::build_initial_levels(&CompactionConfig {
                            max_level: 6,
                            ..Default::default()
                        }),
                    ),
                    (1, cg1,),
                ]),
                max_committed_epoch: 0,
                safe_epoch: 0,
            }
        );
    }
}
