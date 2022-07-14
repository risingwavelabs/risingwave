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

use std::collections::HashSet;

use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{HummockVersion, Level, LevelType, SstableInfo};

use crate::prost_key_range::KeyRangeExt;
use crate::CompactionGroupId;

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
    fn map_level<F: FnMut(&Level)>(
        &self,
        compaction_group_id: CompactionGroupId,
        level_idex: usize,
        f: F,
    );
    fn level_iter<F: FnMut(&Level) -> bool>(&self, compaction_group_id: CompactionGroupId, f: F);
    fn apply_compact_ssts(
        &mut self,
        compaction_group_id: CompactionGroupId,
        delete_sst_levels: &[usize],
        delete_sst_ids_set: &HashSet<u64>,
        insert_sst_level: usize,
        insert_sub_level: u64,
        insert_table_infos: Vec<SstableInfo>,
    );
}

impl HummockVersionExt for HummockVersion {
    fn get_compaction_group_levels(&self, compaction_group_id: CompactionGroupId) -> &Levels {
        self.levels
            .get(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id))
    }

    fn get_compaction_group_levels_mut(
        &mut self,
        compaction_group_id: CompactionGroupId,
    ) -> &mut Levels {
        self.levels
            .get_mut(&compaction_group_id)
            .unwrap_or_else(|| panic!("compaction group {} exists", compaction_group_id))
    }

    fn get_combined_levels(&self) -> Vec<&Level> {
        let mut combined_levels = vec![];
        for level in self.levels.values() {
            combined_levels.extend(level.l0.as_ref().unwrap().sub_levels.iter());
            combined_levels.extend(level.levels.iter());
        }
        combined_levels
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

    fn apply_compact_ssts(
        &mut self,
        compaction_group_id: CompactionGroupId,
        delete_sst_levels: &[usize],
        delete_sst_ids_set: &HashSet<u64>,
        insert_sst_level: usize,
        insert_sub_level_id: u64,
        insert_table_infos: Vec<SstableInfo>,
    ) {
        if let Some(levels) = self.levels.get_mut(&compaction_group_id) {
            for level_idx in delete_sst_levels {
                if *level_idx == 0 {
                    for level in &mut levels.l0.as_mut().unwrap().sub_levels {
                        level_delete_ssts(level, delete_sst_ids_set);
                    }
                } else {
                    level_delete_ssts(&mut levels.levels[*level_idx - 1], delete_sst_ids_set);
                }
            }
            if !insert_table_infos.is_empty() {
                if insert_sst_level == 0 {
                    for level in &mut levels.l0.as_mut().unwrap().sub_levels {
                        if level.sub_level_id == insert_sub_level_id {
                            level_insert_ssts(level, insert_table_infos);
                            break;
                        }
                    }
                } else {
                    level_insert_ssts(&mut levels.levels[insert_sst_level - 1], insert_table_infos);
                }
            }
            if delete_sst_levels.iter().any(|level_id| *level_id == 0) {
                levels
                    .l0
                    .as_mut()
                    .unwrap()
                    .sub_levels
                    .retain(|level| !level.table_infos.is_empty());
                levels.l0.as_mut().unwrap().total_file_size = levels
                    .l0
                    .as_mut()
                    .unwrap()
                    .sub_levels
                    .iter()
                    .map(|level| level.total_file_size)
                    .sum::<u64>();
            }
        }
    }
}

fn level_delete_ssts(operand: &mut Level, delete_sst_ids_superset: &HashSet<u64>) {
    operand
        .table_infos
        .retain(|table| !delete_sst_ids_superset.contains(&table.id));
    operand.total_file_size = operand
        .table_infos
        .iter()
        .map(|table| table.file_size)
        .sum::<u64>();
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
}
