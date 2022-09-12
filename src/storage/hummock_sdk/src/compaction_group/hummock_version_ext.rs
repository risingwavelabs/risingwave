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

use itertools::Itertools;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::hummock_version_delta::LevelDeltas;
use risingwave_pb::hummock::{
    HummockVersion, HummockVersionDelta, Level, LevelType, OverlappingLevel, SstableInfo,
};

use crate::prost_key_range::KeyRangeExt;
use crate::{can_concat, CompactionGroupId, HummockSstableId};

pub struct LevelDeltasSummary {
    pub delete_sst_levels: Vec<u32>,
    pub delete_sst_ids_set: HashSet<u64>,
    pub insert_sst_level_id: u32,
    pub insert_sub_level_id: u64,
    pub insert_table_infos: Vec<SstableInfo>,
}

pub fn summarize_level_deltas(level_deltas: &LevelDeltas) -> LevelDeltasSummary {
    let mut delete_sst_levels = Vec::with_capacity(level_deltas.level_deltas.len());
    let mut delete_sst_ids_set = HashSet::new();
    let mut insert_sst_level_id = u32::MAX;
    let mut insert_sub_level_id = u64::MAX;
    let mut insert_table_infos = vec![];
    for level_delta in &level_deltas.level_deltas {
        if !level_delta.removed_table_ids.is_empty() {
            delete_sst_levels.push(level_delta.level_idx);
            delete_sst_ids_set.extend(level_delta.removed_table_ids.iter().clone());
        }
        if !level_delta.inserted_table_infos.is_empty() {
            insert_sst_level_id = level_delta.level_idx;
            insert_sub_level_id = level_delta.l0_sub_level_id;
            insert_table_infos.extend(level_delta.inserted_table_infos.iter().cloned());
        }
    }

    LevelDeltasSummary {
        delete_sst_levels,
        delete_sst_ids_set,
        insert_sst_level_id,
        insert_sub_level_id,
        insert_table_infos,
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
    fn map_level<F: FnMut(&Level)>(
        &self,
        compaction_group_id: CompactionGroupId,
        level_idex: usize,
        f: F,
    );
    fn num_levels(&self, compaction_group_id: CompactionGroupId) -> usize;
    fn level_iter<F: FnMut(&Level) -> bool>(&self, compaction_group_id: CompactionGroupId, f: F);

    fn get_sst_ids(&self) -> Vec<u64>;
    fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta);
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

    fn apply_version_delta(&mut self, version_delta: &HummockVersionDelta) {
        for (compaction_group_id, level_deltas) in &version_delta.level_deltas {
            let levels = self
                .levels
                .get_mut(compaction_group_id)
                .expect("compaction group should exist");

            let summary = summarize_level_deltas(level_deltas);

            assert!(
                self.max_committed_epoch <= version_delta.max_committed_epoch,
                "new max commit epoch {} is older than the current max commit epoch {}",
                version_delta.max_committed_epoch,
                self.max_committed_epoch
            );
            if self.max_committed_epoch < version_delta.max_committed_epoch {
                // `max_committed_epoch` increases. It must be a `commit_epoch`
                let LevelDeltasSummary {
                    delete_sst_levels,
                    delete_sst_ids_set,
                    insert_sst_level_id,
                    insert_sub_level_id,
                    insert_table_infos,
                } = summary;
                assert_eq!(
                    0, insert_sst_level_id,
                    "we should only add to L0 when we commit an epoch. Inserting into {} {:?}",
                    insert_sst_level_id, insert_table_infos
                );
                assert!(
                    delete_sst_levels.is_empty() && delete_sst_ids_set.is_empty(),
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
        }
        self.id = version_delta.id;
        self.max_committed_epoch = version_delta.max_committed_epoch;
        self.safe_epoch = version_delta.safe_epoch;
    }
}

pub trait HummockLevelsExt {
    fn get_level0(&self) -> &OverlappingLevel;
    fn get_level(&self, idx: usize) -> &Level;
    fn get_level_mut(&mut self, idx: usize) -> &mut Level;
    fn apply_compact_ssts(&mut self, summary: LevelDeltasSummary, local_related_only: bool);
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

    fn apply_compact_ssts(&mut self, summary: LevelDeltasSummary, local_related_only: bool) {
        let LevelDeltasSummary {
            delete_sst_levels,
            delete_sst_ids_set,
            insert_sst_level_id,
            insert_sub_level_id,
            insert_table_infos,
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
        if local_related_only && !deleted {
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
    fn get_sstableinfo_cnt(&self) -> usize;
}

impl HummockVersionDeltaExt for HummockVersionDelta {
    fn get_removed_sst_ids(&self) -> Vec<HummockSstableId> {
        let mut ret = vec![];
        for level_deltas in self.level_deltas.values() {
            for level_delta in &level_deltas.level_deltas {
                for sst_id in &level_delta.removed_table_ids {
                    ret.push(*sst_id);
                }
            }
        }
        ret
    }

    fn get_inserted_sst_ids(&self) -> Vec<HummockSstableId> {
        let mut ret = vec![];
        for level_deltas in self.level_deltas.values() {
            for level_delta in &level_deltas.level_deltas {
                for sst in &level_delta.inserted_table_infos {
                    ret.push(sst.id);
                }
            }
        }
        ret
    }

    fn get_sstableinfo_cnt(&self) -> usize {
        self.level_deltas
            .values()
            .flat_map(|item| item.level_deltas.iter())
            .map(|level_delta| level_delta.inserted_table_infos.len())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_pb::hummock::hummock_version::Levels;
    use risingwave_pb::hummock::{HummockVersion, Level, OverlappingLevel, SstableInfo};

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
}
