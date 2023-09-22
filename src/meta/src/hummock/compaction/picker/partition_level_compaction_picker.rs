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

use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockLevelsExt;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{CompactionConfig, InputLevel, Level, LevelType, OverlappingLevel};

use super::{CompactionInput, CompactionPicker, LocalPickerStatistic};
use crate::hummock::compaction::create_overlap_strategy;
use crate::hummock::compaction::picker::{
    partition_sub_levels, LevelPartition, PartitionIntraSubLevelPicker, TrivialMovePicker,
};
use crate::hummock::level_handler::LevelHandler;

pub struct PartitionLevelCompactionPicker {
    target_level: usize,
    config: Arc<CompactionConfig>,
    base_level_partitions: Vec<LevelPartition>,
}

impl CompactionPicker for PartitionLevelCompactionPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let l0 = levels.l0.as_ref().unwrap();
        if l0.sub_levels.is_empty() {
            return None;
        }
        if l0.sub_levels[0].level_type != LevelType::Nonoverlapping as i32
            && l0.sub_levels[0].table_infos.len() > 1
        {
            stats.skip_by_overlapping += 1;
            return None;
        }

        assert!(levels.can_partition_by_vnode());

        if let Some(mut ret) = self.pick_base_trivial_move(
            l0,
            levels.get_level(self.target_level),
            level_handlers,
            stats,
        ) {
            ret.vnode_partition_count = levels.vnode_partition_count;
            return Some(ret);
        }

        debug_assert!(self.target_level == levels.get_level(self.target_level).level_idx as usize);
        let partitions = partition_sub_levels(levels);
        if let Some(ret) =
            self.pick_multi_level_to_base(levels, level_handlers, partitions.clone(), stats)
        {
            return Some(ret);
        }
        self.pick_l0_intra(levels, level_handlers, partitions)
    }
}

impl PartitionLevelCompactionPicker {
    pub fn new(
        target_level: usize,
        config: Arc<CompactionConfig>,
        base_level_partitions: Vec<LevelPartition>,
    ) -> PartitionLevelCompactionPicker {
        PartitionLevelCompactionPicker {
            target_level,
            config,
            base_level_partitions,
        }
    }

    fn pick_base_trivial_move(
        &self,
        l0: &OverlappingLevel,
        target_level: &Level,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());
        let trivial_move_picker =
            TrivialMovePicker::new(0, self.target_level, overlap_strategy.clone());

        trivial_move_picker.pick_trivial_move_task(
            &l0.sub_levels[0].table_infos,
            &target_level.table_infos,
            level_handlers,
            stats,
        )
    }

    fn pick_multi_level_to_base(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        l0_partitions: Vec<LevelPartition>,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let l0 = levels.l0.as_ref().unwrap();
        let target_level = levels.get_level(self.target_level);
        let vnode_partition_count = levels.vnode_partition_count;
        if !self.base_level_partitions.is_empty() {
            let partitions_score = self
                .base_level_partitions
                .iter()
                .enumerate()
                .map(|(idx, part)| (idx, part.total_file_size))
                .sorted_by_key(|(_, fs)| *fs)
                .collect_vec();

            for (idx, target_level_size) in partitions_score {
                let mut input_levels = vec![];
                let target_part = &self.base_level_partitions[idx].sub_levels[0];
                let mut pending_compact = false;
                if target_part.right_idx > target_part.left_idx {
                    for i in target_part.left_idx..target_part.right_idx {
                        if level_handlers[self.target_level]
                            .is_pending_compact(&target_level.table_infos[i].sst_id)
                        {
                            pending_compact = true;
                            break;
                        }
                    }
                    if pending_compact {
                        continue;
                    }
                }

                let max_compaction_size =
                    std::cmp::max(self.config.max_compaction_bytes / 2, target_level_size);

                let mut total_file_size = 0;
                let mut total_file_count = 0;
                let mut wait_enough = false;
                let l0_part = &l0_partitions[idx];
                for (sub_level_idx, level_part) in l0_part.sub_levels.iter().enumerate() {
                    if total_file_size > max_compaction_size
                        || total_file_count > self.config.level0_max_compact_file_number
                    {
                        wait_enough = true;
                        break;
                    }
                    assert_eq!(
                        level_part.sub_level_id,
                        l0.sub_levels[sub_level_idx].sub_level_id
                    );
                    let mut level_file_size = 0;
                    if level_part.right_idx > level_part.left_idx {
                        for sst in &l0.sub_levels[sub_level_idx].table_infos
                            [level_part.left_idx..level_part.right_idx]
                        {
                            if level_handlers[0].is_pending_compact(&sst.sst_id) {
                                pending_compact = true;
                            }
                            level_file_size += sst.file_size;
                        }

                        if pending_compact {
                            break;
                        }
                        input_levels.push(InputLevel {
                            level_idx: 0,
                            level_type: l0.sub_levels[sub_level_idx].level_type,
                            table_infos: l0.sub_levels[sub_level_idx].table_infos
                                [level_part.left_idx..level_part.right_idx]
                                .to_vec(),
                        });
                        total_file_size += level_file_size;
                        total_file_count += (level_part.right_idx - level_part.left_idx) as u64;
                    }
                }
                if wait_enough || total_file_size > target_level_size {
                    input_levels.reverse();
                    if target_part.right_idx > target_part.left_idx {
                        input_levels.push(InputLevel {
                            level_idx: target_level.level_idx,
                            level_type: target_level.level_type,
                            table_infos: target_level.table_infos
                                [target_part.left_idx..target_part.right_idx]
                                .to_vec(),
                        });
                    }
                    stats.use_vnode_partition = true;
                    return Some(CompactionInput {
                        input_levels,
                        target_level: self.target_level,
                        target_sub_level_id: 0,
                        select_input_size: total_file_size,
                        target_input_size: target_part.total_file_size,
                        total_file_count: total_file_count
                            + (target_part.right_idx - target_part.left_idx) as u64,
                        vnode_partition_count,
                    });
                }
            }
            stats.skip_by_write_amp_limit += 1;
            return None;
        }
        if level_handlers[self.target_level].is_level_pending_compact(target_level) {
            stats.skip_by_pending_files += 1;
            return None;
        }
        let mut input_levels = vec![];
        let mut total_file_size = 0;
        let mut total_file_count = 0;
        let max_compaction_size = std::cmp::min(
            self.config.max_compaction_bytes,
            target_level.total_file_size,
        );
        let mut wait_enough = true;
        for level in &l0.sub_levels {
            if level.level_type() != LevelType::Nonoverlapping
                || level_handlers[0].is_level_pending_compact(level)
            {
                stats.skip_by_pending_files += 1;
                wait_enough = false;
                break;
            }

            if total_file_size > max_compaction_size
                || total_file_count > self.config.level0_max_compact_file_number
            {
                break;
            }
            input_levels.push(InputLevel {
                level_idx: 0,
                level_type: level.level_type,
                table_infos: level.table_infos.clone(),
            });
            total_file_size += level.total_file_size;
            total_file_count += level.table_infos.len() as u64;
        }
        if wait_enough || total_file_size > target_level.total_file_size {
            input_levels.reverse();
            input_levels.push(InputLevel {
                level_idx: target_level.level_idx,
                level_type: target_level.level_type,
                table_infos: target_level.table_infos.clone(),
            });
            return Some(CompactionInput {
                input_levels,
                target_level: self.target_level,
                target_sub_level_id: 0,
                select_input_size: total_file_size,
                target_input_size: target_level.total_file_size,
                total_file_count: total_file_count + target_level.table_infos.len() as u64,
                vnode_partition_count,
            });
        }

        stats.skip_by_count_limit += 1;
        None
    }

    fn pick_l0_intra(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        partitions: Vec<LevelPartition>,
    ) -> Option<CompactionInput> {
        let mut picker = PartitionIntraSubLevelPicker::new(self.config.clone(), partitions);
        picker.pick_compaction(levels, level_handlers, &mut LocalPickerStatistic::default())
    }
}
