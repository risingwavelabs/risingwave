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
    partition_sub_levels, IntraSubLevelPicker, SubLevelPartition, TrivialMovePicker,
};
use crate::hummock::level_handler::LevelHandler;

pub struct LevelCompactionPicker {
    target_level: usize,
    config: Arc<CompactionConfig>,
    base_level_partitions: Vec<SubLevelPartition>,
}

impl CompactionPicker for LevelCompactionPicker {
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

        let is_l0_pending_compact =
            level_handlers[0].is_level_all_pending_compact(&l0.sub_levels[0]);

        if is_l0_pending_compact {
            stats.skip_by_pending_files += 1;
            return None;
        }

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
        let partitions = if levels.can_partition_by_vnode() {
            partition_sub_levels(levels)
        } else {
            vec![]
        };

        if let Some(ret) =
            self.pick_multi_level_to_base(levels, level_handlers, partitions.clone(), stats)
        {
            return Some(ret);
        }

        self.pick_l0_intra(levels, level_handlers, partitions, stats)
    }
}

impl LevelCompactionPicker {
    #[cfg(test)]
    pub fn for_test(target_level: usize, config: Arc<CompactionConfig>) -> LevelCompactionPicker {
        LevelCompactionPicker {
            target_level,
            config,
            base_level_partitions: vec![],
        }
    }

    pub fn new(
        target_level: usize,
        config: Arc<CompactionConfig>,
        base_level_partitions: Vec<SubLevelPartition>,
    ) -> LevelCompactionPicker {
        LevelCompactionPicker {
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
        l0_partitions: Vec<SubLevelPartition>,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let l0 = levels.l0.as_ref().unwrap();
        let target_level = levels.get_level(self.target_level);
        let min_sub_level_id = l0_partitions
            .iter()
            .map(|part| {
                part.sub_levels
                    .last()
                    .map(|info| info.sub_level_id)
                    .unwrap_or(0)
            })
            .min()
            .unwrap_or(0);
        let vnode_partition_count = levels.vnode_partition_count;
        if levels.can_partition_by_vnode()
            && min_sub_level_id > 0
            && !self.base_level_partitions.is_empty()
        {
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
        partitions: Vec<SubLevelPartition>,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let mut picker = IntraSubLevelPicker::new(self.config.clone(), partitions);
        picker.pick_compaction(levels, level_handlers, stats)
    }
}

#[cfg(test)]
pub mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::level_selector::tests::{
        generate_l0_nonoverlapping_multi_sublevels, generate_l0_nonoverlapping_sublevels,
        generate_l0_overlapping_sublevels, generate_level, generate_table,
        push_table_level0_nonoverlapping, push_table_level0_overlapping,
        push_tables_level0_nonoverlapping,
    };
    use crate::hummock::compaction::{CompactionMode, TierCompactionPicker};

    fn create_compaction_picker_for_test() -> LevelCompactionPicker {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .level0_sub_level_compact_level_count(2)
                .build(),
        );
        LevelCompactionPicker::for_test(1, config)
    }

    #[test]
    fn test_compact_l0_to_l1() {
        let mut picker = create_compaction_picker_for_test();
        let l0 = generate_level(
            0,
            vec![
                generate_table(5, 1, 100, 200, 2),
                generate_table(4, 1, 201, 300, 2),
            ],
        );
        let mut levels = Levels {
            l0: Some(OverlappingLevel {
                total_file_size: l0.total_file_size,
                uncompressed_file_size: l0.total_file_size,
                sub_levels: vec![l0],
            }),
            levels: vec![generate_level(
                1,
                vec![
                    generate_table(3, 1, 1, 100, 1),
                    generate_table(2, 1, 101, 150, 1),
                    generate_table(1, 1, 201, 210, 1),
                ],
            )],
            member_table_ids: vec![1],
            ..Default::default()
        };
        let mut local_stats = LocalPickerStatistic::default();
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 5);
        assert_eq!(ret.input_levels[1].table_infos[0].get_sst_id(), 3);

        ret.add_pending_task(0, &mut levels_handler);
        push_table_level0_nonoverlapping(&mut levels, generate_table(6, 1, 100, 200, 2));
        push_table_level0_nonoverlapping(&mut levels, generate_table(7, 1, 301, 333, 4));

        levels.l0.as_mut().unwrap().sub_levels[0]
            .table_infos
            .retain(|table| table.get_sst_id() != 4);
        levels.l0.as_mut().unwrap().total_file_size -= ret.input_levels[0].table_infos[0].file_size;

        levels_handler[0].remove_task(0);
        levels_handler[1].remove_task(0);

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 5);
        assert_eq!(
            ret.input_levels[1]
                .table_infos
                .iter()
                .map(|sst| sst.sst_id)
                .collect_vec(),
            vec![3, 2, 1]
        );
        ret.add_pending_task(1, &mut levels_handler);

        let mut local_stats = LocalPickerStatistic::default();
        // Cannot pick because no idle table in sub-level[0]. (And sub-level[0] is pending
        // actually).
        push_table_level0_overlapping(&mut levels, generate_table(8, 1, 199, 233, 3));
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());
    }

    #[test]
    fn test_selecting_key_range_overlap() {
        // When picking L0->L1, all L1 files overlapped with selecting_key_range should be picked.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .compaction_mode(CompactionMode::Range as i32)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        let mut picker = LevelCompactionPicker::for_test(1, config);

        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: vec![
                generate_table(3, 1, 0, 50, 1),
                generate_table(4, 1, 150, 200, 1),
                generate_table(5, 1, 250, 300, 1),
            ],
            ..Default::default()
        }];
        let mut levels = Levels {
            levels,
            l0: Some(OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
                uncompressed_file_size: 0,
            }),
            member_table_ids: vec![1],
            ..Default::default()
        };
        push_tables_level0_nonoverlapping(&mut levels, vec![generate_table(1, 1, 50, 140, 2)]);
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(7, 1, 200, 250, 2),
                generate_table(8, 1, 400, 500, 2),
            ],
        );

        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();

        // pick
        // l0 [sst_1]
        // l1 [sst_3]
        assert_eq!(ret.input_levels.len(), 2);
        assert_eq!(
            ret.input_levels[0]
                .table_infos
                .iter()
                .map(|t| t.get_sst_id())
                .collect_vec(),
            vec![1]
        );

        assert_eq!(
            ret.input_levels[1]
                .table_infos
                .iter()
                .map(|t| t.get_sst_id())
                .collect_vec(),
            vec![3, 4, 5]
        );
    }

    #[test]
    fn test_l0_to_l1_compact_conflict() {
        // When picking L0->L1, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let mut picker = create_compaction_picker_for_test();
        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping as i32,
            ..Default::default()
        }];
        let mut levels = Levels {
            levels,
            l0: Some(OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
                uncompressed_file_size: 0,
            }),
            member_table_ids: vec![1],
            ..Default::default()
        };
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(1, 1, 100, 300, 2),
                generate_table(2, 1, 350, 500, 2),
            ],
        );
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        // trivial_move
        ret.add_pending_task(0, &mut levels_handler); // pending only for test
        push_tables_level0_nonoverlapping(&mut levels, vec![generate_table(3, 1, 250, 300, 3)]);
        let config: CompactionConfig = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .max_compaction_bytes(1000)
            .sub_level_max_compaction_bytes(150)
            .max_bytes_for_level_multiplier(1)
            .level0_sub_level_compact_level_count(3)
            .build();
        let mut picker = TierCompactionPicker::new(Arc::new(config));

        let ret: Option<CompactionInput> =
            picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());
    }

    #[test]
    fn test_compacting_key_range_overlap_intra_l0() {
        // When picking L0->L0, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let mut picker = create_compaction_picker_for_test();

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(3, 1, 200, 300, 2)],
                ..Default::default()
            }],
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![
                generate_table(1, 1, 100, 210, 2),
                generate_table(2, 1, 200, 250, 2),
            ])),
            member_table_ids: vec![1],
            ..Default::default()
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        ret.add_pending_task(0, &mut levels_handler);

        push_table_level0_overlapping(&mut levels, generate_table(4, 1, 170, 180, 3));
        assert!(picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .is_none());
    }

    #[test]
    fn test_skip_compact_write_amplification_limit() {
        let config: CompactionConfig = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .max_compaction_bytes(1000)
            .sub_level_max_compaction_bytes(150)
            .max_bytes_for_level_multiplier(1)
            .level0_sub_level_compact_level_count(2)
            .build();
        let mut picker = LevelCompactionPicker::for_test(1, Arc::new(config));

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 399, 2),
                    generate_table(2, 1, 400, 699, 2),
                    generate_table(3, 1, 700, 999, 2),
                ],
                total_file_size: 900,
                uncompressed_file_size: 900,
                ..Default::default()
            }],
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![])),
            ..Default::default()
        };
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(4, 1, 100, 180, 2),
                generate_table(5, 1, 400, 450, 2),
                generate_table(6, 1, 600, 700, 2),
            ],
        );

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();
        levels_handler[0].add_pending_task(
            1,
            4,
            &levels.l0.as_ref().unwrap().sub_levels[0].table_infos,
        );
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        // Skip this compaction because the write amplification is too large.
        assert!(ret.is_none());
    }

    #[test]
    fn test_l0_to_l1_break_on_exceed_compaction_size() {
        let mut local_stats = LocalPickerStatistic::default();
        let mut l0 = generate_l0_overlapping_sublevels(vec![
            vec![
                generate_table(4, 1, 10, 90, 1),
                generate_table(5, 1, 210, 220, 1),
            ],
            vec![generate_table(6, 1, 0, 100000, 1)],
            vec![generate_table(7, 1, 0, 100000, 1)],
        ]);
        // We can set level_type only because the input above is valid.
        for s in &mut l0.sub_levels {
            s.level_type = LevelType::Nonoverlapping as i32;
        }
        let levels = Levels {
            l0: Some(l0),
            levels: vec![generate_level(1, vec![generate_table(3, 1, 0, 100000, 1)])],
            member_table_ids: vec![1],
            ..Default::default()
        };
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        // Pick with large max_compaction_bytes results all sub levels included in input.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(500000)
                .sub_level_max_compaction_bytes(50000)
                .max_bytes_for_level_base(500000)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        // Only include sub-level 0 results will violate MAX_WRITE_AMPLIFICATION.
        // So all sub-levels are included to make write amplification < MAX_WRITE_AMPLIFICATION.
        let mut picker = LevelCompactionPicker::for_test(1, config);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 7);
        assert_eq!(
            3,
            ret.input_levels.iter().filter(|l| l.level_idx == 0).count()
        );
        assert_eq!(
            4,
            ret.input_levels
                .iter()
                .filter(|l| l.level_idx == 0)
                .map(|l| l.table_infos.len())
                .sum::<usize>()
        );

        // Pick with small max_compaction_bytes results partial sub levels included in input.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(50000)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        let mut picker = LevelCompactionPicker::for_test(1, config);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 6);
        assert_eq!(
            2,
            ret.input_levels.iter().filter(|l| l.level_idx == 0).count()
        );
        assert_eq!(
            3,
            ret.input_levels
                .iter()
                .filter(|l| l.level_idx == 0)
                .map(|l| l.table_infos.len())
                .sum::<usize>()
        );
    }

    #[test]
    fn test_issue_11154() {
        let mut local_stats = LocalPickerStatistic::default();
        let mut l0 = generate_l0_overlapping_sublevels(vec![
            vec![
                generate_table(4, 1, 1, 200, 1),
                generate_table(5, 1, 400, 600, 1),
            ],
            vec![
                generate_table(6, 1, 1, 200, 1),
                generate_table(7, 1, 400, 600, 1),
            ],
            vec![
                generate_table(8, 1, 1, 200, 1),
                generate_table(9, 1, 400, 600, 1),
            ],
            vec![generate_table(10, 1, 1, 600, 1)],
        ]);
        // We can set level_type only because the input above is valid.
        for s in &mut l0.sub_levels {
            s.level_type = LevelType::Nonoverlapping as i32;
        }
        let levels = Levels {
            l0: Some(l0),
            levels: vec![generate_level(1, vec![generate_table(3, 1, 0, 100000, 1)])],
            member_table_ids: vec![1],
            ..Default::default()
        };
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        // Pick with large max_compaction_bytes results all sub levels included in input.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(800)
                .sub_level_max_compaction_bytes(50000)
                .max_bytes_for_level_base(500000)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        // Only include sub-level 0 results will violate MAX_WRITE_AMPLIFICATION.
        // So all sub-levels are included to make write amplification < MAX_WRITE_AMPLIFICATION.
        let mut picker = LevelCompactionPicker::for_test(1, config);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        // avoid add sst_10 and cause a big task
        assert_eq!(3, ret.input_levels.len());
    }

    #[test]
    fn test_l0_to_l1_break_on_pending_sub_level() {
        let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
            vec![
                generate_table(4, 1, 10, 90, 1),
                generate_table(5, 1, 210, 220, 1),
            ],
            vec![generate_table(6, 1, 0, 100000, 1)],
            vec![generate_table(7, 1, 0, 100000, 1)],
        ]);

        let levels = Levels {
            l0: Some(l0),
            levels: vec![generate_level(1, vec![generate_table(3, 1, 0, 100000, 1)])],
            member_table_ids: vec![1],
            ..Default::default()
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();

        // Create a pending sub-level.
        let pending_level = levels.l0.as_ref().unwrap().sub_levels[1].clone();
        assert_eq!(pending_level.sub_level_id, 1);
        let tier_task_input = CompactionInput {
            input_levels: vec![InputLevel {
                level_idx: 0,
                level_type: pending_level.level_type,
                table_infos: pending_level.table_infos.clone(),
            }],
            target_level: 1,
            target_sub_level_id: pending_level.sub_level_id,
            vnode_partition_count: 0,
        };
        assert!(!levels_handler[0].is_level_pending_compact(&pending_level));
        tier_task_input.add_pending_task(1, &mut levels_handler);
        assert!(levels_handler[0].is_level_pending_compact(&pending_level));

        // Pick with large max_compaction_bytes results all sub levels included in input.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(500000)
                .level0_sub_level_compact_level_count(2)
                .build(),
        );

        // Only include sub-level 0 results will violate MAX_WRITE_AMPLIFICATION.
        // But stopped by pending sub-level when trying to include more sub-levels.
        let mut picker = LevelCompactionPicker::for_test(1, config.clone());
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);

        assert!(ret.is_none());

        // Free the pending sub-level.
        for pending_task_id in &levels_handler[0].pending_tasks_ids() {
            levels_handler[0].remove_task(*pending_task_id);
        }

        // No more pending sub-level so we can get a task now.
        let mut picker = LevelCompactionPicker::for_test(1, config);
        picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
    }

    #[test]
    fn test_l0_to_base_when_all_base_pending() {
        let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
            vec![
                generate_table(4, 1, 10, 90, 1),
                generate_table(5, 1, 1000, 2000, 1),
            ],
            vec![generate_table(6, 1, 10, 90, 1)],
        ]);

        let levels = Levels {
            l0: Some(l0),
            levels: vec![generate_level(1, vec![generate_table(3, 1, 1, 100, 1)])],
            member_table_ids: vec![1],
            ..Default::default()
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();

        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(500000)
                .level0_sub_level_compact_level_count(2)
                .sub_level_max_compaction_bytes(1000)
                .build(),
        );

        let mut picker = LevelCompactionPicker::for_test(1, config);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        // 1. trivial_move
        assert_eq!(2, ret.input_levels.len());
        assert!(ret.input_levels[1].table_infos.is_empty());
        assert_eq!(5, ret.input_levels[0].table_infos[0].sst_id);
        ret.add_pending_task(0, &mut levels_handler);

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(3, ret.input_levels.len());
        assert_eq!(6, ret.input_levels[0].table_infos[0].sst_id);
    }

    #[test]
    fn test_pick_l0_intra() {
        {
            let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
                vec![
                    generate_table(6, 1, 50, 99, 1),
                    generate_table(1, 1, 100, 200, 1),
                    generate_table(2, 1, 250, 300, 1),
                ],
                vec![
                    generate_table(3, 1, 10, 90, 1),
                    generate_table(6, 1, 100, 110, 1),
                ],
                vec![
                    generate_table(4, 1, 50, 99, 1),
                    generate_table(5, 1, 100, 200, 1),
                ],
            ]);
            let levels = Levels {
                l0: Some(l0),
                levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
                member_table_ids: vec![1],
                ..Default::default()
            };
            let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
            levels_handler[1].add_pending_task(100, 1, levels.levels[0].get_table_infos());
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .level0_sub_level_compact_level_count(1)
                    .level0_overlapping_sub_level_compact_level_count(4)
                    .build(),
            );
            let mut picker = LevelCompactionPicker::for_test(1, config);
            let mut local_stats = LocalPickerStatistic::default();
            let ret = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            ret.add_pending_task(1, &mut levels_handler);
            assert_eq!(
                ret.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                3
            );
        }

        {
            // Suppose keyguard [100, 200] [300, 400]
            // will pick sst [1, 3, 4]
            let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
                vec![
                    generate_table(1, 1, 100, 200, 1),
                    generate_table(2, 1, 300, 400, 1),
                ],
                vec![
                    generate_table(3, 1, 100, 200, 1),
                    generate_table(6, 1, 300, 500, 1),
                ],
                vec![
                    generate_table(4, 1, 100, 200, 1),
                    generate_table(5, 1, 300, 400, 1),
                ],
            ]);
            let levels = Levels {
                l0: Some(l0),
                levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
                member_table_ids: vec![1],
                ..Default::default()
            };
            let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
            levels_handler[1].add_pending_task(100, 1, levels.levels[0].get_table_infos());
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .level0_sub_level_compact_level_count(1)
                    .build(),
            );
            let mut picker = LevelCompactionPicker::for_test(1, config);
            let mut local_stats = LocalPickerStatistic::default();
            let ret = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            ret.add_pending_task(1, &mut levels_handler);
            assert_eq!(
                ret.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                3
            );

            assert_eq!(4, ret.input_levels[0].table_infos[0].get_sst_id());
            assert_eq!(3, ret.input_levels[1].table_infos[0].get_sst_id());
            assert_eq!(1, ret.input_levels[2].table_infos[0].get_sst_id());

            // will pick sst [2, 6, 5]
            let ret2 = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

            assert_eq!(
                ret2.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                3
            );

            assert_eq!(5, ret2.input_levels[0].table_infos[0].get_sst_id());
            assert_eq!(6, ret2.input_levels[1].table_infos[0].get_sst_id());
            assert_eq!(2, ret2.input_levels[2].table_infos[0].get_sst_id());
        }

        {
            let l0 = generate_l0_nonoverlapping_multi_sublevels(vec![
                vec![
                    generate_table(1, 1, 100, 149, 1),
                    generate_table(6, 1, 150, 199, 1),
                    generate_table(7, 1, 200, 250, 1),
                    generate_table(2, 1, 300, 400, 1),
                ],
                vec![
                    generate_table(3, 1, 100, 149, 1),
                    generate_table(8, 1, 150, 199, 1),
                    generate_table(9, 1, 200, 250, 1),
                    generate_table(10, 1, 300, 400, 1),
                ],
                vec![
                    generate_table(4, 1, 100, 199, 1),
                    generate_table(11, 1, 200, 250, 1),
                    generate_table(5, 1, 300, 350, 1),
                ],
            ]);
            let levels = Levels {
                l0: Some(l0),
                levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
                member_table_ids: vec![1],
                ..Default::default()
            };
            let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
            levels_handler[1].add_pending_task(100, 1, levels.levels[0].get_table_infos());
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .level0_sub_level_compact_level_count(1)
                    .build(),
            );
            let mut picker = LevelCompactionPicker::for_test(1, config);
            let mut local_stats = LocalPickerStatistic::default();
            let ret = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();
            ret.add_pending_task(1, &mut levels_handler);
            assert_eq!(
                ret.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                3
            );

            assert_eq!(11, ret.input_levels[0].table_infos[0].get_sst_id());
            assert_eq!(9, ret.input_levels[1].table_infos[0].get_sst_id());
            assert_eq!(7, ret.input_levels[2].table_infos[0].get_sst_id());

            let ret2 = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

            assert_eq!(
                ret2.input_levels
                    .iter()
                    .map(|i| i.table_infos.len())
                    .sum::<usize>(),
                3
            );

            assert_eq!(5, ret2.input_levels[0].table_infos[0].get_sst_id());
            assert_eq!(10, ret2.input_levels[1].table_infos[0].get_sst_id());
            assert_eq!(2, ret2.input_levels[2].table_infos[0].get_sst_id());
        }
    }

    fn is_l0_trivial_move(compaction_input: &CompactionInput) -> bool {
        compaction_input.input_levels.len() == 2
            && !compaction_input.input_levels[0].table_infos.is_empty()
            && compaction_input.input_levels[1].table_infos.is_empty()
    }

    #[test]
    fn test_trivial_move() {
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .target_file_size_base(30)
                .level0_sub_level_compact_level_count(20) // reject intra
                .build(),
        );
        let mut picker = LevelCompactionPicker::for_test(1, config);

        // Cannot trivial move because there is only 1 sub-level.
        let l0 = generate_l0_overlapping_sublevels(vec![vec![
            generate_table(1, 1, 100, 110, 1),
            generate_table(2, 1, 150, 250, 1),
        ]]);
        let levels = Levels {
            l0: Some(l0),
            levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
            member_table_ids: vec![1],
            ..Default::default()
        };
        levels_handler[1].add_pending_task(100, 1, levels.levels[0].get_table_infos());
        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // Cannot trivial move because sub-levels are overlapping
        let l0: OverlappingLevel = generate_l0_overlapping_sublevels(vec![
            vec![
                generate_table(1, 1, 100, 110, 1),
                generate_table(2, 1, 150, 250, 1),
            ],
            vec![generate_table(3, 1, 10, 90, 1)],
            vec![generate_table(4, 1, 10, 90, 1)],
            vec![generate_table(5, 1, 10, 90, 1)],
        ]);
        let mut levels = Levels {
            l0: Some(l0),
            levels: vec![generate_level(1, vec![generate_table(100, 1, 0, 1000, 1)])],
            member_table_ids: vec![1],
            ..Default::default()
        };
        assert!(picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .is_none());

        // Cannot trivial move because latter sub-level is overlapping
        levels.l0.as_mut().unwrap().sub_levels[0].level_type = LevelType::Nonoverlapping as i32;
        levels.l0.as_mut().unwrap().sub_levels[1].level_type = LevelType::Overlapping as i32;
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // Cannot trivial move because former sub-level is overlapping
        levels.l0.as_mut().unwrap().sub_levels[0].level_type = LevelType::Overlapping as i32;
        levels.l0.as_mut().unwrap().sub_levels[1].level_type = LevelType::Nonoverlapping as i32;
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // trivial move
        levels.l0.as_mut().unwrap().sub_levels[0].level_type = LevelType::Nonoverlapping as i32;
        levels.l0.as_mut().unwrap().sub_levels[1].level_type = LevelType::Nonoverlapping as i32;
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert!(is_l0_trivial_move(&ret));
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
    }
}
