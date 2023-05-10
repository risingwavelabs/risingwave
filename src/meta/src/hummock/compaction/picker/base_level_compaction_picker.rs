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

use super::min_overlap_compaction_picker::NonOverlapSubLevelPicker;
use super::{CompactionInput, CompactionPicker, LocalPickerStatistic};
use crate::hummock::compaction::create_overlap_strategy;
use crate::hummock::level_handler::LevelHandler;

pub struct LevelCompactionPicker {
    target_level: usize,
    config: Arc<CompactionConfig>,
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

        debug_assert!(self.target_level == levels.get_level(self.target_level).level_idx as usize);
        self.pick_multi_level_to_base(
            l0,
            levels.get_level(self.target_level),
            level_handlers,
            stats,
        )
    }
}

impl LevelCompactionPicker {
    pub fn new(target_level: usize, config: Arc<CompactionConfig>) -> LevelCompactionPicker {
        LevelCompactionPicker {
            target_level,
            config,
        }
    }

    fn pick_multi_level_to_base(
        &self,
        l0: &OverlappingLevel,
        target_level: &Level,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());
        let min_compaction_bytes = self.config.sub_level_max_compaction_bytes;
        let non_overlap_sub_level_picker = NonOverlapSubLevelPicker::new(
            min_compaction_bytes,
            self.config.max_compaction_bytes,
            self.config.level0_sub_level_compact_level_count as usize,
            // The maximum number of sub_level compact level per task
            self.config.level0_max_compact_file_number,
            overlap_strategy.clone(),
        );

        let l0_select_tables_vec = non_overlap_sub_level_picker
            .pick_l0_multi_non_overlap_level(&l0.sub_levels, &level_handlers[0]);
        if l0_select_tables_vec.is_empty() {
            return None;
        }

        let mut skip_by_pending = false;
        let mut skip_by_write_amp = false;
        let mut input_levels = vec![];
        for input in l0_select_tables_vec {
            let l0_select_tables = input
                .sstable_infos
                .iter()
                .flat_map(|select_tables| select_tables.clone())
                .collect_vec();

            let target_level_ssts = overlap_strategy
                .check_base_level_overlap(&l0_select_tables, &target_level.table_infos);

            let mut target_level_size = 0;
            let mut pending_compact = false;
            for sst in &target_level_ssts {
                if level_handlers[target_level.level_idx as usize].is_pending_compact(&sst.sst_id) {
                    pending_compact = true;
                    break;
                }

                target_level_size += sst.file_size;
            }

            if pending_compact {
                skip_by_pending = true;
                continue;
            }

            if (!target_level_ssts.is_empty() || input.sstable_infos.len() > 1)
                && input.sstable_infos.len()
                    < self.config.level0_sub_level_compact_level_count as usize
                && input.total_file_size < target_level_size
            {
                // not trivial move
                skip_by_write_amp = true;
                continue;
            }

            input_levels.push((input, target_level_size, target_level_ssts));
        }

        if input_levels.is_empty() {
            if skip_by_pending {
                stats.skip_by_pending_files += 1;
            }

            if skip_by_write_amp {
                stats.skip_by_write_amp_limit += 1;
            }

            if skip_by_count {
                stats.skip_by_count_limit += 1;
            }
            return None;
        }

        let mb_size = 1024 * 1024;
        let (min_write_amp_input, target_level_size, _) = input_levels
            .iter()
            .min_by(
                |(input, target_level_size, _), (input2, target_level_size2, _)| {
                    let score1 = *target_level_size / mb_size * (input2.total_file_size / mb_size);
                    let score2 = *target_level_size2 / mb_size * (input.total_file_size / mb_size);
                    score1.cmp(&score2)
                },
            )
            .unwrap();
        let min_write_amp_meet = min_write_amp_input.total_file_size >= *target_level_size;
        for (input, target_file_size, target_level_files) in input_levels {
            if min_write_amp_meet && input.total_file_size < target_file_size {
                continue;
            }

            let mut select_level_inputs = input
                .sstable_infos
                .into_iter()
                .map(|table_infos| InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos,
                })
                .collect_vec();
            select_level_inputs.reverse();
            select_level_inputs.push(InputLevel {
                level_idx: target_level.level_idx,
                level_type: target_level.level_type,
                table_infos: target_level_files,
            });
            return Some(CompactionInput {
                input_levels: select_level_inputs,
                target_level: self.target_level,
                target_sub_level_id: 0,
            });
        }
        None
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
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::{CompactionMode, TierCompactionPicker};

    fn create_compaction_picker_for_test() -> LevelCompactionPicker {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        LevelCompactionPicker::new(1, config)
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
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 4);
        assert_eq!(ret.input_levels[1].table_infos[0].get_sst_id(), 1);

        ret.add_pending_task(0, &mut levels_handler);
        {
            push_table_level0_nonoverlapping(&mut levels, generate_table(6, 1, 100, 200, 2));
            push_table_level0_nonoverlapping(&mut levels, generate_table(7, 1, 301, 333, 4));
            let ret2 = picker
                .pick_compaction(&levels, &levels_handler, &mut local_stats)
                .unwrap();

            assert_eq!(ret2.input_levels[0].table_infos.len(), 1);
            assert_eq!(ret2.input_levels[0].table_infos[0].get_sst_id(), 6);
            assert_eq!(ret2.input_levels[1].table_infos[0].get_sst_id(), 5);
        }

        levels.l0.as_mut().unwrap().sub_levels[0]
            .table_infos
            .retain(|table| table.get_sst_id() != 4);
        levels.l0.as_mut().unwrap().total_file_size -= ret.input_levels[0].table_infos[0].file_size;

        levels_handler[0].remove_task(0);
        levels_handler[1].remove_task(0);

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 3);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 6);
        assert_eq!(ret.input_levels[1].table_infos[0].get_sst_id(), 5);
        assert_eq!(ret.input_levels[2].table_infos.len(), 2);
        assert_eq!(ret.input_levels[2].table_infos[0].get_sst_id(), 3);
        assert_eq!(ret.input_levels[2].table_infos[1].get_sst_id(), 2);
        ret.add_pending_task(1, &mut levels_handler);

        let mut local_stats = LocalPickerStatistic::default();
        // Cannot pick because no idle table in sub-level[0]. (And sub-level[0] is pending
        // actually).
        push_table_level0_overlapping(&mut levels, generate_table(8, 1, 199, 233, 3));
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // Don't pick overlapping sub-level 8
        levels_handler[0].remove_task(1);
        levels_handler[1].remove_task(1);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 3);
        assert_eq!(ret.input_levels[0].table_infos[0].get_sst_id(), 6);
        assert_eq!(ret.input_levels[1].table_infos[0].get_sst_id(), 5);
        assert_eq!(ret.input_levels[2].table_infos.len(), 2);
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
        let mut picker = LevelCompactionPicker::new(1, config);

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
            vec![3,]
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
            table_infos: vec![],
            total_file_size: 0,
            sub_level_id: 0,
            uncompressed_file_size: 0,
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
        ret.add_pending_task(0, &mut levels_handler);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        ret.add_pending_task(1, &mut levels_handler);

        push_tables_level0_nonoverlapping(&mut levels, vec![generate_table(3, 1, 250, 300, 3)]);
        let config: CompactionConfig = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .max_compaction_bytes(1000)
            .sub_level_max_compaction_bytes(150)
            .max_bytes_for_level_multiplier(1)
            .level0_sub_level_compact_level_count(3)
            .build();
        let mut picker =
            TierCompactionPicker::new(Arc::new(config), Arc::new(RangeOverlapStrategy::default()));

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
                total_file_size: 0,
                sub_level_id: 0,
                uncompressed_file_size: 0,
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
        let mut picker = LevelCompactionPicker::new(1, Arc::new(config));

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
                sub_level_id: 0,
                uncompressed_file_size: 900,
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
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();
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
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        // Only include sub-level 0 results will violate MAX_WRITE_AMPLIFICATION.
        // So all sub-levels are included to make write amplification < MAX_WRITE_AMPLIFICATION.
        let mut picker = LevelCompactionPicker::new(1, config);
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
        let mut picker = LevelCompactionPicker::new(1, config);
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
            target_level: 0,
            target_sub_level_id: pending_level.sub_level_id,
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
        let mut picker = LevelCompactionPicker::new(1, config.clone());
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // Free the pending sub-level.
        for pending_task_id in &levels_handler[0].pending_tasks_ids() {
            levels_handler[0].remove_task(*pending_task_id);
        }

        // No more pending sub-level so we can get a task now.
        let mut picker = LevelCompactionPicker::new(1, config);
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
            levels: vec![generate_level(1, vec![generate_table(3, 1, 10, 90, 1)])],
            member_table_ids: vec![1],
            ..Default::default()
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();

        let config = Arc::new(
            CompactionConfigBuilder::new()
                .max_compaction_bytes(500000)
                .level0_sub_level_compact_level_count(2)
                .build(),
        );

        let mut picker = LevelCompactionPicker::new(1, config);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(3, ret.input_levels.len());
        assert_eq!(6, ret.input_levels[0].table_infos[0].sst_id);
        assert_eq!(4, ret.input_levels[1].table_infos[0].sst_id);
        assert_eq!(3, ret.input_levels[2].table_infos[0].sst_id);

        // trivial move do not be limited by level0_sub_level_compact_level_count
        ret.add_pending_task(0, &mut levels_handler);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(2, ret.input_levels.len());
        assert_eq!(5, ret.input_levels[0].table_infos[0].sst_id);
        assert_eq!(1, ret.input_levels[1].level_idx);
        assert!(ret.input_levels[1].table_infos.is_empty());
    }
}
