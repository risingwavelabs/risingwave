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

use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{CompactionConfig, InputLevel, LevelType, OverlappingLevel};

use super::min_overlap_compaction_picker::NonOverlapSubLevelPicker;
use super::{
    CompactionInput, CompactionPicker, CompactionTaskValidator, LocalPickerStatistic,
    ValidationRuleType,
};
use crate::hummock::compaction::create_overlap_strategy;
use crate::hummock::compaction::picker::TrivialMovePicker;
use crate::hummock::level_handler::LevelHandler;

pub struct IntraCompactionPicker {
    config: Arc<CompactionConfig>,
    compaction_task_validator: Arc<CompactionTaskValidator>,
}

impl CompactionPicker for IntraCompactionPicker {
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

        if let Some(ret) = self.pick_l0_intra(l0, &level_handlers[0], stats) {
            return Some(ret);
        }

        self.pick_l0_trivial_move_file(l0, level_handlers, stats)
    }
}

impl IntraCompactionPicker {
    #[cfg(test)]
    pub fn new(config: Arc<CompactionConfig>) -> IntraCompactionPicker {
        IntraCompactionPicker {
            compaction_task_validator: Arc::new(CompactionTaskValidator::new(config.clone())),
            config,
        }
    }

    pub fn new_with_validator(
        config: Arc<CompactionConfig>,
        compaction_task_validator: Arc<CompactionTaskValidator>,
    ) -> IntraCompactionPicker {
        IntraCompactionPicker {
            config,
            compaction_task_validator,
        }
    }

    fn pick_l0_intra(
        &self,
        l0: &OverlappingLevel,
        level_handler: &LevelHandler,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());

        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type() != LevelType::Nonoverlapping
                || level.total_file_size > self.config.sub_level_max_compaction_bytes
            {
                continue;
            }

            if level_handler.is_level_all_pending_compact(level) {
                continue;
            }

            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.sub_level_max_compaction_bytes,
            );

            let non_overlap_sub_level_picker = NonOverlapSubLevelPicker::new(
                self.config.sub_level_max_compaction_bytes / 2,
                max_compaction_bytes,
                self.config.level0_sub_level_compact_level_count as usize,
                self.config.level0_max_compact_file_number,
                overlap_strategy.clone(),
            );

            let l0_select_tables_vec = non_overlap_sub_level_picker
                .pick_l0_multi_non_overlap_level(&l0.sub_levels[idx..], level_handler);

            if l0_select_tables_vec.is_empty() {
                continue;
            }

            let mut select_input_size = 0;
            let mut total_file_count = 0;
            for input in l0_select_tables_vec {
                let mut max_level_size = 0;
                for level_select_table in &input.sstable_infos {
                    let level_select_size = level_select_table
                        .iter()
                        .map(|sst| sst.file_size)
                        .sum::<u64>();

                    max_level_size = std::cmp::max(max_level_size, level_select_size);
                }

                let mut select_level_inputs = Vec::with_capacity(input.sstable_infos.len());
                for level_select_sst in input.sstable_infos {
                    if level_select_sst.is_empty() {
                        continue;
                    }
                    select_level_inputs.push(InputLevel {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: level_select_sst,
                    });

                    select_input_size += input.total_file_size;
                    total_file_count += input.total_file_count;
                }
                select_level_inputs.reverse();

                let result = CompactionInput {
                    input_levels: select_level_inputs,
                    target_sub_level_id: level.sub_level_id,
                    select_input_size,
                    total_file_count: total_file_count as u64,
                    ..Default::default()
                };

                if !self.compaction_task_validator.valid_compact_task(
                    &result,
                    ValidationRuleType::Intra,
                    stats,
                ) {
                    continue;
                }

                return Some(result);
            }
        }

        None
    }

    fn pick_l0_trivial_move_file(
        &self,
        l0: &OverlappingLevel,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());

        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Overlapping as i32 || idx + 1 >= l0.sub_levels.len() {
                continue;
            }

            if l0.sub_levels[idx + 1].level_type == LevelType::Overlapping as i32 {
                continue;
            }

            let trivial_move_picker = TrivialMovePicker::new(0, 0, overlap_strategy.clone());

            let select_sst = trivial_move_picker.pick_trivial_move_sst(
                &l0.sub_levels[idx + 1].table_infos,
                &level.table_infos,
                level_handlers,
                stats,
            );

            // only pick tables for trivial move
            if select_sst.is_none() {
                continue;
            }

            let select_sst = select_sst.unwrap();

            // support trivial move cross multi sub_levels
            let mut overlap = overlap_strategy.create_overlap_info();
            overlap.update(&select_sst);

            assert!(overlap
                .check_multiple_overlap(&l0.sub_levels[idx].table_infos)
                .is_empty());
            let mut target_level_idx = idx;
            while target_level_idx > 0 {
                if l0.sub_levels[target_level_idx - 1].level_type
                    != LevelType::Nonoverlapping as i32
                    || !overlap
                        .check_multiple_overlap(&l0.sub_levels[target_level_idx - 1].table_infos)
                        .is_empty()
                {
                    break;
                }
                target_level_idx -= 1;
            }

            let select_input_size = select_sst.file_size;
            let input_levels = vec![
                InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: vec![select_sst],
                },
                InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: vec![],
                },
            ];
            return Some(CompactionInput {
                input_levels,
                target_level: 0,
                target_sub_level_id: l0.sub_levels[target_level_idx].sub_level_id,
                select_input_size,
                total_file_count: 1,
                ..Default::default()
            });
        }
        None
    }
}

#[cfg(test)]
pub mod tests {
    use risingwave_pb::hummock::Level;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::selector::tests::{
        generate_l0_nonoverlapping_multi_sublevels, generate_l0_nonoverlapping_sublevels,
        generate_l0_overlapping_sublevels, generate_level, generate_table,
        push_table_level0_overlapping, push_tables_level0_nonoverlapping,
    };
    use crate::hummock::compaction::TierCompactionPicker;

    fn create_compaction_picker_for_test() -> IntraCompactionPicker {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .level0_sub_level_compact_level_count(1)
                .build(),
        );
        IntraCompactionPicker::new(config)
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
            let mut picker = IntraCompactionPicker::new(config);
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
            let mut picker = IntraCompactionPicker::new(config);
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
            let mut picker = IntraCompactionPicker::new(config);
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
        let mut picker = IntraCompactionPicker::new(config);

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
