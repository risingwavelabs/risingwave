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
use risingwave_pb::hummock::{
    CompactionConfig, InputLevel, LevelType, OverlappingLevel, SstableInfo,
};

use super::{CompactionInput, CompactionPicker, LocalPickerStatistic, MinOverlappingPicker};
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::picker::min_overlap_compaction_picker::NonOverlapSubLevelPicker;
use crate::hummock::level_handler::LevelHandler;

pub struct TierCompactionPicker {
    config: Arc<CompactionConfig>,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl TierCompactionPicker {
    pub fn new(
        config: Arc<CompactionConfig>,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> TierCompactionPicker {
        TierCompactionPicker {
            config,
            overlap_strategy,
        }
    }

    fn pick_multi_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &LevelHandler,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
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

            let tier_sub_level_compact_level_count =
                self.config.level0_sub_level_compact_level_count as usize;
            // FIXME(li0k): Just workaround, need to use more reasonable way to limit task size
            let max_depth = std::cmp::max(
                (self.config.max_compaction_bytes as f64
                    / self.config.sub_level_max_compaction_bytes as f64) as usize,
                tier_sub_level_compact_level_count + 1,
            );
            let non_overlap_sub_level_picker = NonOverlapSubLevelPicker::new(
                0,
                max_compaction_bytes,
                1,
                max_depth,
                self.config.level0_max_compact_file_number,
                self.overlap_strategy.clone(),
            );

            let l0_select_tables_vec = non_overlap_sub_level_picker
                .pick_l0_multi_non_overlap_level(&l0.sub_levels[idx..], level_handler);

            if l0_select_tables_vec.is_empty() {
                continue;
            }

            let mut skip_by_write_amp = false;
            let mut level_select_tables: Vec<Vec<SstableInfo>> = vec![];
            // Limit the number of selection levels for the non-overlapping
            // sub_level at least level0_sub_level_compact_level_count
            for (plan_index, (compaction_bytes, all_file_count, level_select_sst)) in
                l0_select_tables_vec.into_iter().enumerate()
            {
                if plan_index == 0
                    && level_select_sst.len()
                        < self.config.level0_sub_level_compact_level_count as usize
                {
                    // first plan level count smaller than limit
                    break;
                }

                let mut max_level_size = 0;
                for level_select_table in &level_select_sst {
                    let level_select_size = level_select_table
                        .iter()
                        .map(|sst| sst.file_size)
                        .sum::<u64>();

                    max_level_size = std::cmp::max(max_level_size, level_select_size);
                }

                // This limitation would keep our write-amplification no more than
                // ln(max_compaction_bytes/flush_level_bytes) /
                // ln(self.config.level0_sub_level_compact_level_count/2) Here we only use half
                // of level0_sub_level_compact_level_count just for convenient.
                let is_write_amp_large =
                    max_level_size * self.config.level0_sub_level_compact_level_count as u64 / 2
                        >= compaction_bytes;

                if is_write_amp_large
                    && level_select_sst.len() < tier_sub_level_compact_level_count
                    && all_file_count < self.config.level0_max_compact_file_number as usize
                {
                    skip_by_write_amp = true;
                    continue;
                }

                level_select_tables = level_select_sst;
                break;
            }

            if level_select_tables.is_empty() {
                if skip_by_write_amp {
                    stats.skip_by_write_amp_limit += 1;
                }
                continue;
            }

            let mut select_level_inputs = Vec::with_capacity(level_select_tables.len());
            for level_select_sst in level_select_tables {
                select_level_inputs.push(InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: level_select_sst,
                });
            }

            select_level_inputs.reverse();

            return Some(CompactionInput {
                input_levels: select_level_inputs,
                target_level: 0,
                target_sub_level_id: level.sub_level_id,
            });
        }

        None
    }

    fn pick_trivial_move_file(
        &self,
        l0: &OverlappingLevel,
        level_handlers: &[LevelHandler],
    ) -> Option<CompactionInput> {
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Overlapping as i32 || idx + 1 >= l0.sub_levels.len() {
                continue;
            }

            if l0.sub_levels[idx + 1].level_type == LevelType::Overlapping as i32 {
                continue;
            }

            let min_overlap_picker = MinOverlappingPicker::new(
                0,
                0,
                self.config.sub_level_max_compaction_bytes,
                self.overlap_strategy.clone(),
            );

            let (select_tables, target_tables) = min_overlap_picker.pick_tables(
                &l0.sub_levels[idx + 1].table_infos,
                &level.table_infos,
                level_handlers,
            );

            // only pick tables for trivial move
            if select_tables.is_empty() || !target_tables.is_empty() {
                continue;
            }

            // support trivial move cross multi sub_levels
            let mut overlap = self.overlap_strategy.create_overlap_info();
            for sst in &select_tables {
                overlap.update(sst);
            }

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

            let input_levels = vec![
                InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: select_tables,
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
            });
        }
        None
    }

    fn pick_overlapping_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &LevelHandler,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let l0_level_count = l0.sub_levels.len();
        let overlapping_type = LevelType::Overlapping as i32;
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type != overlapping_type {
                continue;
            }

            if level_handler.is_level_pending_compact(level) {
                continue;
            }

            let mut select_level_inputs = vec![InputLevel {
                level_idx: 0,
                level_type: level.level_type,
                table_infos: level.table_infos.clone(),
            }];

            // We assume that the maximum size of each sub_level is sub_level_max_compaction_bytes,
            // so the design here wants to merge multiple overlapping-levels in one compaction
            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.sub_level_max_compaction_bytes
                    * self.config.level0_overlapping_sub_level_compact_level_count as u64,
            );

            let mut compaction_bytes = level.total_file_size;
            let mut compact_file_count = level.table_infos.len() as u64;
            let mut waiting_enough_files = {
                if compaction_bytes > max_compaction_bytes {
                    false
                } else {
                    compact_file_count <= self.config.level0_max_compact_file_number
                }
            };

            for other in &l0.sub_levels[idx + 1..] {
                if compaction_bytes > max_compaction_bytes {
                    waiting_enough_files = false;
                    break;
                }

                if compact_file_count > self.config.level0_max_compact_file_number {
                    waiting_enough_files = false;
                    break;
                }

                if other.level_type != overlapping_type {
                    waiting_enough_files = false;
                    break;
                }

                if level_handler.is_level_pending_compact(other) {
                    break;
                }

                compaction_bytes += other.total_file_size;
                compact_file_count += other.table_infos.len() as u64;
                select_level_inputs.push(InputLevel {
                    level_idx: 0,
                    level_type: other.level_type,
                    table_infos: other.table_infos.clone(),
                });
            }

            // If waiting_enough_files is not satisfied, we will raise the priority of the number of
            // levels to ensure that we can merge as many sub_levels as possible
            let tier_sub_level_compact_level_count =
                self.config.level0_overlapping_sub_level_compact_level_count as usize;
            let min_level0_compact_file_number = self.config.level0_tier_compact_file_number
                * std::cmp::max(1, l0_level_count / tier_sub_level_compact_level_count) as u64;

            if compact_file_count < min_level0_compact_file_number && waiting_enough_files {
                stats.skip_by_count_limit += 1;
                continue;
            }

            select_level_inputs.reverse();

            return Some(CompactionInput {
                input_levels: select_level_inputs,
                target_level: 0,
                target_sub_level_id: level.sub_level_id,
            });
        }
        None
    }
}

impl CompactionPicker for TierCompactionPicker {
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

        if let Some(ret) = self.pick_trivial_move_file(l0, level_handlers) {
            return Some(ret);
        }

        if let Some(ret) = self.pick_overlapping_level(l0, &level_handlers[0], stats) {
            return Some(ret);
        }
        self.pick_multi_level(l0, &level_handlers[0], stats)
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use risingwave_hummock_sdk::compaction_group::hummock_version_ext::new_sub_level;
    use risingwave_pb::hummock::hummock_version::Levels;
    use risingwave_pb::hummock::{LevelType, OverlappingLevel};

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::level_selector::tests::{
        generate_l0_nonoverlapping_multi_sublevels, generate_l0_overlapping_sublevels,
        generate_table, push_table_level0_overlapping,
    };
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::picker::{
        CompactionInput, CompactionPicker, LocalPickerStatistic, TierCompactionPicker,
    };
    use crate::hummock::level_handler::LevelHandler;

    fn is_l0_trivial_move(compaction_input: &CompactionInput) -> bool {
        compaction_input.input_levels.len() == 2
            && !compaction_input.input_levels[0].table_infos.is_empty()
            && compaction_input.input_levels[1].table_infos.is_empty()
    }

    #[test]
    fn test_trivial_move() {
        let levels_handler = vec![LevelHandler::new(0)];
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .target_file_size_base(30)
                .level0_sub_level_compact_level_count(2)
                .level0_overlapping_sub_level_compact_level_count(4)
                .build(),
        );
        let mut picker =
            TierCompactionPicker::new(config, Arc::new(RangeOverlapStrategy::default()));

        // Cannot trivial move because there is only 1 sub-level.
        let l0 = generate_l0_overlapping_sublevels(vec![vec![
            generate_table(1, 1, 100, 110, 1),
            generate_table(2, 1, 150, 250, 1),
        ]]);
        let levels = Levels {
            l0: Some(l0),
            levels: vec![],
            member_table_ids: vec![1],
            ..Default::default()
        };
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
            levels: vec![],
            member_table_ids: vec![1],
            ..Default::default()
        };
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert!(!is_l0_trivial_move(&ret));

        // Cannot trivial move because latter sub-level is overlapping
        levels.l0.as_mut().unwrap().sub_levels[0].level_type = LevelType::Nonoverlapping as i32;
        levels.l0.as_mut().unwrap().sub_levels[1].level_type = LevelType::Overlapping as i32;
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());

        // Cannot trivial move because former sub-level is overlapping
        levels.l0.as_mut().unwrap().sub_levels[0].level_type = LevelType::Overlapping as i32;
        levels.l0.as_mut().unwrap().sub_levels[1].level_type = LevelType::Nonoverlapping as i32;
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert!(!is_l0_trivial_move(&ret));

        // trivial move
        levels.l0.as_mut().unwrap().sub_levels[0].level_type = LevelType::Nonoverlapping as i32;
        levels.l0.as_mut().unwrap().sub_levels[1].level_type = LevelType::Nonoverlapping as i32;
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert!(is_l0_trivial_move(&ret));
        assert_eq!(ret.input_levels[0].table_infos.len(), 1);
    }

    #[test]
    fn test_pick_whole_level_basic() {
        let l0 = generate_l0_overlapping_sublevels(vec![
            vec![
                generate_table(1, 1, 100, 200, 1),
                generate_table(2, 1, 150, 250, 1),
            ],
            vec![generate_table(3, 1, 10, 90, 1)],
            vec![
                generate_table(4, 1, 100, 200, 1),
                generate_table(5, 1, 50, 150, 1),
            ],
            vec![
                generate_table(6, 1, 100, 200, 1),
                generate_table(7, 1, 50, 150, 1),
            ],
        ]);
        let levels = Levels {
            l0: Some(l0),
            levels: vec![],
            ..Default::default()
        };
        let levels_handler = vec![LevelHandler::new(0)];
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .level0_sub_level_compact_level_count(2)
                .level0_overlapping_sub_level_compact_level_count(4)
                .build(),
        );
        let mut picker =
            TierCompactionPicker::new(config, Arc::new(RangeOverlapStrategy::default()));
        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 4);
        assert_eq!(
            ret.input_levels
                .iter()
                .map(|i| i.table_infos.len())
                .sum::<usize>(),
            7
        );

        let empty_level = Levels {
            l0: Some(generate_l0_overlapping_sublevels(vec![])),
            levels: vec![],
            ..Default::default()
        };
        assert!(picker
            .pick_compaction(&empty_level, &levels_handler, &mut local_stats)
            .is_none());
    }

    #[test]
    fn test_pick_multi_level_basic() {
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
                levels: vec![],
                member_table_ids: vec![1],
                ..Default::default()
            };
            let mut levels_handler = vec![LevelHandler::new(0)];
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .level0_sub_level_compact_level_count(1)
                    .level0_overlapping_sub_level_compact_level_count(4)
                    .build(),
            );
            let mut picker =
                TierCompactionPicker::new(config, Arc::new(RangeOverlapStrategy::default()));
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
                levels: vec![],
                member_table_ids: vec![1],
                ..Default::default()
            };
            let mut levels_handler = vec![LevelHandler::new(0)];
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .level0_sub_level_compact_level_count(1)
                    .build(),
            );
            let mut picker =
                TierCompactionPicker::new(config, Arc::new(RangeOverlapStrategy::default()));
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
                levels: vec![],
                member_table_ids: vec![1],
                ..Default::default()
            };
            let mut levels_handler = vec![LevelHandler::new(0)];
            let config = Arc::new(
                CompactionConfigBuilder::new()
                    .level0_sub_level_compact_level_count(1)
                    .build(),
            );
            let mut picker =
                TierCompactionPicker::new(config, Arc::new(RangeOverlapStrategy::default()));
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

    #[test]
    fn test_pick_whole_level_skip_sublevel() {
        let l0 = generate_l0_overlapping_sublevels(vec![
            vec![
                generate_table(4, 1, 10, 90, 1),
                generate_table(5, 1, 200, 220, 1),
            ],
            vec![generate_table(6, 1, 1, 100, 1)],
            vec![generate_table(7, 1, 1, 100, 1)],
            vec![generate_table(8, 1, 1, 100, 1)],
            vec![generate_table(9, 1, 1, 100, 1)],
        ]);

        let mut levels = Levels {
            l0: Some(l0),
            levels: vec![],
            ..Default::default()
        };
        levels.l0.as_mut().unwrap().sub_levels[0].level_type = LevelType::Nonoverlapping as i32;
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .sub_level_max_compaction_bytes(500)
                .max_compaction_bytes(500000)
                .level0_sub_level_compact_level_count(2)
                .level0_overlapping_sub_level_compact_level_count(4)
                .build(),
        );

        let mut local_stats = LocalPickerStatistic::default();
        // sub-level 0 is excluded because it's nonoverlapping and violating
        // sub_level_max_compaction_bytes.
        let mut picker =
            TierCompactionPicker::new(config, Arc::new(RangeOverlapStrategy::default()));
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 4);
        assert_eq!(ret.target_level, 0);
        assert_eq!(ret.target_sub_level_id, 1);
    }

    #[test]
    fn test_write_amp_bug_skip() {
        let l1 = new_sub_level(
            1,
            LevelType::Nonoverlapping,
            vec![
                generate_table(3, 1, 1, 50, 1),
                generate_table(4, 1, 51, 100, 1),
            ],
        );
        let l2 = new_sub_level(
            2,
            LevelType::Nonoverlapping,
            vec![
                generate_table(3, 1, 1, 50, 1),
                generate_table(4, 1, 51, 200, 1),
            ],
        );
        let levels = Levels {
            l0: Some(OverlappingLevel {
                total_file_size: l1.total_file_size + l2.total_file_size,
                uncompressed_file_size: l1.total_file_size + l2.total_file_size,
                sub_levels: vec![l1, l2],
            }),
            levels: vec![],
            member_table_ids: vec![1],
            ..Default::default()
        };
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(4)
                .sub_level_max_compaction_bytes(100)
                .max_compaction_bytes(500000)
                .build(),
        );
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();
        let mut picker =
            TierCompactionPicker::new(config, Arc::new(RangeOverlapStrategy::default()));
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());
    }

    #[test]
    fn test_pick_overlapping_sublevel_more_than_max_compact_file_number() {
        let l0 = generate_l0_overlapping_sublevels(vec![vec![
            generate_table(4, 1, 10, 90, 1),
            generate_table(5, 1, 200, 220, 1),
            generate_table(6, 1, 1, 100, 1),
            generate_table(7, 1, 1, 100, 1),
            generate_table(8, 1, 1, 100, 1),
            generate_table(9, 1, 1, 100, 1),
            generate_table(10, 1, 1, 100, 1),
        ]]);
        let mut levels = Levels {
            l0: Some(l0),
            levels: vec![],
            ..Default::default()
        };
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .sub_level_max_compaction_bytes(100)
                .max_compaction_bytes(500000)
                .level0_sub_level_compact_level_count(2)
                .level0_max_compact_file_number(3)
                .build(),
        );

        let mut local_stats = LocalPickerStatistic::default();
        let mut picker =
            TierCompactionPicker::new(config, Arc::new(RangeOverlapStrategy::default()));
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(1, ret.input_levels.len());

        push_table_level0_overlapping(&mut levels, generate_table(11, 1, 1, 100, 1));
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(1, ret.input_levels.len());
    }
}
