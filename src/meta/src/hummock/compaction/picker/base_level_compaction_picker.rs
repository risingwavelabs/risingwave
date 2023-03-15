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
use rand::thread_rng;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::HummockLevelsExt;
use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{CompactionConfig, InputLevel, Level, LevelType, OverlappingLevel};

use crate::hummock::compaction::{CompactionInput, CompactionPicker, LocalPickerStatistic};
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

        let is_l0_pending_compact = level_handlers[0].is_level_pending_compact(&l0.sub_levels[0]);
        if !self.config.split_by_state_table && is_l0_pending_compact {
            stats.skip_by_pending_files += 1;
            return None;
        }

        if self.config.split_by_state_table {
            let mut member_table_ids = levels.member_table_ids.clone();
            for level in &l0.sub_levels {
                if level.level_type != LevelType::Nonoverlapping as i32 {
                    continue;
                }
                // expand table id because there may be some state-table drop from member_table_ids.
                for sst in &level.table_infos {
                    if sst.table_ids[0] != *member_table_ids.last().unwrap() {
                        member_table_ids.push(sst.table_ids[0]);
                    }
                }
            }
            member_table_ids.sort();
            member_table_ids.dedup();
            use rand::prelude::SliceRandom;
            member_table_ids.shuffle(&mut thread_rng());
            for table_id in member_table_ids {
                if let Some(ret) = self.pick_files_to_target_level(
                    l0,
                    levels.get_level(self.target_level),
                    level_handlers,
                    Some(table_id),
                    stats,
                ) {
                    return Some(ret);
                }
            }
        }
        self.pick_files_to_target_level(
            l0,
            levels.get_level(self.target_level),
            level_handlers,
            None,
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

    fn pick_files_to_target_level(
        &self,
        l0: &OverlappingLevel,
        target_level: &Level,
        level_handlers: &[LevelHandler],
        table_id: Option<u32>,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let mut input_levels = vec![];
        let mut l0_total_file_size = 0;
        for level in &l0.sub_levels {
            // This break is optional. We can include overlapping sub-level actually.
            if level.level_type() != LevelType::Nonoverlapping {
                break;
            }
            if l0_total_file_size >= self.config.max_compaction_bytes {
                break;
            }

            let mut pending_compact = false;
            let mut cur_level_size = 0;
            let mut select_level = InputLevel {
                level_idx: 0,
                level_type: level.level_type,
                table_infos: vec![],
            };
            for sst in &level.table_infos {
                if table_id.map(|id| sst.table_ids[0] == id).unwrap_or(false) {
                    continue;
                }

                if level_handlers[0].is_pending_compact(&sst.id) {
                    pending_compact = true;
                    break;
                }
                cur_level_size += sst.file_size;
                select_level.table_infos.push(sst.clone());
            }
            if pending_compact {
                break;
            }
            if select_level.table_infos.is_empty() {
                continue;
            }

            l0_total_file_size += cur_level_size;
            input_levels.push(select_level);
        }
        if l0_total_file_size == 0 {
            return None;
        }

        let target_level_files = target_level
            .table_infos
            .iter()
            .filter(|sst| table_id.map(|id| sst.table_ids[0] == id).unwrap_or(true));
        let mut target_level_size = 0;
        for sst in target_level_files.clone() {
            if level_handlers[self.target_level].is_pending_compact(&sst.id) {
                return None;
            }
            target_level_size += sst.file_size;
        }

        // do not compact large task
        if target_level_size > l0_total_file_size
        // && l0_total_file_size < self.config.max_compaction_bytes
        {
            stats.skip_by_write_amp_limit += 1;
            return None;
        }

        // reverse because the ix of low sub-level is smaller.
        input_levels.reverse();
        input_levels.push(InputLevel {
            level_idx: self.target_level as u32,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: target_level_files.cloned().collect_vec(),
        });
        Some(CompactionInput {
            input_levels,
            target_level: self.target_level,
            target_sub_level_id: 0,
        })
    }
}

#[cfg(test)]
pub mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::level_selector::tests::{
        generate_l0_nonoverlapping_sublevels, generate_l0_overlapping_sublevels, generate_level,
        generate_table, push_table_level0_nonoverlapping, push_table_level0_overlapping,
        push_tables_level0_nonoverlapping,
    };
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::{CompactionMode, TierCompactionPicker};

    fn create_compaction_picker_for_test() -> LevelCompactionPicker {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
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
        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 5);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 3);
        ret.add_pending_task(0, &mut levels_handler);

        // Cannot pick because sub-level[0] is pending.
        push_table_level0_nonoverlapping(&mut levels, generate_table(6, 1, 100, 200, 2));
        push_table_level0_nonoverlapping(&mut levels, generate_table(7, 1, 301, 333, 4));
        assert!(picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .is_none());

        levels.l0.as_mut().unwrap().sub_levels[0]
            .table_infos
            .retain(|table| table.id != 4);
        levels.l0.as_mut().unwrap().total_file_size -= ret.input_levels[0].table_infos[0].file_size;

        levels_handler[0].remove_task(0);
        levels_handler[1].remove_task(0);

        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 4);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 7);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 6);
        assert_eq!(ret.input_levels[2].table_infos[0].id, 5);
        assert_eq!(ret.input_levels[3].table_infos.len(), 3);
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
        assert_eq!(ret.input_levels.len(), 4);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 7);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 6);
        assert_eq!(ret.input_levels[2].table_infos[0].id, 5);
        assert_eq!(ret.input_levels[3].table_infos.len(), 3);
    }

    #[test]
    fn test_selecting_key_range_overlap() {
        // When picking L0->L1, all L1 files overlapped with selecting_key_range should be picked.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .compaction_mode(CompactionMode::Range as i32)
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
        push_tables_level0_nonoverlapping(&mut levels, vec![generate_table(1, 1, 50, 60, 2)]);
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

        assert_eq!(ret.input_levels.len(), 3);
        assert_eq!(
            ret.input_levels[1]
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![1]
        );

        assert_eq!(
            ret.input_levels[0]
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![7, 8]
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

        push_tables_level0_nonoverlapping(&mut levels, vec![generate_table(3, 1, 250, 300, 3)]);
        let mut picker = TierCompactionPicker::new(
            picker.config.clone(),
            Arc::new(RangeOverlapStrategy::default()),
        );
        assert!(picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .is_none());
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

    // compact the whole level and upper sub-level when the write-amplification is more than 1.5.
    #[test]
    fn test_compact_whole_level_write_amplification_limit() {
        let config = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .max_compaction_bytes(1000)
            .build();
        let mut picker = LevelCompactionPicker::new(1, Arc::new(config));

        let mut levels = Levels {
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 1, 200, 2),
                    generate_table(2, 1, 201, 300, 2),
                    generate_table(3, 1, 401, 500, 2),
                ],
                total_file_size: 590,
                sub_level_id: 0,
                uncompressed_file_size: 590,
            }],
            l0: Some(generate_l0_nonoverlapping_sublevels(vec![])),
            member_table_ids: vec![1],
            ..Default::default()
        };
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(4, 1, 100, 180, 2),
                generate_table(5, 1, 190, 250, 2),
                generate_table(6, 1, 260, 400, 2),
            ],
        );
        push_tables_level0_nonoverlapping(
            &mut levels,
            vec![
                generate_table(7, 1, 100, 180, 2),
                generate_table(8, 1, 190, 250, 2),
                generate_table(9, 1, 260, 400, 2),
            ],
        );
        let levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let mut local_stats = LocalPickerStatistic::default();
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 3);
        assert_eq!(ret.input_levels[2].table_infos[0].id, 1);
        assert_eq!(ret.input_levels[2].table_infos[1].id, 2);
        assert_eq!(ret.input_levels[2].table_infos[2].id, 3);
        levels.levels[0].table_infos[0].file_size += 1600 - levels.levels[0].total_file_size;
        levels.levels[0].total_file_size = 1600;
        let sub_level = &mut levels.l0.as_mut().unwrap().sub_levels[0];
        sub_level.table_infos[0].file_size += 1000 - sub_level.total_file_size;
        sub_level.total_file_size = 1000;
        levels.l0.as_mut().unwrap().sub_levels[1].total_file_size = 1000;
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 2);
    }

    #[test]
    fn test_skip_compact_write_amplification_limit() {
        let mut picker = create_compaction_picker_for_test();
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
                .build(),
        );
        // Only include sub-level 0 results will violate MAX_WRITE_AMPLIFICATION.
        // So all sub-levels are included to make write amplification < MAX_WRITE_AMPLIFICATION.
        let mut picker = LevelCompactionPicker::new(1, config);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos[0].id, 7);
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
                .build(),
        );
        let mut picker = LevelCompactionPicker::new(1, config);
        let ret = picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos[0].id, 6);
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
                .build(),
        );

        // Only include sub-level 0 results will violate MAX_WRITE_AMPLIFICATION.
        // But stopped by pending sub-level when trying to include more sub-levels.
        let mut picker = LevelCompactionPicker::new(1, config.clone());
        assert!(picker
            .pick_compaction(&levels, &levels_handler, &mut local_stats)
            .is_none());

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
}
