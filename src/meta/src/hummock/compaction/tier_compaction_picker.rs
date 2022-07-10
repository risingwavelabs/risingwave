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

use std::sync::Arc;

use risingwave_pb::hummock::hummock_version::Levels;
use risingwave_pb::hummock::{
    CompactionConfig, Level, LevelType, OverlappingLevel, SstableInfo,
};

use crate::hummock::compaction::min_overlap_compaction_picker::MinOverlappingPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::{CompactionInput, CompactionPicker};
use crate::hummock::level_handler::LevelHandler;

pub struct TierCompactionPicker {
    compact_task_id: u64,
    config: Arc<CompactionConfig>,
    overlap_strategy: Arc<dyn OverlapStrategy>,
}

impl TierCompactionPicker {
    pub fn new(
        compact_task_id: u64,
        config: Arc<CompactionConfig>,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> TierCompactionPicker {
        TierCompactionPicker {
            compact_task_id,
            config,
            overlap_strategy,
        }
    }
}
impl TierCompactionPicker {
    fn pick_overlapping_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &mut LevelHandler,
    ) -> Option<CompactionInput> {
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Nonoverlapping as i32 {
                continue;
            }

            if level
                .table_infos
                .iter()
                .any(|table| level_handler.is_pending_compact(&table.id))
            {
                continue;
            }

            let mut compaction_bytes = level.total_file_size;
            let mut select_level_inputs = vec![level.clone()];
            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.max_bytes_for_level_base,
            );

            for other in &l0.sub_levels[idx + 1..] {
                if compaction_bytes >= max_compaction_bytes {
                    break;
                }

                if level.level_type == LevelType::Nonoverlapping as i32 {
                    break;
                }

                if other
                    .table_infos
                    .iter()
                    .any(|table| level_handler.is_pending_compact(&table.id))
                {
                    continue;
                }

                compaction_bytes += other.total_file_size;
                select_level_inputs.push(other.clone());
            }

            if select_level_inputs
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>()
                < self.config.level0_tier_compact_file_number as usize
            {
                continue;
            }

            for input_level in &select_level_inputs {
                level_handler.add_pending_task(self.compact_task_id, &input_level.table_infos);
            }

            return Some(CompactionInput {
                input_levels: select_level_inputs,
                target_level: 0,
                target_sub_level: idx,
            });
        }
        None
    }

    fn pick_sharding_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &mut LevelHandler,
    ) -> Option<CompactionInput> {
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Overlapping as i32
                || level.total_file_size > self.config.sub_level_max_compaction_bytes
            {
                continue;
            }

            if level
                .table_infos
                .iter()
                .any(|table| level_handler.is_pending_compact(&table.id))
            {
                continue;
            }

            let mut compaction_bytes = level.total_file_size;
            let mut select_level_inputs = vec![level.clone()];
            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.sub_level_max_compaction_bytes,
            );

            for other in &l0.sub_levels[idx + 1..] {
                if compaction_bytes >= max_compaction_bytes {
                    break;
                }

                if level.total_file_size > self.config.sub_level_max_compaction_bytes {
                    break;
                }

                if other
                    .table_infos
                    .iter()
                    .any(|table| level_handler.is_pending_compact(&table.id))
                {
                    continue;
                }

                compaction_bytes += other.total_file_size;
                select_level_inputs.push(other.clone());
            }

            if select_level_inputs
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>()
                < self.config.level0_tier_compact_file_number as usize
            {
                continue;
            }

            if select_level_inputs.len() < self.config.level0_tier_compact_file_number as usize / 2
            {
                continue;
            }

            for input_level in &select_level_inputs {
                level_handler.add_pending_task(self.compact_task_id, &input_level.table_infos);
            }

            return Some(CompactionInput {
                input_levels: select_level_inputs,
                target_level: 0,
                target_sub_level: idx,
            });
        }
        None
    }

    fn pick_one_table(
        &self,
        l0: &OverlappingLevel,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput> {
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Overlapping as i32 || level.table_infos.len() < 2 {
                continue;
            }
            if idx + 1 >= l0.sub_levels.len() {
                break;
            }
            if l0.sub_levels[idx + 1].level_type == LevelType::Overlapping as i32
                || l0.sub_levels[idx + 1].table_infos.len() < 2
            {
                continue;
            }

            let min_overlap_picker = MinOverlappingPicker::new(
                self.compact_task_id,
                0,
                0,
                self.overlap_strategy.clone(),
            );

            let (select_tables, target_tables) = min_overlap_picker.pick_tables(
                &l0.sub_levels[idx + 1].table_infos,
                &level.table_infos,
                level_handlers,
            );
            if select_tables.is_empty() {
                continue;
            }
            if target_tables
                .iter()
                .map(|table| table.file_size)
                .sum::<u64>()
                > select_tables
                    .iter()
                    .map(|table| table.file_size)
                    .sum::<u64>()
                    * 2
            {
                continue;
            }
            level_handlers[0].add_pending_task(self.compact_task_id, &select_tables);
            level_handlers[0].add_pending_task(self.compact_task_id, &target_tables);
            return Some(CompactionInput {
                input_levels: vec![
                    Level {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: select_tables,
                        total_file_size: 0,
                    },
                    Level {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: target_tables,
                        total_file_size: 0,
                    },
                ],
                target_level: 0,
                target_sub_level: idx,
            });
        }
        None
    }
}

impl CompactionPicker for TierCompactionPicker {
    fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput> {
        let l0 = levels.l0.as_ref().unwrap();
        if l0.sub_levels.is_empty() {
            return None;
        }
        if let Some(input) = self.pick_overlapping_level(l0, &mut level_handlers[0]) {
            return Some(input);
        }

        if let Some(input) = self.pick_one_table(l0, level_handlers) {
            return Some(input);
        }

        if l0.sub_levels[0].total_file_size < self.config.sub_level_max_compaction_bytes {
            if let Some(input) = self.pick_sharding_level(l0, &mut level_handlers[0]) {
                return Some(input);
            }
        }
        None
    }
}

pub struct LevelCompactionPicker {
    compact_task_id: u64,
    target_level: usize,
    overlap_strategy: Arc<dyn OverlapStrategy>,
    config: Arc<CompactionConfig>,
}

impl CompactionPicker for LevelCompactionPicker {
    fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &mut [LevelHandler],
    ) -> Option<CompactionInput> {
        let select_level = 0;
        let target_level = self.target_level;
        let next_task_id = self.compact_task_id;

        let (select_level_inputs, target_level_inputs) = self.select_input_files(
            &levels.l0.as_ref().unwrap().sub_levels,
            &levels.levels[target_level - 1],
            level_handlers,
        );
        if select_level_inputs.is_empty() {
            return None;
        }

        level_handlers[select_level].add_pending_task(next_task_id, &select_level_inputs);
        level_handlers[target_level].add_pending_task(next_task_id, &target_level_inputs);

        Some(CompactionInput {
            input_levels: vec![
                Level {
                    level_idx: select_level as u32,
                    level_type: LevelType::Overlapping as i32,
                    total_file_size: select_level_inputs
                        .iter()
                        .map(|table| table.file_size)
                        .sum(),
                    table_infos: select_level_inputs,
                },
                Level {
                    level_idx: target_level as u32,
                    level_type: LevelType::Nonoverlapping as i32,
                    total_file_size: target_level_inputs
                        .iter()
                        .map(|table| table.file_size)
                        .sum(),
                    table_infos: target_level_inputs,
                },
            ],
            target_level: self.target_level,
            target_sub_level: 0,
        })
    }
}

impl LevelCompactionPicker {
    pub fn new(
        compact_task_id: u64,
        target_level: usize,
        config: Arc<CompactionConfig>,
        overlap_strategy: Arc<dyn OverlapStrategy>,
    ) -> LevelCompactionPicker {
        LevelCompactionPicker {
            compact_task_id,
            target_level,
            overlap_strategy,
            config,
        }
    }

    fn select_input_files(
        &self,
        l0_sub_levels: &[Level],
        target_level: &Level,
        level_handlers: &mut [LevelHandler],
    ) -> (Vec<SstableInfo>, Vec<SstableInfo>) {
        if l0_sub_levels.is_empty() || l0_sub_levels[0].table_infos.is_empty() {
            return (vec![], vec![]);
        }
        let min_overlap_picker = MinOverlappingPicker::new(
            self.compact_task_id,
            0,
            self.target_level,
            self.overlap_strategy.clone(),
        );
        min_overlap_picker.pick_tables(
            &l0_sub_levels[0].table_infos,
            &target_level.table_infos,
            level_handlers,
        )
    }
}

#[cfg(test)]
pub mod tests {
    use itertools::Itertools;
    use risingwave_pb::hummock::KeyRange;

    use super::*;
    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::overlap_strategy::RangeOverlapStrategy;
    use crate::hummock::compaction::CompactionMode;
    use crate::hummock::test_utils::iterator_test_key_of_epoch;

    pub fn generate_table(
        id: u64,
        table_prefix: u64,
        left: usize,
        right: usize,
        epoch: u64,
    ) -> SstableInfo {
        SstableInfo {
            id,
            key_range: Some(KeyRange {
                left: iterator_test_key_of_epoch(table_prefix, left, epoch),
                right: iterator_test_key_of_epoch(table_prefix, right, epoch),
                inf: false,
            }),
            file_size: (right - left + 1) as u64,
            table_ids: vec![],
        }
    }

    fn create_compaction_picker_for_test() -> LevelCompactionPicker {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(4)
                .min_compaction_bytes(0)
                .max_bytes_for_level_base(50)
                .build(),
        );
        LevelCompactionPicker::new(0, 1, config, Arc::new(RangeOverlapStrategy::default()))
    }

    #[test]
    fn test_compact_l0_to_l1() {
        let picker = create_compaction_picker_for_test();
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(5, 1, 201, 300, 2),
                    generate_table(4, 1, 112, 200, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(3, 1, 0, 100, 1),
                    generate_table(2, 1, 111, 200, 1),
                    generate_table(1, 1, 222, 300, 1),
                    generate_table(0, 1, 301, 400, 1),
                ],
                total_file_size: 0,
            },
        ];
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 1);
        assert_eq!(ret.select_level.table_infos[0].id, 5);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);
        assert_eq!(ret.target_level.table_infos[0].id, 1);

        // no conflict with the last job
        levels[0]
            .table_infos
            .push(generate_table(6, 1, 301, 333, 4));
        levels[0]
            .table_infos
            .push(generate_table(7, 1, 100, 200, 2));
        // pick table 4/7. but skip table 6 because [0_key_test_000100, 1_key_test_000333] will
        // be conflict with the previous job.
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 3);
        assert_eq!(levels_handler[1].get_pending_file_count(), 3);
        assert_eq!(ret.select_level.table_infos.len(), 2);
        assert_eq!(ret.target_level.table_infos.len(), 2);
        assert_eq!(ret.select_level.table_infos[0].id, 4);
        assert_eq!(ret.select_level.table_infos[1].id, 7);
        assert_eq!(ret.target_level.table_infos[0].id, 3);
        assert_eq!(ret.target_level.table_infos[1].id, 2);

        // the first idle table in L0 is table 6 and its confict with the last job so we can not
        // pick table 7.
        let picker = LevelCompactionPicker::new(
            1,
            1,
            Arc::new(CompactionConfigBuilder::new().build()),
            Arc::new(RangeOverlapStrategy::default()),
        );
        levels[0]
            .table_infos
            .push(generate_table(8, 1, 199, 233, 3));
        let ret = picker.pick_compaction(&levels, &mut levels_handler);
        assert!(ret.is_none());

        // compact L0 to L0
        let config = CompactionConfigBuilder::new()
            .level0_tier_compact_file_number(2)
            .build();
        let picker = TierCompactionPicker::new(
            2,
            Arc::new(config),
            Arc::new(RangeOverlapStrategy::default()),
        );
        levels[0]
            .table_infos
            .push(generate_table(9, 1, 100, 400, 3));
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.table_infos.len(), 2);
        assert_eq!(ret.select_level.table_infos[0].id, 8);
        assert_eq!(ret.select_level.table_infos[1].id, 9);
        assert!(ret.target_level.table_infos.is_empty());
        levels_handler[0].remove_task(1);
        levels[0].table_infos.retain(|table| table.id < 7);
        levels[0]
            .table_infos
            .push(generate_table(10, 1, 100, 200, 3));
        levels[0]
            .table_infos
            .push(generate_table(11, 1, 201, 300, 3));
        levels[0]
            .table_infos
            .push(generate_table(12, 1, 301, 400, 3));
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.table_infos.len(), 2);
        assert_eq!(ret.select_level.table_infos[0].id, 6);
        assert_eq!(ret.select_level.table_infos[1].id, 12);
    }

    #[test]
    fn test_selecting_key_range_overlap() {
        // When picking L0->L1, all L1 files overlapped with selecting_key_range should be picked.
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .min_compaction_bytes(0)
                .compaction_mode(CompactionMode::Range as i32)
                .build(),
        );
        let picker =
            LevelCompactionPicker::new(0, 1, config, Arc::new(RangeOverlapStrategy::default()));

        let levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 200, 2),
                    generate_table(2, 1, 400, 500, 2),
                    generate_table(7, 1, 200, 250, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,

                level_type: LevelType::Nonoverlapping as i32,

                table_infos: vec![
                    generate_table(3, 1, 0, 50, 1),
                    generate_table(4, 1, 150, 200, 1),
                    generate_table(5, 1, 250, 300, 1),
                    generate_table(6, 1, 1000, 2000, 1),
                ],
                total_file_size: 0,
            },
        ];

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 2);

        assert_eq!(levels_handler[1].get_pending_file_count(), 2);

        assert_eq!(
            ret.select_level
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![1, 7]
        );

        assert_eq!(
            ret.target_level
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![4, 5]
        );
    }

    #[test]
    fn test_compacting_key_range_overlap_l0() {
        // When picking L0->L1, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let picker = create_compaction_picker_for_test();
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 300, 2),
                    generate_table(2, 1, 250, 500, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![],
                total_file_size: 0,
            },
        ];

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let _ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 2);
        assert_eq!(levels_handler[1].get_pending_file_count(), 0);

        levels[0]
            .table_infos
            .push(generate_table(3, 1, 250, 300, 3));

        assert!(picker
            .pick_compaction(&levels, &mut levels_handler)
            .is_none());
    }

    #[test]
    fn test_compacting_key_range_overlap_l1() {
        // When picking L0->L1, L0's selecting_key_range should not be overlapped with any L1 files
        // under compaction.
        let picker = create_compaction_picker_for_test();

        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![generate_table(1, 1, 200, 250, 2)],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(2, 1, 150, 300, 2)],
                total_file_size: 0,
            },
        ];

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let _ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 1);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);

        levels[0]
            .table_infos
            .push(generate_table(3, 1, 100, 140, 3));

        levels[0]
            .table_infos
            .push(generate_table(4, 1, 400, 500, 3));

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        // Will be trival move. The second file can not be picked up because the range of files
        // [3,4] would be overlap with file [0]
        assert!(ret.target_level.table_infos.is_empty());
        assert_eq!(ret.target_level.level_idx, 1);
        assert_eq!(
            ret.select_level
                .table_infos
                .iter()
                .map(|t| t.id)
                .collect_vec(),
            vec![3]
        );
    }

    #[test]
    fn test_compacting_key_range_overlap_intra_l0() {
        // When picking L0->L0, L0's selecting_key_range should not be overlapped with L0's
        // compacting_key_range.
        let picker = create_compaction_picker_for_test();
        let mut levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 100, 190, 2),
                    generate_table(2, 1, 190, 250, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(3, 1, 200, 300, 2)],
                total_file_size: 0,
            },
        ];

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let _ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();

        assert_eq!(levels_handler[0].get_pending_file_count(), 2);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);

        levels[0]
            .table_infos
            .push(generate_table(4, 1, 170, 180, 3));

        assert!(picker
            .pick_compaction(&levels, &mut levels_handler)
            .is_none());
    }

    #[test]
    fn test_compact_with_write_amplification_limit() {
        let picker = create_compaction_picker_for_test();
        let levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 130, 190, 2),
                    generate_table(2, 1, 190, 250, 2),
                    generate_table(3, 1, 200, 300, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 100, 199, 2),
                    generate_table(5, 1, 200, 260, 2),
                    generate_table(6, 1, 300, 600, 2),
                ],
                total_file_size: 0,
            },
        ];
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];

        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 2);
        assert_eq!(levels_handler[1].get_pending_file_count(), 2);
        assert_eq!(ret.select_level.table_infos[0].id, 1);
        assert_eq!(ret.select_level.table_infos[1].id, 2);
        assert_eq!(ret.target_level.table_infos[0].id, 4);
        assert_eq!(ret.target_level.table_infos[1].id, 5);
    }

    #[test]
    fn test_compact_with_write_amplification_limit_2() {
        let config = Arc::new(
            CompactionConfigBuilder::new()
                .level0_tier_compact_file_number(2)
                .min_compaction_bytes(32)
                .max_bytes_for_level_base(32)
                .build(),
        );
        let picker =
            LevelCompactionPicker::new(0, 1, config, Arc::new(RangeOverlapStrategy::default()));

        let levels = vec![
            Level {
                level_idx: 0,
                level_type: LevelType::Overlapping as i32,
                table_infos: vec![
                    generate_table(1, 1, 10, 16, 2),
                    generate_table(2, 1, 48, 64, 2),
                    generate_table(3, 1, 5, 15, 2),
                ],
                total_file_size: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(4, 1, 10, 50, 2),
                    generate_table(5, 1, 64, 256, 2),
                    generate_table(6, 1, 320, 900, 2),
                ],
                total_file_size: 0,
            },
        ];

        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.select_level.table_infos.len(), 3);
        assert_eq!(ret.target_level.table_infos.len(), 2);
    }
}
