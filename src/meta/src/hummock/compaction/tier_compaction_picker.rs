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
    CompactionConfig, InputLevel, Level, LevelType, OverlappingLevel, SstableInfo,
};

use crate::hummock::compaction::min_overlap_compaction_picker::MinOverlappingPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::{CompactionInput, CompactionPicker};
use crate::hummock::level_handler::LevelHandler;

fn cal_file_size(table_infos: &[SstableInfo]) -> u64 {
    table_infos.iter().map(|table| table.file_size).sum::<u64>()
}

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

            if level_handler.is_level_pending_compact(level) {
                continue;
            }

            let mut compaction_bytes = level.total_file_size;
            let mut select_level_inputs = vec![InputLevel {
                table_infos: level.table_infos.clone(),
                level_idx: 0,
                level_type: level.level_type,
            }];
            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.sub_level_max_compaction_bytes,
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
                    break;
                }

                compaction_bytes += other.total_file_size;
                select_level_inputs.push(InputLevel {
                    table_infos: other.table_infos.clone(),
                    level_idx: 0,
                    level_type: other.level_type,
                });
            }

            if select_level_inputs
                .iter()
                .map(|level| level.table_infos.len())
                .sum::<usize>()
                < self.config.level0_tier_compact_file_number as usize
            {
                continue;
            }

            for input_level in &mut select_level_inputs {
                level_handler.add_pending_task(self.compact_task_id, 0, &input_level.table_infos);
                input_level.level_idx = 0;
            }

            return Some(CompactionInput {
                input_levels: select_level_inputs,
                target_level: 0,
                target_sub_level_id: level.sub_level_id,
            });
        }
        None
    }

    fn pick_sharding_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &mut LevelHandler,
    ) -> Option<CompactionInput> {
        // do not pick the first sub-level because we do not want to block the level compaction.
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == LevelType::Overlapping as i32
                || level.total_file_size > self.config.max_bytes_for_level_base
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
            let mut select_level_inputs = vec![InputLevel {
                level_idx: 0,
                level_type: level.level_type,
                table_infos: level.table_infos.clone(),
            }];
            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.max_bytes_for_level_base,
            );

            for other in &l0.sub_levels[idx + 1..] {
                if compaction_bytes >= max_compaction_bytes {
                    break;
                }

                if level.total_file_size > self.config.max_bytes_for_level_base {
                    break;
                }

                if other
                    .table_infos
                    .iter()
                    .any(|table| level_handler.is_pending_compact(&table.id))
                {
                    break;
                }

                compaction_bytes += other.total_file_size;
                select_level_inputs.push(InputLevel {
                    level_idx: 0,
                    level_type: other.level_type,
                    table_infos: other.table_infos.clone(),
                });
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
                level_handler.add_pending_task(self.compact_task_id, 0, &input_level.table_infos);
            }

            return Some(CompactionInput {
                input_levels: select_level_inputs,
                target_level: 0,
                target_sub_level_id: level.sub_level_id,
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
            if level.level_type == LevelType::Overlapping as i32 || idx + 1 >= l0.sub_levels.len() {
                continue;
            }

            if l0.sub_levels[idx + 1].level_type == LevelType::Overlapping as i32 {
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

            // only pick tables for trivial move
            if select_tables.is_empty() || !target_tables.is_empty() {
                continue;
            }

            level_handlers[0].add_pending_task(self.compact_task_id, 0, &select_tables);
            let mut input_levels = vec![];
            input_levels.push(InputLevel {
                level_idx: 0,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: select_tables,
            });
            input_levels.push(InputLevel {
                level_idx: 0,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: target_tables,
            });
            return Some(CompactionInput {
                input_levels,
                target_level: 0,
                target_sub_level_id: level.sub_level_id,
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

        self.pick_sharding_level(l0, &mut level_handlers[0])
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

        let l0 = levels.l0.as_ref().unwrap();
        if l0.sub_levels.is_empty()
            || l0.sub_levels[0].level_type != LevelType::Nonoverlapping as i32
        {
            return None;
        }

        if level_handlers[select_level].is_level_pending_compact(&l0.sub_levels[0]) {
            return None;
        }

        if levels.levels[target_level - 1].table_infos.is_empty() {
            level_handlers[select_level].add_pending_task(
                next_task_id,
                target_level,
                &l0.sub_levels[0].table_infos,
            );
            return Some(CompactionInput {
                input_levels: vec![
                    InputLevel {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: l0.sub_levels[0].table_infos.clone(),
                    },
                    InputLevel {
                        level_idx: target_level as u32,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: vec![],
                    },
                ],
                target_level: self.target_level,
                target_sub_level_id: 0,
            });
        }

        let mut input_levels = self.pick_min_overlap_tables(l0, &levels.levels, level_handlers);

        if input_levels.is_empty() {
            return None;
        }

        if input_levels[0].table_infos.len() == l0.sub_levels[0].table_infos.len()
            && !input_levels[1].table_infos.is_empty()
        {
            let mut min_write_amplification = cal_file_size(&input_levels[1].table_infos) * 100
                / cal_file_size(&input_levels[0].table_infos);
            let mut info = self.overlap_strategy.create_overlap_info();
            for table in input_levels[0].table_infos.iter() {
                info.update(table);
            }
            let mut l0_total_file_size = l0.sub_levels[0].total_file_size;
            for level in l0.sub_levels[1..].iter() {
                if l0_total_file_size > self.config.max_bytes_for_level_base {
                    break;
                }
                if level_handlers[0].is_level_pending_compact(level) {
                    break;
                }
                for table in level.table_infos.iter() {
                    info.update(table);
                }
                l0_total_file_size += level.total_file_size;
                let target_overlap_files =
                    info.check_multiple_overlap(&levels.levels[target_level - 1].table_infos);
                if target_overlap_files
                    .iter()
                    .any(|table| level_handlers[target_level].is_pending_compact(&table.id))
                {
                    break;
                }
                let target_file_size = cal_file_size(&target_overlap_files);
                let write_amplification = target_file_size * 100 / l0_total_file_size;
                if write_amplification < min_write_amplification {
                    min_write_amplification = write_amplification;
                    // pop target level
                    input_levels.pop().unwrap();
                    input_levels.push(InputLevel {
                        level_idx: 0,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: level.table_infos.to_vec(),
                    });
                    input_levels.push(InputLevel {
                        level_idx: target_level as u32,
                        level_type: LevelType::Nonoverlapping as i32,
                        table_infos: target_overlap_files,
                    });
                } else {
                    break;
                }
            }
        }

        for input_level in &input_levels {
            level_handlers[input_level.level_idx as usize].add_pending_task(
                self.compact_task_id,
                target_level,
                &input_level.table_infos,
            );
        }

        Some(CompactionInput {
            input_levels,
            target_level: self.target_level,
            target_sub_level_id: 0,
        })
    }
}

impl LevelCompactionPicker {
    pub fn new(
        compact_task_id: u64,
        target_level: usize,
        overlap_strategy: Arc<dyn OverlapStrategy>,
        config: Arc<CompactionConfig>,
    ) -> LevelCompactionPicker {
        LevelCompactionPicker {
            compact_task_id,
            target_level,
            overlap_strategy,
            config,
        }
    }

    fn pick_min_overlap_tables(
        &self,
        l0: &OverlappingLevel,
        levels: &[Level],
        level_handlers: &mut [LevelHandler],
    ) -> Vec<InputLevel> {
        let mut input_levels = vec![];
        if levels[self.target_level - 1].total_file_size < self.config.max_bytes_for_level_base
            && !level_handlers[self.target_level]
                .is_level_pending_compact(&levels[self.target_level - 1])
        {
            input_levels.push(InputLevel {
                level_idx: 0,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: l0.sub_levels[0].table_infos.clone(),
            });
            input_levels.push(InputLevel {
                level_idx: self.target_level as u32,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: levels[self.target_level - 1].table_infos.clone(),
            });
            return input_levels;
        }
        let mut l0_total_file_size = 0;
        let mut min_write_amplification = 0;
        for (idx, table) in l0.sub_levels[0].table_infos.iter().enumerate() {
            l0_total_file_size += table.file_size;
            let target_overlap_files = self.overlap_strategy.check_base_level_overlap(
                &l0.sub_levels[0].table_infos[..(idx + 1)],
                &levels[self.target_level - 1].table_infos,
            );
            if target_overlap_files.is_empty() {
                input_levels.push(InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: l0.sub_levels[0].table_infos.clone(),
                });
                input_levels.push(InputLevel {
                    level_idx: self.target_level as u32,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: vec![],
                });
                break;
            }
            if target_overlap_files
                .iter()
                .any(|table| level_handlers[self.target_level].is_pending_compact(&table.id))
            {
                break;
            }
            let target_file_size = cal_file_size(&target_overlap_files);
            let write_amplification = target_file_size * 100 / l0_total_file_size;
            if input_levels.is_empty() || write_amplification < min_write_amplification {
                min_write_amplification = write_amplification;
                input_levels.clear();
                input_levels.push(InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: l0.sub_levels[0].table_infos[..(idx + 1)].to_vec(),
                });
                input_levels.push(InputLevel {
                    level_idx: self.target_level as u32,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: target_overlap_files,
                });
            }
        }
        input_levels
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

    pub fn push_table_level0(levels: &mut Levels, sst: SstableInfo) {
        levels.l0.as_mut().unwrap().sub_levels.push(Level {
            level_idx: 0,
            level_type: LevelType::Overlapping as i32,
            total_file_size: sst.file_size,
            sub_level_id: sst.id,
            table_infos: vec![sst],
        });
    }

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
        LevelCompactionPicker::new(0, 1, Arc::new(RangeOverlapStrategy::default()))
    }

    #[test]
    fn test_compact_l0_to_l1() {
        let picker = create_compaction_picker_for_test();
        let mut levels = Levels {
            l0: Some(OverlappingLevel {
                sub_levels: vec![Level {
                    level_idx: 0,
                    level_type: LevelType::Overlapping as i32,
                    table_infos: vec![
                        generate_table(5, 1, 201, 300, 2),
                        generate_table(4, 1, 112, 200, 2),
                    ],
                    total_file_size: 0,
                    sub_level_id: 0,
                }],
                total_file_size: 0,
            }),
            levels: vec![Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![
                    generate_table(3, 1, 0, 100, 1),
                    generate_table(2, 1, 111, 200, 1),
                    generate_table(1, 1, 222, 300, 1),
                    generate_table(0, 1, 301, 400, 1),
                ],
                total_file_size: 0,
                sub_level_id: 0,
            }],
        };
        let mut levels_handler = vec![LevelHandler::new(0), LevelHandler::new(1)];
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 1);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 5);
        assert_eq!(levels_handler[1].get_pending_file_count(), 1);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 1);

        // no conflict with the last job
        push_table_level0(&mut levels, generate_table(6, 1, 301, 333, 4));
        push_table_level0(&mut levels, generate_table(7, 1, 100, 200, 2));
        // pick table 4/7. but skip table 6 because [0_key_test_000100, 1_key_test_000333] will
        // be conflict with the previous job.
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(levels_handler[0].get_pending_file_count(), 3);
        assert_eq!(levels_handler[1].get_pending_file_count(), 3);
        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 4);
        assert_eq!(ret.input_levels[0].table_infos[1].id, 7);
        assert_eq!(ret.input_levels[1].table_infos[0].id, 3);
        assert_eq!(ret.input_levels[1].table_infos[1].id, 2);

        // the first idle table in L0 is table 6 and its confict with the last job so we can not
        // pick table 7.
        let picker = LevelCompactionPicker::new(1, 1, Arc::new(RangeOverlapStrategy::default()));
        push_table_level0(&mut levels, generate_table(8, 1, 199, 233, 3));
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
        push_table_level0(&mut levels, generate_table(9, 1, 100, 400, 3));
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.input_levels[0].table_infos.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 8);
        assert_eq!(ret.input_levels[0].table_infos[1].id, 9);
        assert!(ret.input_levels[1].table_infos.is_empty());
        levels_handler[0].remove_task(1);
        levels
            .l0
            .unwrap()
            .sub_levels
            .retain(|level| level.table_infos[0].id < 7);
        push_table_level0(&mut levels, generate_table(10, 1, 100, 200, 3));

        push_table_level0(&mut levels, generate_table(11, 1, 201, 300, 3));
        push_table_level0(&mut levels, generate_table(12, 1, 301, 400, 3));
        let ret = picker
            .pick_compaction(&levels, &mut levels_handler)
            .unwrap();
        assert_eq!(ret.input_levels.len(), 2);
        assert_eq!(ret.input_levels[0].table_infos[0].id, 6);
        assert_eq!(ret.input_levels[0].table_infos[1].id, 12);
    }

    #[test]
    fn test_selecting_key_range_overlap() {
        // When picking L0->L1, all L1 files overlapped with selecting_key_range should be picked.
        let picker = LevelCompactionPicker::new(0, 1, Arc::new(RangeOverlapStrategy::default()));

        let levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: vec![
                generate_table(3, 1, 0, 50, 1),
                generate_table(4, 1, 150, 200, 1),
                generate_table(5, 1, 250, 300, 1),
                generate_table(6, 1, 1000, 2000, 1),
            ],
            total_file_size: 0,
            sub_level_id: 0,
        }];
        let mut levels = Levels {
            levels,
            l0: Some(OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
            }),
        };
        push_table_level0(&mut levels, generate_table(1, 1, 100, 200, 2));
        push_table_level0(&mut levels, generate_table(2, 1, 400, 500, 2));
        push_table_level0(&mut levels, generate_table(7, 1, 200, 250, 2));

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
        let mut levels = vec![Level {
            level_idx: 1,
            level_type: LevelType::Nonoverlapping as i32,
            table_infos: vec![],
            total_file_size: 0,
            sub_level_id: 0,
        }];
        let mut levels = Levels {
            levels,
            l0: Some(OverlappingLevel {
                sub_levels: vec![],
                total_file_size: 0,
            }),
        };
        push_table_level0(&mut levels, generate_table(1, 1, 100, 300, 2));
        push_table_level0(&mut levels, generate_table(2, 1, 250, 500, 2));

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
                sub_level_id: 0,
            },
            Level {
                level_idx: 1,
                level_type: LevelType::Nonoverlapping as i32,
                table_infos: vec![generate_table(2, 1, 150, 300, 2)],
                total_file_size: 0,
                sub_level_id: 0,
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
