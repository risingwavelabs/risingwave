// Copyright 2025 RisingWave Labs
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

use risingwave_hummock_sdk::level::{InputLevel, Levels, OverlappingLevel};
use risingwave_pb::hummock::{CompactionConfig, LevelType};

use super::{
    CompactionInput, CompactionPicker, CompactionTaskValidator, LocalPickerStatistic,
    ValidationRuleType,
};
use crate::hummock::level_handler::LevelHandler;

pub struct TierCompactionPicker {
    config: Arc<CompactionConfig>,
    compaction_task_validator: Arc<CompactionTaskValidator>,
}

impl TierCompactionPicker {
    #[cfg(test)]
    pub fn new(config: Arc<CompactionConfig>) -> TierCompactionPicker {
        TierCompactionPicker {
            compaction_task_validator: Arc::new(CompactionTaskValidator::new(config.clone())),
            config,
        }
    }

    pub fn new_with_validator(
        config: Arc<CompactionConfig>,
        compaction_task_validator: Arc<CompactionTaskValidator>,
    ) -> TierCompactionPicker {
        TierCompactionPicker {
            config,
            compaction_task_validator,
        }
    }

    fn pick_overlapping_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &LevelHandler,
        mut vnode_partition_count: u32,
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type != LevelType::Overlapping {
                continue;
            }

            if level.table_infos.is_empty() {
                continue;
            }

            if level_handler.is_level_pending_compact(level) {
                continue;
            }

            let input_level = InputLevel {
                level_idx: 0,
                level_type: level.level_type,
                table_infos: level.table_infos.clone(),
            };

            let mut select_level_inputs = vec![input_level];

            // We assume that the maximum size of each sub_level is sub_level_max_compaction_bytes,
            // so the design here wants to merge multiple overlapping-levels in one compaction
            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.sub_level_max_compaction_bytes
                    * self.config.level0_overlapping_sub_level_compact_level_count as u64,
            );

            let mut compaction_bytes = level.total_file_size;
            let mut compact_file_count = level.table_infos.len() as u64;
            // Limit sstable file count to avoid using too much memory.
            let overlapping_max_compact_file_numer = self.config.level0_max_compact_file_number;

            for other in &l0.sub_levels[idx + 1..] {
                if compaction_bytes > max_compaction_bytes {
                    break;
                }

                if compact_file_count > overlapping_max_compact_file_numer {
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

            select_level_inputs.reverse();
            if compaction_bytes < self.config.sub_level_max_compaction_bytes / 2 {
                vnode_partition_count = 0;
            }

            let result = CompactionInput {
                input_levels: select_level_inputs,
                target_level: 0,
                target_sub_level_id: level.sub_level_id,
                select_input_size: compaction_bytes,
                target_input_size: 0,
                total_file_count: compact_file_count,
                vnode_partition_count,
            };

            if !self.compaction_task_validator.valid_compact_task(
                &result,
                ValidationRuleType::Tier,
                stats,
            ) {
                continue;
            }

            return Some(result);
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
        let l0 = &levels.l0;
        if l0.sub_levels.is_empty() {
            return None;
        }

        self.pick_overlapping_level(
            l0,
            &level_handlers[0],
            self.config.split_weight_by_vnode,
            stats,
        )
    }
}

#[cfg(test)]
pub mod tests {
    use std::sync::Arc;

    use risingwave_hummock_sdk::compaction_group::hummock_version_ext::new_sub_level;
    use risingwave_hummock_sdk::level::{Levels, OverlappingLevel};
    use risingwave_pb::hummock::LevelType;

    use crate::hummock::compaction::compaction_config::CompactionConfigBuilder;
    use crate::hummock::compaction::picker::{
        CompactionPicker, LocalPickerStatistic, TierCompactionPicker,
    };
    use crate::hummock::compaction::selector::tests::{
        generate_l0_overlapping_sublevels, generate_table, push_table_level0_overlapping,
    };
    use crate::hummock::level_handler::LevelHandler;

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
            l0,
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
        let mut picker = TierCompactionPicker::new(config);
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
            l0: generate_l0_overlapping_sublevels(vec![]),
            levels: vec![],
            ..Default::default()
        };
        assert!(
            picker
                .pick_compaction(&empty_level, &levels_handler, &mut local_stats)
                .is_none()
        );
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
        ]);

        let levels = Levels {
            l0,
            levels: vec![],
            ..Default::default()
        };
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
        let mut picker = TierCompactionPicker::new(config);
        let ret = picker.pick_compaction(&levels, &levels_handler, &mut local_stats);
        assert!(ret.is_none());
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
            l0: OverlappingLevel {
                total_file_size: l1.total_file_size + l2.total_file_size,
                uncompressed_file_size: l1.total_file_size + l2.total_file_size,
                sub_levels: vec![l1, l2],
            },
            levels: vec![],
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
        let mut picker = TierCompactionPicker::new(config);
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
            l0,
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
        let mut picker = TierCompactionPicker::new(config);
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
