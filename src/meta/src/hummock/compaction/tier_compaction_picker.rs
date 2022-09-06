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
use risingwave_pb::hummock::{CompactionConfig, InputLevel, LevelType, OverlappingLevel};

use crate::hummock::compaction::min_overlap_compaction_picker::MinOverlappingPicker;
use crate::hummock::compaction::overlap_strategy::OverlapStrategy;
use crate::hummock::compaction::{CompactionInput, CompactionPicker};
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
}
impl TierCompactionPicker {
    fn pick_whole_level(
        &self,
        l0: &OverlappingLevel,
        level_handler: &LevelHandler,
    ) -> Option<CompactionInput> {
        // do not pick the first sub-level because we do not want to block the level compaction.
        let non_overlapping_type = LevelType::Nonoverlapping as i32;
        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type == non_overlapping_type
                && level.total_file_size > self.config.sub_level_max_compaction_bytes
            {
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

            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.max_bytes_for_level_base * 2,
            );

            let mut compaction_bytes = level.total_file_size;
            let mut max_level_size = level.total_file_size;
            let mut compact_file_count = level.table_infos.len();

            for other in &l0.sub_levels[idx + 1..] {
                if compaction_bytes >= max_compaction_bytes {
                    break;
                }

                if level.level_type == non_overlapping_type
                    && level.total_file_size > self.config.sub_level_max_compaction_bytes
                {
                    break;
                }

                if level_handler.is_level_pending_compact(other) {
                    break;
                }

                compaction_bytes += other.total_file_size;
                compact_file_count += other.table_infos.len();
                max_level_size = std::cmp::max(max_level_size, other.total_file_size);
                select_level_inputs.push(InputLevel {
                    level_idx: 0,
                    level_type: other.level_type,
                    table_infos: other.table_infos.clone(),
                });
            }

            if compact_file_count < self.config.level0_tier_compact_file_number as usize {
                continue;
            }

            // This limitation would keep our write-amplification no more than
            // ln(max_compaction_bytes/flush_level_bytes) /
            // ln(self.config.level0_tier_compact_file_number/2) Here we only use half
            // of level0_tier_compact_file_number just for convenient.
            let is_write_amp_large =
                max_level_size * self.config.level0_tier_compact_file_number / 2 > compaction_bytes;

            // do not pick a compact task with large write amplification.
            if level.level_type == non_overlapping_type && is_write_amp_large {
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

            let input_levels = vec![
                InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: select_tables,
                },
                InputLevel {
                    level_idx: 0,
                    level_type: LevelType::Nonoverlapping as i32,
                    table_infos: target_tables,
                },
            ];
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
        level_handlers: &[LevelHandler],
    ) -> Option<CompactionInput> {
        let l0 = levels.l0.as_ref().unwrap();
        if l0.sub_levels.is_empty() {
            return None;
        }

        if let Some(ret) = self.pick_trivial_move_file(l0, level_handlers) {
            return Some(ret);
        }

        self.pick_whole_level(l0, &level_handlers[0])
    }
}
