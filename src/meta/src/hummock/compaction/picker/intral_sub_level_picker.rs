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
use risingwave_pb::hummock::{CompactionConfig, InputLevel, LevelType};

use crate::hummock::compaction::create_overlap_strategy;
use crate::hummock::compaction::picker::min_overlap_compaction_picker::NonOverlapSubLevelPicker;
use crate::hummock::compaction::picker::{CompactionInput, CompactionPicker, LocalPickerStatistic};
use crate::hummock::level_handler::LevelHandler;

pub struct IntraSubLevelPicker {
    config: Arc<CompactionConfig>,
}

impl IntraSubLevelPicker {
    pub fn new(config: Arc<CompactionConfig>) -> Self {
        Self { config }
    }
}

impl CompactionPicker for IntraSubLevelPicker {
    fn pick_compaction(
        &mut self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let l0 = levels.l0.as_ref().unwrap();
        let overlap_strategy = create_overlap_strategy(self.config.compaction_mode());

        for (idx, level) in l0.sub_levels.iter().enumerate() {
            if level.level_type() != LevelType::Nonoverlapping
                || level.total_file_size > self.config.sub_level_max_compaction_bytes
            {
                continue;
            }

            if level_handlers[0].is_level_all_pending_compact(level) {
                continue;
            }

            let max_compaction_bytes = std::cmp::min(
                self.config.max_compaction_bytes,
                self.config.sub_level_max_compaction_bytes,
            );

            let tier_sub_level_compact_level_count =
                self.config.level0_sub_level_compact_level_count as usize;
            let non_overlap_sub_level_picker = NonOverlapSubLevelPicker::new(
                self.config.sub_level_max_compaction_bytes / 2,
                max_compaction_bytes,
                self.config.level0_sub_level_compact_level_count as usize,
                self.config.level0_max_compact_file_number,
                overlap_strategy.clone(),
            );

            let l0_select_tables_vec = non_overlap_sub_level_picker
                .pick_l0_multi_non_overlap_level(&l0.sub_levels[idx..], &level_handlers[0]);

            if l0_select_tables_vec.is_empty() {
                continue;
            }

            let mut skip_by_write_amp = false;
            // Limit the number of selection levels for the non-overlapping
            // sub_level at least level0_sub_level_compact_level_count
            for (plan_index, input) in l0_select_tables_vec.into_iter().enumerate() {
                if plan_index == 0
                    && input.sstable_infos.len()
                        < self.config.level0_sub_level_compact_level_count as usize
                {
                    // first plan level count smaller than limit
                    break;
                }

                let mut max_level_size = 0;
                for level_select_table in &input.sstable_infos {
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
                        >= input.total_file_size;

                if (is_write_amp_large
                    || input.sstable_infos.len() < tier_sub_level_compact_level_count)
                    && input.total_file_count < self.config.level0_max_compact_file_number as usize
                {
                    skip_by_write_amp = true;
                    continue;
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
                }
                select_level_inputs.reverse();
                return Some(CompactionInput {
                    input_levels: select_level_inputs,
                    target_level: 0,
                    target_sub_level_id: level.sub_level_id,
                });
            }

            if skip_by_write_amp {
                stats.skip_by_write_amp_limit += 1;
            }
        }
        None
    }
}
