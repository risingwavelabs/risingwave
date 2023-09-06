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

use std::collections::BTreeSet;
use std::sync::Arc;

use risingwave_pb::hummock::CompactionConfig;

use super::{CompactionInput, LocalPickerStatistic, MAX_COMPACT_LEVEL_COUNT};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CompactionTaskOptimizeRule {
    Tier = 0,
    Intra = 1,
    ToBase = 2,
}

pub struct CompactionTaskValidator {
    config: Arc<CompactionConfig>,
    optimize_rules: BTreeSet<CompactionTaskOptimizeRule>,
}

impl CompactionTaskValidator {
    pub fn new(config: Arc<CompactionConfig>) -> Self {
        CompactionTaskValidator {
            config,
            optimize_rules: BTreeSet::from_iter(
                vec![
                    CompactionTaskOptimizeRule::Tier,
                    CompactionTaskOptimizeRule::Intra,
                    CompactionTaskOptimizeRule::ToBase,
                ]
                .into_iter(),
            ),
        }
    }

    fn valid_tier_compaction(
        &self,
        input: &CompactionInput,
        stats: &mut LocalPickerStatistic,
    ) -> bool {
        // so the design here wants to merge multiple overlapping-levels in one compaction
        let max_compaction_bytes = std::cmp::min(
            self.config.max_compaction_bytes,
            self.config.sub_level_max_compaction_bytes
                * self.config.level0_overlapping_sub_level_compact_level_count as u64,
        );

        // Limit sstable file count to avoid using too much memory.
        let overlapping_max_compact_file_numer = std::cmp::min(
            self.config.level0_max_compact_file_number,
            MAX_COMPACT_LEVEL_COUNT as u64,
        );

        let waiting_enough_files = {
            if input.select_input_size > max_compaction_bytes {
                false
            } else {
                input.total_file_count <= overlapping_max_compact_file_numer
            }
        };

        // If waiting_enough_files is not satisfied, we will raise the priority of the number of
        // levels to ensure that we can merge as many sub_levels as possible
        let tier_sub_level_compact_level_count =
            self.config.level0_overlapping_sub_level_compact_level_count as usize;
        if input.input_levels.len() < tier_sub_level_compact_level_count && waiting_enough_files {
            stats.skip_by_count_limit += 1;
            return false;
        }

        true
    }

    fn valid_intra_compaction(
        &self,
        input: &CompactionInput,
        stats: &mut LocalPickerStatistic,
    ) -> bool {
        let intra_sub_level_compact_level_count =
            self.config.level0_sub_level_compact_level_count as usize;

        if input.input_levels.len() < intra_sub_level_compact_level_count {
            return false;
        }

        let mut max_level_size = 0;
        for select_level in &input.input_levels {
            let level_select_size = select_level
                .table_infos
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
                >= input.select_input_size;

        if is_write_amp_large && input.total_file_count < self.config.level0_max_compact_file_number
        {
            stats.skip_by_write_amp_limit += 1;
            return false;
        }

        if input.input_levels.len() < intra_sub_level_compact_level_count
            && input.total_file_count < self.config.level0_max_compact_file_number
        {
            stats.skip_by_count_limit += 1;
            return false;
        }

        true
    }

    fn valid_base_level_compaction(
        &self,
        input: &CompactionInput,
        stats: &mut LocalPickerStatistic,
    ) -> bool {
        // The size of target level may be too large, we shall skip this compact task and wait
        //  the data in base level compact to lower level.
        if input.target_input_size > self.config.max_compaction_bytes {
            stats.skip_by_count_limit += 1;
            return false;
        }

        if input.select_input_size < input.target_input_size {
            stats.skip_by_write_amp_limit += 1;
            return false;
        }

        true
    }

    pub fn valid_compact_task(
        &self,
        input: &CompactionInput,
        picker_type: CompactionTaskOptimizeRule,
        stats: &mut LocalPickerStatistic,
    ) -> bool {
        if !self.optimize_rules.contains(&picker_type) {
            return true;
        }

        match picker_type {
            CompactionTaskOptimizeRule::Tier => self.valid_tier_compaction(input, stats),
            CompactionTaskOptimizeRule::Intra => self.valid_intra_compaction(input, stats),
            CompactionTaskOptimizeRule::ToBase => self.valid_base_level_compaction(input, stats),
        }
    }
}
