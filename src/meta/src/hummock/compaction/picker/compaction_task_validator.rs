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

use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::config::meta::default::compaction_config;
use risingwave_pb::hummock::CompactionConfig;

use super::{CompactionInput, LocalPickerStatistic};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ValidationRuleType {
    Tier = 0,
    Intra = 1,
    ToBase = 2,
}

pub struct CompactionTaskValidator {
    validation_rules: HashMap<ValidationRuleType, Box<dyn CompactionTaskValidationRule>>,
}

impl CompactionTaskValidator {
    pub fn new(config: Arc<CompactionConfig>) -> Self {
        let mut validation_rules: HashMap<
            ValidationRuleType,
            Box<dyn CompactionTaskValidationRule>,
        > = HashMap::default();

        validation_rules.insert(
            ValidationRuleType::Tier,
            Box::new(TierCompactionTaskValidationRule {
                config: config.clone(),
            }),
        );

        validation_rules.insert(
            ValidationRuleType::Intra,
            Box::new(IntraCompactionTaskValidationRule {
                config: config.clone(),
            }),
        );

        validation_rules.insert(
            ValidationRuleType::ToBase,
            Box::new(BaseCompactionTaskValidationRule { config }),
        );

        CompactionTaskValidator { validation_rules }
    }

    pub fn unused() -> Self {
        CompactionTaskValidator {
            validation_rules: HashMap::default(),
        }
    }

    pub fn valid_compact_task(
        &self,
        input: &CompactionInput,
        picker_type: ValidationRuleType,
        stats: &mut LocalPickerStatistic,
    ) -> bool {
        if let Some(validation_rule) = self.validation_rules.get(&picker_type) {
            validation_rule.validate(input, stats)
        } else {
            true
        }
    }

    pub fn is_enable(&self) -> bool {
        !self.validation_rules.is_empty()
    }
}

pub trait CompactionTaskValidationRule {
    fn validate(&self, input: &CompactionInput, stats: &mut LocalPickerStatistic) -> bool;
}

struct TierCompactionTaskValidationRule {
    config: Arc<CompactionConfig>,
}

impl CompactionTaskValidationRule for TierCompactionTaskValidationRule {
    fn validate(&self, input: &CompactionInput, stats: &mut LocalPickerStatistic) -> bool {
        if input.total_file_count >= self.config.level0_max_compact_file_number
            || input.input_levels.len()
                >= self
                    .config
                    .max_l0_compact_level_count
                    .unwrap_or(compaction_config::max_l0_compact_level_count())
                    as usize
        {
            return true;
        }

        // so the design here wants to merge multiple overlapping-levels in one compaction
        let max_compaction_bytes = std::cmp::min(
            self.config.max_compaction_bytes,
            self.config.sub_level_max_compaction_bytes
                * self.config.level0_overlapping_sub_level_compact_level_count as u64,
        );

        // If waiting_enough_files is not satisfied, we will raise the priority of the number of
        // levels to ensure that we can merge as many sub_levels as possible
        let tier_sub_level_compact_level_count =
            self.config.level0_overlapping_sub_level_compact_level_count as usize;
        if input.input_levels.len() < tier_sub_level_compact_level_count
            && input.select_input_size < max_compaction_bytes
        {
            stats.skip_by_count_limit += 1;
            return false;
        }
        true
    }
}

struct IntraCompactionTaskValidationRule {
    config: Arc<CompactionConfig>,
}

impl CompactionTaskValidationRule for IntraCompactionTaskValidationRule {
    fn validate(&self, input: &CompactionInput, stats: &mut LocalPickerStatistic) -> bool {
        if (input.total_file_count >= self.config.level0_max_compact_file_number
            && input.input_levels.len() > 1)
            || input.input_levels.len()
                >= self
                    .config
                    .max_l0_compact_level_count
                    .unwrap_or(compaction_config::max_l0_compact_level_count())
                    as usize
        {
            return true;
        }

        let intra_sub_level_compact_level_count =
            self.config.level0_sub_level_compact_level_count as usize;

        if input.input_levels.len() < intra_sub_level_compact_level_count {
            stats.skip_by_count_limit += 1;
            return false;
        }

        let mut max_level_size = 0;
        for select_level in &input.input_levels {
            let level_select_size = select_level
                .table_infos
                .iter()
                .map(|sst| sst.sst_size)
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

        if is_write_amp_large {
            stats.skip_by_write_amp_limit += 1;
            return false;
        }

        true
    }
}

struct BaseCompactionTaskValidationRule {
    config: Arc<CompactionConfig>,
}

impl CompactionTaskValidationRule for BaseCompactionTaskValidationRule {
    fn validate(&self, input: &CompactionInput, stats: &mut LocalPickerStatistic) -> bool {
        if input.total_file_count >= self.config.level0_max_compact_file_number
            || input.input_levels.len()
                >= self
                    .config
                    .max_l0_compact_level_count
                    .unwrap_or(compaction_config::max_l0_compact_level_count())
                    as usize
        {
            return true;
        }

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
}
