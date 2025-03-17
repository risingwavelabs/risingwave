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

use risingwave_hummock_sdk::level::Levels;
use risingwave_pb::hummock::{CompactionConfig, LevelType};

use super::{
    CompactionInput, CompactionPicker, CompactionTaskValidator, LevelCompactionPicker,
    LocalPickerStatistic, TierCompactionPicker,
};
use crate::hummock::compaction::CompactionDeveloperConfig;
use crate::hummock::compaction::picker::intra_compaction_picker::WholeLevelCompactionPicker;
use crate::hummock::level_handler::LevelHandler;

pub struct EmergencyCompactionPicker {
    target_level: usize,
    config: Arc<CompactionConfig>,
    developer_config: Arc<CompactionDeveloperConfig>,
}

impl EmergencyCompactionPicker {
    pub fn new(
        target_level: usize,
        config: Arc<CompactionConfig>,
        developer_config: Arc<CompactionDeveloperConfig>,
    ) -> Self {
        Self {
            target_level,
            config,
            developer_config,
        }
    }

    pub fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let unused_validator = Arc::new(CompactionTaskValidator::unused());
        let l0 = &levels.l0;
        let overlapping_count = l0
            .sub_levels
            .iter()
            .filter(|level| level.level_type == LevelType::Overlapping)
            .count();
        let no_overlap_count = l0
            .sub_levels
            .iter()
            .filter(|level| {
                level.level_type == LevelType::Nonoverlapping && level.vnode_partition_count == 0
            })
            .count();
        let partitioned_count = l0
            .sub_levels
            .iter()
            .filter(|level| {
                level.level_type == LevelType::Nonoverlapping && level.vnode_partition_count > 0
            })
            .count();
        // We trigger `EmergencyCompactionPicker` only when some unexpected condition cause the number of l0 levels increase and the origin strategy
        // can not compact those data to lower level. But if most of these levels are overlapping level, it is dangerous to compact small data of non-overlapping sub level
        // to base level, it will cost a lot of compactor resource because of large write-amplification.
        if (self.config.split_weight_by_vnode == 0 && no_overlap_count > overlapping_count)
            || (self.config.split_weight_by_vnode > 0
                && partitioned_count > no_overlap_count
                && partitioned_count > overlapping_count)
        {
            let mut base_level_compaction_picker = LevelCompactionPicker::new_with_validator(
                self.target_level,
                self.config.clone(),
                unused_validator.clone(),
                self.developer_config.clone(),
            );

            if let Some(ret) =
                base_level_compaction_picker.pick_compaction(levels, level_handlers, stats)
            {
                return Some(ret);
            }
        }
        if self.config.split_weight_by_vnode > 0
            && no_overlap_count > partitioned_count
            && no_overlap_count > overlapping_count
        {
            let intral_level_compaction_picker =
                WholeLevelCompactionPicker::new(self.config.clone(), unused_validator.clone());

            if let Some(ret) = intral_level_compaction_picker.pick_whole_level(
                &levels.l0,
                &level_handlers[0],
                self.config.split_weight_by_vnode,
                stats,
            ) {
                return Some(ret);
            }
        }
        let mut tier_compaction_picker =
            TierCompactionPicker::new_with_validator(self.config.clone(), unused_validator);

        tier_compaction_picker.pick_compaction(levels, level_handlers, stats)
    }
}
