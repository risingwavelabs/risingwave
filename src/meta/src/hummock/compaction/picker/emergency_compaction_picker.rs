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
use risingwave_pb::hummock::CompactionConfig;

use super::{
    CompactionInput, CompactionPicker, CompactionTaskValidator, LevelCompactionPicker,
    LocalPickerStatistic, TierCompactionPicker,
};
use crate::hummock::level_handler::LevelHandler;

pub struct EmergencyCompactionPicker {
    target_level: usize,
    config: Arc<CompactionConfig>,
}

impl EmergencyCompactionPicker {
    pub fn new(target_level: usize, config: Arc<CompactionConfig>) -> Self {
        Self {
            target_level,
            config,
        }
    }

    pub fn pick_compaction(
        &self,
        levels: &Levels,
        level_handlers: &[LevelHandler],
        stats: &mut LocalPickerStatistic,
    ) -> Option<CompactionInput> {
        let unused_validator = Arc::new(CompactionTaskValidator::unused());

        let mut base_level_compaction_picker = LevelCompactionPicker::new_with_validator(
            self.target_level,
            self.config.clone(),
            unused_validator.clone(),
        );

        if let Some(ret) =
            base_level_compaction_picker.pick_compaction(levels, level_handlers, stats)
        {
            return Some(ret);
        }

        let mut tier_compaction_picker =
            TierCompactionPicker::new_with_validator(self.config.clone(), unused_validator);

        tier_compaction_picker.pick_compaction(levels, level_handlers, stats)
    }
}
