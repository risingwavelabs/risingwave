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

use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::CompactionConfig;

#[derive(Debug, Default, Clone, PartialEq)]
pub struct CompactionGroup {
    pub group_id: CompactionGroupId,
    pub compaction_config: Arc<CompactionConfig>,
}

impl CompactionGroup {
    pub fn new(
        group_id: impl Into<CompactionGroupId>,
        compaction_config: CompactionConfig,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            compaction_config: Arc::new(compaction_config),
        }
    }

    pub fn group_id(&self) -> CompactionGroupId {
        self.group_id
    }

    pub fn compaction_config(&self) -> Arc<CompactionConfig> {
        self.compaction_config.clone()
    }
}

impl From<&risingwave_pb::hummock::CompactionGroup> for CompactionGroup {
    fn from(compaction_group: &risingwave_pb::hummock::CompactionGroup) -> Self {
        Self {
            group_id: compaction_group.id,
            compaction_config: Arc::new(
                compaction_group
                    .compaction_config
                    .as_ref()
                    .cloned()
                    .unwrap(),
            ),
        }
    }
}

impl From<&CompactionGroup> for risingwave_pb::hummock::CompactionGroup {
    fn from(compaction_group: &CompactionGroup) -> Self {
        Self {
            id: compaction_group.group_id,
            compaction_config: Some(compaction_group.compaction_config.as_ref().clone()),
        }
    }
}

impl CompactionGroup {
    pub fn max_estimated_group_size(&self) -> u64 {
        let max_level = self.compaction_config.max_level as usize;
        let base_level_size = self.compaction_config.max_bytes_for_level_base;
        let level_multiplier = self.compaction_config.max_bytes_for_level_multiplier;

        fn size_for_levels(level_index: usize, base_size: u64, multiplier: u64) -> u64 {
            if level_index == 0 {
                base_size
            } else {
                base_size * multiplier.pow(level_index as u32)
            }
        }

        (0..max_level)
            .map(|level_index| size_for_levels(level_index, base_level_size, level_multiplier))
            .sum()
    }
}
