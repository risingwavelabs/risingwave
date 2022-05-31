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

use std::borrow::Borrow;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_hummock_sdk::CompactionGroupId;

use crate::hummock::compaction::level_selector::DynamicLevelSelector;
use crate::hummock::compaction::overlap_strategy::{
    HashStrategy, OverlapStrategy, RangeOverlapStrategy,
};
use crate::hummock::compaction::{CompactStatus, CompactionConfig, CompactionMode};
use crate::model::MetadataModel;

const HUMMOCK_COMPACTION_STATUS_CF_NAME: &str = "cf/hummock_compaction_status";

impl MetadataModel for CompactStatus {
    type KeyType = CompactionGroupId;
    type ProstType = risingwave_pb::hummock::CompactStatus;

    fn cf_name() -> String {
        String::from(HUMMOCK_COMPACTION_STATUS_CF_NAME)
    }

    fn to_protobuf(&self) -> Self::ProstType {
        self.into()
    }

    fn from_protobuf(prost: Self::ProstType) -> Self {
        prost.borrow().into()
    }

    fn key(&self) -> Result<Self::KeyType> {
        Ok(self.compaction_group_id)
    }
}

impl From<&CompactStatus> for risingwave_pb::hummock::CompactStatus {
    fn from(status: &CompactStatus) -> Self {
        risingwave_pb::hummock::CompactStatus {
            compaction_group_id: status.compaction_group_id,
            level_handlers: status.level_handlers.iter().map_into().collect(),
            compaction_config: Some(status.compaction_config.borrow().into()),
        }
    }
}

impl From<CompactStatus> for risingwave_pb::hummock::CompactStatus {
    fn from(status: CompactStatus) -> Self {
        status.borrow().into()
    }
}

impl From<&risingwave_pb::hummock::CompactStatus> for CompactStatus {
    fn from(status: &risingwave_pb::hummock::CompactStatus) -> Self {
        let compaction_config: CompactionConfig = status.compaction_config.as_ref().unwrap().into();
        let overlap_strategy: Arc<dyn OverlapStrategy> = match &compaction_config.compaction_mode {
            CompactionMode::RangeMode => Arc::new(RangeOverlapStrategy::default()),
            CompactionMode::ConsistentHashMode => Arc::new(HashStrategy::default()),
        };
        let compaction_selector =
            DynamicLevelSelector::new(Arc::new(compaction_config.clone()), overlap_strategy);
        CompactStatus {
            compaction_group_id: status.compaction_group_id,
            level_handlers: status.level_handlers.iter().map_into().collect(),
            compaction_config,
            compaction_selector: Arc::new(compaction_selector),
        }
    }
}

impl From<risingwave_pb::hummock::compaction_config::CompactionMode> for CompactionMode {
    fn from(mode: risingwave_pb::hummock::compaction_config::CompactionMode) -> Self {
        match mode {
            risingwave_pb::hummock::compaction_config::CompactionMode::RangeMode => {
                CompactionMode::RangeMode
            }
            risingwave_pb::hummock::compaction_config::CompactionMode::ConsistentHashMode => {
                CompactionMode::ConsistentHashMode
            }
        }
    }
}

impl From<CompactionMode> for risingwave_pb::hummock::compaction_config::CompactionMode {
    fn from(mode: CompactionMode) -> Self {
        match mode {
            CompactionMode::RangeMode => {
                risingwave_pb::hummock::compaction_config::CompactionMode::RangeMode
            }
            CompactionMode::ConsistentHashMode => {
                risingwave_pb::hummock::compaction_config::CompactionMode::ConsistentHashMode
            }
        }
    }
}

impl From<&risingwave_pb::hummock::CompactionConfig> for CompactionConfig {
    fn from(config: &risingwave_pb::hummock::CompactionConfig) -> Self {
        Self {
            max_bytes_for_level_base: config.max_bytes_for_level_base,
            max_level: config.max_level as usize,
            max_bytes_for_level_multiplier: config.max_bytes_for_level_multiplier,
            max_compaction_bytes: config.max_compaction_bytes,
            min_compaction_bytes: config.min_compaction_bytes,
            level0_tigger_file_numer: config.level0_tigger_file_numer as usize,
            level0_tier_compact_file_number: config.level0_tier_compact_file_number as usize,
            compaction_mode: config.compaction_mode().into(),
        }
    }
}

impl From<&CompactionConfig> for risingwave_pb::hummock::CompactionConfig {
    fn from(config: &CompactionConfig) -> Self {
        risingwave_pb::hummock::CompactionConfig {
            max_bytes_for_level_base: config.max_bytes_for_level_base,
            max_level: config.max_level as u64,
            max_bytes_for_level_multiplier: config.max_bytes_for_level_multiplier,
            max_compaction_bytes: config.max_compaction_bytes,
            min_compaction_bytes: config.min_compaction_bytes,
            level0_tigger_file_numer: config.level0_tigger_file_numer as u64,
            level0_tier_compact_file_number: config.level0_tier_compact_file_number as u64,
            compaction_mode: risingwave_pb::hummock::compaction_config::CompactionMode::from(
                config.compaction_mode.clone(),
            ) as i32,
        }
    }
}
