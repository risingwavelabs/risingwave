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

use risingwave_common::constants::hummock::CompactionFilterFlag;
use risingwave_pb::hummock::compaction_config::CompactionMode;
use risingwave_pb::hummock::CompactionConfig;

const DEFAULT_MAX_COMPACTION_BYTES: u64 = 2 * 1024 * 1024 * 1024; // 2GB
const DEFAULT_MIN_COMPACTION_BYTES: u64 = 128 * 1024 * 1024; // 128MB
const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 512 * 1024 * 1024; // 512MB

// decrease this configure when the generation of checkpoint barrier is not frequent.
const DEFAULT_TIER_COMPACT_TRIGGER_NUMBER: u64 = 6;
const DEFAULT_TARGET_FILE_SIZE_BASE: u64 = 32 * 1024 * 1024; // 32MB
const DEFAULT_MAX_SUB_COMPACTION: u32 = 4;
const MAX_LEVEL: u64 = 6;
const DEFAULT_LEVEL_MULTIPLIER: u64 = 5;
const DEFAULT_MAX_SPACE_RECLAIM_BYTES: u64 = 512 * 1024 * 1024; // 512MB;
const DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_SUB_LEVEL_NUMBER: u64 = 1000;
const DEFAULT_MAX_COMPACTION_FILE_COUNT: u64 = 96;
const DEFAULT_MIN_SUB_LEVEL_COMPACT_LEVEL_COUNT: u32 = 3;

pub struct CompactionConfigBuilder {
    config: CompactionConfig,
}

impl CompactionConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: CompactionConfig {
                max_bytes_for_level_base: DEFAULT_MAX_BYTES_FOR_LEVEL_BASE,
                max_bytes_for_level_multiplier: DEFAULT_LEVEL_MULTIPLIER,
                max_level: MAX_LEVEL,
                max_compaction_bytes: DEFAULT_MAX_COMPACTION_BYTES,
                sub_level_max_compaction_bytes: DEFAULT_MIN_COMPACTION_BYTES,
                level0_tier_compact_file_number: DEFAULT_TIER_COMPACT_TRIGGER_NUMBER,
                target_file_size_base: DEFAULT_TARGET_FILE_SIZE_BASE,
                compaction_mode: CompactionMode::Range as i32,
                // support compression setting per level
                // L0/L1 and L2 do not use compression algorithms
                // L3 - L4 use Lz4, else use Zstd
                compression_algorithm: vec![
                    "None".to_string(),
                    "None".to_string(),
                    "None".to_string(),
                    "Lz4".to_string(),
                    "Lz4".to_string(),
                    "Zstd".to_string(),
                    "Zstd".to_string(),
                ],
                compaction_filter_mask: (CompactionFilterFlag::STATE_CLEAN
                    | CompactionFilterFlag::TTL)
                    .into(),
                max_sub_compaction: DEFAULT_MAX_SUB_COMPACTION,
                max_space_reclaim_bytes: DEFAULT_MAX_SPACE_RECLAIM_BYTES,
                split_by_state_table: false,
                level0_stop_write_threshold_sub_level_number:
                    DEFAULT_LEVEL0_STOP_WRITE_THRESHOLD_SUB_LEVEL_NUMBER,
                // This configure variable shall be larger than level0_tier_compact_file_number, and
                // it shall meet the following condition:
                //    level0_max_compact_file_number * target_file_size_base >
                // max_bytes_for_level_base
                level0_max_compact_file_number: DEFAULT_MAX_COMPACTION_FILE_COUNT,
                level0_sub_level_compact_level_count: DEFAULT_MIN_SUB_LEVEL_COMPACT_LEVEL_COUNT,
            },
        }
    }

    pub fn with_config(config: CompactionConfig) -> Self {
        Self { config }
    }

    pub fn build(self) -> CompactionConfig {
        if let Err(reason) = validate_compaction_config(&self.config) {
            tracing::warn!("Bad compaction config: {}", reason);
        }
        self.config
    }
}

/// Returns Ok if `config` is valid,
/// or the reason why it's invalid.
pub fn validate_compaction_config(config: &CompactionConfig) -> Result<(), String> {
    let sub_level_number_threshold_min = 1;
    if config.level0_stop_write_threshold_sub_level_number < sub_level_number_threshold_min {
        return Err(format!(
            "{} is too small for level0_stop_write_threshold_sub_level_number, expect >= {}",
            config.level0_stop_write_threshold_sub_level_number, sub_level_number_threshold_min
        ));
    }
    Ok(())
}

impl Default for CompactionConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

macro_rules! builder_field {
    ($( $name:ident: $type:ty ),* ,) => {
        impl CompactionConfigBuilder {
            $(
                pub fn $name(mut self, v:$type) -> Self {
                    self.config.$name = v;
                    self
                }
            )*
        }
    }
}

builder_field! {
    max_bytes_for_level_base: u64,
    max_bytes_for_level_multiplier: u64,
    max_level: u64,
    max_compaction_bytes: u64,
    sub_level_max_compaction_bytes: u64,
    level0_tier_compact_file_number: u64,
    compaction_mode: i32,
    compression_algorithm: Vec<String>,
    compaction_filter_mask: u32,
    target_file_size_base: u64,
    max_sub_compaction: u32,
    max_space_reclaim_bytes: u64,
    level0_stop_write_threshold_sub_level_number: u64,
    level0_max_compact_file_number: u64,
    level0_sub_level_compact_level_count: u32,
}
