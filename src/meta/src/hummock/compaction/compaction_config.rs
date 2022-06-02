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

use risingwave_pb::hummock::compaction_config::CompactionMode;
use risingwave_pb::hummock::CompactionConfig as ProstCompactionConfig;

const DEFAULT_MAX_COMPACTION_BYTES: u64 = 4 * 1024 * 1024 * 1024; // 4GB
const DEFAULT_MIN_COMPACTION_BYTES: u64 = 128 * 1024 * 1024; // 128MB
const DEFAULT_MAX_BYTES_FOR_LEVEL_BASE: u64 = 1024 * 1024 * 1024; // 1GB

// decrease this configure when the generation of checkpoint barrier is not frequent.
const DEFAULT_TIER_COMPACT_TRIGGER_NUMBER: u64 = 16;
const MAX_LEVEL: u64 = 6;

#[derive(PartialEq, Clone, Debug)]
pub struct CompactionConfig {
    inner: ProstCompactionConfig,
}

impl CompactionConfig {
    pub fn new(inner: ProstCompactionConfig) -> Self {
        Self { inner }
    }

    pub fn inner(&self) -> &ProstCompactionConfig {
        &self.inner
    }
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            inner: ProstCompactionConfig {
                max_bytes_for_level_base: DEFAULT_MAX_BYTES_FOR_LEVEL_BASE,
                max_bytes_for_level_multiplier: 10,
                max_level: MAX_LEVEL,
                max_compaction_bytes: DEFAULT_MAX_COMPACTION_BYTES,
                min_compaction_bytes: DEFAULT_MIN_COMPACTION_BYTES,
                level0_tigger_file_numer: DEFAULT_TIER_COMPACT_TRIGGER_NUMBER * 2,
                level0_tier_compact_file_number: DEFAULT_TIER_COMPACT_TRIGGER_NUMBER,
                compaction_mode: CompactionMode::ConsistentHash as i32,
            },
        }
    }
}

pub struct CompactionConfigBuilder {
    config: ProstCompactionConfig,
}

impl CompactionConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: CompactionConfig::default().inner,
        }
    }

    pub fn new_with(config: CompactionConfig) -> Self {
        Self {
            config: config.inner,
        }
    }

    pub fn build(self) -> CompactionConfig {
        CompactionConfig::new(self.config)
    }
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
                pub fn $name(&self, v:$type) -> Self {
                    Self {
                        config: ProstCompactionConfig {
                            $name: v,
                            ..self.config
                        },
                    }
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
    min_compaction_bytes: u64,
    level0_tigger_file_numer: u64,
    level0_tier_compact_file_number: u64,
    compaction_mode: i32,
}
