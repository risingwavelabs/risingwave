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

use risingwave_common::config::StreamingConfig;
use serde::{Deserialize, Serialize};

use crate::backup_restore::BackupConfig;

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct MetaNodeConfig {
    #[serde(default)]
    pub meta: MetaConfig,
    #[serde(default)]
    pub streaming: StreamingConfig,
    #[serde(default)]
    pub backup: BackupConfig,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetaConfig {
    /// Threshold used by worker node to filter out new SSTs when scanning object store, during
    /// full SST GC.
    #[serde(default = "default::min_sst_retention_time_sec")]
    pub min_sst_retention_time_sec: u64,

    /// The spin interval when collecting global GC watermark in hummock
    #[serde(default = "default::collect_gc_watermark_spin_interval_sec")]
    pub collect_gc_watermark_spin_interval_sec: u64,

    /// Schedule compaction for all compaction groups with this interval.
    #[serde(default = "default::periodic_compaction_interval_sec")]
    pub periodic_compaction_interval_sec: u64,

    /// Interval of GC metadata in meta store and stale SSTs in object store.
    #[serde(default = "default::vacuum_interval_sec")]
    pub vacuum_interval_sec: u64,
}

impl Default for MetaConfig {
    fn default() -> Self {
        toml::from_str("").unwrap()
    }
}

mod default {
    pub fn min_sst_retention_time_sec() -> u64 {
        604800
    }

    pub fn collect_gc_watermark_spin_interval_sec() -> u64 {
        5
    }

    pub fn periodic_compaction_interval_sec() -> u64 {
        60
    }

    pub fn vacuum_interval_sec() -> u64 {
        30
    }
}
