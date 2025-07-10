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

use risingwave_common_proc_macro::ConfigDoc;
use serde::{Deserialize, Serialize};
use serde_default::DefaultFromSerde;

#[derive(Clone, Debug, Serialize, Deserialize, DefaultFromSerde, ConfigDoc)]
pub struct CompactionConfig {
    #[serde(default = "default::compaction_config::max_bytes_for_level_base")]
    pub max_bytes_for_level_base: u64,
    #[serde(default = "default::compaction_config::max_bytes_for_level_multiplier")]
    pub max_bytes_for_level_multiplier: u64,
    #[serde(default = "default::compaction_config::max_compaction_bytes")]
    pub max_compaction_bytes: u64,
    #[serde(default = "default::compaction_config::sub_level_max_compaction_bytes")]
    pub sub_level_max_compaction_bytes: u64,
    #[serde(default = "default::compaction_config::level0_tier_compact_file_number")]
    pub level0_tier_compact_file_number: u64,
    #[serde(default = "default::compaction_config::target_file_size_base")]
    pub target_file_size_base: u64,
    #[serde(default = "default::compaction_config::compaction_filter_mask")]
    pub compaction_filter_mask: u32,
    #[serde(default = "default::compaction_config::max_sub_compaction")]
    pub max_sub_compaction: u32,
    #[serde(default = "default::compaction_config::level0_stop_write_threshold_sub_level_number")]
    pub level0_stop_write_threshold_sub_level_number: u64,
    #[serde(default = "default::compaction_config::level0_sub_level_compact_level_count")]
    pub level0_sub_level_compact_level_count: u32,
    #[serde(
        default = "default::compaction_config::level0_overlapping_sub_level_compact_level_count"
    )]
    pub level0_overlapping_sub_level_compact_level_count: u32,
    #[serde(default = "default::compaction_config::max_space_reclaim_bytes")]
    pub max_space_reclaim_bytes: u64,
    #[serde(default = "default::compaction_config::level0_max_compact_file_number")]
    pub level0_max_compact_file_number: u64,
    #[serde(default = "default::compaction_config::tombstone_reclaim_ratio")]
    pub tombstone_reclaim_ratio: u32,
    #[serde(default = "default::compaction_config::enable_emergency_picker")]
    pub enable_emergency_picker: bool,
    #[serde(default = "default::compaction_config::max_level")]
    pub max_level: u32,
    #[serde(default = "default::compaction_config::sst_allowed_trivial_move_min_size")]
    pub sst_allowed_trivial_move_min_size: u64,
    #[serde(default = "default::compaction_config::sst_allowed_trivial_move_max_count")]
    pub sst_allowed_trivial_move_max_count: u32,
    #[serde(default = "default::compaction_config::max_l0_compact_level_count")]
    pub max_l0_compact_level_count: u32,
    #[serde(default = "default::compaction_config::disable_auto_group_scheduling")]
    pub disable_auto_group_scheduling: bool,
    #[serde(default = "default::compaction_config::max_overlapping_level_size")]
    pub max_overlapping_level_size: u64,
    #[serde(default = "default::compaction_config::emergency_level0_sst_file_count")]
    pub emergency_level0_sst_file_count: u32,
    #[serde(default = "default::compaction_config::emergency_level0_sub_level_partition")]
    pub emergency_level0_sub_level_partition: u32,
    #[serde(default = "default::compaction_config::level0_stop_write_threshold_max_sst_count")]
    pub level0_stop_write_threshold_max_sst_count: u32,
    #[serde(default = "default::compaction_config::level0_stop_write_threshold_max_size")]
    pub level0_stop_write_threshold_max_size: u64,
    #[serde(default = "default::compaction_config::enable_optimize_l0_interval_selection")]
    pub enable_optimize_l0_interval_selection: bool,
}

mod default {
    pub mod compaction_config {
        pub fn max_bytes_for_level_base() -> u64 { 536870912 }
        pub fn max_bytes_for_level_multiplier() -> u64 { 5 }
        pub fn max_compaction_bytes() -> u64 { 2147483648 }
        pub fn sub_level_max_compaction_bytes() -> u64 { 134217728 }
        pub fn level0_tier_compact_file_number() -> u64 { 12 }
        pub fn target_file_size_base() -> u64 { 33554432 }
        pub fn compaction_filter_mask() -> u32 { 6 }
        pub fn max_sub_compaction() -> u32 { 4 }
        pub fn level0_stop_write_threshold_sub_level_number() -> u64 { 300 }
        pub fn level0_sub_level_compact_level_count() -> u32 { 3 }
        pub fn level0_overlapping_sub_level_compact_level_count() -> u32 { 12 }
        pub fn max_space_reclaim_bytes() -> u64 { 536870912 }
        pub fn level0_max_compact_file_number() -> u64 { 40 }
        pub fn tombstone_reclaim_ratio() -> u32 { 40 }
        pub fn enable_emergency_picker() -> bool { true }
        pub fn max_level() -> u32 { 6 }
        pub fn max_l0_compact_level_count() -> u32 { 42 }
        pub fn sst_allowed_trivial_move_min_size() -> u64 { 134217728 }
        pub fn disable_auto_group_scheduling() -> bool { false }
        pub fn max_overlapping_level_size() -> u64 { 10737418240 }
        pub fn sst_allowed_trivial_move_max_count() -> u32 { 2 }
        pub fn emergency_level0_sst_file_count() -> u32 { 100 }
        pub fn emergency_level0_sub_level_partition() -> u32 { 4 }
        pub fn level0_stop_write_threshold_max_sst_count() -> u32 { 800 }
        pub fn level0_stop_write_threshold_max_size() -> u64 { 21474836480 }
        pub fn enable_optimize_l0_interval_selection() -> bool { true }
    }
}