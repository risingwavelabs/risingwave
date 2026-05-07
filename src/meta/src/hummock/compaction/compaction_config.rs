// Copyright 2022 RisingWave Labs
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

use risingwave_common::config::CompactionConfig as CompactionConfigOpt;
use risingwave_common::config::meta::default::compaction_config;
use risingwave_hummock_sdk::filter_utils::{
    parse_sstable_filter_kind, parse_sstable_filter_layout,
};
use risingwave_pb::hummock::CompactionConfig;
use risingwave_pb::hummock::compaction_config::CompactionMode;

pub struct CompactionConfigBuilder {
    config: CompactionConfig,
}

impl CompactionConfigBuilder {
    pub fn new() -> Self {
        #[allow(deprecated)]
        Self {
            config: CompactionConfig {
                max_bytes_for_level_base: compaction_config::max_bytes_for_level_base(),
                max_bytes_for_level_multiplier: compaction_config::max_bytes_for_level_multiplier(),
                max_level: compaction_config::max_level() as u64,
                max_compaction_bytes: compaction_config::max_compaction_bytes(),
                sub_level_max_compaction_bytes: compaction_config::sub_level_max_compaction_bytes(),
                level0_tier_compact_file_number: compaction_config::level0_tier_compact_file_number(
                ),
                target_file_size_base: compaction_config::target_file_size_base(),
                compaction_mode: CompactionMode::Range as i32,
                // support compression setting per level
                // L0/L1 and L2 do not use compression algorithms
                // L3 - L4 use Lz4, else use Zstd
                compression_algorithm: vec![
                    "None".to_owned(),
                    "None".to_owned(),
                    "None".to_owned(),
                    "Lz4".to_owned(),
                    "Lz4".to_owned(),
                    "Zstd".to_owned(),
                    "Zstd".to_owned(),
                ],
                sstable_filter_kind: compaction_config::sstable_filter_kind(),
                sstable_filter_layout: compaction_config::sstable_filter_layout(),
                compaction_filter_mask: compaction_config::compaction_filter_mask(),
                max_sub_compaction: compaction_config::max_sub_compaction(),
                max_space_reclaim_bytes: compaction_config::max_space_reclaim_bytes(),
                split_by_state_table: false,
                split_weight_by_vnode: 0,
                level0_stop_write_threshold_sub_level_number:
                    compaction_config::level0_stop_write_threshold_sub_level_number(),
                // This configure variable shall be larger than level0_tier_compact_file_number, and
                // it shall meet the following condition:
                //    level0_max_compact_file_number * target_file_size_base >
                // max_bytes_for_level_base
                level0_max_compact_file_number: compaction_config::level0_max_compact_file_number(),
                level0_sub_level_compact_level_count:
                    compaction_config::level0_sub_level_compact_level_count(),
                level0_overlapping_sub_level_compact_level_count:
                    compaction_config::level0_overlapping_sub_level_compact_level_count(),
                tombstone_reclaim_ratio: compaction_config::tombstone_reclaim_ratio(),
                enable_emergency_picker: compaction_config::enable_emergency_picker(),
                max_l0_compact_level_count: Some(compaction_config::max_l0_compact_level_count()),
                sst_allowed_trivial_move_min_size: Some(
                    compaction_config::sst_allowed_trivial_move_min_size(),
                ),
                disable_auto_group_scheduling: Some(
                    compaction_config::disable_auto_group_scheduling(),
                ),
                max_overlapping_level_size: Some(compaction_config::max_overlapping_level_size()),
                sst_allowed_trivial_move_max_count: Some(
                    compaction_config::sst_allowed_trivial_move_max_count(),
                ),
                emergency_level0_sst_file_count: Some(
                    compaction_config::emergency_level0_sst_file_count(),
                ),
                emergency_level0_sub_level_partition: Some(
                    compaction_config::emergency_level0_sub_level_partition(),
                ),
                level0_stop_write_threshold_max_sst_count: Some(
                    compaction_config::level0_stop_write_threshold_max_sst_count(),
                ),
                level0_stop_write_threshold_max_size: Some(
                    compaction_config::level0_stop_write_threshold_max_size(),
                ),
                enable_optimize_l0_interval_selection: Some(
                    compaction_config::enable_optimize_l0_interval_selection(),
                ),
                vnode_aligned_level_size_threshold: None,
                blocked_xor_filter_kv_count_threshold:
                    compaction_config::blocked_xor_filter_kv_count_threshold(),
                max_vnode_key_range_bytes: compaction_config::max_vnode_key_range_bytes(),
            },
        }
    }

    pub fn with_config(config: CompactionConfig) -> Self {
        Self { config }
    }

    pub fn with_opt(opt: &CompactionConfigOpt) -> Self {
        Self::new()
            .max_bytes_for_level_base(opt.max_bytes_for_level_base)
            .max_bytes_for_level_multiplier(opt.max_bytes_for_level_multiplier)
            .max_compaction_bytes(opt.max_compaction_bytes)
            .sub_level_max_compaction_bytes(opt.sub_level_max_compaction_bytes)
            .level0_tier_compact_file_number(opt.level0_tier_compact_file_number)
            .target_file_size_base(opt.target_file_size_base)
            .compaction_filter_mask(opt.compaction_filter_mask)
            .max_sub_compaction(opt.max_sub_compaction)
            .level0_stop_write_threshold_sub_level_number(
                opt.level0_stop_write_threshold_sub_level_number,
            )
            .level0_sub_level_compact_level_count(opt.level0_sub_level_compact_level_count)
            .level0_overlapping_sub_level_compact_level_count(
                opt.level0_overlapping_sub_level_compact_level_count,
            )
            .max_space_reclaim_bytes(opt.max_space_reclaim_bytes)
            .level0_max_compact_file_number(opt.level0_max_compact_file_number)
            .tombstone_reclaim_ratio(opt.tombstone_reclaim_ratio)
            .max_level(opt.max_level as u64)
            .sst_allowed_trivial_move_min_size(Some(opt.sst_allowed_trivial_move_min_size))
            .sst_allowed_trivial_move_max_count(Some(opt.sst_allowed_trivial_move_max_count))
            .max_overlapping_level_size(Some(opt.max_overlapping_level_size))
            .emergency_level0_sst_file_count(Some(opt.emergency_level0_sst_file_count))
            .emergency_level0_sub_level_partition(Some(opt.emergency_level0_sub_level_partition))
            .level0_stop_write_threshold_max_sst_count(Some(
                opt.level0_stop_write_threshold_max_sst_count,
            ))
            .level0_stop_write_threshold_max_size(Some(opt.level0_stop_write_threshold_max_size))
            .enable_optimize_l0_interval_selection(Some(opt.enable_optimize_l0_interval_selection))
            .blocked_xor_filter_kv_count_threshold(opt.blocked_xor_filter_kv_count_threshold)
            .max_vnode_key_range_bytes(opt.max_vnode_key_range_bytes)
            .sstable_filter_kind(opt.sstable_filter_kind.clone())
            .sstable_filter_layout(opt.sstable_filter_layout.clone())
    }

    pub fn build(self) -> CompactionConfig {
        let mut config = self.config;
        if let Err(reason) = validate_compaction_config(&config) {
            // Avoid crashing later in compaction task planning due to invalid per-level filter
            // configs. Fallback to legacy behavior (xor16 + auto) and keep other fields unchanged.
            tracing::warn!(
                "Bad compaction config: {}. Falling back to default sstable filter kind/layout.",
                reason
            );
            config.sstable_filter_kind.clear();
            config.sstable_filter_layout.clear();
        }
        config
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
    if !config.sstable_filter_kind.is_empty() {
        if config.sstable_filter_kind.len() < config.max_level as usize + 1 {
            return Err(format!(
                "sstable_filter_kind must provide at least {} entries for max_level {}",
                config.max_level + 1,
                config.max_level
            ));
        }
        for filter_kind in &config.sstable_filter_kind {
            parse_sstable_filter_kind(filter_kind)?;
        }
    }

    if !config.sstable_filter_layout.is_empty() {
        if config.sstable_filter_layout.len() < config.max_level as usize + 1 {
            return Err(format!(
                "sstable_filter_layout must provide at least {} entries for max_level {}",
                config.max_level + 1,
                config.max_level
            ));
        }
        for layout in &config.sstable_filter_layout {
            parse_sstable_filter_layout(layout)?;
        }
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
    sstable_filter_kind: Vec<String>,
    sstable_filter_layout: Vec<String>,
    compaction_filter_mask: u32,
    target_file_size_base: u64,
    max_sub_compaction: u32,
    max_space_reclaim_bytes: u64,
    level0_stop_write_threshold_sub_level_number: u64,
    level0_max_compact_file_number: u64,
    level0_sub_level_compact_level_count: u32,
    level0_overlapping_sub_level_compact_level_count: u32,
    tombstone_reclaim_ratio: u32,
    sst_allowed_trivial_move_min_size: Option<u64>,
    sst_allowed_trivial_move_max_count: Option<u32>,
    disable_auto_group_scheduling: Option<bool>,
    max_overlapping_level_size: Option<u64>,
    emergency_level0_sst_file_count: Option<u32>,
    emergency_level0_sub_level_partition: Option<u32>,
    level0_stop_write_threshold_max_sst_count: Option<u32>,
    level0_stop_write_threshold_max_size: Option<u64>,
    enable_optimize_l0_interval_selection: Option<bool>,
    blocked_xor_filter_kv_count_threshold: Option<u64>,
    max_vnode_key_range_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::{CompactionConfigBuilder, validate_compaction_config};

    #[test]
    fn test_validate_compaction_config_accepts_missing_filter_config() {
        let mut config = CompactionConfigBuilder::new().build();
        config.sstable_filter_kind.clear();
        config.sstable_filter_layout.clear();
        assert!(validate_compaction_config(&config).is_ok());
    }

    #[test]
    fn test_validate_compaction_config_rejects_short_filter_layout() {
        let mut config = CompactionConfigBuilder::new().build();
        config.sstable_filter_layout = vec!["auto".to_owned()];
        assert!(validate_compaction_config(&config).is_err());
    }

    #[test]
    fn test_validate_compaction_config_rejects_invalid_filter_layout_value() {
        let mut config = CompactionConfigBuilder::new().build();
        config.sstable_filter_layout = vec!["auto".to_owned(); config.max_level as usize + 1];
        config.sstable_filter_layout[0] = "blocked".to_owned();
        assert!(validate_compaction_config(&config).is_err());
    }

    #[test]
    fn test_validate_compaction_config_rejects_short_filter_kind() {
        let mut config = CompactionConfigBuilder::new().build();
        config.sstable_filter_kind = vec!["xor16".to_owned()];
        assert!(validate_compaction_config(&config).is_err());
    }
}
