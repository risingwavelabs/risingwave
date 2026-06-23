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

use risingwave_common::config::meta::default::compaction_config;
use risingwave_pb::hummock::{CompactionConfig, PbSstableFilterLayout, PbSstableFilterType};

/// Determines whether the key count is large enough to warrant using a blocked xor filter.
///
/// # Arguments
/// * `kv_count` - The total number of keys
/// * `blocked_kv_count_threshold` - Optional configured threshold. If None, uses
///   `DEFAULT_BLOCKED_XOR_FILTER_KV_COUNT_THRESHOLD`.
///
/// # Returns
/// `true` if `kv_count` exceeds the threshold, indicating blocked xor filter should be used.
pub fn should_use_blocked_xor_filter_by_kv_count(
    kv_count: u64,
    blocked_kv_count_threshold: Option<u64>,
) -> bool {
    let threshold = blocked_kv_count_threshold
        .unwrap_or(compaction_config::DEFAULT_BLOCKED_XOR_FILTER_KV_COUNT_THRESHOLD);
    kv_count > threshold
}

pub fn parse_sstable_filter_type(filter_type: &str) -> Result<PbSstableFilterType, String> {
    match filter_type {
        "none" => Ok(PbSstableFilterType::SstableFilterNone),
        "xor16" => Ok(PbSstableFilterType::SstableFilterXor16),
        "xor8" => Ok(PbSstableFilterType::SstableFilterXor8),
        _ => Err(format!("unsupported sstable filter type: {filter_type}")),
    }
}

pub fn parse_sstable_filter_layout(layout: &str) -> Result<PbSstableFilterLayout, String> {
    match layout {
        "auto" => Ok(PbSstableFilterLayout::Auto),
        "plain" => Ok(PbSstableFilterLayout::Plain),
        "blocked" => Ok(PbSstableFilterLayout::Blocked),
        _ => Err(format!("unsupported sstable filter layout: {layout}")),
    }
}

pub fn get_sstable_filter_type(
    compaction_config: &CompactionConfig,
    _base_level: usize,
    level: usize,
) -> Result<PbSstableFilterType, String> {
    if compaction_config.sstable_filter_type.is_empty() {
        // Backward compatibility: old compaction configs did not carry the filter type field.
        // Default to xor16 for all levels.
        return Ok(PbSstableFilterType::SstableFilterXor16);
    }

    let raw_filter_type = compaction_config
        .sstable_filter_type
        .get(level)
        .ok_or_else(|| format!("sstable_filter_type is not configured for level {level}"))?;

    parse_sstable_filter_type(raw_filter_type)
}

pub fn must_resolve_sstable_filter_type(
    compaction_config: &CompactionConfig,
    base_level: usize,
    level: usize,
) -> PbSstableFilterType {
    get_sstable_filter_type(compaction_config, base_level, level)
        .unwrap_or_else(|err| panic!("invalid sstable_filter_type compaction config: {err}"))
}

pub fn get_sstable_filter_layout(
    compaction_config: &CompactionConfig,
    _base_level: usize,
    level: usize,
) -> Result<PbSstableFilterLayout, String> {
    if compaction_config.sstable_filter_layout.is_empty() {
        return Ok(PbSstableFilterLayout::Auto);
    }

    let raw_layout = compaction_config
        .sstable_filter_layout
        .get(level)
        .ok_or_else(|| format!("sstable_filter_layout is not configured for level {level}"))?;

    parse_sstable_filter_layout(raw_layout)
}

pub fn must_resolve_sstable_filter_layout(
    compaction_config: &CompactionConfig,
    base_level: usize,
    level: usize,
) -> PbSstableFilterLayout {
    get_sstable_filter_layout(compaction_config, base_level, level)
        .unwrap_or_else(|err| panic!("invalid sstable_filter_layout compaction config: {err}"))
}

#[cfg(test)]
mod tests {
    use risingwave_pb::hummock::CompactionConfig;

    use super::{
        PbSstableFilterLayout, PbSstableFilterType, get_sstable_filter_layout,
        get_sstable_filter_type, parse_sstable_filter_layout, parse_sstable_filter_type,
    };

    #[test]
    fn test_parse_sstable_filter_type() {
        assert_eq!(
            parse_sstable_filter_type("none").unwrap(),
            PbSstableFilterType::SstableFilterNone
        );
        assert_eq!(
            parse_sstable_filter_type("xor16").unwrap(),
            PbSstableFilterType::SstableFilterXor16
        );
        assert_eq!(
            parse_sstable_filter_type("xor8").unwrap(),
            PbSstableFilterType::SstableFilterXor8
        );
        assert!(parse_sstable_filter_type("XOR8").is_err());
        assert!(parse_sstable_filter_type("bfuse8").is_err());
    }

    #[test]
    fn test_parse_sstable_filter_layout() {
        assert_eq!(
            parse_sstable_filter_layout("auto").unwrap(),
            PbSstableFilterLayout::Auto
        );
        assert_eq!(
            parse_sstable_filter_layout("plain").unwrap(),
            PbSstableFilterLayout::Plain
        );
        assert_eq!(
            parse_sstable_filter_layout("blocked").unwrap(),
            PbSstableFilterLayout::Blocked
        );
        assert!(parse_sstable_filter_layout("blocked ").is_err());
        assert!(parse_sstable_filter_layout("none").is_err());
        assert!(parse_sstable_filter_layout("unknown").is_err());
    }

    #[test]
    fn test_get_sstable_filter_type() {
        let config = CompactionConfig {
            sstable_filter_type: vec!["xor8".to_owned(), "none".to_owned(), "xor16".to_owned()],
            ..Default::default()
        };
        assert_eq!(
            get_sstable_filter_type(&config, 2, 0).unwrap(),
            PbSstableFilterType::SstableFilterXor8
        );
        assert_eq!(
            get_sstable_filter_type(&config, 2, 1).unwrap(),
            PbSstableFilterType::SstableFilterNone
        );
        assert_eq!(
            get_sstable_filter_type(&config, 2, 2).unwrap(),
            PbSstableFilterType::SstableFilterXor16
        );
        assert!(get_sstable_filter_type(&config, 2, 3).is_err());

        let config = CompactionConfig {
            sstable_filter_type: vec![],
            ..Default::default()
        };
        assert_eq!(
            get_sstable_filter_type(&config, 2, 0).unwrap(),
            PbSstableFilterType::SstableFilterXor16
        );
        assert_eq!(
            get_sstable_filter_type(&config, 2, 6).unwrap(),
            PbSstableFilterType::SstableFilterXor16
        );
    }

    #[test]
    fn test_get_sstable_filter_layout() {
        let config = CompactionConfig {
            sstable_filter_layout: vec![
                "plain".to_owned(),
                "auto".to_owned(),
                "blocked".to_owned(),
            ],
            ..Default::default()
        };
        assert_eq!(
            get_sstable_filter_layout(&config, 2, 0).unwrap(),
            PbSstableFilterLayout::Plain
        );
        assert_eq!(
            get_sstable_filter_layout(&config, 2, 1).unwrap(),
            PbSstableFilterLayout::Auto
        );
        assert_eq!(
            get_sstable_filter_layout(&config, 2, 2).unwrap(),
            PbSstableFilterLayout::Blocked
        );
        assert!(get_sstable_filter_layout(&config, 2, 3).is_err());

        let config = CompactionConfig {
            sstable_filter_layout: vec![],
            ..Default::default()
        };
        assert_eq!(
            get_sstable_filter_layout(&config, 2, 0).unwrap(),
            PbSstableFilterLayout::Auto
        );
        assert_eq!(
            get_sstable_filter_layout(&config, 2, 6).unwrap(),
            PbSstableFilterLayout::Auto
        );
    }
}
