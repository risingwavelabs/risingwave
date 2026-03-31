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

/// Determines whether the key count is large enough to warrant using a block-based filter.
///
/// # Arguments
/// * `kv_count` - The total number of keys
/// * `max_kv_count` - Optional configured threshold. If None, uses `DEFAULT_MAX_KV_COUNT_FOR_XOR16`
///
/// # Returns
/// `true` if `kv_count` exceeds the threshold, indicating block-based filter should be used
pub fn is_kv_count_too_large_for_xor16(kv_count: u64, max_kv_count: Option<u64>) -> bool {
    let threshold = max_kv_count.unwrap_or(compaction_config::DEFAULT_MAX_KV_COUNT_FOR_XOR16);
    kv_count > threshold
}

pub fn parse_sstable_filter_kind(kind: &str) -> Result<PbSstableFilterType, String> {
    match kind.trim().to_ascii_lowercase().as_str() {
        "xor16" => Ok(PbSstableFilterType::SstableFilterXor16),
        "xor8" => Ok(PbSstableFilterType::SstableFilterXor8),
        _ => Err(format!("unsupported sstable filter kind: {kind}")),
    }
}

pub fn parse_sstable_filter_layout(layout: &str) -> Result<PbSstableFilterLayout, String> {
    match layout.trim().to_ascii_lowercase().as_str() {
        "" | "auto" => Ok(PbSstableFilterLayout::Auto),
        "plain" | "normal" | "nonblocked" | "non_blocked" | "non-blocked" => {
            Ok(PbSstableFilterLayout::Plain)
        }
        _ => Err(format!("unsupported sstable filter layout: {layout}")),
    }
}

pub fn get_sstable_filter_kind(
    compaction_config: &CompactionConfig,
    _base_level: usize,
    level: usize,
) -> Result<PbSstableFilterType, String> {
    if compaction_config.sstable_filter_kind.is_empty() {
        // Backward compatibility: old compaction configs did not carry sstable_filter_kind.
        // Default to xor16 for all levels.
        return Ok(PbSstableFilterType::SstableFilterXor16);
    }

    let raw_kind = compaction_config
        .sstable_filter_kind
        .get(level)
        .ok_or_else(|| format!("sstable_filter_kind is not configured for level {level}"))?;

    parse_sstable_filter_kind(raw_kind)
}

pub fn must_resolve_sstable_filter_kind(
    compaction_config: &CompactionConfig,
    base_level: usize,
    level: usize,
) -> PbSstableFilterType {
    get_sstable_filter_kind(compaction_config, base_level, level)
        .unwrap_or_else(|err| panic!("invalid sstable_filter_kind compaction config: {err}"))
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
        PbSstableFilterLayout, PbSstableFilterType, get_sstable_filter_kind,
        get_sstable_filter_layout, parse_sstable_filter_kind, parse_sstable_filter_layout,
    };

    #[test]
    fn test_parse_sstable_filter_kind() {
        assert_eq!(
            parse_sstable_filter_kind("xor16").unwrap(),
            PbSstableFilterType::SstableFilterXor16
        );
        assert_eq!(
            parse_sstable_filter_kind("XOR8").unwrap(),
            PbSstableFilterType::SstableFilterXor8
        );
        assert!(parse_sstable_filter_kind("bfuse8").is_err());
    }

    #[test]
    fn test_parse_sstable_filter_layout() {
        assert_eq!(
            parse_sstable_filter_layout("auto").unwrap(),
            PbSstableFilterLayout::Auto
        );
        assert_eq!(
            parse_sstable_filter_layout("").unwrap(),
            PbSstableFilterLayout::Auto
        );
        assert_eq!(
            parse_sstable_filter_layout("plain").unwrap(),
            PbSstableFilterLayout::Plain
        );
        assert_eq!(
            parse_sstable_filter_layout("NORMAL").unwrap(),
            PbSstableFilterLayout::Plain
        );
        assert!(parse_sstable_filter_layout("blocked").is_err());
    }

    #[test]
    fn test_get_sstable_filter_kind_for_level() {
        let config = CompactionConfig {
            sstable_filter_kind: vec!["xor8".to_owned(), "xor16".to_owned(), "xor8".to_owned()],
            ..Default::default()
        };
        assert_eq!(
            get_sstable_filter_kind(&config, 2, 0).unwrap(),
            PbSstableFilterType::SstableFilterXor8
        );
        assert_eq!(
            get_sstable_filter_kind(&config, 2, 1).unwrap(),
            PbSstableFilterType::SstableFilterXor16
        );
        assert_eq!(
            get_sstable_filter_kind(&config, 2, 2).unwrap(),
            PbSstableFilterType::SstableFilterXor8
        );
        assert!(get_sstable_filter_kind(&config, 2, 3).is_err());
    }

    #[test]
    fn test_get_sstable_filter_kind_default_when_missing() {
        let config = CompactionConfig {
            sstable_filter_kind: vec![],
            ..Default::default()
        };
        assert_eq!(
            get_sstable_filter_kind(&config, 2, 0).unwrap(),
            PbSstableFilterType::SstableFilterXor16
        );
        assert_eq!(
            get_sstable_filter_kind(&config, 2, 6).unwrap(),
            PbSstableFilterType::SstableFilterXor16
        );
    }

    #[test]
    fn test_get_sstable_filter_layout_for_level() {
        let config = CompactionConfig {
            sstable_filter_layout: vec!["plain".to_owned(), "auto".to_owned(), "normal".to_owned()],
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
            PbSstableFilterLayout::Plain
        );
        assert!(get_sstable_filter_layout(&config, 2, 3).is_err());
    }

    #[test]
    fn test_get_sstable_filter_layout_default_when_missing() {
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
