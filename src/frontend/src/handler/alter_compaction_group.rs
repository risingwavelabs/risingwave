// Copyright 2026 RisingWave Labs
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

use pgwire::pg_response::StatementType;
use risingwave_common::config::meta::default::compaction_config;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::CompressionAlgorithm;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_sqlparser::ast::{
    AlterCompactionGroupOperation, ConfigParam, SetVariableValue, SetVariableValueSingle, Value,
};

use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};

/// Wire encoding for clearing optional `u64` compaction configs.
///
/// Note: meta currently treats both `u64::MIN` and `u64::MAX` as "unset" for historical reasons.
/// We always *send* `u64::MAX` to avoid ambiguity, since `0` can be a valid user value.
const OPTIONAL_U64_UNSET_WIRE: u64 = u64::MAX;

pub async fn handle_alter_compaction_group(
    handler_args: HandlerArgs,
    group_ids: Vec<u64>,
    operation: AlterCompactionGroupOperation,
) -> Result<RwPgResponse> {
    // Only superuser can alter compaction group config
    if !handler_args.session.is_super_user() {
        return Err(ErrorCode::PermissionDenied(
            "must be superuser to execute ALTER COMPACTION GROUP command".to_owned(),
        )
        .into());
    }

    let configs = match operation {
        AlterCompactionGroupOperation::Set { configs } => {
            build_compaction_config_from_params(&configs)?
        }
    };

    if configs.is_empty() {
        return Err(ErrorCode::InvalidInputSyntax(
            "no valid compaction config specified".to_owned(),
        )
        .into());
    }

    let meta_client = handler_args.session.env().meta_client();
    meta_client
        .update_compaction_config(group_ids.clone(), configs)
        .await?;

    Ok(RwPgResponse::builder(StatementType::ALTER_SYSTEM)
        .notice(format!(
            "Compaction group(s) {:?} config updated successfully",
            group_ids
        ))
        .into())
}

fn build_compaction_config_from_params(params: &[ConfigParam]) -> Result<Vec<MutableConfig>> {
    let mut configs = vec![];

    for param in params {
        // Config parameter names are ASCII identifiers. Prefer ASCII-only case folding.
        let name = param.param.real_value().to_ascii_lowercase();
        let value = &param.value;

        let config = match name.as_str() {
            "max_bytes_for_level_base" => MutableConfig::MaxBytesForLevelBase(parse_u64_value(
                value,
                &name,
                compaction_config::max_bytes_for_level_base(),
            )?),
            "max_bytes_for_level_multiplier" => {
                MutableConfig::MaxBytesForLevelMultiplier(parse_u64_value(
                    value,
                    &name,
                    compaction_config::max_bytes_for_level_multiplier(),
                )?)
            }
            "max_compaction_bytes" => MutableConfig::MaxCompactionBytes(parse_u64_value(
                value,
                &name,
                compaction_config::max_compaction_bytes(),
            )?),
            "sub_level_max_compaction_bytes" => {
                MutableConfig::SubLevelMaxCompactionBytes(parse_u64_value(
                    value,
                    &name,
                    compaction_config::sub_level_max_compaction_bytes(),
                )?)
            }
            "level0_tier_compact_file_number" => {
                MutableConfig::Level0TierCompactFileNumber(parse_u64_value(
                    value,
                    &name,
                    compaction_config::level0_tier_compact_file_number(),
                )?)
            }
            "target_file_size_base" => MutableConfig::TargetFileSizeBase(parse_u64_value(
                value,
                &name,
                compaction_config::target_file_size_base(),
            )?),
            "compaction_filter_mask" => MutableConfig::CompactionFilterMask(parse_u32_value(
                value,
                &name,
                compaction_config::compaction_filter_mask(),
            )?),
            "max_sub_compaction" => MutableConfig::MaxSubCompaction(parse_u32_value(
                value,
                &name,
                compaction_config::max_sub_compaction(),
            )?),
            "level0_stop_write_threshold_sub_level_number" => {
                MutableConfig::Level0StopWriteThresholdSubLevelNumber(parse_u64_value(
                    value,
                    &name,
                    compaction_config::level0_stop_write_threshold_sub_level_number(),
                )?)
            }
            "level0_sub_level_compact_level_count" => {
                MutableConfig::Level0SubLevelCompactLevelCount(parse_u32_value(
                    value,
                    &name,
                    compaction_config::level0_sub_level_compact_level_count(),
                )?)
            }
            "max_space_reclaim_bytes" => MutableConfig::MaxSpaceReclaimBytes(parse_u64_value(
                value,
                &name,
                compaction_config::max_space_reclaim_bytes(),
            )?),
            "level0_max_compact_file_number" => {
                MutableConfig::Level0MaxCompactFileNumber(parse_u64_value(
                    value,
                    &name,
                    compaction_config::level0_max_compact_file_number(),
                )?)
            }
            "level0_overlapping_sub_level_compact_level_count" => {
                MutableConfig::Level0OverlappingSubLevelCompactLevelCount(parse_u32_value(
                    value,
                    &name,
                    compaction_config::level0_overlapping_sub_level_compact_level_count(),
                )?)
            }
            "enable_emergency_picker" => MutableConfig::EnableEmergencyPicker(parse_bool_value(
                value,
                &name,
                compaction_config::enable_emergency_picker(),
            )?),
            "tombstone_reclaim_ratio" => MutableConfig::TombstoneReclaimRatio(parse_u32_value(
                value,
                &name,
                compaction_config::tombstone_reclaim_ratio(),
            )?),
            "compression_algorithm" => {
                // Format: 'level:algorithm' e.g., '3:lz4' or '6:zstd'
                // This sets the compression algorithm for a specific LSM-tree level
                match value {
                    // For DEFAULT, reset *all* levels to the built-in defaults.
                    // (This matches meta's `CompactionConfigBuilder::new()` default behavior.)
                    SetVariableValue::Default => {
                        let max_level = compaction_config::max_level();
                        for level in 0..=max_level {
                            configs.push(MutableConfig::CompressionAlgorithm(
                                CompressionAlgorithm {
                                    level,
                                    compression_algorithm:
                                        compaction_config::compression_algorithm_for_level(level)
                                            .to_owned(),
                                },
                            ));
                        }
                        continue;
                    }
                    _ => {
                        let algorithm = parse_compression_algorithm_with_level(value, &name)?;
                        MutableConfig::CompressionAlgorithm(algorithm)
                    }
                }
            }
            "max_l0_compact_level_count" => MutableConfig::MaxL0CompactLevelCount(parse_u32_value(
                value,
                &name,
                compaction_config::max_l0_compact_level_count(),
            )?),
            "sst_allowed_trivial_move_min_size" => {
                MutableConfig::SstAllowedTrivialMoveMinSize(parse_u64_value(
                    value,
                    &name,
                    compaction_config::sst_allowed_trivial_move_min_size(),
                )?)
            }
            "disable_auto_group_scheduling" => {
                MutableConfig::DisableAutoGroupScheduling(parse_bool_value(
                    value,
                    &name,
                    compaction_config::disable_auto_group_scheduling(),
                )?)
            }
            "max_overlapping_level_size" => {
                MutableConfig::MaxOverlappingLevelSize(parse_u64_value(
                    value,
                    &name,
                    compaction_config::max_overlapping_level_size(),
                )?)
            }
            "sst_allowed_trivial_move_max_count" => {
                MutableConfig::SstAllowedTrivialMoveMaxCount(parse_u32_value(
                    value,
                    &name,
                    compaction_config::sst_allowed_trivial_move_max_count(),
                )?)
            }
            "emergency_level0_sst_file_count" => {
                MutableConfig::EmergencyLevel0SstFileCount(parse_u32_value(
                    value,
                    &name,
                    compaction_config::emergency_level0_sst_file_count(),
                )?)
            }
            "emergency_level0_sub_level_partition" => {
                MutableConfig::EmergencyLevel0SubLevelPartition(parse_u32_value(
                    value,
                    &name,
                    compaction_config::emergency_level0_sub_level_partition(),
                )?)
            }
            "level0_stop_write_threshold_max_sst_count" => {
                MutableConfig::Level0StopWriteThresholdMaxSstCount(parse_u32_value(
                    value,
                    &name,
                    compaction_config::level0_stop_write_threshold_max_sst_count(),
                )?)
            }
            "level0_stop_write_threshold_max_size" => {
                MutableConfig::Level0StopWriteThresholdMaxSize(parse_u64_value(
                    value,
                    &name,
                    compaction_config::level0_stop_write_threshold_max_size(),
                )?)
            }
            "enable_optimize_l0_interval_selection" => {
                MutableConfig::EnableOptimizeL0IntervalSelection(parse_bool_value(
                    value,
                    &name,
                    compaction_config::enable_optimize_l0_interval_selection(),
                )?)
            }
            "vnode_aligned_level_size_threshold" => {
                MutableConfig::VnodeAlignedLevelSizeThreshold(parse_optional_u64_value(
                    value,
                    &name,
                    compaction_config::vnode_aligned_level_size_threshold(),
                )?)
            }
            "max_kv_count_for_xor16" => {
                MutableConfig::MaxKvCountForXor16(parse_optional_u64_value(
                    value,
                    &name,
                    compaction_config::max_kv_count_for_xor16(),
                )?)
            }
            _ => {
                return Err(ErrorCode::InvalidInputSyntax(format!(
                    "unknown compaction config parameter: {}",
                    name
                ))
                .into());
            }
        };
        configs.push(config);
    }

    Ok(configs)
}

fn expect_single_literal<'a>(value: &'a SetVariableValue, name: &str) -> Result<&'a Value> {
    match value {
        SetVariableValue::Single(SetVariableValueSingle::Literal(v)) => Ok(v),
        _ => Err(ErrorCode::InvalidInputSyntax(format!(
            "expected a single literal value for {}, got {}",
            name, value
        ))
        .into()),
    }
}

fn parse_u64_value(value: &SetVariableValue, name: &str, default_value: u64) -> Result<u64> {
    if matches!(value, SetVariableValue::Default) {
        return Ok(default_value);
    }

    let v = expect_single_literal(value, name)?;
    match v {
        Value::Number(n) => n.parse::<u64>().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!("invalid u64 value for {}: {}", name, n)).into()
        }),
        _ => Err(ErrorCode::InvalidInputSyntax(format!(
            "expected numeric value for {}, got {}",
            name, v
        ))
        .into()),
    }
}

fn parse_u32_value(value: &SetVariableValue, name: &str, default_value: u32) -> Result<u32> {
    if matches!(value, SetVariableValue::Default) {
        return Ok(default_value);
    }

    let v = expect_single_literal(value, name)?;
    match v {
        Value::Number(n) => n.parse::<u32>().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!("invalid u32 value for {}: {}", name, n)).into()
        }),
        _ => Err(ErrorCode::InvalidInputSyntax(format!(
            "expected numeric value for {}, got {}",
            name, v
        ))
        .into()),
    }
}

fn parse_bool_value(value: &SetVariableValue, name: &str, default_value: bool) -> Result<bool> {
    if matches!(value, SetVariableValue::Default) {
        return Ok(default_value);
    }

    let v = expect_single_literal(value, name)?;
    match v {
        Value::Boolean(b) => Ok(*b),
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => {
            match s.to_ascii_lowercase().as_str() {
                "true" | "on" | "1" => Ok(true),
                "false" | "off" | "0" => Ok(false),
                _ => Err(ErrorCode::InvalidInputSyntax(format!(
                    "invalid boolean value for {}: {}",
                    name, s
                ))
                .into()),
            }
        }
        _ => Err(ErrorCode::InvalidInputSyntax(format!(
            "expected boolean value for {}, got {}",
            name, v
        ))
        .into()),
    }
}

fn parse_optional_u64_value(
    value: &SetVariableValue,
    name: &str,
    default_value: Option<u64>,
) -> Result<u64> {
    if matches!(value, SetVariableValue::Default) {
        return Ok(default_value.unwrap_or(OPTIONAL_U64_UNSET_WIRE));
    }

    parse_u64_value(
        value,
        name,
        default_value.unwrap_or(OPTIONAL_U64_UNSET_WIRE),
    )
}

fn parse_compression_algorithm_with_level(
    value: &SetVariableValue,
    name: &str,
) -> Result<CompressionAlgorithm> {
    // Format: 'level:algorithm' e.g., '3:lz4', '6:zstd', '0:none'
    // level: non-negative integer (validation is done by meta service)
    // algorithm: none, lz4, zstd
    let v = expect_single_literal(value, name)?;
    let input_str = match v {
        Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => s.to_ascii_lowercase(),
        _ => {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "expected string value for {} in format 'level:algorithm' (e.g., '3:lz4'), got {}",
                name, v
            ))
            .into());
        }
    };

    let (level_str, algo_str) = input_str.split_once(':').ok_or_else(|| {
        ErrorCode::InvalidInputSyntax(format!(
            "invalid format for {}: '{}', expected 'level:algorithm' (e.g., '3:lz4', '6:zstd')",
            name, input_str
        ))
    })?;

    let level: u32 = level_str.parse().map_err(|_| {
        ErrorCode::InvalidInputSyntax(format!(
            "invalid level for {}: '{}', expected a non-negative integer",
            name, level_str
        ))
    })?;

    // Note: level validation (whether it exceeds max_level) is done by meta service
    // since max_level is per-compaction-group configuration

    let algo_name = match algo_str {
        "none" => "None",
        "lz4" => "Lz4",
        "zstd" => "Zstd",
        _ => {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "invalid compression algorithm for {}: '{}', expected one of: none, lz4, zstd",
                name, algo_str
            ))
            .into());
        }
    };

    Ok(CompressionAlgorithm {
        level,
        compression_algorithm: algo_name.to_owned(),
    })
}
