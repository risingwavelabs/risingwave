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
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::CompressionAlgorithm;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_sqlparser::ast::{AlterCompactionGroupOperation, SqlOption, SqlOptionValue, Value};

use super::{HandlerArgs, RwPgResponse};
use crate::error::{ErrorCode, Result};

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
            build_compaction_config_from_options(&configs)?
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

fn build_compaction_config_from_options(options: &[SqlOption]) -> Result<Vec<MutableConfig>> {
    let mut configs = vec![];

    for option in options {
        let name = option.name.real_value().to_lowercase();
        let value = &option.value;

        let config = match name.as_str() {
            "max_bytes_for_level_base" => {
                MutableConfig::MaxBytesForLevelBase(parse_u64_value(value, &name)?)
            }
            "max_bytes_for_level_multiplier" => {
                MutableConfig::MaxBytesForLevelMultiplier(parse_u64_value(value, &name)?)
            }
            "max_compaction_bytes" => {
                MutableConfig::MaxCompactionBytes(parse_u64_value(value, &name)?)
            }
            "sub_level_max_compaction_bytes" => {
                MutableConfig::SubLevelMaxCompactionBytes(parse_u64_value(value, &name)?)
            }
            "level0_tier_compact_file_number" => {
                MutableConfig::Level0TierCompactFileNumber(parse_u64_value(value, &name)?)
            }
            "target_file_size_base" => {
                MutableConfig::TargetFileSizeBase(parse_u64_value(value, &name)?)
            }
            "compaction_filter_mask" => {
                MutableConfig::CompactionFilterMask(parse_u32_value(value, &name)?)
            }
            "max_sub_compaction" => MutableConfig::MaxSubCompaction(parse_u32_value(value, &name)?),
            "level0_stop_write_threshold_sub_level_number" => {
                MutableConfig::Level0StopWriteThresholdSubLevelNumber(parse_u64_value(
                    value, &name,
                )?)
            }
            "level0_sub_level_compact_level_count" => {
                MutableConfig::Level0SubLevelCompactLevelCount(parse_u32_value(value, &name)?)
            }
            "max_space_reclaim_bytes" => {
                MutableConfig::MaxSpaceReclaimBytes(parse_u64_value(value, &name)?)
            }
            "level0_max_compact_file_number" => {
                MutableConfig::Level0MaxCompactFileNumber(parse_u64_value(value, &name)?)
            }
            "level0_overlapping_sub_level_compact_level_count" => {
                MutableConfig::Level0OverlappingSubLevelCompactLevelCount(parse_u32_value(
                    value, &name,
                )?)
            }
            "enable_emergency_picker" => {
                MutableConfig::EnableEmergencyPicker(parse_bool_value(value, &name)?)
            }
            "tombstone_reclaim_ratio" => {
                MutableConfig::TombstoneReclaimRatio(parse_u32_value(value, &name)?)
            }
            "compression_algorithm" => {
                // Format: 'level:algorithm' e.g., '3:lz4' or '6:zstd'
                // This sets the compression algorithm for a specific LSM-tree level
                let algorithm = parse_compression_algorithm_with_level(value, &name)?;
                MutableConfig::CompressionAlgorithm(algorithm)
            }
            "max_l0_compact_level_count" => {
                MutableConfig::MaxL0CompactLevelCount(parse_u32_value(value, &name)?)
            }
            "sst_allowed_trivial_move_min_size" => {
                MutableConfig::SstAllowedTrivialMoveMinSize(parse_u64_value(value, &name)?)
            }
            "disable_auto_group_scheduling" => {
                MutableConfig::DisableAutoGroupScheduling(parse_bool_value(value, &name)?)
            }
            "max_overlapping_level_size" => {
                MutableConfig::MaxOverlappingLevelSize(parse_u64_value(value, &name)?)
            }
            "sst_allowed_trivial_move_max_count" => {
                MutableConfig::SstAllowedTrivialMoveMaxCount(parse_u32_value(value, &name)?)
            }
            "emergency_level0_sst_file_count" => {
                MutableConfig::EmergencyLevel0SstFileCount(parse_u32_value(value, &name)?)
            }
            "emergency_level0_sub_level_partition" => {
                MutableConfig::EmergencyLevel0SubLevelPartition(parse_u32_value(value, &name)?)
            }
            "level0_stop_write_threshold_max_sst_count" => {
                MutableConfig::Level0StopWriteThresholdMaxSstCount(parse_u32_value(value, &name)?)
            }
            "level0_stop_write_threshold_max_size" => {
                MutableConfig::Level0StopWriteThresholdMaxSize(parse_u64_value(value, &name)?)
            }
            "enable_optimize_l0_interval_selection" => {
                MutableConfig::EnableOptimizeL0IntervalSelection(parse_bool_value(value, &name)?)
            }
            "vnode_aligned_level_size_threshold" => {
                MutableConfig::VnodeAlignedLevelSizeThreshold(parse_u64_value(value, &name)?)
            }
            "max_kv_count_for_xor16" => {
                MutableConfig::MaxKvCountForXor16(parse_u64_value(value, &name)?)
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

fn parse_u64_value(value: &SqlOptionValue, name: &str) -> Result<u64> {
    match value {
        SqlOptionValue::Value(Value::Number(n)) => n.parse::<u64>().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!("invalid u64 value for {}: {}", name, n)).into()
        }),
        _ => Err(ErrorCode::InvalidInputSyntax(format!(
            "expected numeric value for {}, got {:?}",
            name, value
        ))
        .into()),
    }
}

fn parse_u32_value(value: &SqlOptionValue, name: &str) -> Result<u32> {
    match value {
        SqlOptionValue::Value(Value::Number(n)) => n.parse::<u32>().map_err(|_| {
            ErrorCode::InvalidInputSyntax(format!("invalid u32 value for {}: {}", name, n)).into()
        }),
        _ => Err(ErrorCode::InvalidInputSyntax(format!(
            "expected numeric value for {}, got {:?}",
            name, value
        ))
        .into()),
    }
}

fn parse_bool_value(value: &SqlOptionValue, name: &str) -> Result<bool> {
    match value {
        SqlOptionValue::Value(Value::Boolean(b)) => Ok(*b),
        SqlOptionValue::Value(Value::SingleQuotedString(s))
        | SqlOptionValue::Value(Value::DoubleQuotedString(s)) => match s.to_lowercase().as_str() {
            "true" | "on" | "1" => Ok(true),
            "false" | "off" | "0" => Ok(false),
            _ => Err(ErrorCode::InvalidInputSyntax(format!(
                "invalid boolean value for {}: {}",
                name, s
            ))
            .into()),
        },
        _ => Err(ErrorCode::InvalidInputSyntax(format!(
            "expected boolean value for {}, got {:?}",
            name, value
        ))
        .into()),
    }
}

fn parse_compression_algorithm_with_level(
    value: &SqlOptionValue,
    name: &str,
) -> Result<CompressionAlgorithm> {
    // Format: 'level:algorithm' e.g., '3:lz4', '6:zstd', '0:none'
    // level: 0-6 (LSM-tree levels)
    // algorithm: none, lz4, zstd
    let input_str = match value {
        SqlOptionValue::Value(Value::SingleQuotedString(s))
        | SqlOptionValue::Value(Value::DoubleQuotedString(s)) => s.to_lowercase(),
        _ => {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "expected string value for {} in format 'level:algorithm' (e.g., '3:lz4'), got {:?}",
                name, value
            ))
            .into());
        }
    };

    let parts: Vec<&str> = input_str.split(':').collect();
    if parts.len() != 2 {
        return Err(ErrorCode::InvalidInputSyntax(format!(
            "invalid format for {}: '{}', expected 'level:algorithm' (e.g., '3:lz4', '6:zstd')",
            name, input_str
        ))
        .into());
    }

    let level: u32 = parts[0].parse().map_err(|_| {
        ErrorCode::InvalidInputSyntax(format!(
            "invalid level for {}: '{}', expected a non-negative integer",
            name, parts[0]
        ))
    })?;

    // Note: level validation (whether it exceeds max_level) is done by meta service
    // since max_level is per-compaction-group configuration

    let algo_name = match parts[1] {
        "none" => "None",
        "lz4" => "Lz4",
        "zstd" => "Zstd",
        _ => {
            return Err(ErrorCode::InvalidInputSyntax(format!(
                "invalid compression algorithm for {}: '{}', expected one of: none, lz4, zstd",
                name, parts[1]
            ))
            .into());
        }
    };

    Ok(CompressionAlgorithm {
        level,
        compression_algorithm: algo_name.to_owned(),
    })
}
