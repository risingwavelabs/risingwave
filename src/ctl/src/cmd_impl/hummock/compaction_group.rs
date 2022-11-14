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

use risingwave_hummock_sdk::CompactionGroupId;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;

use crate::common::MetaServiceOpts;

pub async fn list_compaction_group() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    let result = meta_client.risectl_list_compaction_group().await?;
    println!("{:#?}", result);
    Ok(())
}

pub async fn update_compaction_config(
    ids: Vec<CompactionGroupId>,
    configs: Vec<MutableConfig>,
) -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    meta_client
        .risectl_update_compaction_config(ids.as_slice(), configs.as_slice())
        .await?;
    println!(
        "Succeed: update compaction groups {:#?}\n with configs {:#?}",
        ids, configs
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn build_compaction_config_vec(
    max_bytes_for_level_base: Option<u64>,
    max_bytes_for_level_multiplier: Option<u64>,
    max_compaction_bytes: Option<u64>,
    sub_level_max_compaction_bytes: Option<u64>,
    level0_trigger_file_number: Option<u64>,
    level0_tier_compact_file_number: Option<u64>,
    target_file_size_base: Option<u64>,
    compaction_filter_mask: Option<u32>,
    max_sub_compaction: Option<u32>,
) -> Vec<MutableConfig> {
    let mut configs = vec![];
    if let Some(c) = max_bytes_for_level_base {
        configs.push(MutableConfig::MaxBytesForLevelBase(c));
    }
    if let Some(c) = max_bytes_for_level_multiplier {
        configs.push(MutableConfig::MaxBytesForLevelMultiplier(c));
    }
    if let Some(c) = max_compaction_bytes {
        configs.push(MutableConfig::MaxCompactionBytes(c));
    }
    if let Some(c) = sub_level_max_compaction_bytes {
        configs.push(MutableConfig::SubLevelMaxCompactionBytes(c));
    }
    if let Some(c) = level0_trigger_file_number {
        configs.push(MutableConfig::Level0TriggerFileNumber(c));
    }
    if let Some(c) = level0_tier_compact_file_number {
        configs.push(MutableConfig::Level0TierCompactFileNumber(c));
    }
    if let Some(c) = target_file_size_base {
        configs.push(MutableConfig::TargetFileSizeBase(c));
    }
    if let Some(c) = compaction_filter_mask {
        configs.push(MutableConfig::CompactionFilterMask(c));
    }
    if let Some(c) = max_sub_compaction {
        configs.push(MutableConfig::MaxSubCompaction(c));
    }
    configs
}
