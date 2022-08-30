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

use anyhow::anyhow;
use risingwave_hummock_sdk::compaction_config::CompactionConfigBuilder;

use crate::common::MetaServiceOpts;

pub async fn list_compaction_group() -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    let result = meta_client.list_compaction_group().await?;
    println!("{:#?}", result);
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub async fn update_compaction_config(
    compaction_group_id: u64,
    max_bytes_for_level_base: Option<u64>,
    max_bytes_for_level_multiplier: Option<u64>,
    max_compaction_bytes: Option<u64>,
    sub_level_max_compaction_bytes: Option<u64>,
    level0_trigger_file_number: Option<u64>,
    level0_tier_compact_file_number: Option<u64>,
    target_file_size_base: Option<u64>,
    compaction_filter_mask: Option<u64>,
    max_sub_compaction: Option<u64>,
) -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    let result = meta_client.list_compaction_group().await?;
    // Fetch current compaction config.
    let compaction_group = result
        .into_iter()
        .find(|cg| cg.id == compaction_group_id)
        .ok_or_else(|| anyhow!("unknown compaction group {}", compaction_group_id))?;
    // Apply changes to compaction config.
    let mut builder =
        CompactionConfigBuilder::new_with(compaction_group.compaction_config.unwrap());
    if let Some(max_bytes_for_level_base) = max_bytes_for_level_base {
        builder = builder.max_bytes_for_level_base(max_bytes_for_level_base);
    }
    if let Some(max_bytes_for_level_multiplier) = max_bytes_for_level_multiplier {
        builder = builder.max_bytes_for_level_base(max_bytes_for_level_multiplier);
    }
    if let Some(max_compaction_bytes) = max_compaction_bytes {
        builder = builder.max_bytes_for_level_base(max_compaction_bytes);
    }
    if let Some(sub_level_max_compaction_bytes) = sub_level_max_compaction_bytes {
        builder = builder.max_bytes_for_level_base(sub_level_max_compaction_bytes);
    }
    if let Some(level0_trigger_file_number) = level0_trigger_file_number {
        builder = builder.max_bytes_for_level_base(level0_trigger_file_number);
    }
    if let Some(level0_tier_compact_file_number) = level0_tier_compact_file_number {
        builder = builder.max_bytes_for_level_base(level0_tier_compact_file_number);
    }
    if let Some(target_file_size_base) = target_file_size_base {
        builder = builder.max_bytes_for_level_base(target_file_size_base);
    }
    if let Some(compaction_filter_mask) = compaction_filter_mask {
        builder = builder.max_bytes_for_level_base(compaction_filter_mask);
    }
    if let Some(max_sub_compaction) = max_sub_compaction {
        builder = builder.max_bytes_for_level_base(max_sub_compaction);
    }
    let new_compaction_config = builder.build();
    // Commit new compaction config.
    meta_client
        .update_compaction_config(compaction_group_id, new_compaction_config.clone())
        .await?;
    println!(
        "Applied compaction config {:#?}\nto compaction group {}",
        new_compaction_config, compaction_group_id
    );
    Ok(())
}
