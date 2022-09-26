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

use risingwave_pb::hummock::WriteLimiterThreshold;

use crate::common::MetaServiceOpts;

pub async fn set_write_limiter_threshold(
    max_sub_level_number: u64,
    max_delay_sec: u64,
    per_file_delay_sec: f32,
) -> anyhow::Result<()> {
    let meta_opts = MetaServiceOpts::from_env()?;
    let meta_client = meta_opts.create_meta_client().await?;
    let threshold = WriteLimiterThreshold {
        max_sub_level_number,
        max_delay_sec,
        per_file_delay_sec,
    };
    meta_client
        .risectl_set_write_limiter_threshold(threshold.clone())
        .await?;
    println!("Set write_limiter_threshold to {:#?}", threshold);
    Ok(())
}
