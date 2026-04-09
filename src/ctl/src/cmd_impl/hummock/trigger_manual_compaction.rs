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

use risingwave_hummock_sdk::HummockSstableId;
use risingwave_pb::id::{CompactionGroupId, JobId};
use risingwave_rpc_client::HummockMetaClient;
use tokio::time::{Duration, sleep};

use crate::CtlContext;

pub async fn trigger_manual_compaction(
    context: &CtlContext,
    compaction_group_id: CompactionGroupId,
    table_id: JobId,
    levels: Vec<u32>,
    sst_ids: Vec<HummockSstableId>,
    exclusive: bool,
    retry_interval_ms: u64,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    for level in levels {
        tracing::info!("Triggering manual compaction for level {level}...");
        loop {
            let result = meta_client
                .trigger_manual_compaction(
                    compaction_group_id,
                    table_id,
                    level,
                    sst_ids.clone(),
                    exclusive,
                )
                .await;
            match &result {
                Ok(should_retry) if *should_retry => {
                    if exclusive {
                        tracing::info!(
                            "Manual compaction is blocked by ongoing tasks. Sleeping {} ms before retrying.",
                            retry_interval_ms
                        );
                        sleep(Duration::from_millis(retry_interval_ms)).await;
                        continue;
                    }
                    tracing::info!("Level {level}: {:#?}", result);
                    break;
                }
                Ok(_) => {
                    tracing::info!("Level {level}: {:#?}", result);
                    break;
                }
                Err(_) => {
                    tracing::info!("Level {level}: {:#?}", result);
                    break;
                }
            }
        }
    }
    Ok(())
}
