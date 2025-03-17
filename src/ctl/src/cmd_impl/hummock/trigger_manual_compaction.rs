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

use risingwave_rpc_client::HummockMetaClient;

use crate::CtlContext;

pub async fn trigger_manual_compaction(
    context: &CtlContext,
    compaction_group_id: u64,
    table_id: u32,
    level: u32,
    sst_ids: Vec<u64>,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let result = meta_client
        .trigger_manual_compaction(compaction_group_id, table_id, level, sst_ids)
        .await;
    println!("{:#?}", result);
    Ok(())
}
