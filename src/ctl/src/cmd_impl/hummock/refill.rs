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

use std::time::Duration;

use anyhow::Context;
use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::monitor::EndpointExt;
use risingwave_pb::common::WorkerType;
use risingwave_pb::monitor_service::GetTableCacheRefillStatsRequest;
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use serde::Serialize;
use serde_json::Value;
use tonic::transport::Endpoint;

use crate::common::CtlContext;

#[derive(Serialize)]
struct RefillStatsEntry {
    worker_id: u32,
    host: String,
    port: u32,
    stats: Value,
}

pub async fn refill_stats(context: &CtlContext) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let worker_nodes = meta_client.get_cluster_info().await?.worker_nodes;

    let futures = worker_nodes
        .into_iter()
        .filter(|worker| worker.r#type() == WorkerType::ComputeNode)
        .map(|worker| async move {
            let host = worker.get_host().context("compute node host is missing")?;
            let endpoint = format!("http://{}:{}", host.host, host.port);
            let channel = Endpoint::from_shared(endpoint)?
                .connect_timeout(Duration::from_secs(5))
                .monitored_connect("grpc-table-cache-refill-stats-client", Default::default())
                .await?;
            let mut client = MonitorServiceClient::new(channel);
            let response = client
                .get_table_cache_refill_stats(GetTableCacheRefillStatsRequest {})
                .await?
                .into_inner();
            let stats: Value = serde_json::from_str(&response.stats)
                .context("failed to parse table cache refill stats json")?;
            Ok::<_, anyhow::Error>(RefillStatsEntry {
                worker_id: worker.id.as_raw_id(),
                host: host.host.clone(),
                port: host.port as _,
                stats,
            })
        })
        .collect_vec();

    let results = try_join_all(futures).await?;
    println!("{}", serde_json::to_string_pretty(&results)?);
    Ok(())
}

pub async fn warm_up_table_cache(
    context: &CtlContext,
    table_id: TableId,
    concurrency: u32,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let (key_count, worker_count) = meta_client
        .warm_up_table_cache(table_id, concurrency)
        .await?;
    println!(
        "Warmed up cache for table {table_id} on {worker_count} compute nodes ({key_count} keys scanned)"
    );
    Ok(())
}
