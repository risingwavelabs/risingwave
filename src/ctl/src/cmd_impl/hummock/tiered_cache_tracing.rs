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

use std::time::Duration;

use futures::future::try_join_all;
use itertools::Itertools;
use risingwave_common::monitor::EndpointExt;
use risingwave_pb::monitor_service::TieredCacheTracingRequest;
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use tonic::transport::Endpoint;

use crate::common::CtlContext;

pub async fn tiered_cache_tracing(
    context: &CtlContext,
    enable: bool,
    record_hybrid_insert_threshold_ms: Option<u32>,
    record_hybrid_get_threshold_ms: Option<u32>,
    record_hybrid_obtain_threshold_ms: Option<u32>,
    record_hybrid_remove_threshold_ms: Option<u32>,
    record_hybrid_fetch_threshold_ms: Option<u32>,
) -> anyhow::Result<()> {
    let meta_client = context.meta_client().await?;
    let info = meta_client.get_cluster_info().await?;
    let futures = info
        .get_worker_nodes()
        .iter()
        .map(|worker_node| async {
            let addr = worker_node.get_host().unwrap();
            let channel = Endpoint::from_shared(format!("http://{}:{}", addr.host, addr.port))?
                .connect_timeout(Duration::from_secs(5))
                .monitored_connect("grpc-tiered-cache-tracing-client", Default::default())
                .await?;
            let mut client = MonitorServiceClient::new(channel);
            client
                .tiered_cache_tracing(TieredCacheTracingRequest {
                    enable,
                    record_hybrid_insert_threshold_ms,
                    record_hybrid_get_threshold_ms,
                    record_hybrid_obtain_threshold_ms,
                    record_hybrid_remove_threshold_ms,
                    record_hybrid_fetch_threshold_ms,
                })
                .await?;
            Ok::<_, anyhow::Error>(())
        })
        .collect_vec();
    try_join_all(futures).await?;
    Ok(())
}
