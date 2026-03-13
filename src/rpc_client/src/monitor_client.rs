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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use risingwave_common::config::RpcClientConfig;
use risingwave_common::monitor::EndpointExt as _;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use risingwave_pb::monitor_service::{
    AnalyzeHeapRequest, AnalyzeHeapResponse, GetProfileStatsRequest, GetProfileStatsResponse,
    GetStreamingStatsRequest, GetStreamingStatsResponse, HeapProfilingRequest,
    HeapProfilingResponse, ListHeapProfilingRequest, ListHeapProfilingResponse, ProfilingRequest,
    ProfilingResponse, StackTraceRequest, StackTraceResponse,
};
use tonic::transport::Endpoint;

use crate::channel::{Channel, WrappedChannelExt};
use crate::error::{Result, RpcError};
use crate::{RpcClient, RpcClientPool};

/// Client for monitoring and profiling.
///
/// Can be applied to any type of worker node, though some methods may not be supported. See
/// documentation of each method for availability.
#[derive(Clone)]
pub struct MonitorClient {
    pub monitor_client: MonitorServiceClient<Channel>,
}

impl MonitorClient {
    pub async fn new(host_addr: HostAddr, opts: &RpcClientConfig) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &host_addr))?
            .connect_timeout(Duration::from_secs(opts.connect_timeout_secs))
            .monitored_connect("grpc-monitor-client", Default::default())
            .await?
            .wrapped();

        Ok(Self {
            monitor_client: MonitorServiceClient::new(channel),
        })
    }

    /// Available on meta node, compute node, and compactor node.
    ///
    /// Note: for meta node, it will gather await tree from all nodes in the cluster besides itself.
    pub async fn await_tree(&self, req: StackTraceRequest) -> Result<StackTraceResponse> {
        Ok(self
            .monitor_client
            .clone()
            .stack_trace(req)
            .await
            .map_err(RpcError::from_monitor_status)?
            .into_inner())
    }

    /// Available on compute node.
    pub async fn get_streaming_stats(&self) -> Result<GetStreamingStatsResponse> {
        Ok(self
            .monitor_client
            .clone()
            .get_streaming_stats(GetStreamingStatsRequest::default())
            .await
            .map_err(RpcError::from_monitor_status)?
            .into_inner())
    }

    /// Available on compute node.
    pub async fn get_profile_stats(
        &self,
        request: GetProfileStatsRequest,
    ) -> Result<GetProfileStatsResponse> {
        Ok(self
            .monitor_client
            .clone()
            .get_profile_stats(request)
            .await
            .map_err(RpcError::from_monitor_status)?
            .into_inner())
    }

    /// Available on meta node, compute node, compactor node, and frontend node.
    pub async fn profile(&self, sleep_s: u64) -> Result<ProfilingResponse> {
        Ok(self
            .monitor_client
            .clone()
            .profiling(ProfilingRequest { sleep_s })
            .await
            .map_err(RpcError::from_monitor_status)?
            .into_inner())
    }

    /// Available on meta node, compute node, compactor node, and frontend node.
    pub async fn heap_profile(&self, dir: String) -> Result<HeapProfilingResponse> {
        Ok(self
            .monitor_client
            .clone()
            .heap_profiling(HeapProfilingRequest { dir })
            .await
            .map_err(RpcError::from_monitor_status)?
            .into_inner())
    }

    /// Available on meta node, compute node, compactor node, and frontend node.
    pub async fn list_heap_profile(&self) -> Result<ListHeapProfilingResponse> {
        Ok(self
            .monitor_client
            .clone()
            .list_heap_profiling(ListHeapProfilingRequest {})
            .await
            .map_err(RpcError::from_monitor_status)?
            .into_inner())
    }

    /// Available on meta node, compute node, compactor node, and frontend node.
    pub async fn analyze_heap(&self, path: String) -> Result<AnalyzeHeapResponse> {
        Ok(self
            .monitor_client
            .clone()
            .analyze_heap(AnalyzeHeapRequest { path })
            .await
            .map_err(RpcError::from_monitor_status)?
            .into_inner())
    }
}

pub type MonitorClientPool = RpcClientPool<MonitorClient>;
pub type MonitorClientPoolRef = Arc<MonitorClientPool>;

#[async_trait]
impl RpcClient for MonitorClient {
    async fn new_client(host_addr: HostAddr, opts: &RpcClientConfig) -> Result<Self> {
        Self::new(host_addr, opts).await
    }
}
