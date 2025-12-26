use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use risingwave_common::config::RpcClientConfig;
use risingwave_common::monitor::EndpointExt as _;
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use risingwave_pb::monitor_service::{
    AnalyzeHeapRequest, AnalyzeHeapResponse, GetStreamingStatsRequest, GetStreamingStatsResponse,
    HeapProfilingRequest, HeapProfilingResponse, ListHeapProfilingRequest,
    ListHeapProfilingResponse, ProfilingRequest, ProfilingResponse, StackTraceRequest,
    StackTraceResponse,
};
use tonic::transport::Endpoint;

use crate::channel::{Channel, WrappedChannelExt};
use crate::error::{Result, RpcError};
use crate::{RpcClient, RpcClientPool};

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

    pub async fn stack_trace(&self, req: StackTraceRequest) -> Result<StackTraceResponse> {
        Ok(self
            .monitor_client
            .clone()
            .stack_trace(req)
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn get_streaming_stats(&self) -> Result<GetStreamingStatsResponse> {
        Ok(self
            .monitor_client
            .clone()
            .get_streaming_stats(GetStreamingStatsRequest::default())
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn profile(&self, sleep_s: u64) -> Result<ProfilingResponse> {
        Ok(self
            .monitor_client
            .clone()
            .profiling(ProfilingRequest { sleep_s })
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn heap_profile(&self, dir: String) -> Result<HeapProfilingResponse> {
        Ok(self
            .monitor_client
            .clone()
            .heap_profiling(HeapProfilingRequest { dir })
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn list_heap_profile(&self) -> Result<ListHeapProfilingResponse> {
        Ok(self
            .monitor_client
            .clone()
            .list_heap_profiling(ListHeapProfilingRequest {})
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn analyze_heap(&self, path: String) -> Result<AnalyzeHeapResponse> {
        Ok(self
            .monitor_client
            .clone()
            .analyze_heap(AnalyzeHeapRequest { path })
            .await
            .map_err(RpcError::from_compute_status)?
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
