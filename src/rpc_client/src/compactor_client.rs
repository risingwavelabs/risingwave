// Copyright 2023 RisingWave Labs
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

use risingwave_common::util::addr::HostAddr;
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::hummock::{
    GetNewSstIdsRequest, GetNewSstIdsResponse, ReportCompactionTaskRequest,
    ReportCompactionTaskResponse, ReportFullScanTaskRequest, ReportFullScanTaskResponse,
    ReportVacuumTaskRequest, ReportVacuumTaskResponse,
};
use risingwave_pb::meta::system_params_service_client::SystemParamsServiceClient;
use risingwave_pb::meta::{GetSystemParamsRequest, GetSystemParamsResponse};
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use risingwave_pb::monitor_service::{StackTraceRequest, StackTraceResponse};
use tokio::sync::RwLock;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tonic::transport::{Channel, Endpoint};

use crate::error::Result;
const ENDPOINT_KEEP_ALIVE_INTERVAL_SEC: u64 = 60;
// See `Endpoint::keep_alive_timeout`
const ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC: u64 = 60;

const DEFAULT_RETRY_INTERVAL: u64 = 20;
const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_RETRY_MAX_ATTEMPTS: usize = 3;
#[derive(Clone)]
pub struct CompactorClient {
    pub monitor_client: MonitorServiceClient<Channel>,
}

impl CompactorClient {
    pub async fn new(host_addr: HostAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &host_addr))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await?;
        Ok(Self {
            monitor_client: MonitorServiceClient::new(channel),
        })
    }

    pub async fn stack_trace(&self) -> Result<StackTraceResponse> {
        Ok(self
            .monitor_client
            .to_owned()
            .stack_trace(StackTraceRequest::default())
            .await?
            .into_inner())
    }
}

#[derive(Debug, Clone)]
pub struct GrpcCompactorProxyClientCore {
    hummock_client: HummockManagerServiceClient<Channel>,
    system_params_client: SystemParamsServiceClient<Channel>,
}

impl GrpcCompactorProxyClientCore {
    pub(crate) fn new(channel: Channel) -> Self {
        let hummock_client =
            HummockManagerServiceClient::new(channel.clone()).max_decoding_message_size(usize::MAX);
        let system_params_client = SystemParamsServiceClient::new(channel);

        Self {
            hummock_client,
            system_params_client,
        }
    }
}

/// Client to proxy server. Cloning the instance is lightweight.
///
/// Todo(wcy-fdu): add refresh client interface.
#[derive(Debug, Clone)]
pub struct GrpcCompactorProxyClient {
    pub core: Arc<RwLock<GrpcCompactorProxyClientCore>>,
    endpoint: String,
}

impl GrpcCompactorProxyClient {
    pub fn new(channel: Channel, endpoint: String) -> Self {
        let core = Arc::new(RwLock::new(GrpcCompactorProxyClientCore::new(channel)));
        Self { core, endpoint }
    }

    async fn recreate_core(&self) {
        tracing::info!("GrpcCompactorProxyClient rpc transfer failed, try to reconnect");
        let channel = self.connect_to_endpoint().await;
        let mut core = self.core.write().await;
        *core = GrpcCompactorProxyClientCore::new(channel);
    }

    async fn connect_to_endpoint(&self) -> Channel {
        let endpoint_str: &'static str = Box::leak(self.endpoint.clone().into_boxed_str());
        let endpoint = Endpoint::from_static(endpoint_str);
        endpoint
            .http2_keep_alive_interval(Duration::from_secs(ENDPOINT_KEEP_ALIVE_INTERVAL_SEC))
            .keep_alive_timeout(Duration::from_secs(ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC))
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .expect("Failed to create channel via proxy rpc endpoint.")
    }

    pub async fn get_new_sst_ids(
        &self,
        request: GetNewSstIdsRequest,
    ) -> std::result::Result<tonic::Response<GetNewSstIdsResponse>, tonic::Status> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut hummock_client = self.core.read().await.hummock_client.clone();
                let rpc_res = hummock_client.get_new_sst_ids(request.clone()).await;
                if rpc_res.is_err() {
                    self.recreate_core().await;
                }
                rpc_res
            },
            Self::should_retry,
        )
        .await
    }

    pub async fn report_compaction_task(
        &self,
        request: ReportCompactionTaskRequest,
    ) -> std::result::Result<tonic::Response<ReportCompactionTaskResponse>, tonic::Status> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut hummock_client = self.core.read().await.hummock_client.clone();
                let rpc_res = hummock_client.report_compaction_task(request.clone()).await;
                if rpc_res.is_err() {
                    self.recreate_core().await;
                }
                rpc_res
            },
            Self::should_retry,
        )
        .await
    }

    pub async fn report_full_scan_task(
        &self,
        request: ReportFullScanTaskRequest,
    ) -> std::result::Result<tonic::Response<ReportFullScanTaskResponse>, tonic::Status> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut hummock_client = self.core.read().await.hummock_client.clone();
                let rpc_res = hummock_client.report_full_scan_task(request.clone()).await;
                if rpc_res.is_err() {
                    self.recreate_core().await;
                }
                rpc_res
            },
            Self::should_retry,
        )
        .await
    }

    pub async fn report_vacuum_task(
        &self,
        request: ReportVacuumTaskRequest,
    ) -> std::result::Result<tonic::Response<ReportVacuumTaskResponse>, tonic::Status> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut hummock_client = self.core.read().await.hummock_client.clone();
                let rpc_res = hummock_client.report_vacuum_task(request.clone()).await;
                if rpc_res.is_err() {
                    self.recreate_core().await;
                }
                rpc_res
            },
            Self::should_retry,
        )
        .await
    }

    pub async fn get_system_params(
        &self,
    ) -> std::result::Result<tonic::Response<GetSystemParamsResponse>, tonic::Status> {
        tokio_retry::RetryIf::spawn(
            Self::get_retry_strategy(),
            || async {
                let mut system_params_client = self.core.read().await.system_params_client.clone();
                let rpc_res = system_params_client
                    .get_system_params(GetSystemParamsRequest {})
                    .await;
                if rpc_res.is_err() {
                    self.recreate_core().await;
                }
                rpc_res
            },
            Self::should_retry,
        )
        .await
    }

    #[inline(always)]
    fn get_retry_strategy() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(DEFAULT_RETRY_INTERVAL)
            .max_delay(DEFAULT_RETRY_MAX_DELAY)
            .take(DEFAULT_RETRY_MAX_ATTEMPTS)
            .map(jitter)
    }

    #[inline(always)]
    fn should_retry(status: &tonic::Status) -> bool {
        if status.code() == tonic::Code::Unavailable
            || status.code() == tonic::Code::Unknown
            || (status.code() == tonic::Code::Unauthenticated
                && status.message().contains("invalid auth token"))
        {
            return true;
        }
        false
    }
}
