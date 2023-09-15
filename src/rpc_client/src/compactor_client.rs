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
use risingwave_pb::monitor_service::monitor_service_client::MonitorServiceClient;
use risingwave_pb::monitor_service::{StackTraceRequest, StackTraceResponse};
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};

use crate::error::Result;

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
    pub system_params_client: SystemParamsServiceClient<Channel>,
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
}

impl GrpcCompactorProxyClient {
    pub fn new(channel: Channel) -> Self {
        let core = Arc::new(RwLock::new(GrpcCompactorProxyClientCore::new(channel)));
        Self { core }
    }

    fn _recreate_core(&self, _channel: Channel) {
        todo!()
    }

    pub async fn get_new_sst_ids(
        &mut self,
        request: GetNewSstIdsRequest,
    ) -> std::result::Result<tonic::Response<GetNewSstIdsResponse>, tonic::Status> {
        let mut hummock_client = self.core.read().await.hummock_client.clone();
        hummock_client.get_new_sst_ids(request).await
    }

    pub async fn report_compaction_task(
        &mut self,
        request: ReportCompactionTaskRequest,
    ) -> std::result::Result<tonic::Response<ReportCompactionTaskResponse>, tonic::Status> {
        let mut hummock_client = self.core.read().await.hummock_client.clone();
        hummock_client.report_compaction_task(request).await
    }

    pub async fn report_full_scan_task(
        &mut self,
        request: ReportFullScanTaskRequest,
    ) -> std::result::Result<tonic::Response<ReportFullScanTaskResponse>, tonic::Status> {
        let mut hummock_client = self.core.read().await.hummock_client.clone();
        hummock_client.report_full_scan_task(request).await
    }

    pub async fn report_vacuum_task(
        &mut self,
        request: ReportVacuumTaskRequest,
    ) -> std::result::Result<tonic::Response<ReportVacuumTaskResponse>, tonic::Status> {
        let mut hummock_client = self.core.read().await.hummock_client.clone();
        hummock_client.report_vacuum_task(request).await
    }
}
