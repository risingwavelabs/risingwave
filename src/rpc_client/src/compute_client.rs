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

use std::fmt::{Debug, Formatter};
use std::time::Duration;

use futures::StreamExt;
use log::trace;
use risingwave_common::array::DataChunk;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::plan::exchange_info::DistributionMode;
use risingwave_pb::plan::{ExchangeInfo, PlanFragment, PlanNode, TaskId, TaskOutputId};
use risingwave_pb::task_service::exchange_service_client::ExchangeServiceClient;
use risingwave_pb::task_service::task_service_client::TaskServiceClient;
use risingwave_pb::task_service::{
    CreateTaskRequest, CreateTaskResponse, GetDataRequest, GetDataResponse, GetStreamRequest,
    GetStreamResponse,
};
use tonic::transport::{Channel, Endpoint};
use tonic::Streaming;

#[derive(Clone)]
pub struct ComputeClient {
    pub exchange_client: ExchangeServiceClient<Channel>,
    pub task_client: TaskServiceClient<Channel>,
    pub addr: HostAddr,
}

impl ComputeClient {
    pub async fn new(addr: HostAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &addr))
            .map_err(|e| InternalError(format!("{}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .to_rw_result_with(|| format!("failed to connect to {}", &addr))?;
        let exchange_client = ExchangeServiceClient::new(channel.clone());
        let task_client = TaskServiceClient::new(channel);
        Ok(Self {
            exchange_client,
            task_client,
            addr,
        })
    }

    pub async fn get_data(&self, output_id: TaskOutputId) -> Result<GrpcExchangeSource> {
        let stream = self.get_data_inner(output_id.clone()).await?;
        Ok(GrpcExchangeSource {
            stream,
            task_id: output_id.get_task_id().unwrap().clone(),
            output_id,
        })
    }

    async fn get_data_inner(&self, output_id: TaskOutputId) -> Result<Streaming<GetDataResponse>> {
        Ok(self
            .exchange_client
            .to_owned()
            .get_data(GetDataRequest {
                task_output_id: Some(output_id.clone()),
            })
            .await
            .to_rw_result()?
            .into_inner())
    }

    pub async fn get_stream(
        &self,
        up_fragment_id: u32,
        down_fragment_id: u32,
    ) -> Result<Streaming<GetStreamResponse>> {
        Ok(self
            .exchange_client
            .to_owned()
            .get_stream(GetStreamRequest {
                up_fragment_id,
                down_fragment_id,
            })
            .await
            .to_rw_result_with(|| {
                format!(
                    "failed to create stream from remote_input {} from fragment {} to fragment {}",
                    self.addr, up_fragment_id, down_fragment_id
                )
            })?
            .into_inner())
    }

    // TODO: Remove this
    pub async fn create_task(&self, task_id: TaskId, plan: PlanNode, epoch: u64) -> Result<()> {
        let plan = PlanFragment {
            root: Some(plan),
            exchange_info: Some(ExchangeInfo {
                mode: DistributionMode::Single as i32,
                ..Default::default()
            }),
        };
        let _ = self
            .create_task_inner(CreateTaskRequest {
                task_id: Some(task_id),
                plan: Some(plan),
                epoch,
            })
            .await?;
        Ok(())
    }

    pub async fn create_task2(
        &self,
        task_id: TaskId,
        plan: PlanFragment,
        epoch: u64,
    ) -> Result<()> {
        let _ = self
            .create_task_inner(CreateTaskRequest {
                task_id: Some(task_id),
                plan: Some(plan),
                epoch,
            })
            .await?;
        Ok(())
    }

    async fn create_task_inner(&self, req: CreateTaskRequest) -> Result<CreateTaskResponse> {
        Ok(self
            .task_client
            .to_owned()
            .create_task(req)
            .await
            .to_rw_result()?
            .into_inner())
    }
}

/// Each ExchangeSource maps to one task, it takes the execution result from task chunk by chunk.
#[async_trait::async_trait]
pub trait ExchangeSource: Send + Debug {
    async fn take_data(&mut self) -> Result<Option<DataChunk>>;
}

/// Use grpc client as the source.
pub struct GrpcExchangeSource {
    stream: Streaming<GetDataResponse>,

    output_id: TaskOutputId,
    task_id: TaskId,
}

impl GrpcExchangeSource {
    pub async fn create(addr: HostAddr, output_id: TaskOutputId) -> Result<Self> {
        let client = ComputeClient::new(addr).await?;
        client.get_data(output_id).await
    }
}

impl Debug for GrpcExchangeSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcExchangeSource")
            .field("task_output_id", &self.output_id)
            .finish()
    }
}

#[async_trait::async_trait]
impl ExchangeSource for GrpcExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunk>> {
        let res = match self.stream.next().await {
            None => return Ok(None),
            Some(r) => r,
        };
        let task_data = res.to_rw_result()?;
        let data = DataChunk::from_protobuf(task_data.get_record_batch()?)?.compact()?;

        trace!(
            "Receiver task: {:?}, output = {:?}, data = {:?}",
            self.task_id,
            self.output_id,
            data
        );

        Ok(Some(data))
    }
}
