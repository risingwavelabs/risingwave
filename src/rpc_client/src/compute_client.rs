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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use risingwave_common::config::{MAX_CONNECTION_WINDOW_SIZE, RpcClientConfig, STREAM_WINDOW_SIZE};
use risingwave_common::id::{ActorId, FragmentId};
use risingwave_common::monitor::{EndpointExt, TcpConfig};
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::tracing::TracingContext;
use risingwave_pb::batch_plan::{PlanFragment, TaskId, TaskOutputId};
use risingwave_pb::compute::config_service_client::ConfigServiceClient;
use risingwave_pb::compute::{
    ResizeCacheRequest, ResizeCacheResponse, ShowConfigRequest, ShowConfigResponse,
};
use risingwave_pb::id::PartialGraphId;
use risingwave_pb::plan_common::ExprContext;
use risingwave_pb::task_service::batch_exchange_service_client::BatchExchangeServiceClient;
use risingwave_pb::task_service::stream_exchange_service_client::StreamExchangeServiceClient;
use risingwave_pb::task_service::task_service_client::TaskServiceClient;
use risingwave_pb::task_service::{
    CancelTaskRequest, CancelTaskResponse, CreateTaskRequest, ExecuteRequest, FastInsertRequest,
    FastInsertResponse, GetDataRequest, GetDataResponse, GetStreamRequest, GetStreamResponse,
    PbPermits, TaskInfoResponse, permits,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Streaming;
use tonic::transport::{Channel, Endpoint};

use crate::error::{Result, RpcError};
use crate::{RpcClient, RpcClientPool};

// TODO: this client has too many roles, e.g.
// - batch MPP task query execution
// - batch exchange
// - streaming exchange
// We should consider splitting them into different clients.
#[derive(Clone)]
pub struct ComputeClient {
    pub batch_exchange_client: BatchExchangeServiceClient<Channel>,
    pub stream_exchange_client: StreamExchangeServiceClient<Channel>,
    pub task_client: TaskServiceClient<Channel>,
    pub config_client: ConfigServiceClient<Channel>,
    pub addr: HostAddr,
}

impl ComputeClient {
    pub async fn new(addr: HostAddr, opts: &RpcClientConfig) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", &addr))?
            .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE)
            .initial_stream_window_size(STREAM_WINDOW_SIZE)
            .connect_timeout(Duration::from_secs(opts.connect_timeout_secs))
            .monitored_connect(
                "grpc-compute-client",
                TcpConfig {
                    tcp_nodelay: true,
                    ..Default::default()
                },
            )
            .await?;
        Ok(Self::with_channel(addr, channel))
    }

    pub fn with_channel(addr: HostAddr, channel: Channel) -> Self {
        let batch_exchange_client =
            BatchExchangeServiceClient::new(channel.clone()).max_decoding_message_size(usize::MAX);
        let stream_exchange_client =
            StreamExchangeServiceClient::new(channel.clone()).max_decoding_message_size(usize::MAX);
        let task_client =
            TaskServiceClient::new(channel.clone()).max_decoding_message_size(usize::MAX);
        let config_client = ConfigServiceClient::new(channel);
        Self {
            batch_exchange_client,
            stream_exchange_client,
            task_client,
            config_client,
            addr,
        }
    }

    pub async fn get_data(&self, output_id: TaskOutputId) -> Result<Streaming<GetDataResponse>> {
        Ok(self
            .batch_exchange_client
            .clone()
            .get_data(GetDataRequest {
                task_output_id: Some(output_id),
            })
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn get_stream(
        &self,
        up_actor_id: ActorId,
        down_actor_id: ActorId,
        up_fragment_id: FragmentId,
        down_fragment_id: FragmentId,
        up_partial_graph_id: PartialGraphId,
        term_id: String,
    ) -> Result<(
        Streaming<GetStreamResponse>,
        mpsc::UnboundedSender<permits::Value>,
    )> {
        use risingwave_pb::task_service::get_stream_request::*;

        // Create channel used for the downstream to add back the permits to the upstream.
        let (permits_tx, permits_rx) = mpsc::unbounded_channel();

        let request_stream = futures::stream::once(futures::future::ready(
            // `Get` as the first request.
            GetStreamRequest {
                value: Some(Value::Get(Get {
                    up_actor_id,
                    down_actor_id,
                    up_fragment_id,
                    down_fragment_id,
                    up_partial_graph_id,
                    term_id,
                })),
            },
        ))
        .chain(
            // `AddPermits` as the followings.
            UnboundedReceiverStream::new(permits_rx).map(|permits| GetStreamRequest {
                value: Some(Value::AddPermits(PbPermits {
                    value: Some(permits),
                })),
            }),
        );

        let response_stream = self
            .stream_exchange_client
            .clone()
            .get_stream(request_stream)
            .await
            .inspect_err(|_| {
                tracing::error!(
                    "failed to create stream from remote_input {} from actor {} to actor {}",
                    self.addr,
                    up_actor_id,
                    down_actor_id
                )
            })
            .map_err(RpcError::from_compute_status)?
            .into_inner();

        Ok((response_stream, permits_tx))
    }

    pub async fn create_task(
        &self,
        task_id: TaskId,
        plan: PlanFragment,
        expr_context: ExprContext,
    ) -> Result<Streaming<TaskInfoResponse>> {
        Ok(self
            .task_client
            .clone()
            .create_task(CreateTaskRequest {
                task_id: Some(task_id),
                plan: Some(plan),
                tracing_context: TracingContext::from_current_span().to_protobuf(),
                expr_context: Some(expr_context),
            })
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn execute(&self, req: ExecuteRequest) -> Result<Streaming<GetDataResponse>> {
        Ok(self
            .task_client
            .clone()
            .execute(req)
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn cancel(&self, req: CancelTaskRequest) -> Result<CancelTaskResponse> {
        Ok(self
            .task_client
            .clone()
            .cancel_task(req)
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn fast_insert(&self, req: FastInsertRequest) -> Result<FastInsertResponse> {
        Ok(self
            .task_client
            .clone()
            .fast_insert(req)
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn show_config(&self) -> Result<ShowConfigResponse> {
        Ok(self
            .config_client
            .clone()
            .show_config(ShowConfigRequest {})
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }

    pub async fn resize_cache(&self, request: ResizeCacheRequest) -> Result<ResizeCacheResponse> {
        Ok(self
            .config_client
            .clone()
            .resize_cache(request)
            .await
            .map_err(RpcError::from_compute_status)?
            .into_inner())
    }
}

#[async_trait]
impl RpcClient for ComputeClient {
    async fn new_client(host_addr: HostAddr, opts: &RpcClientConfig) -> Result<Self> {
        Self::new(host_addr, opts).await
    }
}

pub type ComputeClientPool = RpcClientPool<ComputeClient>;
pub type ComputeClientPoolRef = Arc<ComputeClientPool>; // TODO: no need for `Arc` since clone is cheap and shared
