use std::net::SocketAddr;
use std::time::Duration;

use futures::StreamExt;
use log::trace;
use risingwave_common::array::DataChunk;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::plan::exchange_info::DistributionMode;
use risingwave_pb::plan::{ExchangeInfo, PlanFragment, PlanNode, TaskId, TaskSinkId};
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
    pub addr: SocketAddr,
}

impl ComputeClient {
    pub async fn new(addr: &SocketAddr) -> Result<Self> {
        let channel = Endpoint::from_shared(format!("http://{}", *addr))
            .map_err(|e| InternalError(format!("{}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .to_rw_result_with(format!("failed to connect to {}", *addr))?;
        let exchange_client = ExchangeServiceClient::new(channel.clone());
        let task_client = TaskServiceClient::new(channel);
        Ok(Self {
            addr: *addr,
            exchange_client,
            task_client,
        })
    }

    pub async fn get_data(&self, sink_id: TaskSinkId) -> Result<GrpcExchangeSource> {
        let stream = self.get_data_inner(sink_id.clone()).await?;
        let addr = self.addr;
        Ok(GrpcExchangeSource {
            client: self.clone(),
            stream,
            addr,
            task_id: sink_id.get_task_id().unwrap().clone(),
            sink_id,
        })
    }

    async fn get_data_inner(&self, sink_id: TaskSinkId) -> Result<Streaming<GetDataResponse>> {
        Ok(self
            .exchange_client
            .to_owned()
            .get_data(GetDataRequest {
                sink_id: Some(sink_id.clone()),
            })
            .await
            .to_rw_result_with(format!(
                "failed to create stream {:?} for sink_id={:?}",
                self.addr, sink_id
            ))?
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
            .to_rw_result_with(format!(
                "failed to create stream from remote_input {} from fragment {} to fragment {}",
                self.addr, up_fragment_id, down_fragment_id
            ))?
            .into_inner())
    }

    pub async fn create_task(&self, task_id: TaskId, plan: PlanNode) -> Result<()> {
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
                ..Default::default()
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
pub trait ExchangeSource: Send {
    async fn take_data(&mut self) -> Result<Option<DataChunk>>;
}

/// Use grpc client as the source.
pub struct GrpcExchangeSource {
    client: ComputeClient,
    stream: Streaming<GetDataResponse>,

    // Address of the remote endpoint.
    addr: SocketAddr,
    sink_id: TaskSinkId,
    task_id: TaskId,
}

impl GrpcExchangeSource {
    pub async fn create(addr: SocketAddr, sink_id: TaskSinkId) -> Result<Self> {
        let client = ComputeClient::new(&addr).await?;
        client.get_data(sink_id).await
    }
}

#[async_trait::async_trait]
impl ExchangeSource for GrpcExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunk>> {
        let res = match self.stream.next().await {
            None => return Ok(None),
            Some(r) => r,
        };
        let task_data =
            res.to_rw_result_with(format!("failed to take data from stream ({:?})", self.addr))?;
        let data = DataChunk::from_protobuf(task_data.get_record_batch()?)?.compact()?;

        trace!(
            "Receiver task: {:?}, sink = {:?}, data = {:?}",
            self.task_id,
            self.sink_id,
            data
        );

        Ok(Some(data))
    }
}
