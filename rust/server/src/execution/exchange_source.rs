use crate::array::DataChunk;
use crate::error::ErrorCode::GrpcError;
use crate::error::Result;
use crate::task::{GlobalTaskEnv, TaskSink};
use futures::StreamExt;
use grpcio::{ChannelBuilder, ClientSStreamReceiver, Environment};
use risingwave_proto::task_service::{TaskData, TaskSinkId};
use risingwave_proto::task_service_grpc::ExchangeServiceClient;
use std::net::SocketAddr;
use std::sync::Arc;

/// Each ExchangeSource maps to one task, it takes the execution result from task chunk by chunk.
#[async_trait::async_trait]
pub trait ExchangeSource: Send {
    async fn take_data(&mut self) -> Result<Option<DataChunk>>;
}

/// Use grpc client as the source.
pub struct GrpcExchangeSource {
    client: ExchangeServiceClient,
    stream: ClientSStreamReceiver<TaskData>,

    // Address of the remote endpoint.
    addr: SocketAddr,
    sink_id: TaskSinkId,
}

impl GrpcExchangeSource {
    pub fn create(addr: SocketAddr, sink_id: TaskSinkId) -> Result<Self> {
        let channel =
            ChannelBuilder::new(Arc::new(Environment::new(2))).connect(addr.to_string().as_str());
        let client = ExchangeServiceClient::new(channel);
        let stream = client.get_data(&sink_id).map_err(|e| {
            GrpcError(
                format!(
                    "failed to create stream {} for sink_id={}",
                    addr,
                    sink_id.get_sink_id()
                ),
                e,
            )
        })?;
        Ok(Self {
            client,
            stream,
            sink_id,
            addr,
        })
    }
}

#[async_trait::async_trait]
impl ExchangeSource for GrpcExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunk>> {
        let res = match self.stream.next().await {
            None => return Ok(None),
            Some(r) => r,
        };
        let task_data = res.map_err(|e| {
            GrpcError(
                format!("failed to take data from stream ({:?})", self.addr),
                e,
            )
        })?;
        Ok(Some(DataChunk::from_protobuf(
            task_data.get_record_batch(),
        )?))
    }
}

/// Exchange data from a local task execution.
pub struct LocalExchangeSource {
    task_sink: TaskSink,
}

impl LocalExchangeSource {
    pub fn create(sink_id: TaskSinkId, env: GlobalTaskEnv) -> Result<Self> {
        let task_sink = env.task_manager().take_sink(&sink_id)?;
        Ok(Self { task_sink })
    }
}

#[async_trait::async_trait]
impl ExchangeSource for LocalExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunk>> {
        Ok(self.task_sink.direct_take_data().await?)
    }
}
