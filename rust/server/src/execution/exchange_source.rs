use crate::array2::{DataChunk, DataChunkRef};
use crate::error::ErrorCode::{GrpcError, InternalError};
use crate::error::{Result, RwError};
use crate::task::{GlobalTaskEnv, TaskExecution};
use futures::StreamExt;
use grpcio::{CallOption, ChannelBuilder, ClientSStreamReceiver, Environment};
use risingwave_proto::task_service::{TaskData, TaskSinkId};
use risingwave_proto::task_service_grpc::ExchangeServiceClient;
use std::net::SocketAddr;
use std::sync::Arc;

/// Each ExchangeSource maps to one task, it takes the execution result from task chunk by chunk.
#[async_trait::async_trait]
pub trait ExchangeSource: Send {
    async fn take_data(&mut self) -> Result<Option<DataChunkRef>>;
}

/// Use grpc client as the source.
pub struct GrpcExchangeSource {
    client: ExchangeServiceClient,
    addr: SocketAddr,
    sink_id: TaskSinkId,
}

impl GrpcExchangeSource {
    pub fn create(addr: SocketAddr, sink_id: TaskSinkId) -> Result<Self> {
        let channel =
            ChannelBuilder::new(Arc::new(Environment::new(2))).connect(addr.to_string().as_str());
        let client = ExchangeServiceClient::new(channel);
        Ok(Self {
            client,
            sink_id,
            addr,
        })
    }

    fn create_stream(&self) -> Result<ClientSStreamReceiver<TaskData>> {
        let opt = CallOption::default();
        self.client.get_data_opt(&self.sink_id, opt).map_err(|e| {
            RwError::from(GrpcError(
                format!(
                    "failed to create stream {} for sink_id={}",
                    self.addr,
                    self.sink_id.get_sink_id()
                ),
                e,
            ))
        })
    }
}

#[async_trait::async_trait]
impl ExchangeSource for GrpcExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunkRef>> {
        let mut stream = self.create_stream()?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        let addr = self.addr;
        self.client.spawn(async move {
            let res: Result<Option<TaskData>> = match stream.next().await {
                Some(r) => r
                    .map_err(|e| {
                        RwError::from(GrpcError("failed to take data from stream".to_string(), e))
                    })
                    .map(Some),
                None => Ok(None),
            };
            if matches!(tx.send(res), Err(_)) {
                error!("broken channel of stream from {}", addr);
            }
        });

        // Awaits for the client's result.
        let opt = rx.await.map_err(|e| {
            RwError::from(InternalError(format!(
                "failed to receive from channel: {}",
                e
            )))
        })??;
        match opt {
            Some(task_data) => Ok(Some(Arc::new(DataChunk::from_protobuf(
                task_data.get_record_batch(),
            )?))),
            None => Ok(None),
        }
    }
}

/// Exchange data from a local task execution.
pub struct LocalExchangeSource {
    task: Box<TaskExecution>,
    sink_id: TaskSinkId,
}

impl LocalExchangeSource {
    pub fn create(sink_id: TaskSinkId, env: GlobalTaskEnv) -> Result<Self> {
        let task = env.task_manager().take_task(&sink_id)?;
        Ok(Self { sink_id, task })
    }
}

#[async_trait::async_trait]
impl ExchangeSource for LocalExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunkRef>> {
        self.task.direct_take_data(self.sink_id.get_sink_id()).await
    }
}
