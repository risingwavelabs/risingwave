use std::net::SocketAddr;
use std::time::Duration;

use futures::StreamExt;
use risingwave_common::array::DataChunk;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::task_service::exchange_service_client::ExchangeServiceClient;
use risingwave_pb::task_service::{GetDataRequest, GetDataResponse};
use tonic::transport::{Channel, Endpoint};
use tonic::Streaming;

use crate::task::{BatchTaskEnv, TaskId, TaskSink, TaskSinkId};

/// Each ExchangeSource maps to one task, it takes the execution result from task chunk by chunk.
#[async_trait::async_trait]
pub trait ExchangeSource: Send {
    async fn take_data(&mut self) -> Result<Option<DataChunk>>;
}

/// Use grpc client as the source.
pub struct GrpcExchangeSource {
    client: ExchangeServiceClient<Channel>,
    stream: Streaming<GetDataResponse>,

    // Address of the remote endpoint.
    addr: SocketAddr,
    sink_id: TaskSinkId,

    /// Id of task which contains the `ExchangeExecutor` of this source.
    task_id: TaskId,
}

impl GrpcExchangeSource {
    pub async fn create(addr: SocketAddr, task_id: TaskId, sink_id: TaskSinkId) -> Result<Self> {
        let mut client = ExchangeServiceClient::new(
            Endpoint::from_shared(format!("http://{}", addr))
                .map_err(|e| InternalError(format!("{}", e)))?
                .connect_timeout(Duration::from_secs(5))
                .connect()
                .await
                .to_rw_result_with(format!("failed to connect to {}", addr))?,
        );
        let stream = client
            .get_data(GetDataRequest {
                sink_id: Some(sink_id.to_prost()),
            })
            .await
            .to_rw_result_with(format!(
                "failed to create stream {:?} for sink_id={:?}",
                addr, sink_id
            ))?
            .into_inner();
        Ok(Self {
            client,
            stream,
            addr,
            sink_id,
            task_id,
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
        let task_data =
            res.to_rw_result_with(format!("failed to take data from stream ({:?})", self.addr))?;
        let data = DataChunk::from_protobuf(task_data.get_record_batch())?.compact()?;

        trace!(
            "Receiver task: {:?}, sink = {:?}, data = {:?}",
            self.task_id,
            self.sink_id,
            data
        );

        Ok(Some(data))
    }
}

/// Exchange data from a local task execution.
pub struct LocalExchangeSource {
    task_sink: TaskSink,

    /// Id of task which contains the `ExchangeExecutor` of this source.
    task_id: TaskId,
}

impl LocalExchangeSource {
    pub fn create(sink_id: TaskSinkId, env: BatchTaskEnv, task_id: TaskId) -> Result<Self> {
        let task_sink = env.task_manager().take_sink(&sink_id.to_prost())?;
        Ok(Self { task_sink, task_id })
    }
}

#[async_trait::async_trait]
impl ExchangeSource for LocalExchangeSource {
    async fn take_data(&mut self) -> Result<Option<DataChunk>> {
        let ret = self.task_sink.direct_take_data().await?;
        if let Some(data) = ret {
            let data = data.compact()?;
            debug!(
                "Receiver task: {:?}, source task sink: {:?}, data: {:?}",
                self.task_id,
                self.task_sink.id(),
                data
            );
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread::sleep;
    use std::time::Duration;

    use risingwave_common::util::addr::get_host_port;
    use risingwave_pb::data::{DataChunk, StreamMessage};
    use risingwave_pb::task_service::exchange_service_server::{
        ExchangeService, ExchangeServiceServer,
    };
    use risingwave_pb::task_service::{GetDataRequest, GetDataResponse, GetStreamRequest};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    use crate::execution::exchange_source::{ExchangeSource, GrpcExchangeSource};
    use crate::task::{TaskId, TaskSinkId};

    struct FakeExchangeService {
        rpc_called: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ExchangeService for FakeExchangeService {
        type GetDataStream = ReceiverStream<Result<GetDataResponse, Status>>;
        type GetStreamStream = ReceiverStream<std::result::Result<StreamMessage, Status>>;

        async fn get_data(
            &self,
            _: Request<GetDataRequest>,
        ) -> Result<Response<Self::GetDataStream>, Status> {
            let (tx, rx) = tokio::sync::mpsc::channel(10);
            self.rpc_called.store(true, Ordering::SeqCst);
            for _ in [0..3] {
                tx.send(Ok(GetDataResponse {
                    status: None,
                    record_batch: Some(DataChunk::default()),
                }))
                .await
                .unwrap();
            }
            Ok(Response::new(ReceiverStream::new(rx)))
        }

        async fn get_stream(
            &self,
            _request: Request<GetStreamRequest>,
        ) -> Result<Response<Self::GetStreamStream>, Status> {
            unimplemented!()
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_exchange_client() {
        let rpc_called = Arc::new(AtomicBool::new(false));
        let server_run = Arc::new(AtomicBool::new(false));
        let addr = get_host_port("127.0.0.1:12345").unwrap();

        // Start a server.
        let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
        let exchange_svc = ExchangeServiceServer::new(FakeExchangeService {
            rpc_called: rpc_called.clone(),
        });
        let cp_server_run = server_run.clone();
        let join_handle = tokio::spawn(async move {
            cp_server_run.store(true, Ordering::SeqCst);
            tonic::transport::Server::builder()
                .add_service(exchange_svc)
                .serve_with_shutdown(addr, async move {
                    shutdown_recv.recv().await;
                })
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(1));
        assert!(server_run.load(Ordering::SeqCst));

        let mut src = GrpcExchangeSource::create(addr, TaskId::default(), TaskSinkId::default())
            .await
            .unwrap();
        for _ in [0..3] {
            assert!(src.take_data().await.unwrap().is_some());
        }
        assert!(src.take_data().await.unwrap().is_none());
        assert!(rpc_called.load(Ordering::SeqCst));

        // Gracefully terminate the server.
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_unconnectable_node() {
        let addr = get_host_port("127.0.0.1:1001").unwrap();
        let res = GrpcExchangeSource::create(addr, TaskId::default(), TaskSinkId::default()).await;
        assert!(res.is_err());
    }
}
