use risingwave_common::array::DataChunk;
use risingwave_common::error::Result;
use risingwave_rpc_client::ExchangeSource;

use crate::task::{BatchEnvironment, TaskId, TaskSink, TaskSinkId};

/// Exchange data from a local task execution.
pub struct LocalExchangeSource {
    task_sink: TaskSink,

    /// Id of task which contains the `ExchangeExecutor` of this source.
    task_id: TaskId,
}

impl LocalExchangeSource {
    pub fn create(sink_id: TaskSinkId, env: BatchEnvironment, task_id: TaskId) -> Result<Self> {
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
            trace!(
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

    use risingwave_pb::data::DataChunk;
    use risingwave_pb::plan::TaskSinkId;
    use risingwave_pb::task_service::exchange_service_server::{
        ExchangeService, ExchangeServiceServer,
    };
    use risingwave_pb::task_service::{
        GetDataRequest, GetDataResponse, GetStreamRequest, GetStreamResponse,
    };
    use risingwave_rpc_client::{ExchangeSource, GrpcExchangeSource};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    struct FakeExchangeService {
        rpc_called: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ExchangeService for FakeExchangeService {
        type GetDataStream = ReceiverStream<Result<GetDataResponse, Status>>;
        type GetStreamStream = ReceiverStream<std::result::Result<GetStreamResponse, Status>>;

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
        let addr = "127.0.0.1:12345".parse().unwrap();

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

        let mut src = GrpcExchangeSource::create(addr, TaskSinkId::default())
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
        let addr = "127.0.0.1:1001".parse().unwrap();
        let res = GrpcExchangeSource::create(addr, TaskSinkId::default()).await;
        assert!(res.is_err());
    }
}
