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
use std::future::Future;

use risingwave_common::array::DataChunk;
use risingwave_common::error::Result;

use crate::exchange_source::ExchangeSource;
use crate::task::{BatchTaskContext, TaskId, TaskOutput, TaskOutputId};

/// Exchange data from a local task execution.
pub struct LocalExchangeSource {
    task_output: TaskOutput,

    /// Id of task which contains the `ExchangeExecutor` of this source.
    task_id: TaskId,
}

impl LocalExchangeSource {
    pub fn create<C: BatchTaskContext>(
        output_id: TaskOutputId,
        context: C,
        task_id: TaskId,
    ) -> Result<Self> {
        let task_output = context.get_task_output(output_id)?;
        Ok(Self {
            task_output,
            task_id,
        })
    }
}

impl Debug for LocalExchangeSource {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalExchangeSource")
            .field("task_output_id", self.task_output.id())
            .finish()
    }
}

impl ExchangeSource for LocalExchangeSource {
    type TakeDataFuture<'a> = impl Future<Output = Result<Option<DataChunk>>>;

    fn take_data(&mut self) -> Self::TakeDataFuture<'_> {
        async {
            let ret = self.task_output.direct_take_data().await?;
            if let Some(data) = ret {
                let data = data.compact()?;
                trace!(
                    "Receiver task: {:?}, source task output: {:?}, data: {:?}",
                    self.task_id,
                    self.task_output.id(),
                    data
                );
                Ok(Some(data))
            } else {
                Ok(None)
            }
        }
    }

    fn get_task_id(&self) -> TaskId {
        self.task_id.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use risingwave_common::util::addr::HostAddr;
    use risingwave_pb::batch_plan::{TaskId, TaskOutputId};
    use risingwave_pb::data::DataChunk;
    use risingwave_pb::task_service::exchange_service_server::{
        ExchangeService, ExchangeServiceServer,
    };
    use risingwave_pb::task_service::{
        GetDataRequest, GetDataResponse, GetStreamRequest, GetStreamResponse,
    };
    use risingwave_rpc_client::ComputeClient;
    use tokio::time::sleep;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    use crate::exchange_source::ExchangeSource;
    use crate::execution::grpc_exchange::GrpcExchangeSource;

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
            for _ in 0..3 {
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

    #[tokio::test]
    async fn test_exchange_client() {
        let rpc_called = Arc::new(AtomicBool::new(false));
        let server_run = Arc::new(AtomicBool::new(false));
        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();

        // Start a server.
        let (shutdown_send, shutdown_recv) = tokio::sync::oneshot::channel();
        let exchange_svc = ExchangeServiceServer::new(FakeExchangeService {
            rpc_called: rpc_called.clone(),
        });
        let cp_server_run = server_run.clone();
        let join_handle = tokio::spawn(async move {
            cp_server_run.store(true, Ordering::SeqCst);
            tonic::transport::Server::builder()
                .add_service(exchange_svc)
                .serve_with_shutdown(addr, async move {
                    shutdown_recv.await.unwrap();
                })
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(1)).await;
        assert!(server_run.load(Ordering::SeqCst));

        let client = ComputeClient::new(addr.into()).await.unwrap();
        let task_output_id = TaskOutputId {
            task_id: Some(TaskId::default()),
            ..Default::default()
        };
        let mut src = GrpcExchangeSource::create(client, task_output_id, None)
            .await
            .unwrap();
        for _ in 0..3 {
            assert!(src.take_data().await.unwrap().is_some());
        }
        assert!(src.take_data().await.unwrap().is_none());
        assert!(rpc_called.load(Ordering::SeqCst));

        // Gracefully terminate the server.
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
