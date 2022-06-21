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

use std::pin::Pin;
use std::task::{Context, Poll};

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use futures_async_stream::for_await;
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_pb::task_service::GetStreamResponse;
use risingwave_rpc_client::ComputeClient;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;

use super::error::StreamExecutorError;
use super::*;
use crate::executor::monitor::StreamingMetrics;
use crate::task::UpDownActorIds;

/// Receive data from `gRPC` and forwards to `MergerExecutor`/`ReceiverExecutor`
pub struct RemoteInput {
    stream: Streaming<GetStreamResponse>,
    sender: Sender<Message>,
    up_down_ids: UpDownActorIds,
    metrics: Arc<StreamingMetrics>,
}

impl RemoteInput {
    /// Create a remote input from compute client and related info. Should provide the corresponding
    /// compute client of where the actor is placed.
    pub async fn create(
        client: ComputeClient,
        up_down_ids: UpDownActorIds,
        sender: Sender<Message>,
        metrics: Arc<StreamingMetrics>,
    ) -> Result<Self> {
        let stream = client.get_stream(up_down_ids.0, up_down_ids.1).await?;
        Ok(Self {
            stream,
            sender,
            up_down_ids,
            metrics,
        })
    }

    pub async fn run(self) {
        let up_actor_id = self.up_down_ids.0.to_string();
        let down_actor_id = self.up_down_ids.1.to_string();
        #[for_await]
        for data_res in self.stream {
            match data_res {
                Ok(stream_msg) => {
                    let bytes = Message::get_encoded_len(&stream_msg);
                    let msg_res = Message::from_protobuf(
                        stream_msg
                            .get_message()
                            .expect("no message in stream response!"),
                    );
                    self.metrics
                        .exchange_recv_size
                        .with_label_values(&[&up_actor_id, &down_actor_id])
                        .inc_by(bytes as u64);
                    match msg_res {
                        Ok(msg) => {
                            self.sender.send(msg).await.unwrap();
                        }
                        Err(e) => {
                            error!("RemoteInput forward message error:{}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    error!("RemoteInput tonic error status:{}", e);
                    break;
                }
            }
        }
    }
}

/// `MergeExecutor` merges data from multiple channels. Dataflow from one channel
/// will be stopped on barrier.
pub struct MergeExecutor {
    /// Upstream channels.
    upstreams: Vec<Receiver<Message>>,

    /// Belonged actor id.
    actor_id: u32,

    info: ExecutorInfo,

    /// Actor operator context
    status: OperatorInfoStatus,

    /// Metrics
    metrics: Arc<StreamingMetrics>,
}

impl MergeExecutor {
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        actor_id: u32,
        inputs: Vec<Receiver<Message>>,
        actor_context: ActorContextRef,
        receiver_id: u64,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            upstreams: inputs,
            actor_id,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "MergeExecutor".to_string(),
            },
            status: OperatorInfoStatus::new(actor_context, receiver_id),
            metrics,
        }
    }
}

#[async_trait]
impl Executor for MergeExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        let upstreams = self.upstreams;
        // Futures of all active upstreams.
        let status = self.status;
        let select_all =
            SelectReceivers::new(self.actor_id, status, upstreams, self.metrics.clone());
        // Channels that're blocked by the barrier to align.

        select_all.boxed()
    }

    fn schema(&self) -> &Schema {
        &self.info.schema
    }

    fn pk_indices(&self) -> PkIndicesRef {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

pub struct SelectReceivers {
    blocks: Vec<ReceiverStream<Message>>,
    upstreams: Vec<ReceiverStream<Message>>,
    barrier: Option<Barrier>,
    last_base: usize,
    status: OperatorInfoStatus,
    actor_id: u32,
    metrics: Arc<StreamingMetrics>,
}

impl SelectReceivers {
    fn new(
        actor_id: u32,
        status: OperatorInfoStatus,
        upstreams: Vec<Receiver<Message>>,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            blocks: Vec::with_capacity(upstreams.len()),
            upstreams: upstreams.into_iter().map(ReceiverStream::new).collect(),
            last_base: 0,
            actor_id,
            status,
            barrier: None,
            metrics,
        }
    }
}

impl Unpin for SelectReceivers {}

impl Stream for SelectReceivers {
    type Item = std::result::Result<Message, StreamExecutorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut poll_count = 0;
        while poll_count < self.upstreams.len() {
            let idx = (poll_count + self.last_base) % self.upstreams.len();
            match self.upstreams[idx].poll_next_unpin(cx) {
                Poll::Pending => {
                    poll_count += 1;
                    continue;
                }
                Poll::Ready(item) => {
                    let message = item.expect(
                        "upstream channel closed unexpectedly, please check error in upstream executors"
                    );
                    match message {
                        Message::Barrier(barrier) => {
                            let rc = self.upstreams.swap_remove(idx);
                            self.blocks.push(rc);
                            if let Some(current_barrier) = self.barrier.as_ref() {
                                if current_barrier.epoch != barrier.epoch {
                                    return Poll::Ready(Some(Err(
                                        StreamExecutorError::align_barrier(
                                            current_barrier.clone(),
                                            barrier,
                                        ),
                                    )));
                                }
                            } else {
                                self.barrier = Some(barrier);
                            }
                            poll_count = 0;
                        }
                        Message::Chunk(chunk) => {
                            self.metrics
                                .actor_in_record_cnt
                                .with_label_values(&[&self.actor_id.to_string()])
                                .inc_by(chunk.cardinality().try_into().unwrap());
                            let message = Message::Chunk(chunk);

                            self.status.next_message(&message);
                            self.last_base = (idx + 1) % self.upstreams.len();
                            return Poll::Ready(Some(Ok(message)));
                        }
                    }
                }
            }
        }
        if self.upstreams.is_empty() {
            if let Some(barrier) = self.barrier.take() {
                // If this barrier acquire the executor stop, we do not reset the upstreams
                // so that the next call would return `Poll::Ready(None)`.
                if !barrier.is_to_stop_actor(self.actor_id) {
                    self.upstreams = std::mem::take(&mut self.blocks);
                }
                let message = Message::Barrier(barrier);
                self.status.next_message(&message);
                Poll::Ready(Some(Ok(message)))
            } else {
                Poll::Ready(None)
            }
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use assert_matches::assert_matches;
    use itertools::Itertools;
    use madsim::collections::HashSet;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_pb::data::StreamMessage;
    use risingwave_pb::task_service::exchange_service_server::{
        ExchangeService, ExchangeServiceServer,
    };
    use risingwave_pb::task_service::{
        GetDataRequest, GetDataResponse, GetStreamRequest, GetStreamResponse,
    };
    use risingwave_rpc_client::ComputeClient;
    use tokio::sync::mpsc::channel;
    use tokio::time::sleep;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::executor::merge::RemoteInput;
    use crate::executor::{Barrier, Executor, Mutation};

    fn build_test_chunk(epoch: u64) -> StreamChunk {
        // The number of items in `ops` is the epoch count.
        let ops = vec![Op::Insert; epoch as usize];
        StreamChunk::new(ops, vec![], None)
    }

    #[tokio::test]
    async fn test_merger() {
        const CHANNEL_NUMBER: usize = 10;
        let mut txs = Vec::with_capacity(CHANNEL_NUMBER);
        let mut rxs = Vec::with_capacity(CHANNEL_NUMBER);
        for _i in 0..CHANNEL_NUMBER {
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            txs.push(tx);
            rxs.push(rx);
        }
        let metrics = Arc::new(StreamingMetrics::unused());
        let merger = MergeExecutor::new(
            Schema::default(),
            vec![],
            0,
            rxs,
            ActorContext::create(),
            0,
            metrics,
        );
        let mut handles = Vec::with_capacity(CHANNEL_NUMBER);

        let epochs = (10..1000u64).step_by(10).collect_vec();

        for tx in txs {
            let epochs = epochs.clone();
            let handle = tokio::spawn(async move {
                for epoch in epochs {
                    tx.send(Message::Chunk(build_test_chunk(epoch)))
                        .await
                        .unwrap();
                    tx.send(Message::Barrier(Barrier::new_test_barrier(epoch)))
                        .await
                        .unwrap();
                    sleep(Duration::from_millis(1)).await;
                }
                tx.send(Message::Barrier(
                    Barrier::new_test_barrier(1000)
                        .with_mutation(Mutation::Stop(HashSet::default())),
                ))
                .await
                .unwrap();
            });
            handles.push(handle);
        }

        let mut merger = merger.boxed().execute();
        for epoch in epochs {
            // expect n chunks
            for _ in 0..CHANNEL_NUMBER {
                assert_matches!(merger.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
                    assert_eq!(chunk.ops().len() as u64, epoch);
                });
            }
            // expect a barrier
            assert_matches!(merger.next().await.unwrap().unwrap(), Message::Barrier(Barrier{epoch:barrier_epoch,mutation:_,..}) => {
                assert_eq!(barrier_epoch.curr, epoch);
            });
        }
        assert_matches!(
            merger.next().await.unwrap().unwrap(),
            Message::Barrier(Barrier {
                mutation,
                ..
            }) if mutation.as_deref().unwrap().is_stop()
        );

        for handle in handles {
            handle.await.unwrap();
        }
    }

    struct FakeExchangeService {
        rpc_called: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ExchangeService for FakeExchangeService {
        type GetDataStream = ReceiverStream<std::result::Result<GetDataResponse, Status>>;
        type GetStreamStream = ReceiverStream<std::result::Result<GetStreamResponse, Status>>;

        async fn get_data(
            &self,
            _: Request<GetDataRequest>,
        ) -> std::result::Result<Response<Self::GetDataStream>, Status> {
            unimplemented!()
        }

        async fn get_stream(
            &self,
            _request: Request<GetStreamRequest>,
        ) -> std::result::Result<Response<Self::GetStreamStream>, Status> {
            let (tx, rx) = tokio::sync::mpsc::channel(10);
            self.rpc_called.store(true, Ordering::SeqCst);
            // send stream_chunk
            let stream_chunk = StreamChunk::default().to_protobuf();
            tx.send(Ok(GetStreamResponse {
                message: Some(StreamMessage {
                    stream_message: Some(
                        risingwave_pb::data::stream_message::StreamMessage::StreamChunk(
                            stream_chunk,
                        ),
                    ),
                }),
            }))
            .await
            .unwrap();
            // send barrier
            let barrier = Barrier::new_test_barrier(12345);
            tx.send(Ok(GetStreamResponse {
                message: Some(StreamMessage {
                    stream_message: Some(
                        risingwave_pb::data::stream_message::StreamMessage::Barrier(
                            barrier.to_protobuf(),
                        ),
                    ),
                }),
            }))
            .await
            .unwrap();
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_stream_exchange_client() {
        let rpc_called = Arc::new(AtomicBool::new(false));
        let server_run = Arc::new(AtomicBool::new(false));
        let addr = "127.0.0.1:12348".parse().unwrap();

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
        let (tx, mut rx) = channel(16);
        let input_handle = tokio::spawn(async move {
            let remote_input = RemoteInput::create(
                ComputeClient::new(addr.into()).await.unwrap(),
                (0, 0),
                tx,
                Arc::new(StreamingMetrics::unused()),
            )
            .await
            .unwrap();
            remote_input.run().await
        });
        assert_matches!(rx.recv().await.unwrap(), Message::Chunk(chunk) => {
            let (ops, columns, visibility) = chunk.into_inner();
            assert_eq!(ops.len() as u64, 0);
            assert_eq!(columns.len() as u64, 0);
            assert_eq!(visibility, None);
        });
        assert_matches!(rx.recv().await.unwrap(), Message::Barrier(Barrier { epoch: barrier_epoch, mutation: _, .. }) => {
            assert_eq!(barrier_epoch.curr, 12345);
        });
        assert!(rpc_called.load(Ordering::SeqCst));
        input_handle.await.unwrap();
        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
