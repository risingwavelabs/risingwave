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
use futures::channel::mpsc::{Receiver, Sender};
use futures::{SinkExt, Stream, StreamExt};
use futures_async_stream::{for_await, try_stream};
use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;
use risingwave_pb::task_service::GetStreamResponse;
use risingwave_rpc_client::ComputeClient;
use tonic::Streaming;

use super::error::StreamExecutorError;
use super::*;
use crate::task::UpDownActorIds;

/// Receive data from `gRPC` and forwards to `MergerExecutor`/`ReceiverExecutor`
pub struct RemoteInput {
    stream: Streaming<GetStreamResponse>,
    sender: Sender<Message>,
}

impl RemoteInput {
    /// Create a remote input from compute client and related info. Should provide the corresponding
    /// compute client of where the actor is placed.
    pub async fn create(
        client: ComputeClient,
        up_down_ids: UpDownActorIds,
        sender: Sender<Message>,
    ) -> Result<Self> {
        let stream = client.get_stream(up_down_ids.0, up_down_ids.1).await?;
        Ok(Self { stream, sender })
    }

    pub async fn run(mut self) {
        #[for_await]
        for data_res in self.stream {
            match data_res {
                Ok(stream_msg) => {
                    let msg_res = Message::from_protobuf(
                        stream_msg
                            .get_message()
                            .expect("no message in stream response!"),
                    );
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
}

impl MergeExecutor {
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        actor_id: u32,
        inputs: Vec<Receiver<Message>>,
    ) -> Self {
        Self {
            upstreams: inputs,
            actor_id,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "MergeExecutor".to_string(),
            },
        }
    }
}

#[async_trait]
impl Executor for MergeExecutor {
    fn execute(self: Box<Self>) -> BoxedMessageStream {
        self.execute_inner().boxed()
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
    blocks: Vec<Receiver<Message>>,
    upstreams: Vec<Receiver<Message>>,
    barrier: Option<Barrier>,
    last_base: usize,
    actor_id: u32,
}

impl SelectReceivers {
    fn new(actor_id: u32, upstreams: Vec<Receiver<Message>>) -> Self {
        Self {
            blocks: Vec::with_capacity(upstreams.len()),
            upstreams,
            last_base: 0,
            actor_id,
            barrier: None,
        }
    }
}

impl Unpin for SelectReceivers {}

impl Stream for SelectReceivers {
    type Item = Message;

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
                            self.barrier = Some(barrier);
                            poll_count = 0;
                        }
                        Message::Chunk(chunk) => {
                            self.last_base = (idx + 1) % self.upstreams.len();
                            return Poll::Ready(Some(Message::Chunk(chunk)));
                        }
                    }
                }
            }
        }
        if self.upstreams.is_empty() {
            assert!(self.barrier.is_some());
            let upstreams = std::mem::replace(&mut self.blocks, vec![]);
            self.upstreams = upstreams;
            let barrier = self.barrier.take().unwrap();
            if barrier.is_to_stop_actor(self.actor_id) {
                Poll::Ready(None)
            } else {
                Poll::Ready(Some(Message::Barrier(barrier)))
            }
        } else {
            Poll::Pending
        }
    }
}

impl MergeExecutor {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self) {
        let upstreams = self.upstreams;

        // Futures of all active upstreams.
        let mut select_all = SelectReceivers::new(self.actor_id, upstreams);
        // Channels that're blocked by the barrier to align.
        for message in select_all.next().await {
            yield message;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use assert_matches::assert_matches;
    use futures::channel::mpsc::channel;
    use futures::SinkExt;
    use itertools::Itertools;
    use madsim::collections::HashSet;
    use madsim::time::sleep;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_pb::data::StreamMessage;
    use risingwave_pb::task_service::exchange_service_server::{
        ExchangeService, ExchangeServiceServer,
    };
    use risingwave_pb::task_service::{
        GetDataRequest, GetDataResponse, GetStreamRequest, GetStreamResponse,
    };
    use risingwave_rpc_client::ComputeClient;
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

    #[madsim::test]
    async fn test_merger() {
        const CHANNEL_NUMBER: usize = 10;
        let mut txs = Vec::with_capacity(CHANNEL_NUMBER);
        let mut rxs = Vec::with_capacity(CHANNEL_NUMBER);
        for _i in 0..CHANNEL_NUMBER {
            let (tx, rx) = futures::channel::mpsc::channel(16);
            txs.push(tx);
            rxs.push(rx);
        }
        let merger = MergeExecutor::new(Schema::default(), vec![], 0, rxs);
        let mut handles = Vec::with_capacity(CHANNEL_NUMBER);

        let epochs = (10..1000u64).step_by(10).collect_vec();

        for mut tx in txs {
            let epochs = epochs.clone();
            let handle = madsim::task::spawn(async move {
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
            handle.await;
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

    #[madsim::test(flavor = "multi_thread")]
    async fn test_stream_exchange_client() {
        let rpc_called = Arc::new(AtomicBool::new(false));
        let server_run = Arc::new(AtomicBool::new(false));
        let addr = "127.0.0.1:12348".parse().unwrap();

        // Start a server.
        let (shutdown_send, mut shutdown_recv) = tokio::sync::mpsc::unbounded_channel();
        let exchange_svc = ExchangeServiceServer::new(FakeExchangeService {
            rpc_called: rpc_called.clone(),
        });
        let cp_server_run = server_run.clone();
        let join_handle = madsim::task::spawn(async move {
            cp_server_run.store(true, Ordering::SeqCst);
            tonic::transport::Server::builder()
                .add_service(exchange_svc)
                .serve_with_shutdown(addr, async move {
                    shutdown_recv.recv().await;
                })
                .await
                .unwrap();
        });

        sleep(Duration::from_secs(1)).await;
        assert!(server_run.load(Ordering::SeqCst));
        let (tx, mut rx) = channel(16);
        let input_handle = madsim::task::spawn(async move {
            let remote_input =
                RemoteInput::create(ComputeClient::new(addr.into()).await.unwrap(), (0, 0), tx)
                    .await
                    .unwrap();
            remote_input.run().await
        });
        assert_matches!(rx.next().await.unwrap(), Message::Chunk(chunk) => {
            let (ops, columns, visibility) = chunk.into_inner();
            assert_eq!(ops.len() as u64, 0);
            assert_eq!(columns.len() as u64, 0);
            assert_eq!(visibility, None);
        });
        assert_matches!(rx.next().await.unwrap(), Message::Barrier(Barrier { epoch: barrier_epoch, mutation: _, .. }) => {
            assert_eq!(barrier_epoch.curr, 12345);
        });
        assert!(rpc_called.load(Ordering::SeqCst));
        input_handle.await;
        shutdown_send.send(()).unwrap();
        join_handle.await;
    }
}
