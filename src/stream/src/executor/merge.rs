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

use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::{for_await, try_stream};
use madsim::time::Instant;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::addr::is_local_address;
use risingwave_pb::task_service::GetStreamResponse;
use risingwave_rpc_client::ComputeClient;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Streaming;

use super::error::StreamExecutorError;
use super::*;
use crate::executor::monitor::StreamingMetrics;
use crate::task::{FragmentId, SharedContext, UpDownActorIds, UpDownFragmentIds};

/// Receive data from `gRPC` and forwards to `MergerExecutor`/`ReceiverExecutor`
pub struct RemoteInput {
    stream: Streaming<GetStreamResponse>,
    sender: Sender<Message>,
    up_down_ids: UpDownActorIds,
    up_down_frag: UpDownFragmentIds,
    metrics: Arc<StreamingMetrics>,
}

impl RemoteInput {
    /// Create a remote input from compute client and related info. Should provide the corresponding
    /// compute client of where the actor is placed.
    pub async fn create(
        client: ComputeClient,
        up_down_ids: UpDownActorIds,
        up_down_frag: UpDownFragmentIds,
        sender: Sender<Message>,
        metrics: Arc<StreamingMetrics>,
    ) -> Result<Self> {
        let stream = client
            .get_stream(up_down_ids.0, up_down_ids.1, up_down_frag.0, up_down_frag.1)
            .await?;
        Ok(Self {
            stream,
            sender,
            up_down_ids,
            up_down_frag,
            metrics,
        })
    }

    pub async fn run(self) {
        let up_actor_id = self.up_down_ids.0.to_string();
        let down_actor_id = self.up_down_ids.1.to_string();
        let up_fragment_id = self.up_down_frag.0.to_string();
        let down_fragment_id = self.up_down_frag.1.to_string();

        let mut rr = 0;
        const SAMPLING_FREQUENCY: u64 = 100;

        #[for_await]
        for data_res in self.stream {
            match data_res {
                Ok(stream_msg) => {
                    let bytes = Message::get_encoded_len(&stream_msg);
                    self.metrics
                        .exchange_recv_size
                        .with_label_values(&[&up_actor_id, &down_actor_id])
                        .inc_by(bytes as u64);

                    self.metrics
                        .exchange_frag_recv_size
                        .with_label_values(&[&up_fragment_id, &down_fragment_id])
                        .inc_by(bytes as u64);

                    // add deserialization duration metric with given sampling frequency
                    let msg_res = if rr % SAMPLING_FREQUENCY == 0 {
                        let start_time = Instant::now();
                        let msg_res = Message::from_protobuf(
                            stream_msg
                                .get_message()
                                .expect("no message in stream response!"),
                        );
                        self.metrics
                            .actor_sampled_deserialize_duration_ns
                            .with_label_values(&[&down_actor_id])
                            .inc_by(start_time.elapsed().as_nanos() as u64);
                        msg_res
                    } else {
                        Message::from_protobuf(
                            stream_msg
                                .get_message()
                                .expect("no message in stream response!"),
                        )
                    };
                    rr += 1;

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

/// Create an input for merge and receiver executor. For local upstream actor, this will be simply a
/// channel receiver. For remote upstream actor, this will spawn a long running [`RemoteInput`] task
/// to receive messages from `gRPC` exchange service and return the receiver.
// TODO: there's no need to use 4 channels for remote input. We may introduce a trait `Input` and
// directly receive the message from the `RemoteInput`, just like the `Output`.
pub(crate) fn new_input(
    context: &SharedContext,
    metrics: Arc<StreamingMetrics>,
    actor_id: ActorId,
    fragment_id: FragmentId,
    upstream_actor_id: ActorId,
    upstream_fragment_id: FragmentId,
) -> Result<Upstream> {
    let upstream_addr = context
        .get_actor_info(&upstream_actor_id)?
        .get_host()?
        .into();
    if !is_local_address(&context.addr, &upstream_addr) {
        // Get the sender for `RemoteInput` to forward received messages to receivers in
        // `ReceiverExecutor` or `MergeExecutor`.
        let sender = context.take_sender(&(upstream_actor_id, actor_id))?;
        // Spawn the `RemoteInput`.
        let pool = context.compute_client_pool.clone();
        tokio::spawn(async move {
            let remote_input = RemoteInput::create(
                pool.get_client_for_addr(upstream_addr).await?,
                (upstream_actor_id, actor_id),
                (upstream_fragment_id, fragment_id),
                sender,
                metrics,
            )
            .await
            .inspect_err(|e| error!("Spawn remote input fails:{}", e))?;

            remote_input.run().await;
            Ok::<_, RwError>(())
        });
    }
    let rx = context.take_receiver(&(upstream_actor_id, actor_id))?;

    Ok((upstream_actor_id, rx))
}

type Upstream = (ActorId, Receiver<Message>);

/// `MergeExecutor` merges data from multiple channels. Dataflow from one channel
/// will be stopped on barrier.
pub struct MergeExecutor {
    /// Upstream channels.
    upstreams: Vec<Upstream>,

    /// Belonged actor id.
    actor_id: ActorId,

    /// Belonged fragment id.
    fragment_id: FragmentId,

    /// Upstream fragment id.
    upstream_fragment_id: FragmentId,

    info: ExecutorInfo,

    /// Actor operator context.
    status: OperatorInfoStatus,

    /// Shared context of the stream manager.
    context: Arc<SharedContext>,

    /// Streaming metrics.
    metrics: Arc<StreamingMetrics>,
}

impl MergeExecutor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        schema: Schema,
        pk_indices: PkIndices,
        actor_id: ActorId,
        fragment_id: FragmentId,
        upstream_fragment_id: FragmentId,
        inputs: Vec<Upstream>,
        context: Arc<SharedContext>,
        actor_context: ActorContextRef,
        receiver_id: u64,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            upstreams: inputs,
            actor_id,
            fragment_id,
            upstream_fragment_id,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "MergeExecutor".to_string(),
            },
            status: OperatorInfoStatus::new(actor_context, receiver_id),
            context,
            metrics,
        }
    }

    #[cfg(test)]
    pub fn for_test(inputs: Vec<Receiver<Message>>) -> Self {
        Self::new(
            Schema::default(),
            vec![],
            114,
            514,
            1919,
            inputs
                .into_iter()
                .enumerate()
                .map(|(i, r)| (i as ActorId, r))
                .collect(),
            SharedContext::for_test().into(),
            ActorContext::create(),
            810,
            StreamingMetrics::unused().into(),
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self: Box<Self>) {
        // Futures of all active upstreams.
        let select_all = SelectReceivers::new(self.actor_id, self.upstreams);
        let actor_id_str = self.actor_id.to_string();

        // Channels that're blocked by the barrier to align.
        pin_mut!(select_all);
        while let Some(msg) = select_all.next().await {
            let msg: Message = msg?;
            self.status.next_message(&msg);

            match &msg {
                Message::Chunk(chunk) => {
                    self.metrics
                        .actor_in_record_cnt
                        .with_label_values(&[&actor_id_str])
                        .inc_by(chunk.cardinality() as _);
                }
                Message::Barrier(barrier) => {
                    if let Some(update) = barrier.as_update_merge(self.actor_id) {
                        // Create new upstreams receivers.
                        let new_upstreams = update
                            .added_upstream_actor_id
                            .iter()
                            .map(|&upstream_actor_id| {
                                new_input(
                                    &self.context,
                                    self.metrics.clone(),
                                    self.actor_id,
                                    self.fragment_id,
                                    upstream_actor_id,
                                    self.upstream_fragment_id,
                                )
                            })
                            .try_collect()
                            .map_err(|_| anyhow::anyhow!("failed to create upstream receivers"))?;

                        // Poll the first barrier from the new upstreams. It must be the same as the
                        // one we polled from original upstreams.
                        let mut select_new = SelectReceivers::new(self.actor_id, new_upstreams);
                        let new_barrier = expect_first_barrier(&mut select_new).await?;
                        assert_eq!(barrier, &new_barrier);

                        // Add the new upstreams to select.
                        select_all.add_upstreams_from(select_new);

                        // Remove upstreams.
                        select_all.remove_upstreams(
                            &update.removed_upstream_actor_id.iter().copied().collect(),
                        );
                    }
                }
            }

            yield msg;
        }
    }
}

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
    blocks: Vec<Upstream>,
    upstreams: Vec<Upstream>,
    barrier: Option<Barrier>,
    last_base: usize,
    actor_id: u32,
}

impl SelectReceivers {
    fn new(actor_id: u32, upstreams: Vec<Upstream>) -> Self {
        Self {
            blocks: Vec::with_capacity(upstreams.len()),
            upstreams,
            last_base: 0,
            actor_id,
            barrier: None,
        }
    }

    /// Consume `other` and add its upstreams to `self`.
    fn add_upstreams_from(&mut self, other: Self) {
        assert!(self.blocks.is_empty() && self.barrier.is_none());
        assert!(other.blocks.is_empty() && other.barrier.is_none());
        assert_eq!(self.actor_id, other.actor_id);

        self.upstreams.extend(other.upstreams);
        self.last_base = 0;
    }

    /// Remove upstreams from `self` in `upstream_actor_ids`.
    fn remove_upstreams(&mut self, upstream_actor_ids: &HashSet<ActorId>) {
        assert!(self.blocks.is_empty() && self.barrier.is_none());

        self.upstreams
            .retain(|(id, _)| !upstream_actor_ids.contains(id));
        self.last_base = 0;
    }
}

impl Stream for SelectReceivers {
    type Item = std::result::Result<Message, StreamExecutorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut poll_count = 0;
        while poll_count < self.upstreams.len() {
            let idx = (poll_count + self.last_base) % self.upstreams.len();
            match self.upstreams[idx].1.poll_recv(cx) {
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
                            let message = Message::Chunk(chunk);
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
                if !barrier.is_stop_actor(self.actor_id) {
                    self.upstreams = std::mem::take(&mut self.blocks);
                }
                let message = Message::Barrier(barrier);
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
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use assert_matches::assert_matches;
    use futures::FutureExt;
    use itertools::Itertools;
    use risingwave_common::array::{Op, StreamChunk};
    use risingwave_pb::stream_plan::StreamMessage;
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
    use crate::task::test_utils::{add_local_channels, helper_make_local_actor};

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
        let merger = MergeExecutor::for_test(rxs);
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

    #[tokio::test]
    async fn test_configuration_change() {
        let schema = Schema { fields: vec![] };

        let actor_id = 233;
        let ctx = Arc::new(SharedContext::for_test());
        let metrics = Arc::new(StreamingMetrics::unused());

        // 1. Register info and channels in context.
        {
            let mut actor_infos = ctx.actor_infos.write();

            for local_actor_id in [actor_id, 234, 235, 238] {
                actor_infos.insert(local_actor_id, helper_make_local_actor(local_actor_id));
            }
        }
        add_local_channels(
            ctx.clone(),
            vec![(234, actor_id), (235, actor_id), (238, actor_id)],
        );

        let inputs: Vec<_> = [234, 235]
            .into_iter()
            .map(|upstream_actor_id| {
                new_input(&ctx, metrics.clone(), actor_id, 0, upstream_actor_id, 0)
            })
            .try_collect()
            .unwrap();

        let merge = MergeExecutor::new(
            schema,
            vec![],
            actor_id,
            0,
            0,
            inputs,
            ctx.clone(),
            ActorContext::create(),
            233,
            metrics.clone(),
        )
        .boxed()
        .execute();

        pin_mut!(merge);

        // 2. Take downstream receivers.
        let txs = [234, 235, 238]
            .into_iter()
            .map(|id| (id, ctx.take_sender(&(id, actor_id)).unwrap()))
            .collect::<HashMap<_, _>>();
        macro_rules! send {
            ($actors:expr, $msg:expr) => {
                for actor in $actors {
                    txs.get(&actor).unwrap().send($msg).await.unwrap();
                }
            };
        }
        macro_rules! recv {
            () => {
                merge.next().now_or_never().flatten().transpose().unwrap()
            };
        }

        // 3. Send a chunk.
        send!([234, 235], Message::Chunk(StreamChunk::default()));
        recv!().unwrap().as_chunk().unwrap(); // We should be able to receive the chunk twice.
        recv!().unwrap().as_chunk().unwrap();
        assert!(recv!().is_none());

        // 4. Send a configuration change barrier.
        let merge_updates = maplit::hashmap! {
            actor_id => MergeUpdate {
                added_upstream_actor_id: vec![238],
                removed_upstream_actor_id: vec![235],
            }
        };

        let b1 = Barrier::new_test_barrier(1).with_mutation(Mutation::Update {
            dispatchers: Default::default(),
            merges: merge_updates,
        });
        send!([234, 235], Message::Barrier(b1.clone()));
        assert!(recv!().is_none()); // We should not receive the barrier, since merger is waiting for the new upstream 238.

        send!([238], Message::Barrier(b1.clone()));
        recv!().unwrap().as_barrier().unwrap(); // We should now receive the barrier.

        // 5. Send a chunk.
        send!([234, 238], Message::Chunk(StreamChunk::default()));
        recv!().unwrap().as_chunk().unwrap(); // We should be able to receive the chunk twice, since 235 is removed.
        recv!().unwrap().as_chunk().unwrap();
        assert!(recv!().is_none());
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
                        risingwave_pb::stream_plan::stream_message::StreamMessage::StreamChunk(
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
                        risingwave_pb::stream_plan::stream_message::StreamMessage::Barrier(
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
