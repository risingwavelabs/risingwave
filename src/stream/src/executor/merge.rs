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

use anyhow::anyhow;
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;

use super::error::StreamExecutorError;
use super::exchange::input::BoxedInput;
use super::*;
use crate::executor::exchange::input::new_input;
use crate::executor::monitor::StreamingMetrics;
use crate::task::{FragmentId, SharedContext};

/// `MergeExecutor` merges data from multiple channels. Dataflow from one channel
/// will be stopped on barrier.
pub struct MergeExecutor {
    /// Upstream channels.
    upstreams: Vec<BoxedInput>,

    /// The context of the actor.
    actor_context: ActorContextRef,

    /// Belonged fragment id.
    fragment_id: FragmentId,

    /// Upstream fragment id.
    upstream_fragment_id: FragmentId,

    /// Logical Operator Info
    info: ExecutorInfo,

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
        ctx: ActorContextRef,
        fragment_id: FragmentId,
        upstream_fragment_id: FragmentId,
        inputs: Vec<BoxedInput>,
        context: Arc<SharedContext>,
        _receiver_id: u64,
        metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            upstreams: inputs,
            actor_context: ctx,
            fragment_id,
            upstream_fragment_id,
            info: ExecutorInfo {
                schema,
                pk_indices,
                identity: "MergeExecutor".to_string(),
            },
            context,
            metrics,
        }
    }

    #[cfg(test)]
    pub fn for_test(inputs: Vec<tokio::sync::mpsc::Receiver<Message>>) -> Self {
        use super::exchange::input::LocalInput;

        Self::new(
            Schema::default(),
            vec![],
            ActorContext::create(114),
            514,
            1919,
            inputs.into_iter().map(LocalInput::for_test).collect(),
            SharedContext::for_test().into(),
            810,
            StreamingMetrics::unused().into(),
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(self: Box<Self>) {
        // Futures of all active upstreams.
        let select_all = SelectReceivers::new(self.actor_context.id, self.upstreams);
        let actor_id = self.actor_context.id;
        let actor_id_str = actor_id.to_string();
        let upstream_fragment_id_str = self.upstream_fragment_id.to_string();

        // Channels that're blocked by the barrier to align.
        let mut start_time = minstant::Instant::now();
        pin_mut!(select_all);
        while let Some(msg) = select_all.next().await {
            self.metrics
                .actor_input_buffer_blocking_duration_ns
                .with_label_values(&[&actor_id_str, &upstream_fragment_id_str])
                .inc_by(start_time.elapsed().as_nanos() as u64);
            let mut msg: Message = msg?;

            match &mut msg {
                Message::Chunk(chunk) => {
                    self.metrics
                        .actor_in_record_cnt
                        .with_label_values(&[&actor_id_str])
                        .inc_by(chunk.cardinality() as _);
                }
                Message::Barrier(barrier) => {
                    tracing::trace!(
                        target: "events::barrier::path",
                        actor_id = actor_id,
                        "receiver receives barrier from path: {:?}",
                        barrier.passed_actors
                    );
                    barrier.passed_actors.push(actor_id);

                    if let Some(update) = barrier.as_update_merge(self.actor_context.id) {
                        if !update.added_upstream_actor_id.is_empty() {
                            // Create new upstreams receivers.
                            let new_upstreams = update
                                .added_upstream_actor_id
                                .iter()
                                .map(|&upstream_actor_id| {
                                    new_input(
                                        &self.context,
                                        self.metrics.clone(),
                                        self.actor_context.id,
                                        self.fragment_id,
                                        upstream_actor_id,
                                        self.upstream_fragment_id,
                                    )
                                })
                                .try_collect()
                                .map_err(|e| anyhow!("failed to create upstream receivers: {e}"))?;

                            // Poll the first barrier from the new upstreams. It must be the same as the
                            // one we polled from original upstreams.
                            let mut select_new =
                                SelectReceivers::new(self.actor_context.id, new_upstreams);
                            let new_barrier = expect_first_barrier(&mut select_new).await?;
                            assert_eq!(barrier, &new_barrier);

                            // Add the new upstreams to select.
                            select_all.add_upstreams_from(select_new);
                        }

                        // Remove upstreams.
                        select_all.remove_upstreams(
                            &update.removed_upstream_actor_id.iter().copied().collect(),
                        );
                    }
                }
            }

            yield msg;
            start_time = minstant::Instant::now();
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
    blocks: Vec<BoxedInput>,
    upstreams: Vec<BoxedInput>,
    barrier: Option<Barrier>,
    last_base: usize,
    actor_id: u32,
}

impl SelectReceivers {
    fn new(actor_id: u32, upstreams: Vec<BoxedInput>) -> Self {
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
            .retain(|u| !upstream_actor_ids.contains(&u.actor_id()));
        self.last_base = 0;
    }
}

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
                    let message = item
                        .expect("upstream closed unexpectedly, please check error in upstream executors")
                        .expect("upstream error");

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
                if !barrier.is_stop_or_update_drop_actor(self.actor_id) {
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
    use risingwave_rpc_client::ComputeClientPool;
    use tokio::time::sleep;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::{Request, Response, Status};

    use super::*;
    use crate::executor::exchange::input::RemoteInput;
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
        let (untouched, old, new) = (234, 235, 238); // upstream actors
        let ctx = Arc::new(SharedContext::for_test());
        let metrics = Arc::new(StreamingMetrics::unused());

        // 1. Register info and channels in context.
        {
            let mut actor_infos = ctx.actor_infos.write();

            for local_actor_id in [actor_id, untouched, old, new] {
                actor_infos.insert(local_actor_id, helper_make_local_actor(local_actor_id));
            }
        }
        add_local_channels(
            ctx.clone(),
            vec![(untouched, actor_id), (old, actor_id), (new, actor_id)],
        );

        let inputs: Vec<_> = [untouched, old]
            .into_iter()
            .map(|upstream_actor_id| {
                new_input(&ctx, metrics.clone(), actor_id, 0, upstream_actor_id, 0)
            })
            .try_collect()
            .unwrap();

        let merge = MergeExecutor::new(
            schema,
            vec![],
            ActorContext::create(actor_id),
            0,
            0,
            inputs,
            ctx.clone(),
            233,
            metrics.clone(),
        )
            .boxed()
            .execute();

        pin_mut!(merge);

        // 2. Take downstream receivers.
        let txs = [untouched, old, new]
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
        send!([untouched, old], Message::Chunk(StreamChunk::default()));
        recv!().unwrap().as_chunk().unwrap(); // We should be able to receive the chunk twice.
        recv!().unwrap().as_chunk().unwrap();
        assert!(recv!().is_none());

        // 4. Send a configuration change barrier.
        let merge_updates = maplit::hashmap! {
            actor_id => MergeUpdate {
                added_upstream_actor_id: vec![new],
                removed_upstream_actor_id: vec![old],
            }
        };

        let b1 = Barrier::new_test_barrier(1).with_mutation(Mutation::Update {
            dispatchers: Default::default(),
            merges: merge_updates,
            vnode_bitmaps: Default::default(),
            dropped_actors: Default::default(),
        });
        send!([untouched, old], Message::Barrier(b1.clone()));
        assert!(recv!().is_none()); // We should not receive the barrier, since merger is waiting for the new upstream new.

        send!([new], Message::Barrier(b1.clone()));
        recv!().unwrap().as_barrier().unwrap(); // We should now receive the barrier.

        // 5. Send a chunk.
        send!([untouched, new], Message::Chunk(StreamChunk::default()));
        recv!().unwrap().as_chunk().unwrap(); // We should be able to receive the chunk twice, since old is removed.
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

    #[tokio::test]
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

        let remote_input = {
            let pool = ComputeClientPool::default();
            RemoteInput::new(
                pool,
                addr.into(),
                (0, 0),
                (0, 0),
                Arc::new(StreamingMetrics::unused()),
            )
        };

        pin_mut!(remote_input);

        assert_matches!(remote_input.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
            let (ops, columns, visibility) = chunk.into_inner();
            assert_eq!(ops.len() as u64, 0);
            assert_eq!(columns.len() as u64, 0);
            assert_eq!(visibility, None);
        });
        assert_matches!(remote_input.next().await.unwrap().unwrap(), Message::Barrier(Barrier { epoch: barrier_epoch, mutation: _, .. }) => {
            assert_eq!(barrier_epoch.curr, 12345);
        });
        assert!(rpc_called.load(Ordering::SeqCst));

        shutdown_send.send(()).unwrap();
        join_handle.await.unwrap();
    }
}
