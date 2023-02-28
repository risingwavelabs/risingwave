// Copyright 2023 RisingWave Labs
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

use std::collections::BTreeMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::anyhow;
use futures::stream::{FusedStream, FuturesUnordered, StreamFuture};
use futures::{pin_mut, Stream, StreamExt};
use futures_async_stream::try_stream;
use risingwave_common::catalog::Schema;

use super::error::StreamExecutorError;
use super::exchange::input::BoxedInput;
use super::watermark::*;
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
        executor_id: u64,
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
                identity: format!("MergeExecutor {:X}", executor_id),
            },
            context,
            metrics,
        }
    }

    #[cfg(test)]
    pub fn for_test(inputs: Vec<super::exchange::permit::Receiver>, schema: Schema) -> Self {
        use super::exchange::input::LocalInput;
        use crate::executor::exchange::input::Input;

        Self::new(
            schema,
            vec![],
            ActorContext::create(114),
            514,
            1919,
            1024,
            inputs
                .into_iter()
                .enumerate()
                .map(|(idx, input)| LocalInput::new(input, idx as ActorId).boxed_input())
                .collect(),
            SharedContext::for_test().into(),
            810,
            StreamingMetrics::unused().into(),
        )
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    async fn execute_inner(mut self: Box<Self>) {
        // Futures of all active upstreams.
        let select_all = SelectReceivers::new(self.actor_context.id, self.upstreams);
        let actor_id = self.actor_context.id;
        let actor_id_str = actor_id.to_string();
        let mut upstream_fragment_id_str = self.upstream_fragment_id.to_string();

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
                Message::Watermark(_) => {
                    // Do nothing.
                }
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

                    if let Some(update) =
                        barrier.as_update_merge(self.actor_context.id, self.upstream_fragment_id)
                    {
                        let new_upstream_fragment_id = update
                            .new_upstream_fragment_id
                            .unwrap_or(self.upstream_fragment_id);
                        let added_upstream_actor_id = update.added_upstream_actor_id.clone();
                        let removed_upstream_actor_id: HashSet<_> =
                            if update.new_upstream_fragment_id.is_some() {
                                select_all.upstream_actor_ids().iter().copied().collect()
                            } else {
                                update.removed_upstream_actor_id.iter().copied().collect()
                            };

                        // `Watermark` of upstream may become stale after upstream scaling.
                        select_all
                            .buffered_watermarks
                            .values_mut()
                            .for_each(|buffers| buffers.clear());

                        if !added_upstream_actor_id.is_empty() {
                            // Create new upstreams receivers.
                            let new_upstreams: Vec<_> = added_upstream_actor_id
                                .iter()
                                .map(|&upstream_actor_id| {
                                    new_input(
                                        &self.context,
                                        self.metrics.clone(),
                                        self.actor_context.id,
                                        self.fragment_id,
                                        upstream_actor_id,
                                        new_upstream_fragment_id,
                                    )
                                })
                                .try_collect()
                                .map_err(|e| anyhow!("failed to create upstream receivers: {e}"))?;

                            // Poll the first barrier from the new upstreams. It must be the same as
                            // the one we polled from original upstreams.
                            let mut select_new =
                                SelectReceivers::new(self.actor_context.id, new_upstreams);
                            let new_barrier = expect_first_barrier(&mut select_new).await?;
                            assert_eq!(barrier, &new_barrier);

                            // Add the new upstreams to select.
                            select_all.add_upstreams_from(select_new);

                            // Add buffers to the buffered watermarks for all cols
                            select_all
                                .buffered_watermarks
                                .values_mut()
                                .for_each(|buffers| {
                                    buffers.add_buffers(added_upstream_actor_id.clone())
                                });
                        }

                        if !removed_upstream_actor_id.is_empty() {
                            // Remove upstreams.
                            select_all.remove_upstreams(&removed_upstream_actor_id);

                            for buffers in select_all.buffered_watermarks.values_mut() {
                                // Call `check_heap` in case the only upstream(s) that does not have
                                // watermark in heap is removed
                                buffers.remove_buffer(removed_upstream_actor_id.clone());
                            }
                        }

                        self.upstream_fragment_id = new_upstream_fragment_id;
                        upstream_fragment_id_str = new_upstream_fragment_id.to_string();

                        select_all.update_actor_ids();
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

    fn pk_indices(&self) -> PkIndicesRef<'_> {
        &self.info.pk_indices
    }

    fn identity(&self) -> &str {
        &self.info.identity
    }
}

/// A stream for merging messages from multiple upstreams.
pub struct SelectReceivers {
    /// The barrier we're aligning to. If this is `None`, then `blocked_upstreams` is empty.
    barrier: Option<Barrier>,
    /// The upstreams that're blocked by the `barrier`.
    blocked: Vec<BoxedInput>,
    /// The upstreams that're not blocked and can be polled.
    active: FuturesUnordered<StreamFuture<BoxedInput>>,
    /// All upstream actor ids.
    upstream_actor_ids: Vec<ActorId>,

    /// The actor id of this fragment.
    actor_id: u32,
    /// watermark column index -> `BufferedWatermarks`
    buffered_watermarks: BTreeMap<usize, BufferedWatermarks<ActorId>>,
}

impl Stream for SelectReceivers {
    type Item = std::result::Result<Message, StreamExecutorError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.active.is_terminated() {
            // This only happens if we've been asked to stop.
            assert!(self.blocked.is_empty());
            return Poll::Ready(None);
        }

        loop {
            match futures::ready!(self.active.poll_next_unpin(cx)) {
                // Directly forward the error.
                Some((Some(Err(e)), _)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                // Handle the message from some upstream.
                Some((Some(Ok(message)), remaining)) => {
                    let actor_id = remaining.actor_id();
                    match message {
                        Message::Chunk(chunk) => {
                            // Continue polling this upstream by pushing it back to `active`.
                            self.active.push(remaining.into_future());
                            return Poll::Ready(Some(Ok(Message::Chunk(chunk))));
                        }
                        Message::Watermark(watermark) => {
                            // Continue polling this upstream by pushing it back to `active`.
                            self.active.push(remaining.into_future());
                            if let Some(watermark) = self.handle_watermark(actor_id, watermark) {
                                return Poll::Ready(Some(Ok(Message::Watermark(watermark))));
                            }
                        }
                        Message::Barrier(barrier) => {
                            // Block this upstream by pushing it to `blocked`.
                            self.blocked.push(remaining);
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
                        }
                    }
                }
                // If one upstream is finished, we finish the whole stream with an error. This
                // should not happen normally as we use the barrier as the control message.
                Some((None, r)) => {
                    return Poll::Ready(Some(Err(StreamExecutorError::channel_closed(format!(
                        "exchange from actor {} to actor {} closed unexpectedly",
                        r.actor_id(),
                        self.actor_id
                    )))))
                }
                // There's no active upstreams. Process the barrier and resume the blocked ones.
                None => break,
            }
        }

        assert!(self.active.is_terminated());
        let barrier = self.barrier.take().unwrap();

        // If this barrier asks the actor to stop, we do not reset the active upstreams so that the
        // next call would return `Poll::Ready(None)` due to `is_terminated`.
        let upstreams = std::mem::take(&mut self.blocked);
        if barrier.is_stop_or_update_drop_actor(self.actor_id) {
            drop(upstreams);
        } else {
            self.extend_active(upstreams);
            assert!(!self.active.is_terminated());
        }

        Poll::Ready(Some(Ok(Message::Barrier(barrier))))
    }
}

impl SelectReceivers {
    fn new(actor_id: u32, upstreams: Vec<BoxedInput>) -> Self {
        assert!(!upstreams.is_empty());
        let upstream_actor_ids = upstreams.iter().map(|input| input.actor_id()).collect();
        let mut this = Self {
            blocked: Vec::with_capacity(upstreams.len()),
            active: Default::default(),
            actor_id,
            barrier: None,
            upstream_actor_ids,
            buffered_watermarks: Default::default(),
        };
        this.extend_active(upstreams);
        this
    }

    /// Extend the active upstreams with the given upstreams. The current stream must be at the
    /// clean state right after a barrier.
    fn extend_active(&mut self, upstreams: impl IntoIterator<Item = BoxedInput>) {
        assert!(self.blocked.is_empty() && self.barrier.is_none());

        self.active
            .extend(upstreams.into_iter().map(|s| s.into_future()));
    }

    fn upstream_actor_ids(&self) -> &[ActorId] {
        &self.upstream_actor_ids
    }

    fn update_actor_ids(&mut self) {
        self.upstream_actor_ids = self
            .blocked
            .iter()
            .map(|input| input.actor_id())
            .chain(
                self.active
                    .iter()
                    .map(|input| input.get_ref().unwrap().actor_id()),
            )
            .collect();
    }

    /// Handle a new watermark message. Optionally returns the watermark message to emit.
    fn handle_watermark(&mut self, actor_id: ActorId, watermark: Watermark) -> Option<Watermark> {
        let col_idx = watermark.col_idx;
        // Insert a buffer watermarks when first received from a column.
        let watermarks = self
            .buffered_watermarks
            .entry(col_idx)
            .or_insert_with(|| BufferedWatermarks::with_ids(self.upstream_actor_ids.clone()));
        watermarks.handle_watermark(actor_id, watermark)
    }

    /// Consume `other` and add its upstreams to `self`. The two streams must be at the clean state
    /// right after a barrier.
    fn add_upstreams_from(&mut self, other: Self) {
        assert!(self.blocked.is_empty() && self.barrier.is_none());
        assert!(other.blocked.is_empty() && other.barrier.is_none());
        assert_eq!(self.actor_id, other.actor_id);

        self.active.extend(other.active);
    }

    /// Remove upstreams from `self` in `upstream_actor_ids`. The current stream must be at the
    /// clean state right after a barrier.
    fn remove_upstreams(&mut self, upstream_actor_ids: &HashSet<ActorId>) {
        assert!(self.blocked.is_empty() && self.barrier.is_none());

        let new_upstreams = std::mem::take(&mut self.active)
            .into_iter()
            .map(|s| s.into_inner().unwrap())
            .filter(|u| !upstream_actor_ids.contains(&u.actor_id()));
        self.extend_active(new_upstreams);
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
    use risingwave_common::types::ScalarImpl;
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
    use tonic::{Request, Response, Status, Streaming};

    use super::*;
    use crate::executor::exchange::input::RemoteInput;
    use crate::executor::exchange::permit::channel_for_test;
    use crate::executor::{Barrier, Executor, Mutation};
    use crate::task::test_utils::helper_make_local_actor;

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
            let (tx, rx) = channel_for_test();
            txs.push(tx);
            rxs.push(rx);
        }
        let merger = MergeExecutor::for_test(rxs, Schema::default());
        let mut handles = Vec::with_capacity(CHANNEL_NUMBER);

        let epochs = (10..1000u64).step_by(10).collect_vec();

        for (tx_id, tx) in txs.into_iter().enumerate() {
            let epochs = epochs.clone();
            let handle = tokio::spawn(async move {
                for epoch in epochs {
                    if epoch % 20 == 0 {
                        tx.send(Message::Chunk(build_test_chunk(epoch)))
                            .await
                            .unwrap();
                    } else {
                        tx.send(Message::Watermark(Watermark {
                            col_idx: (epoch as usize / 20 + tx_id) % CHANNEL_NUMBER,
                            data_type: DataType::Int64,
                            val: ScalarImpl::Int64(epoch as i64),
                        }))
                        .await
                        .unwrap();
                    }
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
            if epoch % 20 == 0 {
                for _ in 0..CHANNEL_NUMBER {
                    assert_matches!(merger.next().await.unwrap().unwrap(), Message::Chunk(chunk) => {
                        assert_eq!(chunk.ops().len() as u64, epoch);
                    });
                }
            } else if epoch as usize / 20 >= CHANNEL_NUMBER - 1 {
                for _ in 0..CHANNEL_NUMBER {
                    assert_matches!(merger.next().await.unwrap().unwrap(), Message::Watermark(watermark) => {
                        assert_eq!(watermark.val, ScalarImpl::Int64((epoch - 20 * (CHANNEL_NUMBER as u64 - 1)) as i64));
                    });
                }
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

        // 1. Register info in context.
        {
            let mut actor_infos = ctx.actor_infos.write();

            for local_actor_id in [actor_id, untouched, old, new] {
                actor_infos.insert(local_actor_id, helper_make_local_actor(local_actor_id));
            }
        }
        // untouched -> actor_id
        // old -> actor_id
        // new -> actor_id

        let (upstream_fragment_id, fragment_id) = (10, 18);

        let inputs: Vec<_> = [untouched, old]
            .into_iter()
            .map(|upstream_actor_id| {
                new_input(
                    &ctx,
                    metrics.clone(),
                    actor_id,
                    fragment_id,
                    upstream_actor_id,
                    upstream_fragment_id,
                )
            })
            .try_collect()
            .unwrap();

        let merge = MergeExecutor::new(
            schema,
            vec![],
            ActorContext::create(actor_id),
            fragment_id,
            upstream_fragment_id,
            1024,
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
            (actor_id, upstream_fragment_id) => MergeUpdate {
                actor_id,
                upstream_fragment_id,
                new_upstream_fragment_id: None,
                added_upstream_actor_id: vec![new],
                removed_upstream_actor_id: vec![old],
            }
        };

        let b1 = Barrier::new_test_barrier(1).with_mutation(Mutation::Update {
            dispatchers: Default::default(),
            merges: merge_updates,
            vnode_bitmaps: Default::default(),
            dropped_actors: Default::default(),
            actor_splits: Default::default(),
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
            _request: Request<Streaming<GetStreamRequest>>,
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
                permits: 1,
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
                permits: 0,
            }))
            .await
            .unwrap();
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }

    #[tokio::test]
    async fn test_stream_exchange_client() {
        const BATCHED_PERMITS: usize = 1024;
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
                BATCHED_PERMITS,
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
