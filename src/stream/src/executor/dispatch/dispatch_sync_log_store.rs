// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::VecDeque;
use std::future::pending;
use std::mem::replace;

use anyhow::anyhow;
use futures::FutureExt;
use futures::future::{BoxFuture, Either, select};
use risingwave_common::must_match;
use risingwave_pb::stream_plan;
use risingwave_storage::StateStore;
use risingwave_storage::store::StateStoreRead;
use rw_futures_util::drop_either_future;
use tokio::sync::mpsc::UnboundedReceiver;

use super::{DispatchExecutor, DispatchExecutorInner, dispatch_message_batch};
use crate::common::log_store_impl::kv_log_store::reader::LogStoreReadStateStreamRangeStart;
use crate::common::log_store_impl::kv_log_store::state::LogStoreReadState;
use crate::common::log_store_impl::kv_log_store::{FIRST_SEQ_ID, LogStoreVnodeProgress};
use crate::executor::prelude::*;
use crate::executor::sync_log_store_impl::{
    ReadFuture, SyncKvLogStoreContext, SyncedLogStoreBuffer, WriteFuture, WriteFutureEvent,
    aligned_message_stream, apply_pause_resume_mutation, init_local_log_store_state,
    process_chunk_flushed, process_upstream_chunk, write_barrier,
};
use crate::executor::{StreamConsumer, SyncedKvLogStoreMetrics};
use crate::task::NewOutputRequest;

/// Executor that pairs a synced KV log store with a dispatcher so the log store can
/// advance and write independently of downstream backpressure.
///
/// This keeps upstream executors being polled even when downstream is slow or blocked, decoupling
/// log store progress from downstream polling.
pub struct SyncLogStoreDispatchExecutor<S: StateStore> {
    pub(super) input: Executor,
    pub(super) inner: DispatchExecutorInner,
    pub(super) log_store_context: SyncKvLogStoreContext<S>,
}

impl<S: StateStore> SyncLogStoreDispatchExecutor<S> {
    pub(crate) async fn new(
        input: Executor,
        new_output_request_rx: UnboundedReceiver<(ActorId, NewOutputRequest)>,
        dispatchers: Vec<stream_plan::Dispatcher>,
        actor_context: &ActorContextRef,
        log_store_context: SyncKvLogStoreContext<S>,
    ) -> StreamResult<Self> {
        let DispatchExecutor { input, inner } =
            DispatchExecutor::new(input, new_output_request_rx, dispatchers, actor_context).await?;

        tracing::info!(
            actor_id = %actor_context.id,
            "synclogstore dispatch executor info"
        );

        Ok(Self {
            input,
            inner,
            log_store_context,
        })
    }
}

type DispatchingFuture = BoxFuture<'static, (DispatchExecutorInner, StreamResult<()>)>;
enum DispatchType {
    ChunkOrWatermark,
    Barrier(Barrier),
}
enum ConsumerFuture<S: StateStoreRead> {
    ReadingChunk {
        read_future: ReadFuture<S>,
        inner: DispatchExecutorInner,
    },
    Dispatching {
        future: DispatchingFuture,
        read_future: ReadFuture<S>,
        kind: DispatchType,
    },
    Empty,
}

enum ConsumerFutureEvent {
    ReadOutChunk(StreamChunk),
    BarrierDispatched(Barrier),
    ChunkDispatched,
}

impl<S: StateStoreRead> ConsumerFuture<S> {
    fn dispatch(
        mut inner: DispatchExecutorInner,
        message: Message,
        read_future: ReadFuture<S>,
    ) -> Self {
        tracing::trace!("consumer_future: dispatching future created");
        let kind = match &message {
            Message::Barrier(barrier) => DispatchType::Barrier(barrier.clone()),
            Message::Chunk(_) | Message::Watermark(_) => DispatchType::ChunkOrWatermark,
        };
        let batch = message.into_batch();
        let fut = async move {
            let r = dispatch_message_batch(&mut inner, batch)
                .await
                .map(|barrier_batch| {
                    if let Some(barrier_batch) = barrier_batch {
                        debug_assert_eq!(barrier_batch.len(), 1);
                    }
                });
            (inner, r)
        }
        .boxed();
        Self::Dispatching {
            future: fut,
            read_future,
            kind,
        }
    }

    fn read_chunk(inner: DispatchExecutorInner, read_future: ReadFuture<S>) -> Self {
        tracing::trace!("consumer_future: reading chunk future created");
        Self::ReadingChunk { read_future, inner }
    }

    async fn next_event(
        &mut self,
        barriers: &mut VecDeque<Message>,
        progress: &mut LogStoreVnodeProgress,
        read_state: &LogStoreReadState<S>,
        buffer: &mut SyncedLogStoreBuffer,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> StreamResult<(DispatchExecutorInner, ConsumerFutureEvent, ReadFuture<S>)> {
        if !barriers.is_empty()
            && let ConsumerFuture::ReadingChunk { .. } = self
        {
            let msg = barriers
                .pop_back()
                .expect("barrier queue should not be empty!");

            let (read_future, inner) = must_match!(
                std::mem::replace(self, ConsumerFuture::<S>::Empty),
                ConsumerFuture::ReadingChunk { read_future, inner } => (read_future, inner)
            );
            *self = Self::dispatch(inner, msg, read_future);
        }
        match self {
            ConsumerFuture::ReadingChunk { read_future, .. } => {
                let chunk = read_future
                    .next_chunk(progress, read_state, buffer, metrics)
                    .await?;
                let (read_future, inner) = must_match!(
                    replace(self, ConsumerFuture::<S>::Empty),
                    ConsumerFuture::ReadingChunk { read_future, inner } => (read_future, inner)
                );
                Ok((inner, ConsumerFutureEvent::ReadOutChunk(chunk), read_future))
            }
            ConsumerFuture::Dispatching { future, .. } => {
                let (inner, result) = future.await;
                result?;
                must_match!(replace(self, ConsumerFuture::<S>::Empty), ConsumerFuture::Dispatching { read_future,kind, .. } => {
                    let event = match kind {
                        DispatchType::Barrier(msg) => ConsumerFutureEvent::BarrierDispatched(msg),
                        DispatchType::ChunkOrWatermark => ConsumerFutureEvent::ChunkDispatched,
                    };
                    Ok((inner, event, read_future))
                })
            }
            ConsumerFuture::Empty => {
                unreachable!("ConsumerFuture::Empty should be handled!")
            }
        }
    }
}

impl<S: StateStore> StreamConsumer for SyncLogStoreDispatchExecutor<S> {
    type BarrierStream = impl Stream<Item = StreamResult<Barrier>> + Send;

    fn execute(mut self: Box<Self>) -> Self::BarrierStream {
        #[try_stream]
        async move {
            let actor_id = self.inner.actor_id;
            let log_store_config = self.log_store_context;

            let mut barriers = VecDeque::<Message>::new();
            let mut input = self.input.execute();

            let first_barrier = expect_first_barrier(&mut input).await?;
            let first_write_epoch = first_barrier.epoch;
            yield first_barrier.clone();

            // Dispatch the first barrier before initializing the log store states
            let first_barrier_batch = dispatch_message_batch(
                &mut self.inner,
                Message::Barrier(first_barrier.clone()).into_batch(),
            )
            .await?;
            debug_assert_eq!(
                first_barrier_batch
                    .as_ref()
                    .map(|barrier_batch| barrier_batch.len()),
                Some(1)
            );

            let (read_state, initial_write_state) =
                init_local_log_store_state(&log_store_config, first_write_epoch).await?;

            let initial_write_epoch = first_write_epoch;
            let mut pause_stream = first_barrier.is_pause_on_startup();

            if log_store_config.aligned {
                let aligned_stream = aligned_message_stream::<S>(
                    actor_id,
                    input,
                    read_state,
                    initial_write_state,
                    log_store_config.metrics.clone(),
                    initial_write_epoch,
                );

                #[for_await]
                for message in aligned_stream {
                    if let Some(barrier_batch) =
                        dispatch_message_batch(&mut self.inner, message?.into_batch()).await?
                    {
                        // Now Synclogstoredispatchexecutor only support sending out single barrier
                        debug_assert_eq!(barrier_batch.len(), 1);
                        for barrier in barrier_batch {
                            yield barrier;
                        }
                    }
                }
                return Ok(());
            }

            let mut seq_id = FIRST_SEQ_ID;
            let mut buffer = SyncedLogStoreBuffer::new(
                log_store_config.max_buffer_size,
                log_store_config.chunk_size,
                &log_store_config.metrics,
            );

            let log_store_stream = read_state
                .read_persisted_log_store(
                    log_store_config.metrics.persistent_log_read_metrics.clone(),
                    initial_write_epoch.curr,
                    LogStoreReadStateStreamRangeStart::Unbounded,
                )
                .await?;

            let mut log_store_stream = tokio_stream::StreamExt::peekable(log_store_stream);
            let mut clean_state = log_store_stream.peek().await.is_none();
            tracing::trace!(?clean_state);

            let mut progress = LogStoreVnodeProgress::None;
            let read_future_state = ReadFuture::ReadingPersistedStream(log_store_stream);
            let mut consumer_future_state = ConsumerFuture::ReadingChunk {
                inner: self.inner,
                read_future: read_future_state,
            };

            let mut write_future_state =
                WriteFuture::receive_from_upstream(input, initial_write_state);
            let mut end_of_stream = false;

            loop {
                let select_result = {
                    let consumer_future = async {
                        let should_pause_consumer = pause_stream
                            && barriers.is_empty()
                            && matches!(
                                &consumer_future_state,
                                ConsumerFuture::ReadingChunk { .. }
                            );
                        if should_pause_consumer {
                            pending().await
                        } else {
                            consumer_future_state
                                .next_event(
                                    &mut barriers,
                                    &mut progress,
                                    &read_state,
                                    &mut buffer,
                                    &log_store_config.metrics,
                                )
                                .await
                        }
                    };
                    pin_mut!(consumer_future);
                    let write_future = async {
                        if end_of_stream {
                            pending().await
                        } else {
                            write_future_state
                                .next_event(&log_store_config.metrics)
                                .await
                        }
                    };
                    pin_mut!(write_future);
                    let output = select(write_future, consumer_future).await;
                    drop_either_future(output)
                };

                match select_result {
                    Either::Left(_write_result) => {
                        drop(write_future_state);
                        let (stream, mut write_state, either) = _write_result?;
                        match either {
                            WriteFutureEvent::UpstreamMessageReceived(msg) => match msg {
                                Message::Chunk(chunk) => {
                                    let (new_seq_id, next_write_future) = process_upstream_chunk(
                                        seq_id,
                                        stream,
                                        write_state,
                                        chunk,
                                        &mut buffer,
                                    );
                                    seq_id = new_seq_id;
                                    write_future_state = next_write_future;
                                }
                                Message::Barrier(barrier) => {
                                    if clean_state
                                        && barrier.kind.is_checkpoint()
                                        && !buffer.is_empty()
                                    {
                                        write_future_state = WriteFuture::paused(
                                            log_store_config.pause_duration_ms,
                                            barrier,
                                            stream,
                                            write_state,
                                        );
                                        clean_state = false;
                                        log_store_config.metrics.unclean_state.inc();
                                    } else {
                                        apply_pause_resume_mutation(&barrier, &mut pause_stream);
                                        let write_state_post_write_barrier = write_barrier(
                                            actor_id,
                                            &mut write_state,
                                            barrier.clone(),
                                            &log_store_config.metrics,
                                            progress.take(),
                                            &mut buffer,
                                        )
                                        .await?;
                                        seq_id = FIRST_SEQ_ID;
                                        let update_vnode_bitmap =
                                            barrier.as_update_vnode_bitmap(actor_id);
                                        if update_vnode_bitmap.is_some() {
                                            return Err(anyhow!("vnode bitmap update during dispatch is not supported.").into());
                                        }

                                        write_state_post_write_barrier
                                            .post_yield_barrier(None)
                                            .await?;

                                        let is_stop_barrier = barrier.is_stop(actor_id);
                                        if is_stop_barrier {
                                            end_of_stream = true;
                                            write_future_state = WriteFuture::Empty;
                                        } else {
                                            write_future_state = WriteFuture::receive_from_upstream(
                                                stream,
                                                write_state,
                                            );
                                        }
                                        barriers.push_front(Message::Barrier(barrier));
                                    }
                                }
                                Message::Watermark(_watermark) => {
                                    write_future_state =
                                        WriteFuture::receive_from_upstream(stream, write_state);
                                }
                            },
                            WriteFutureEvent::ChunkFlushed(info) => {
                                write_future_state = process_chunk_flushed(
                                    stream,
                                    write_state,
                                    info,
                                    &mut buffer,
                                    &log_store_config.metrics,
                                );
                            }
                        }
                    }
                    Either::Right(consumer_result) => {
                        drop(consumer_future_state);
                        let (inner, event, read_future) = consumer_result?;
                        if !clean_state
                            && matches!(&read_future, ReadFuture::Idle)
                            && buffer.is_empty()
                        {
                            clean_state = true;
                            log_store_config.metrics.clean_state.inc();

                            if let WriteFuture::Paused { sleep_future, .. } =
                                &mut write_future_state
                            {
                                assert!(buffer.has_available_capacity());
                                *sleep_future = None;
                            }
                        }
                        match event {
                            ConsumerFutureEvent::ReadOutChunk(chunk) => {
                                log_store_config
                                    .metrics
                                    .total_read_count
                                    .inc_by(chunk.cardinality() as _);
                                consumer_future_state = ConsumerFuture::dispatch(
                                    inner,
                                    Message::Chunk(chunk),
                                    read_future,
                                );
                            }
                            ConsumerFutureEvent::ChunkDispatched => {
                                consumer_future_state =
                                    ConsumerFuture::read_chunk(inner, read_future);
                            }
                            ConsumerFutureEvent::BarrierDispatched(barrier) => {
                                yield barrier;
                                consumer_future_state =
                                    ConsumerFuture::read_chunk(inner, read_future);
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::StreamExt;
    use risingwave_common::array::{StreamChunk, StreamChunkTestExt};
    use risingwave_common::bitmap::Bitmap;
    use risingwave_common::catalog::{Field, Schema};
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::types::DataType;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_pb::stream_plan::{DispatcherType, PbDispatchOutputMapping};
    use risingwave_storage::memory::MemoryStateStore;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio::time::{Duration, timeout};

    use super::*;
    use crate::assert_stream_chunk_eq;
    use crate::common::log_store_impl::kv_log_store::KV_LOG_STORE_V2_INFO;
    use crate::common::log_store_impl::kv_log_store::serde::LogStoreRowSerde;
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        check_stream_chunk_eq, gen_test_log_store_table,
    };
    use crate::executor::exchange::permit::channel_for_test;
    use crate::executor::receiver::ReceiverExecutor;
    use crate::executor::{
        ActorContext, BarrierInner as Barrier, MessageInner as Message, Mutation, StopMutation,
    };
    use crate::task::barrier_test_utils::LocalBarrierTestEnv;

    fn init_logger() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_ansi(false)
            .try_init();
    }

    /// Build a minimal `SyncLogStoreDispatchExecutor` and ensure it can consume the first barrier.
    #[tokio::test]
    async fn test_init_sync_log_store_dispatch_executor() {
        init_logger();

        let actor_id = 4242.into();
        let downstream_actor = 5252.into();

        let barrier_test_env = LocalBarrierTestEnv::for_test().await;
        let (tx, rx) = channel_for_test();

        // Prepare downstream output channels before creating the executor so `collect_outputs` can
        // resolve them immediately.
        let (new_output_request_tx, new_output_request_rx) = unbounded_channel();
        let (down_tx, down_rx) = channel_for_test();
        new_output_request_tx
            .send((downstream_actor, NewOutputRequest::Local(down_tx)))
            .unwrap();

        // Dispatcher setup mirrors the simple dispatcher in dispatch.rs tests.
        let dispatcher = stream_plan::Dispatcher {
            r#type: DispatcherType::Simple as _,
            dispatcher_id: 7.into(),
            downstream_actor_id: vec![downstream_actor],
            output_mapping: PbDispatchOutputMapping::identical(0).into(),
            ..Default::default()
        };

        // Log store config mirrors the synced kv log store tests.
        let pk_info = &KV_LOG_STORE_V2_INFO;
        let table = gen_test_log_store_table(pk_info);
        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        let serde = LogStoreRowSerde::new(&table, vnodes, pk_info);
        let log_store_config = SyncKvLogStoreContext {
            table_id: table.id,
            fragment_id: 0.into(),
            serde,
            state_store: MemoryStateStore::new(),
            max_buffer_size: 16,
            pause_duration_ms: Duration::from_millis(10),
            aligned: false,
            chunk_size: 128,
            metrics: SyncedKvLogStoreMetrics::for_test(),
        };
        let first_barrier = Barrier::new_test_barrier(test_epoch(1));
        barrier_test_env.inject_barrier(&first_barrier, [actor_id]);
        barrier_test_env.flush_all_events().await;

        let input = Executor::new(
            Default::default(),
            ReceiverExecutor::for_test(
                actor_id,
                rx,
                barrier_test_env.local_barrier_manager.clone(),
            )
            .boxed(),
        );

        let executor = SyncLogStoreDispatchExecutor::new(
            input,
            new_output_request_rx,
            vec![dispatcher],
            &ActorContext::for_test(actor_id),
            log_store_config,
        )
        .await
        .unwrap();

        // Drive the executor: send the first barrier and ensure it is surfaced.
        let barrier_stream = Box::new(executor).execute();
        pin_mut!(barrier_stream);

        tx.send(Message::Barrier(first_barrier.clone().into_dispatcher()).into())
            .await
            .unwrap();

        let observed = barrier_stream.next().await.unwrap().unwrap();
        assert_eq!(observed.epoch.curr, test_epoch(1));

        // Downstream receiver is wired and ready for later assertions in follow-up tests.
        drop(down_rx);
    }

    /// Mirror `sync_kv_log_store::test_barrier_persisted_read`, but assert the dispatched output
    /// order: chunk(1) -> chunk(2) -> barrier(2), while barrier(1) is surfaced via barrier stream.
    #[tokio::test]
    async fn test_barrier_chunk_ordering_in_dispatch() {
        init_logger();

        let actor_id = 4242.into();
        let downstream_actor = 5252.into();

        let barrier_test_env = LocalBarrierTestEnv::for_test().await;
        let (tx, rx) = channel_for_test();

        let (new_output_request_tx, new_output_request_rx) = unbounded_channel();
        let (down_tx, mut down_rx) = channel_for_test();
        new_output_request_tx
            .send((downstream_actor, NewOutputRequest::Local(down_tx)))
            .unwrap();

        let dispatcher = stream_plan::Dispatcher {
            r#type: DispatcherType::Simple as _,
            dispatcher_id: 7.into(),
            downstream_actor_id: vec![downstream_actor],
            output_mapping: PbDispatchOutputMapping::identical(2).into(),
            ..Default::default()
        };

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let table = gen_test_log_store_table(pk_info);
        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        let serde = LogStoreRowSerde::new(&table, vnodes, pk_info);
        let log_store_config = SyncKvLogStoreContext {
            table_id: table.id,
            fragment_id: 0.into(),
            serde,
            state_store: MemoryStateStore::new(),
            // Big enough to avoid forced chunk flushes in this test.
            max_buffer_size: 1024,
            pause_duration_ms: Duration::from_millis(10),
            aligned: false,
            chunk_size: 1024,
            metrics: SyncedKvLogStoreMetrics::for_test(),
        };

        // Inject the first barrier before subscribing, so the barrier worker has an actor state
        // for `actor_id` to register the barrier sender.
        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        barrier_test_env.inject_barrier(&barrier1, [actor_id]);
        barrier_test_env.flush_all_events().await;

        let input_schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Varchar),
            ],
        };
        let input = Executor::new(
            ExecutorInfo {
                schema: input_schema,
                stream_key: vec![0],
                ..Default::default()
            },
            ReceiverExecutor::for_test(
                actor_id,
                rx,
                barrier_test_env.local_barrier_manager.clone(),
            )
            .boxed(),
        );

        let executor = SyncLogStoreDispatchExecutor::new(
            input,
            new_output_request_rx,
            vec![dispatcher],
            &ActorContext::for_test(actor_id),
            log_store_config,
        )
        .await
        .unwrap();

        let (barrier_out_tx, mut barrier_out_rx) = unbounded_channel();
        let barrier_driver = tokio::spawn(async move {
            let barrier_stream = Box::new(executor).execute();
            futures::pin_mut!(barrier_stream);
            while let Some(item) = barrier_stream.next().await {
                barrier_out_tx.send(item).ok();
            }
        });

        tx.send(Message::Barrier(barrier1.clone().into_dispatcher()).into())
            .await
            .unwrap();

        let observed1 = timeout(Duration::from_secs(1), barrier_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(observed1.epoch.curr, test_epoch(1));

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive barrier(1)");
        let barriers = msg.as_barrier_batch().unwrap();
        assert_eq!(barriers.len(), 1);
        assert_eq!(barriers[0].epoch.curr, test_epoch(1));

        // chunk(1), chunk(2)
        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            + 10  11",
        );
        let chunk_2 = StreamChunk::from_pretty(
            "  I   T
            -  5  10
            -  6  10
            -  8  10
            U- 10  11
            U+ 10  10",
        );
        tx.send(Message::Chunk(chunk_1.clone()).into())
            .await
            .unwrap();
        tx.send(Message::Chunk(chunk_2.clone()).into())
            .await
            .unwrap();

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive chunk(1)");
        assert_stream_chunk_eq!(msg.as_chunk().unwrap(), chunk_1);

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive chunk(2)");
        assert_stream_chunk_eq!(msg.as_chunk().unwrap(), chunk_2);

        // barrier(2)
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        barrier_test_env.inject_barrier(&barrier2, [actor_id]);
        barrier_test_env.flush_all_events().await;
        tx.send(Message::Barrier(barrier2.clone().into_dispatcher()).into())
            .await
            .unwrap();

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive barrier(2)");
        let barriers = msg.as_barrier_batch().unwrap();
        assert_eq!(barriers.len(), 1);
        assert_eq!(barriers[0].epoch.curr, test_epoch(2));

        let observed2 = timeout(Duration::from_secs(1), barrier_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(observed2.epoch.curr, test_epoch(2));

        barrier_driver.abort();
    }

    #[tokio::test]
    async fn test_aligned_barrier_chunk_ordering_in_dispatch() {
        init_logger();

        let actor_id = 4242.into();
        let downstream_actor = 5252.into();

        let barrier_test_env = LocalBarrierTestEnv::for_test().await;
        let (tx, rx) = channel_for_test();

        let (new_output_request_tx, new_output_request_rx) = unbounded_channel();
        let (down_tx, mut down_rx) = channel_for_test();
        new_output_request_tx
            .send((downstream_actor, NewOutputRequest::Local(down_tx)))
            .unwrap();

        let dispatcher = stream_plan::Dispatcher {
            r#type: DispatcherType::Simple as _,
            dispatcher_id: 7.into(),
            downstream_actor_id: vec![downstream_actor],
            output_mapping: PbDispatchOutputMapping::identical(2).into(),
            ..Default::default()
        };

        let pk_info = &KV_LOG_STORE_V2_INFO;
        let table = gen_test_log_store_table(pk_info);
        let vnodes = Some(Arc::new(Bitmap::ones(VirtualNode::COUNT_FOR_TEST)));
        let serde = LogStoreRowSerde::new(&table, vnodes, pk_info);
        let log_store_config = SyncKvLogStoreContext {
            table_id: table.id,
            fragment_id: 0.into(),
            serde,
            state_store: MemoryStateStore::new(),
            max_buffer_size: 1024,
            pause_duration_ms: Duration::from_millis(10),
            aligned: true,
            chunk_size: 1024,
            metrics: SyncedKvLogStoreMetrics::for_test(),
        };

        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        barrier_test_env.inject_barrier(&barrier1, [actor_id]);
        barrier_test_env.flush_all_events().await;

        let input_schema = Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Varchar),
            ],
        };
        let input = Executor::new(
            ExecutorInfo {
                schema: input_schema,
                stream_key: vec![0],
                ..Default::default()
            },
            ReceiverExecutor::for_test(
                actor_id,
                rx,
                barrier_test_env.local_barrier_manager.clone(),
            )
            .boxed(),
        );

        let executor = SyncLogStoreDispatchExecutor::new(
            input,
            new_output_request_rx,
            vec![dispatcher],
            &ActorContext::for_test(actor_id),
            log_store_config,
        )
        .await
        .unwrap();

        let (barrier_out_tx, mut barrier_out_rx) = unbounded_channel();
        let barrier_driver = tokio::spawn(async move {
            let barrier_stream = Box::new(executor).execute();
            futures::pin_mut!(barrier_stream);
            while let Some(item) = barrier_stream.next().await {
                barrier_out_tx.send(item).ok();
            }
        });

        tx.send(Message::Barrier(barrier1.clone().into_dispatcher()).into())
            .await
            .unwrap();

        let observed1 = timeout(Duration::from_secs(1), barrier_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(observed1.epoch.curr, test_epoch(1));

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive barrier(1)");
        let barriers = msg.as_barrier_batch().unwrap();
        assert_eq!(barriers.len(), 1);
        assert_eq!(barriers[0].epoch.curr, test_epoch(1));

        let chunk_1 = StreamChunk::from_pretty(
            "  I   T
            +  5  10
            +  6  10
            +  8  10
            +  9  10
            + 10  11",
        );
        let chunk_2 = StreamChunk::from_pretty(
            "  I   T
            -  5  10
            -  6  10
            -  8  10
            U- 10  11
            U+ 10  10",
        );
        tx.send(Message::Chunk(chunk_1.clone()).into())
            .await
            .unwrap();
        tx.send(Message::Chunk(chunk_2.clone()).into())
            .await
            .unwrap();

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive chunk(1)");
        assert_stream_chunk_eq!(msg.as_chunk().unwrap(), chunk_1);

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive chunk(2)");
        assert_stream_chunk_eq!(msg.as_chunk().unwrap(), chunk_2);

        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        barrier_test_env.inject_barrier(&barrier2, [actor_id]);
        barrier_test_env.flush_all_events().await;
        tx.send(Message::Barrier(barrier2.clone().into_dispatcher()).into())
            .await
            .unwrap();

        let msg = timeout(Duration::from_secs(1), down_rx.recv())
            .await
            .unwrap()
            .expect("downstream should receive barrier(2)");
        let barriers = msg.as_barrier_batch().unwrap();
        assert_eq!(barriers.len(), 1);
        assert_eq!(barriers[0].epoch.curr, test_epoch(2));

        let observed2 = timeout(Duration::from_secs(1), barrier_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(observed2.epoch.curr, test_epoch(2));

        barrier_driver.abort();
    }
}
