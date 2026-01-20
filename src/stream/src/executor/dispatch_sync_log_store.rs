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
use std::pin::Pin;
use std::time::Duration;

use anyhow::anyhow;
use futures::FutureExt;
use futures::future::{BoxFuture, Either, select};
use futures::stream::StreamFuture;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::must_match;
use risingwave_pb::stream_plan;
use risingwave_storage::StateStore;
use risingwave_storage::store::{
    LocalStateStore, NewLocalOptions, OpConsistencyLevel, StateStoreRead,
};
use rw_futures_util::drop_either_future;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{Instant, Sleep, sleep_until};
use tokio_stream::adapters::Peekable;
use tracing::Instrument;

use super::{DispatchExecutor, DispatchExecutorInner, DispatcherImpl, MessageBatch};
use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::reader::LogStoreReadStateStreamRangeStart;
use crate::common::log_store_impl::kv_log_store::reader::timeout_auto_rebuild::TimeoutAutoRebuildIter;
use crate::common::log_store_impl::kv_log_store::serde::{
    KvLogStoreItem, LogStoreItemMergeStream, LogStoreRowSerde,
};
use crate::common::log_store_impl::kv_log_store::state::{
    LogStoreReadState, LogStoreStateWriteChunkFuture, LogStoreWriteState, new_log_store_state,
};
use crate::common::log_store_impl::kv_log_store::{FIRST_SEQ_ID, LogStoreVnodeProgress, SeqId};
use crate::executor::prelude::*;
use crate::executor::sync_kv_log_store::{
    ReadFlushedChunkFuture, SyncedLogStoreBuffer, write_barrier,
};
use crate::executor::{FlushedChunkInfo, StreamConsumer, SyncedKvLogStoreMetrics};
use crate::task::NewOutputRequest;

pub struct SyncLogStoreDispatchConfig<S: StateStore> {
    pub table_id: TableId,
    pub serde: LogStoreRowSerde,
    pub state_store: S,
    pub max_buffer_size: usize,
    pub pause_duration_ms: Duration,
    pub aligned: bool,
    pub chunk_size: usize,
    pub metrics: SyncedKvLogStoreMetrics,
}

pub struct SyncLogStoreDispatchExecutor<S: StateStore> {
    pub(super) input: Executor,
    pub(super) inner: DispatchExecutorInner,
    pub(super) log_store_config: SyncLogStoreDispatchConfig<S>,
}

impl<S: StateStore> SyncLogStoreDispatchExecutor<S> {
    pub(crate) async fn new(
        input: Executor,
        new_output_request_rx: UnboundedReceiver<(ActorId, NewOutputRequest)>,
        dispatchers: Vec<stream_plan::Dispatcher>,
        actor_context: &ActorContextRef,
        log_store_config: SyncLogStoreDispatchConfig<S>,
    ) -> StreamResult<Self> {
        let mut executor = DispatchExecutor::new_inner(
            input,
            new_output_request_rx,
            vec![],
            actor_context.id,
            actor_context.fragment_id,
            actor_context.config.clone(),
            actor_context.streaming_metrics.clone(),
        );
        let inner = &mut executor.inner;
        for dispatcher in dispatchers {
            let outputs = inner
                .collect_outputs(&dispatcher.downstream_actor_id)
                .await?;
            let dispatcher = DispatcherImpl::new(outputs, &dispatcher)?;
            let dispatcher = inner.metrics.monitor_dispatcher(dispatcher);
            inner.dispatchers.push(dispatcher);
        }
        let DispatchExecutor { input, inner } = executor;

        tracing::info!(
            actor_id = %actor_context.id,
            "synclogstore dispatch executor info"
        );

        Ok(Self {
            input,
            inner,
            log_store_config,
        })
    }
}

enum WriteFuture<S: LocalStateStore> {
    ReceiveFromUpstream {
        future: StreamFuture<BoxedMessageStream>,
        write_state: LogStoreWriteState<S>,
    },
    FlushingChunk {
        epoch: u64,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        future: Pin<Box<LogStoreStateWriteChunkFuture<S>>>,
        stream: BoxedMessageStream,
    },
    Paused {
        start_instant: Instant,
        sleep_future: Option<Pin<Box<Sleep>>>,
        barrier: Barrier,
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
    },
    Empty,
}

enum WriteFutureEvent {
    UpstreamMessageReceived(Message),
    ChunkFlushed(FlushedChunkInfo),
    EndofStream,
}

impl<S: LocalStateStore> WriteFuture<S> {
    fn flush_chunk(
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
        chunk: StreamChunk,
        epoch: u64,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
    ) -> Self {
        tracing::trace!(
            start_seq_id,
            end_seq_id,
            epoch,
            cardinality = chunk.cardinality(),
            "write_future: flushing chunk"
        );
        Self::FlushingChunk {
            epoch,
            start_seq_id,
            end_seq_id,
            future: Box::pin(write_state.into_write_chunk_future(
                chunk,
                epoch,
                start_seq_id,
                end_seq_id,
            )),
            stream,
        }
    }

    fn receive_from_upstream(
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
    ) -> Self {
        Self::ReceiveFromUpstream {
            future: futures::StreamExt::into_future(stream),
            write_state,
        }
    }

    fn paused(
        pause_duration: Duration,
        barrier: Barrier,
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
    ) -> Self {
        let now = Instant::now();
        tracing::trace!(?now, ?pause_duration, "write_future_pause");
        Self::Paused {
            start_instant: now,
            sleep_future: Some(Box::pin(sleep_until(now + pause_duration))),
            barrier,
            stream,
            write_state,
        }
    }

    async fn next_event(
        &mut self,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> StreamResult<(BoxedMessageStream, LogStoreWriteState<S>, WriteFutureEvent)> {
        match self {
            WriteFuture::ReceiveFromUpstream { future, .. } => {
                let (opt, stream) = future.await;

                match opt {
                    Some(result) => {
                        must_match!(replace(self, WriteFuture::Empty), WriteFuture::ReceiveFromUpstream { write_state, .. } => {
                            result.map(|item| {
                                (
                                    stream,
                                    write_state,
                                    WriteFutureEvent::UpstreamMessageReceived(item),
                                )
                            })
                            .map_err(Into::into)
                        })
                    }
                    None => {
                        must_match!(replace(self, WriteFuture::Empty), WriteFuture::ReceiveFromUpstream { write_state, .. } => {
                            Ok((stream, write_state, WriteFutureEvent::EndofStream))
                        })
                    }
                }
            }
            WriteFuture::FlushingChunk { future, .. } => {
                let (write_state, result) = future.await;
                let result = must_match!(replace(self, WriteFuture::Empty), WriteFuture::FlushingChunk { epoch, start_seq_id, end_seq_id, stream, ..  } => {
                    result.map(|(flush_info, vnode_bitmap)| {
                        (stream, write_state, WriteFutureEvent::ChunkFlushed(FlushedChunkInfo {
                            epoch,
                            start_seq_id,
                            end_seq_id,
                            flush_info,
                            vnode_bitmap,
                        }))
                    })
                });
                result.map_err(Into::into)
            }
            WriteFuture::Paused {
                start_instant,
                sleep_future,
                ..
            } => {
                if let Some(fut) = sleep_future.as_mut() {
                    fut.await;
                    metrics
                        .pause_duration_ns
                        .inc_by(start_instant.elapsed().as_nanos() as _);
                    tracing::trace!("resuming write future");
                }
                must_match!(
                    replace(self, WriteFuture::Empty),
                    WriteFuture::Paused {
                        barrier,
                        stream,
                        write_state,
                        ..
                    } => Ok((stream, write_state, WriteFutureEvent::UpstreamMessageReceived(Message::Barrier(barrier))))
                )
            }
            WriteFuture::Empty => {
                unreachable!("should not be polled after ready")
            }
        }
    }
}

type DispatchingFuture = BoxFuture<'static, (DispatchExecutorInner, StreamResult<()>)>;
enum DispatchType {
    ChunkOrWatermark,
    Barrier(Barrier),
}

type PersistedStream<S> = Peekable<Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>>;

enum ReadFuture<S: StateStoreRead> {
    ReadingPersistedStream(PersistedStream<S>),
    ReadingFlushedChunk {
        future: ReadFlushedChunkFuture,
        end_seq_id: SeqId,
    },
    Idle,
}

impl<S: StateStoreRead> ReadFuture<S> {
    pub async fn next_chunk(
        &mut self,
        progress: &mut LogStoreVnodeProgress,
        read_state: &LogStoreReadState<S>,
        buffer: &mut SyncedLogStoreBuffer,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        match self {
            ReadFuture::ReadingPersistedStream(stream) => {
                while let Some((epoch, item)) = futures::TryStreamExt::try_next(stream).await? {
                    match item {
                        KvLogStoreItem::Barrier { vnodes, .. } => {
                            tracing::trace!(epoch, "read logstore barrier");
                            // update the progress
                            progress.apply_aligned(vnodes, epoch, None);
                            continue;
                        }
                        KvLogStoreItem::StreamChunk {
                            chunk,
                            progress: chunk_progress,
                        } => {
                            tracing::trace!("read logstore chunk of size: {}", chunk.cardinality());
                            progress.apply_per_vnode(epoch, chunk_progress);
                            return Ok(Some(chunk));
                        }
                    }
                }
                *self = ReadFuture::Idle;
            }
            ReadFuture::ReadingFlushedChunk { .. } | ReadFuture::Idle => {}
        }
        match self {
            ReadFuture::ReadingPersistedStream(_) => {
                unreachable!("must have finished read persisted stream when reaching here")
            }
            ReadFuture::ReadingFlushedChunk { .. } => {}
            ReadFuture::Idle => loop {
                let Some((item_epoch, item)) = buffer.pop_front() else {
                    return Ok(None);
                };
                match item {
                    LogStoreBufferItem::StreamChunk {
                        chunk,
                        start_seq_id,
                        end_seq_id,
                        flushed,
                        ..
                    } => {
                        metrics.buffer_read_count.inc_by(chunk.cardinality() as _);
                        tracing::trace!(
                            start_seq_id,
                            end_seq_id,
                            flushed,
                            cardinality = chunk.cardinality(),
                            "read buffered chunk of size"
                        );
                        progress.apply_aligned(
                            read_state.vnodes().clone(),
                            item_epoch,
                            Some(end_seq_id),
                        );
                        return Ok(Some(chunk));
                    }
                    LogStoreBufferItem::Flushed {
                        vnode_bitmap,
                        start_seq_id,
                        end_seq_id,
                        chunk_id,
                    } => {
                        tracing::trace!(start_seq_id, end_seq_id, chunk_id, "read flushed chunk");
                        let read_metrics = metrics.flushed_buffer_read_metrics.clone();
                        let future = read_state
                            .read_flushed_chunk(
                                vnode_bitmap,
                                chunk_id,
                                start_seq_id,
                                end_seq_id,
                                item_epoch,
                                read_metrics,
                            )
                            .boxed();
                        *self = ReadFuture::ReadingFlushedChunk { future, end_seq_id };
                        break;
                    }
                    LogStoreBufferItem::Barrier { .. } => {
                        tracing::trace!(item_epoch, "read buffer barrier");
                        progress.apply_aligned(read_state.vnodes().clone(), item_epoch, None);
                        continue;
                    }
                }
            },
        }

        let (future, end_seq_id) = match self {
            ReadFuture::ReadingPersistedStream(_) | ReadFuture::Idle => {
                unreachable!("should be at ReadingFlushedChunk")
            }
            ReadFuture::ReadingFlushedChunk { future, end_seq_id } => (future, *end_seq_id),
        };

        let (_, chunk, epoch) = future.await?;
        progress.apply_aligned(read_state.vnodes().clone(), epoch, Some(end_seq_id));
        tracing::trace!(
            end_seq_id,
            "read flushed chunk of size: {}",
            chunk.cardinality()
        );
        *self = ReadFuture::Idle;
        Ok(Some(chunk))
    }
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
    WaitingForChunks,
}

impl<S: StateStoreRead> ConsumerFuture<S> {
    fn dispatch(
        mut inner: DispatchExecutorInner,
        message: Message,
        read_future: ReadFuture<S>,
    ) -> Self {
        tracing::trace!("consumer_future: dispatching future created");
        let (fut, kind) = match message {
            Message::Chunk(chunk) => {
                let batch = MessageBatch::Chunk(chunk);
                let fut = async move {
                    let r = inner
                        .dispatch(batch)
                        .instrument(tracing::info_span!("dispatch_chunk"))
                        .instrument_await("dispatch_chunk")
                        .await;
                    (inner, r)
                }
                .boxed();
                (fut, DispatchType::ChunkOrWatermark)
            }
            Message::Barrier(barrier) => {
                let to_dispatch = MessageBatch::BarrierBatch(vec![barrier.clone()]);
                let fut = async move {
                    let r = inner
                        .dispatch(to_dispatch)
                        .instrument(tracing::info_span!("dispatch_barrier_batch"))
                        .instrument_await("dispatch_barrier_batch")
                        .await;
                    inner.metrics.metrics.barrier_batch_size.observe(1.0);
                    (inner, r)
                }
                .boxed();
                (fut, DispatchType::Barrier(barrier))
            }
            Message::Watermark(watermark) => {
                let batch = MessageBatch::Watermark(watermark);
                let fut = async move {
                    let r = inner
                        .dispatch(batch)
                        .instrument(tracing::info_span!("dispatch_watermark"))
                        .instrument_await("dispatch_watermark")
                        .await;
                    (inner, r)
                }
                .boxed();
                (fut, DispatchType::ChunkOrWatermark)
            }
        };
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
                std::mem::replace(self, ConsumerFuture::Empty),
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
                    replace(self, ConsumerFuture::Empty),
                    ConsumerFuture::ReadingChunk { read_future, inner } => (read_future, inner)
                );
                match chunk {
                    Some(chunk) => {
                        Ok((inner, ConsumerFutureEvent::ReadOutChunk(chunk), read_future))
                    }
                    None => Ok((inner, ConsumerFutureEvent::WaitingForChunks, read_future)),
                }
            }
            ConsumerFuture::Dispatching { future, .. } => {
                let (inner, result) = future.await;
                result?;
                must_match!(replace(self, ConsumerFuture::Empty), ConsumerFuture::Dispatching { read_future,kind, .. } => {
                    Ok((inner, match kind {
                        DispatchType::Barrier(msg) => ConsumerFutureEvent::BarrierDispatched(msg),
                        DispatchType::ChunkOrWatermark => ConsumerFutureEvent::ChunkDispatched,
                    }, read_future))
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
        let _max_barrier_count_per_batch = self.inner.actor_config.developer.max_barrier_batch_size;

        #[try_stream]
        async move {
            let actor_id = self.inner.actor_id;
            let log_store_config = self.log_store_config;

            let mut barriers = VecDeque::<Message>::new();
            let mut input = self.input.execute();

            let first_barrier = expect_first_barrier(&mut input).await?;
            let first_write_epoch = first_barrier.epoch;
            yield first_barrier.clone();

            // Dispatch the first barrier before initializing the log store states
            self.inner
                .dispatch(MessageBatch::BarrierBatch(vec![first_barrier.clone()]))
                .instrument(tracing::info_span!("dispatch_barrier_batch"))
                .instrument_await("dispatch_barrier_batch")
                .await?;
            self.inner.metrics.metrics.barrier_batch_size.observe(1.0);

            let local_state_store = log_store_config
                .state_store
                .new_local(NewLocalOptions {
                    table_id: log_store_config.table_id,
                    op_consistency_level: OpConsistencyLevel::Inconsistent,
                    table_option: TableOption {
                        retention_seconds: None,
                    },
                    is_replicated: false,
                    vnodes: log_store_config.serde.vnodes().clone(),
                    upload_on_flush: false,
                })
                .await;

            let (read_state, mut initial_write_state) = new_log_store_state(
                log_store_config.table_id,
                local_state_store,
                log_store_config.serde,
                log_store_config.chunk_size,
            );
            initial_write_state.init(first_write_epoch).await?;

            let initial_write_epoch = first_write_epoch;
            let mut pause_stream = first_barrier.is_pause_on_startup();

            if log_store_config.aligned {
                tracing::info!("aligned mode");
                let log_store_stream = read_state
                    .read_persisted_log_store(
                        log_store_config.metrics.persistent_log_read_metrics.clone(),
                        initial_write_epoch.curr,
                        LogStoreReadStateStreamRangeStart::Unbounded,
                    )
                    .await?;

                #[for_await]
                for message in log_store_stream {
                    let (_epoch, message) = message?;
                    match message {
                        KvLogStoreItem::Barrier { .. } => {
                            continue;
                        }
                        KvLogStoreItem::StreamChunk { chunk, .. } => {
                            self.inner
                                .dispatch(MessageBatch::Chunk(chunk))
                                .instrument(tracing::info_span!("dispatch_chunk"))
                                .instrument_await("dispatch_chunk")
                                .await?;
                        }
                    }
                }

                let mut realigned_logstore = false;

                #[for_await]
                for message in input {
                    match message? {
                        Message::Barrier(barrier) => {
                            let is_checkpoint = barrier.is_checkpoint();
                            let mut progress = LogStoreVnodeProgress::None;
                            progress.apply_aligned(
                                read_state.vnodes().clone(),
                                barrier.epoch.prev,
                                None,
                            );
                            let post_seal = initial_write_state
                                .seal_current_epoch(barrier.epoch.curr, progress.take());
                            let update_vnode_bitmap = barrier.as_update_vnode_bitmap(actor_id);
                            if update_vnode_bitmap.is_some() {
                                return Err(anyhow!(
                                    "updating vnode bitmap in place is not supported any more!"
                                )
                                .into());
                            }

                            self.inner
                                .dispatch(MessageBatch::BarrierBatch(vec![barrier.clone()]))
                                .instrument(tracing::info_span!("dispatch_barrier_batch"))
                                .instrument_await("dispatch_barrier_batch")
                                .await?;
                            post_seal.post_yield_barrier(None).await?;
                            if !realigned_logstore && is_checkpoint {
                                realigned_logstore = true;
                                tracing::info!("realigned logstore");
                            }
                        }
                        Message::Chunk(chunk) => {
                            self.inner
                                .dispatch(MessageBatch::Chunk(chunk))
                                .instrument(tracing::info_span!("dispatch_chunk"))
                                .instrument_await("dispatch_chunk")
                                .await?;
                        }
                        Message::Watermark(watermark) => {
                            self.inner
                                .dispatch(MessageBatch::Watermark(watermark))
                                .instrument(tracing::info_span!("dispatch_watermark"))
                                .instrument_await("dispatch_watermark")
                                .await?;
                        }
                    }
                }
                return Ok(());
            }

            let mut seq_id = FIRST_SEQ_ID;
            let mut buffer = SyncedLogStoreBuffer {
                buffer: VecDeque::new(),
                current_size: 0,
                max_size: log_store_config.max_buffer_size,
                max_chunk_size: log_store_config.chunk_size,
                next_chunk_id: 0,
                metrics: log_store_config.metrics.clone(),
                flushed_count: 0,
            };

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
            let mut write_done = false;

            loop {
                // Avoid a busy-loop when there's no buffered data to dispatch. In that case we
                // should wait for upstream, but still exit promptly once upstream is done.
                let consumer_idle = matches!(
                    &consumer_future_state,
                    ConsumerFuture::ReadingChunk {
                        read_future: ReadFuture::Idle,
                        ..
                    }
                );
                if write_done && barriers.is_empty() && buffer.is_empty() && consumer_idle {
                    break;
                }

                let select_result = {
                    let should_wait_for_upstream =
                        barriers.is_empty() && buffer.is_empty() && consumer_idle;
                    let consumer_future = async {
                        let should_pause_consumer = pause_stream
                            && barriers.is_empty()
                            && matches!(
                                &consumer_future_state,
                                ConsumerFuture::ReadingChunk { .. }
                            );
                        if should_pause_consumer || should_wait_for_upstream {
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
                        if write_done {
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
                                    let start_seq_id = seq_id;
                                    let new_seq_id = seq_id + chunk.cardinality() as SeqId;
                                    let end_seq_id = new_seq_id - 1;
                                    let epoch = write_state.epoch().curr;
                                    tracing::trace!(
                                        start_seq_id,
                                        end_seq_id,
                                        new_seq_id,
                                        epoch,
                                        cardinality = chunk.cardinality(),
                                        "received chunk"
                                    );
                                    if let Some(chunk_to_flush) = buffer.add_or_flush_chunk(
                                        start_seq_id,
                                        end_seq_id,
                                        chunk,
                                        epoch,
                                    ) {
                                        seq_id = new_seq_id;
                                        write_future_state = WriteFuture::flush_chunk(
                                            stream,
                                            write_state,
                                            chunk_to_flush,
                                            epoch,
                                            start_seq_id,
                                            end_seq_id,
                                        );
                                    } else {
                                        seq_id = new_seq_id;
                                        write_future_state =
                                            WriteFuture::receive_from_upstream(stream, write_state);
                                    }
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
                                        if let Some(mutation) = barrier.mutation.as_deref() {
                                            match mutation {
                                                Mutation::Pause => {
                                                    pause_stream = true;
                                                }
                                                Mutation::Resume => {
                                                    pause_stream = false;
                                                }
                                                _ => {}
                                            }
                                        }
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

                                        barriers.push_front(Message::Barrier(barrier));
                                        write_state_post_write_barrier
                                            .post_yield_barrier(None)
                                            .await?;
                                        write_future_state =
                                            WriteFuture::receive_from_upstream(stream, write_state);
                                    }
                                }
                                Message::Watermark(_watermark) => {
                                    write_future_state =
                                        WriteFuture::receive_from_upstream(stream, write_state);
                                }
                            },
                            WriteFutureEvent::ChunkFlushed(info) => {
                                buffer.add_flushed_item_to_buffer(
                                    info.start_seq_id,
                                    info.end_seq_id,
                                    info.vnode_bitmap,
                                    info.epoch,
                                );
                                log_store_config
                                    .metrics
                                    .storage_write_count
                                    .inc_by(info.flush_info.flush_count as _);
                                log_store_config
                                    .metrics
                                    .storage_write_size
                                    .inc_by(info.flush_info.flush_size as _);
                                write_future_state =
                                    WriteFuture::receive_from_upstream(stream, write_state);
                            }
                            WriteFutureEvent::EndofStream => {
                                write_done = true;
                                write_future_state = WriteFuture::Empty;
                                continue;
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
                                assert!(buffer.current_size < log_store_config.max_buffer_size);
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
                            ConsumerFutureEvent::WaitingForChunks => {
                                if write_done {
                                    break;
                                };
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
    use tokio::time::timeout;

    use super::*;
    use crate::assert_stream_chunk_eq;
    use crate::common::log_store_impl::kv_log_store::KV_LOG_STORE_V2_INFO;
    use crate::common::log_store_impl::kv_log_store::test_utils::{
        check_stream_chunk_eq, gen_test_log_store_table,
    };
    use crate::executor::exchange::permit::channel_for_test;
    use crate::executor::receiver::ReceiverExecutor;
    use crate::executor::{ActorContext, BarrierInner as Barrier, MessageInner as Message};
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
        let log_store_config = SyncLogStoreDispatchConfig {
            table_id: table.id,
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
        let log_store_config = SyncLogStoreDispatchConfig {
            table_id: table.id,
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
}
