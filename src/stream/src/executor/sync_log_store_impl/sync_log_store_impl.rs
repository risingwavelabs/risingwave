use std::collections::VecDeque;
use std::future::pending;
use std::mem::replace;
use std::pin::Pin;

use anyhow::anyhow;
use futures::future::{BoxFuture, Either, select};
use futures::stream::StreamFuture;
use futures::{FutureExt, StreamExt, TryStreamExt, pin_mut};
#[allow(unused_imports)]
use futures_async_stream::{for_await, try_stream};
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::must_match;
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::sink::log_store::{ChunkId, LogStoreResult};
use risingwave_storage::StateStore;
use risingwave_storage::store::{
    LocalStateStore, NewLocalOptions, OpConsistencyLevel, StateStoreRead,
};
use rw_futures_util::drop_either_future;
use tokio::time::{Duration, Instant, Sleep, sleep_until};
use tokio_stream::adapters::Peekable;

use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::reader::LogStoreReadStateStreamRangeStart;
use crate::common::log_store_impl::kv_log_store::reader::timeout_auto_rebuild::TimeoutAutoRebuildIter;
use crate::common::log_store_impl::kv_log_store::serde::{
    KvLogStoreItem, LogStoreItemMergeStream, LogStoreRowSerde,
};
use crate::common::log_store_impl::kv_log_store::state::{
    LogStorePostSealCurrentEpoch, LogStoreReadState, LogStoreStateWriteChunkFuture,
    LogStoreWriteState, new_log_store_state,
};
use crate::common::log_store_impl::kv_log_store::{
    Epoch, FIRST_SEQ_ID, FlushInfo, LogStoreVnodeProgress, SeqId,
};
use crate::executor::sync_kv_log_store::metrics::SyncedKvLogStoreMetrics;
use crate::executor::sync_kv_log_store::{SyncedKvLogStoreExecutor, SyncedLogStoreBuffer};
use crate::executor::{
    Barrier, BoxedMessageStream, Message, Mutation, StreamExecutorError, StreamExecutorResult,
    expect_first_barrier,
};
use crate::task::ActorId;

pub(crate) type ReadFlushedChunkFuture =
    BoxFuture<'static, LogStoreResult<(ChunkId, StreamChunk, Epoch)>>;

pub(crate) struct FlushedChunkInfo {
    pub(crate) epoch: u64,
    pub(crate) start_seq_id: SeqId,
    pub(crate) end_seq_id: SeqId,
    pub(crate) flush_info: FlushInfo,
    pub(crate) vnode_bitmap: Bitmap,
}

pub(crate) struct SyncedKvLogStoreContext<S: StateStore> {
    pub(crate) table_id: TableId,
    pub(crate) metrics: SyncedKvLogStoreMetrics,
    pub(crate) serde: LogStoreRowSerde,
    pub(crate) state_store: S,
    pub(crate) max_buffer_size: usize,
    pub(crate) chunk_size: usize,
    pub(crate) pause_duration_ms: Duration,
    pub(crate) aligned: bool,
}

pub(crate) type LocalLogStoreReadState<S> =
    LogStoreReadState<<<S as StateStore>::Local as LocalStateStore>::FlushedSnapshotReader>;
pub(crate) type LocalLogStoreWriteState<S> = LogStoreWriteState<<S as StateStore>::Local>;
pub(crate) type RawPersistedStream<S> =
    Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>;

pub(crate) struct SyncedKvLogStoreExecutorInner<S: StateStore> {
    context: SyncedKvLogStoreContext<S>,
}

impl<S: StateStore> SyncedKvLogStoreExecutorInner<S> {
    pub(crate) fn new(context: SyncedKvLogStoreContext<S>) -> Self {
        Self { context }
    }

    pub(crate) fn metrics(&self) -> &SyncedKvLogStoreMetrics {
        &self.context.metrics
    }

    pub(crate) async fn init_local_log_store_state(
        context: &SyncedKvLogStoreContext<S>,
        first_write_epoch: EpochPair,
    ) -> StreamExecutorResult<(LocalLogStoreReadState<S>, LocalLogStoreWriteState<S>)> {
        let local_state_store = context
            .state_store
            .new_local(NewLocalOptions {
                table_id: context.table_id,
                op_consistency_level: OpConsistencyLevel::Inconsistent,
                table_option: TableOption {
                    retention_seconds: None,
                },
                is_replicated: false,
                vnodes: context.serde.vnodes().clone(),
                upload_on_flush: false,
            })
            .await;

        let (read_state, mut initial_write_state) = new_log_store_state(
            context.table_id,
            local_state_store,
            context.serde.clone(),
            context.chunk_size,
        );
        initial_write_state.init(first_write_epoch).await?;
        Ok((read_state, initial_write_state))
    }

    pub(crate) async fn open_persisted_log_store_stream<R: StateStoreRead>(
        read_state: &LogStoreReadState<R>,
        metrics: &SyncedKvLogStoreMetrics,
        first_write_epoch: u64,
    ) -> StreamExecutorResult<RawPersistedStream<R>> {
        read_state
            .read_persisted_log_store(
                metrics.persistent_log_read_metrics.clone(),
                first_write_epoch,
                LogStoreReadStateStreamRangeStart::Unbounded,
            )
            .await
            .map_err(Into::into)
    }

    pub(crate) fn init_log_store_buffer(
        max_buffer_size: usize,
        chunk_size: usize,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> SyncedLogStoreBuffer {
        SyncedLogStoreBuffer {
            buffer: VecDeque::new(),
            current_size: 0,
            max_size: max_buffer_size,
            max_chunk_size: chunk_size,
            next_chunk_id: 0,
            metrics: metrics.clone(),
            flushed_count: 0,
        }
    }

    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub(crate) async fn aligned_message_stream(
        actor_id: ActorId,
        input: BoxedMessageStream,
        read_state: LocalLogStoreReadState<S>,
        mut initial_write_state: LocalLogStoreWriteState<S>,
        metrics: SyncedKvLogStoreMetrics,
        initial_write_epoch: EpochPair,
    ) {
        tracing::info!("aligned mode");
        // We want to realign the buffer and the stream.
        // We just block the upstream input stream,
        // and wait until the persisted logstore is empty.
        // Then after that we can consume the input stream.
        let log_store_stream =
            Self::open_persisted_log_store_stream(&read_state, &metrics, initial_write_epoch.curr)
                .await?;

        #[for_await]
        for message in log_store_stream {
            let (_epoch, message) = message?;
            match message {
                KvLogStoreItem::Barrier { .. } => {
                    continue;
                }
                KvLogStoreItem::StreamChunk { chunk, .. } => {
                    yield Message::Chunk(chunk);
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
                    progress.apply_aligned(read_state.vnodes().clone(), barrier.epoch.prev, None);
                    // Truncate the logstore.
                    let post_seal =
                        initial_write_state.seal_current_epoch(barrier.epoch.curr, progress.take());
                    let update_vnode_bitmap = barrier.as_update_vnode_bitmap(actor_id);
                    if update_vnode_bitmap.is_some() {
                        return Err(anyhow!(
                            "updating vnode bitmap in place is not supported any more!"
                        )
                        .into());
                    }
                    yield Message::Barrier(barrier);
                    post_seal.post_yield_barrier(None).await?;
                    if !realigned_logstore && is_checkpoint {
                        realigned_logstore = true;
                        tracing::info!("realigned logstore");
                    }
                }
                Message::Chunk(chunk) => {
                    yield Message::Chunk(chunk);
                }
                Message::Watermark(watermark) => {
                    yield Message::Watermark(watermark);
                }
            }
        }
    }
}

impl<S: StateStore> SyncedKvLogStoreExecutor<S> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub(crate) async fn execute_inner(self) {
        let Self {
            actor_context,
            upstream,
            inner,
        } = self;
        let context = inner.context;

        let mut input = upstream.execute();

        // init first epoch + local state store
        let first_barrier = expect_first_barrier(&mut input).await?;
        let first_write_epoch = first_barrier.epoch;
        yield Message::Barrier(first_barrier.clone());

        let (read_state, initial_write_state) =
            SyncedKvLogStoreExecutorInner::<S>::init_local_log_store_state(
                &context,
                first_write_epoch,
            )
            .await?;

        let mut pause_stream = first_barrier.is_pause_on_startup();
        let initial_write_epoch = first_write_epoch;

        if context.aligned {
            let aligned_stream = SyncedKvLogStoreExecutorInner::<S>::aligned_message_stream(
                actor_context.id,
                input,
                read_state,
                initial_write_state,
                context.metrics.clone(),
                initial_write_epoch,
            );
            #[for_await]
            for message in aligned_stream {
                yield message?;
            }
            return Ok(());
        }

        let mut seq_id = FIRST_SEQ_ID;
        let mut buffer = SyncedKvLogStoreExecutorInner::<S>::init_log_store_buffer(
            context.max_buffer_size,
            context.chunk_size,
            &context.metrics,
        );

        let log_store_stream = SyncedKvLogStoreExecutorInner::<S>::open_persisted_log_store_stream(
            &read_state,
            &context.metrics,
            initial_write_epoch.curr,
        )
        .await?;

        let mut log_store_stream = tokio_stream::StreamExt::peekable(log_store_stream);
        let mut clean_state = log_store_stream.peek().await.is_none();
        tracing::trace!(?clean_state);

        let mut read_future_state = ReadFuture::ReadingPersistedStream(log_store_stream);

        let mut write_future_state = WriteFuture::receive_from_upstream(input, initial_write_state);

        let mut progress = LogStoreVnodeProgress::None;

        loop {
            let select_result = {
                let read_future = async {
                    if pause_stream {
                        pending().await
                    } else {
                        read_future_state
                            .next_chunk(&mut progress, &read_state, &mut buffer, &context.metrics)
                            .await
                    }
                };
                pin_mut!(read_future);
                let write_future = write_future_state.next_event(&context.metrics);
                pin_mut!(write_future);
                let output = select(write_future, read_future).await;
                drop_either_future(output)
            };
            match select_result {
                Either::Left(result) => {
                    // drop the future to ensure that the future must be reset later
                    drop(write_future_state);
                    let (stream, mut write_state, either) = result?;
                    match either {
                        WriteFutureEvent::UpstreamMessageReceived(msg) => {
                            match msg {
                                Message::Barrier(barrier) => {
                                    if clean_state
                                        && barrier.kind.is_checkpoint()
                                        && !buffer.is_empty()
                                    {
                                        write_future_state = WriteFuture::paused(
                                            context.pause_duration_ms,
                                            barrier,
                                            stream,
                                            write_state,
                                        );
                                        clean_state = false;
                                        context.metrics.unclean_state.inc();
                                    } else {
                                        SyncedKvLogStoreExecutorInner::<S>::apply_pause_resume_mutation(
                                            &barrier,
                                            &mut pause_stream,
                                        );
                                        let write_state_post_write_barrier =
                                            SyncedKvLogStoreExecutorInner::<S>::write_barrier(
                                                actor_context.id,
                                                &mut write_state,
                                                barrier.clone(),
                                                &context.metrics,
                                                progress.take(),
                                                &mut buffer,
                                            )
                                            .await?;
                                        seq_id = FIRST_SEQ_ID;
                                        let update_vnode_bitmap =
                                            barrier.as_update_vnode_bitmap(actor_context.id);
                                        if update_vnode_bitmap.is_some() {
                                            return Err(anyhow!("vnode bitmap update during dispatch is not supported.").into());
                                        }
                                        yield Message::Barrier(barrier);

                                        write_state_post_write_barrier
                                            .post_yield_barrier(None)
                                            .await?;
                                        write_future_state =
                                            WriteFuture::receive_from_upstream(stream, write_state);
                                    }
                                }
                                Message::Chunk(chunk) => {
                                    let (new_seq_id, next_write_future) =
                                        SyncedKvLogStoreExecutorInner::<S>::process_upstream_chunk(
                                            seq_id,
                                            stream,
                                            write_state,
                                            chunk,
                                            &mut buffer,
                                        );
                                    seq_id = new_seq_id;
                                    write_future_state = next_write_future;
                                }
                                // FIXME(kwannoel): This should truncate the logstore,
                                // it will not bypass like barrier.
                                Message::Watermark(_watermark) => {
                                    write_future_state =
                                        WriteFuture::receive_from_upstream(stream, write_state);
                                }
                            }
                        }
                        WriteFutureEvent::ChunkFlushed(info) => {
                            write_future_state =
                                SyncedKvLogStoreExecutorInner::<S>::process_chunk_flushed(
                                    stream,
                                    write_state,
                                    info,
                                    &mut buffer,
                                    &context.metrics,
                                );
                        }
                    }
                }
                Either::Right(result) => {
                    if !clean_state
                        && matches!(read_future_state, ReadFuture::Idle)
                        && buffer.is_empty()
                    {
                        clean_state = true;
                        context.metrics.clean_state.inc();

                        // Let write future resume immediately
                        if let WriteFuture::Paused { sleep_future, .. } = &mut write_future_state {
                            tracing::trace!("resuming paused future");
                            assert!(buffer.current_size < context.max_buffer_size);
                            *sleep_future = None;
                        }
                    }
                    let chunk = result?;
                    context
                        .metrics
                        .total_read_count
                        .inc_by(chunk.cardinality() as _);

                    yield Message::Chunk(chunk);
                }
            }
        }
    }
}

impl<S: StateStore> SyncedKvLogStoreExecutorInner<S> {
    pub(crate) fn apply_pause_resume_mutation(barrier: &Barrier, pause_stream: &mut bool) {
        if let Some(mutation) = barrier.mutation.as_deref() {
            match mutation {
                Mutation::Pause => {
                    *pause_stream = true;
                }
                Mutation::Resume => {
                    *pause_stream = false;
                }
                _ => {}
            }
        }
    }

    pub(crate) fn process_upstream_chunk<LS: LocalStateStore>(
        seq_id: SeqId,
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<LS>,
        chunk: StreamChunk,
        buffer: &mut SyncedLogStoreBuffer,
    ) -> (SeqId, WriteFuture<LS>) {
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
        let next_write_future = if let Some(chunk_to_flush) =
            buffer.add_or_flush_chunk(start_seq_id, end_seq_id, chunk, epoch)
        {
            WriteFuture::flush_chunk(
                stream,
                write_state,
                chunk_to_flush,
                epoch,
                start_seq_id,
                end_seq_id,
            )
        } else {
            WriteFuture::receive_from_upstream(stream, write_state)
        };
        (new_seq_id, next_write_future)
    }

    pub(crate) fn process_chunk_flushed<LS: LocalStateStore>(
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<LS>,
        info: FlushedChunkInfo,
        buffer: &mut SyncedLogStoreBuffer,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> WriteFuture<LS> {
        buffer.add_flushed_item_to_buffer(
            info.start_seq_id,
            info.end_seq_id,
            info.vnode_bitmap,
            info.epoch,
        );
        metrics
            .storage_write_count
            .inc_by(info.flush_info.flush_count as _);
        metrics
            .storage_write_size
            .inc_by(info.flush_info.flush_size as _);
        WriteFuture::receive_from_upstream(stream, write_state)
    }

    pub(crate) async fn write_barrier<'a, LS: LocalStateStore>(
        actor_id: ActorId,
        write_state: &'a mut LogStoreWriteState<LS>,
        barrier: Barrier,
        metrics: &SyncedKvLogStoreMetrics,
        progress: LogStoreVnodeProgress,
        buffer: &mut SyncedLogStoreBuffer,
    ) -> StreamExecutorResult<LogStorePostSealCurrentEpoch<'a, LS>> {
        tracing::trace!(%actor_id, ?progress, "applying truncation");
        // TODO(kwannoel): As an optimization we can also change flushed chunks to be flushed items
        // to reduce memory consumption of logstore.

        let epoch = barrier.epoch.prev;
        let mut writer = write_state.start_writer(false);
        writer.write_barrier(epoch, barrier.is_checkpoint())?;

        if barrier.is_checkpoint() {
            for (epoch, item) in buffer.buffer.iter_mut().rev() {
                match item {
                    LogStoreBufferItem::StreamChunk {
                        chunk,
                        start_seq_id,
                        end_seq_id,
                        flushed,
                        ..
                    } => {
                        if !*flushed {
                            writer.write_chunk(chunk, *epoch, *start_seq_id, *end_seq_id)?;
                            *flushed = true;
                        } else {
                            break;
                        }
                    }
                    LogStoreBufferItem::Flushed { .. } | LogStoreBufferItem::Barrier { .. } => {}
                }
            }
        }

        // Apply truncation
        let (flush_info, _) = writer.finish().await?;
        metrics
            .storage_write_count
            .inc_by(flush_info.flush_count as _);
        metrics
            .storage_write_size
            .inc_by(flush_info.flush_size as _);
        let post_seal = write_state.seal_current_epoch(barrier.epoch.curr, progress);

        // Add to buffer
        buffer.buffer.push_back((
            epoch,
            LogStoreBufferItem::Barrier {
                is_checkpoint: barrier.is_checkpoint(),
                next_epoch: barrier.epoch.curr,
                schema_change: None,
                is_stop: false,
            },
        ));
        buffer.next_chunk_id = 0;
        buffer.update_unconsumed_buffer_metrics();

        Ok(post_seal)
    }
}

pub(crate) enum WriteFuture<S: LocalStateStore> {
    /// We trigger a brief pause to let the `ReadFuture` be polled in the following scenarios:
    /// - When seeing an upstream data chunk, when the buffer becomes full, and the state is clean.
    /// - When seeing a checkpoint barrier, when the buffer is not empty, and the state is clean.
    ///
    /// On pausing, we will transition to a dirty state.
    ///
    /// We trigger resume to let the `ReadFuture` to be polled in the following scenarios:
    /// - After the pause duration.
    /// - After the read future consumes a chunk.
    Paused {
        start_instant: Instant,
        sleep_future: Option<Pin<Box<Sleep>>>,
        barrier: Barrier,
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>, // Just used to hold the state
    },
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
    Empty,
}

pub(crate) enum WriteFutureEvent {
    UpstreamMessageReceived(Message),
    ChunkFlushed(FlushedChunkInfo),
}

impl<S: LocalStateStore> WriteFuture<S> {
    pub(crate) fn flush_chunk(
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

    pub(crate) fn receive_from_upstream(
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
    ) -> Self {
        Self::ReceiveFromUpstream {
            future: stream.into_future(),
            write_state,
        }
    }

    pub(crate) fn paused(
        duration: Duration,
        barrier: Barrier,
        stream: BoxedMessageStream,
        write_state: LogStoreWriteState<S>,
    ) -> Self {
        let now = Instant::now();
        tracing::trace!(?now, ?duration, "write_future_pause");
        Self::Paused {
            start_instant: now,
            sleep_future: Some(Box::pin(sleep_until(now + duration))),
            barrier,
            stream,
            write_state,
        }
    }

    pub(crate) async fn next_event(
        &mut self,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> StreamExecutorResult<(BoxedMessageStream, LogStoreWriteState<S>, WriteFutureEvent)> {
        match self {
            WriteFuture::Paused {
                start_instant,
                sleep_future,
                ..
            } => {
                if let Some(sleep_future) = sleep_future {
                    sleep_future.await;
                    metrics
                        .pause_duration_ns
                        .inc_by(start_instant.elapsed().as_nanos() as _);
                    tracing::trace!("resuming write future");
                }
                must_match!(replace(self, WriteFuture::Empty), WriteFuture::Paused { stream, write_state, barrier, .. } => {
                    Ok((stream, write_state, WriteFutureEvent::UpstreamMessageReceived(Message::Barrier(barrier))))
                })
            }
            WriteFuture::ReceiveFromUpstream { future, .. } => {
                let (opt, stream) = future.await;
                must_match!(replace(self, WriteFuture::Empty), WriteFuture::ReceiveFromUpstream { write_state, .. } => {
                    opt
                    .ok_or_else(|| anyhow!("end of upstream input").into())
                    .and_then(|result| result.map(|item| {
                        (stream, write_state, WriteFutureEvent::UpstreamMessageReceived(item))
                    }))
                })
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
            WriteFuture::Empty => {
                unreachable!("should not be polled after ready")
            }
        }
    }
}

pub(crate) type PersistedStream<S> =
    Peekable<Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>>;

pub(crate) enum ReadFuture<S: StateStoreRead> {
    ReadingPersistedStream(PersistedStream<S>),
    ReadingFlushedChunk {
        future: ReadFlushedChunkFuture,
        end_seq_id: SeqId,
    },
    Idle,
}

// Read methods
impl<S: StateStoreRead> ReadFuture<S> {
    pub(crate) async fn next_chunk(
        &mut self,
        progress: &mut LogStoreVnodeProgress,
        read_state: &LogStoreReadState<S>,
        buffer: &mut SyncedLogStoreBuffer,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> StreamExecutorResult<StreamChunk> {
        match self {
            ReadFuture::ReadingPersistedStream(stream) => {
                while let Some((epoch, item)) = stream.try_next().await? {
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
                            return Ok(chunk);
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
                    return pending().await;
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
                        return Ok(chunk);
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
        Ok(chunk)
    }
}
