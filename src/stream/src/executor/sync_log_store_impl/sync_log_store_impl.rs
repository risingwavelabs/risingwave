use std::collections::VecDeque;
use std::future::pending;
use std::mem::replace;
use std::pin::Pin;

use anyhow::anyhow;
use futures::future::BoxFuture;
use futures::stream::StreamFuture;
use futures::{FutureExt, StreamExt, TryStreamExt};
#[allow(unused_imports)]
use futures_async_stream::{for_await, try_stream};
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::Bitmap;
use risingwave_common::catalog::{TableId, TableOption};
use risingwave_common::id::FragmentId;
use risingwave_common::must_match;
use risingwave_common::util::epoch::EpochPair;
use risingwave_connector::sink::log_store::{ChunkId, LogStoreResult};
use risingwave_storage::StateStore;
use risingwave_storage::store::{
    LocalStateStore, NewLocalOptions, OpConsistencyLevel, StateStoreRead,
};
use risingwave_storage::store::timeout_auto_rebuild::TimeoutAutoRebuildIter;
use tokio::time::{Duration, Instant, Sleep, sleep_until};
use tokio_stream::adapters::Peekable;

use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::reader::LogStoreReadStateStreamRangeStart;
use crate::common::log_store_impl::kv_log_store::serde::{
    KvLogStoreItem, LogStoreItemMergeStream, LogStoreRowSerde,
};
use crate::common::log_store_impl::kv_log_store::state::{
    LogStorePostSealCurrentEpoch, LogStoreReadState, LogStoreStateWriteChunkFuture,
    LogStoreWriteState, new_log_store_state,
};
use crate::common::log_store_impl::kv_log_store::{
    Epoch, FlushInfo, LogStoreVnodeProgress, SeqId,
};
use crate::executor::sync_kv_log_store::metrics::SyncedKvLogStoreMetrics;
use crate::executor::{
    Barrier, BoxedMessageStream, Message, Mutation, StreamExecutorError, StreamExecutorResult,
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

pub(crate) struct SyncKvLogStoreContext<S: StateStore> {
    pub(crate) table_id: TableId,
    pub(crate) fragment_id: FragmentId,
    pub(crate) metrics: SyncedKvLogStoreMetrics,
    pub(crate) serde: LogStoreRowSerde,
    pub(crate) state_store: S,
    pub(crate) max_buffer_size: usize,
    pub(crate) chunk_size: usize,
    pub(crate) pause_duration_ms: Duration,
    pub(crate) aligned: bool,
}

pub(crate) struct SyncedLogStoreBuffer {
    buffer: VecDeque<(u64, LogStoreBufferItem)>,
    current_size: usize,
    max_size: usize,
    max_chunk_size: usize,
    next_chunk_id: ChunkId,
    metrics: SyncedKvLogStoreMetrics,
    flushed_count: usize,
}

impl SyncedLogStoreBuffer {
    pub(crate) fn new(
        max_buffer_size: usize,
        chunk_size: usize,
        metrics: &SyncedKvLogStoreMetrics,
    ) -> Self {
        Self {
            buffer: VecDeque::new(),
            current_size: 0,
            max_size: max_buffer_size,
            max_chunk_size: chunk_size,
            next_chunk_id: 0,
            metrics: metrics.clone(),
            flushed_count: 0,
        }
    }

    pub(crate) fn has_available_capacity(&self) -> bool {
        self.current_size < self.max_size
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.current_size == 0
    }

    pub(crate) fn add_or_flush_chunk(
        &mut self,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        chunk: StreamChunk,
        epoch: u64,
    ) -> Option<StreamChunk> {
        let current_size = self.current_size;
        let chunk_size = chunk.cardinality();

        tracing::trace!(
            current_size,
            chunk_size,
            max_size = self.max_size,
            "checking chunk size"
        );
        let should_flush_chunk = current_size + chunk_size > self.max_size;
        if should_flush_chunk {
            tracing::trace!(start_seq_id, end_seq_id, epoch, "flushing chunk",);
            Some(chunk)
        } else {
            tracing::trace!(start_seq_id, end_seq_id, epoch, "buffering chunk",);
            self.add_chunk_to_buffer(chunk, start_seq_id, end_seq_id, epoch);
            None
        }
    }

    /// After flushing a chunk, we will preserve a `FlushedItem` inside the buffer.
    /// This doesn't contain any data, but it contains the metadata to read the flushed chunk.
    pub(crate) fn add_flushed_item_to_buffer(
        &mut self,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        new_vnode_bitmap: Bitmap,
        epoch: u64,
    ) {
        let new_chunk_size = (end_seq_id - start_seq_id + 1) as usize;

        if let Some((
            item_epoch,
            LogStoreBufferItem::Flushed {
                start_seq_id: prev_start_seq_id,
                end_seq_id: prev_end_seq_id,
                vnode_bitmap,
                ..
            },
        )) = self.buffer.back_mut()
            && let flushed_chunk_size = (*prev_end_seq_id - *prev_start_seq_id + 1) as usize
            && let projected_flushed_chunk_size = flushed_chunk_size + new_chunk_size
            && projected_flushed_chunk_size <= self.max_chunk_size
        {
            assert!(
                *prev_end_seq_id < start_seq_id,
                "prev end_seq_id {} should be smaller than current start_seq_id {}",
                end_seq_id,
                start_seq_id
            );
            assert_eq!(
                epoch, *item_epoch,
                "epoch of newly added flushed item must be the same as the last flushed item"
            );
            *prev_end_seq_id = end_seq_id;
            *vnode_bitmap |= new_vnode_bitmap;
        } else {
            let chunk_id = self.next_chunk_id;
            self.next_chunk_id += 1;
            self.buffer.push_back((
                epoch,
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    vnode_bitmap: new_vnode_bitmap,
                    chunk_id,
                },
            ));
            self.flushed_count += 1;
            tracing::trace!(
                "adding flushed item to buffer: start_seq_id: {start_seq_id}, end_seq_id: {end_seq_id}, chunk_id: {chunk_id}"
            );
        }
        // FIXME(kwannoel): Seems these metrics are updated _after_ the flush info is reported.
        self.update_unconsumed_buffer_metrics();
    }

    fn add_chunk_to_buffer(
        &mut self,
        chunk: StreamChunk,
        start_seq_id: SeqId,
        end_seq_id: SeqId,
        epoch: u64,
    ) {
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;
        self.current_size += chunk.cardinality();
        self.buffer.push_back((
            epoch,
            LogStoreBufferItem::StreamChunk {
                chunk,
                start_seq_id,
                end_seq_id,
                flushed: false,
                chunk_id,
            },
        ));
        self.update_unconsumed_buffer_metrics();
    }

    pub(crate) fn pop_front(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        let item = self.buffer.pop_front();
        match &item {
            Some((_, LogStoreBufferItem::Flushed { .. })) => {
                self.flushed_count -= 1;
            }
            Some((_, LogStoreBufferItem::StreamChunk { chunk, .. })) => {
                self.current_size -= chunk.cardinality();
            }
            _ => {}
        }
        self.update_unconsumed_buffer_metrics();
        item
    }

    pub(crate) fn update_unconsumed_buffer_metrics(&self) {
        let mut epoch_count = 0;
        let mut row_count = 0;
        for (_, item) in &self.buffer {
            match item {
                LogStoreBufferItem::StreamChunk { chunk, .. } => {
                    row_count += chunk.cardinality();
                }
                LogStoreBufferItem::Flushed {
                    start_seq_id,
                    end_seq_id,
                    ..
                } => {
                    row_count += (end_seq_id - start_seq_id) as usize;
                }
                LogStoreBufferItem::Barrier { .. } => {
                    epoch_count += 1;
                }
            }
        }
        self.metrics.buffer_unconsumed_epoch_count.set(epoch_count);
        self.metrics.buffer_unconsumed_row_count.set(row_count as _);
        self.metrics
            .buffer_unconsumed_item_count
            .set(self.buffer.len() as _);
        self.metrics.buffer_unconsumed_min_epoch.set(
            self.buffer
                .front()
                .map(|(epoch, _)| *epoch)
                .unwrap_or_default() as _,
        );
    }
}

pub(crate) type LocalLogStoreReadState<S> =
    LogStoreReadState<<<S as StateStore>::Local as LocalStateStore>::FlushedSnapshotReader>;
pub(crate) type LocalLogStoreWriteState<S> = LogStoreWriteState<<S as StateStore>::Local>;

pub(crate) async fn init_local_log_store_state<S: StateStore>(
    context: &SyncKvLogStoreContext<S>,
    first_write_epoch: EpochPair,
) -> StreamExecutorResult<(LocalLogStoreReadState<S>, LocalLogStoreWriteState<S>)> {
    let local_state_store = context
        .state_store
        .new_local(NewLocalOptions {
            table_id: context.table_id,
            fragment_id: context.fragment_id,
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

#[try_stream(ok = Message, error = StreamExecutorError)]
pub(crate) async fn aligned_message_stream<S: StateStore>(
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
    let log_store_stream = read_state
        .read_persisted_log_store(
            metrics.persistent_log_read_metrics.clone(),
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
                    return Err(anyhow!("updating vnode bitmap in place is not supported any more!")
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
    let cardinality = chunk.cardinality();
    if cardinality == 0 {
        tracing::warn!(
            epoch = write_state.epoch().curr,
            "received empty chunk (cardinality=0), skipping"
        );
        return (
            seq_id,
            WriteFuture::receive_from_upstream(stream, write_state),
        );
    }

    let start_seq_id = seq_id;
    let new_seq_id = seq_id + cardinality as SeqId;
    let end_seq_id = new_seq_id - 1;
    let epoch = write_state.epoch().curr;
    tracing::trace!(
        start_seq_id,
        end_seq_id,
        new_seq_id,
        epoch,
        cardinality,
        "received chunk"
    );
    let next_write_future =
        if let Some(chunk_to_flush) = buffer.add_or_flush_chunk(start_seq_id, end_seq_id, chunk, epoch)
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
