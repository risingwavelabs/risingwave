// Copyright 2025 RisingWave Labs
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

//! This contains the synced kv log store implementation.
//!
//! The synced kv log store polls two futures:
//!
//! 1. Upstream: upstream message source
//!
//!    It will write stream messages to the log store buffer. e.g. `Message::Barrier`, `Message::Chunk`, ...
//!    When writing a stream chunk, if the log store buffer is full, it will:
//!      a. Flush the buffer to the log store.
//!      b. Convert the stream chunk into a reference (`LogStoreBufferItem::Flushed`)
//!         which can read the corresponding chunks in the log store.
//!         We will compact adjacent references,
//!         so it can read multiple chunks if there's a build up.
//!
//!    On receiving barriers, it will:
//!      a. Apply truncation to historical data in the logstore.
//!      b. Flush and checkpoint the logstore data.
//!
//! 2. State store + buffer + recently flushed chunks: the storage components of the logstore.
//!
//!    It will read all historical data from the logstore first. This can be done just by
//!    constructing a state store stream, which will read all data until the latest epoch.
//!    This is a static snapshot of data.
//!    For any subsequently flushed chunks, we will read them via
//!    `flushed_chunk_future`. See the next paragraph below.
//!
//!    We will next read `flushed_chunk_future` (if there's one pre-existing one), see below for how
//!    it's constructed, what it is.
//!
//!    Finally we will pop the earliest item in the buffer.
//!    - If it's a chunk yield it.
//!    - If it's a watermark yield it.
//!    - If it's a flushed chunk reference (`LogStoreBufferItem::Flushed`),
//!      we will read the corresponding chunks in the log store.
//!      This is done by constructing a `flushed_chunk_future` which will read the log store
//!      using the `seq_id`.
//!   - Barrier,
//!     because they are directly propagated from the upstream when polling it.
//!
//! TODO(kwannoel):
//! - [] Add metrics
//! - [] Add tests
//! - [] Handle watermark r/w
//! - [] Handle paused stream

use std::collections::VecDeque;
use std::pin::Pin;

use await_tree::InstrumentAwait;
use futures::future::BoxFuture;
use futures::FutureExt;
use futures_async_stream::try_stream;
use itertools::Itertools;
use parking_lot::Mutex;
use risingwave_common::array::StreamChunk;
use risingwave_common::bitmap::{Bitmap, BitmapBuilder};
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VnodeBitmapExt;
use risingwave_common_estimate_size::EstimateSize;
use risingwave_connector::sink::log_store::{ChunkId, LogStoreResult};
use risingwave_hummock_sdk::table_watermark::{VnodeWatermark, WatermarkDirection};
use risingwave_storage::store::{LocalStateStore, SealCurrentEpochOptions, StateStoreRead};
use risingwave_storage::StateStore;
use tokio::select;
use tokio_stream::StreamExt;

use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::reader::read_flushed_chunk;
use crate::common::log_store_impl::kv_log_store::reader::timeout_auto_rebuild::TimeoutAutoRebuildIter;
use crate::common::log_store_impl::kv_log_store::serde::{
    KvLogStoreItem, LogStoreItemMergeStream, LogStoreRowSerde,
};
use crate::common::log_store_impl::kv_log_store::{
    FlushInfo, KvLogStoreMetrics, KvLogStoreReadMetrics, ReaderTruncationOffsetType, SeqIdType,
    FIRST_SEQ_ID,
};
use crate::executor::{
    Barrier, BoxedMessageStream, Message, StreamExecutorError, StreamExecutorResult,
};

type StateStoreStream<S> = Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>;
type ReadFlushedChunkFuture = BoxFuture<'static, LogStoreResult<(ChunkId, StreamChunk, u64)>>;

struct SyncedKvLogStore<S: StateStore, LS: LocalStateStore> {
    table_id: TableId,
    read_metrics: KvLogStoreReadMetrics,
    metrics: KvLogStoreMetrics,
    serde: LogStoreRowSerde,
    seq_id: SeqIdType,
    truncation_offset: Option<ReaderTruncationOffsetType>,

    // Upstream
    upstream: BoxedMessageStream,

    // Log store state
    state_store_stream: Option<StateStoreStream<S>>,
    flushed_chunk_future: Option<ReadFlushedChunkFuture>,
    state_store: S,
    local_state_store: LS,
    buffer: Mutex<SyncedLogStoreBuffer>,
}
// Stream interface
impl<S: StateStore, LS: LocalStateStore> SyncedKvLogStore<S, LS> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_id: TableId,
        read_metrics: KvLogStoreReadMetrics,
        metrics: KvLogStoreMetrics,
        serde: LogStoreRowSerde,
        seq_id: SeqIdType,
        state_store: S,
        local_state_store: LS,
        buffer_max_size: usize,
        upstream: BoxedMessageStream,
    ) -> Self {
        Self {
            table_id,
            read_metrics,
            metrics: metrics.clone(),
            serde,
            seq_id,
            truncation_offset: None,
            state_store_stream: None,
            flushed_chunk_future: None,
            state_store,
            local_state_store,
            buffer: Mutex::new(SyncedLogStoreBuffer {
                buffer: VecDeque::new(),
                max_size: buffer_max_size,
                next_chunk_id: 0,
                metrics,
            }),
            upstream,
        }
    }
}

// Stream interface
impl<S: StateStore, LS: LocalStateStore> SyncedKvLogStore<S, LS> {
    #[try_stream(ok = Message, error = StreamExecutorError)]
    pub async fn into_stream(mut self) {
        loop {
            if let Some(msg) = self.next().await? {
                yield msg;
            }
        }
    }

    async fn next(&mut self) -> StreamExecutorResult<Option<Message>> {
        select! {
            // read from log store
            logstore_item = Self::try_next_item(
                self.table_id,
                &self.read_metrics,
                &self.serde,
                &mut self.truncation_offset,
                &mut self.state_store_stream,
                &mut self.flushed_chunk_future,
                self.state_store.clone(),
                &mut self.buffer
            ) => {
                let logstore_item = logstore_item?;
                Ok(logstore_item.map(Message::Chunk))
            }

            // poll from upstream
            upstream_item = self.upstream.next() => {
                match upstream_item {
                    None => Ok(None),
                    Some(upstream_item) => {
                        match upstream_item? {
                            Message::Barrier(barrier) => {
                                Self::write_barrier(
                                    &mut self.local_state_store,
                                    &self.serde,
                                    barrier.clone(),
                                    &mut self.metrics,
                                    self.truncation_offset,
                                    &mut self.seq_id,
                                    &mut self.buffer,
                                ).await?;
                                Ok(Some(Message::Barrier(barrier)))
                            }
                            Message::Chunk(chunk) => {
                                let start_seq_id = self.seq_id;
                                self.seq_id += chunk.cardinality() as SeqIdType;
                                let end_seq_id = self.seq_id - 1;
                                Self::write_chunk(
                                    &self.metrics,
                                    &self.serde,
                                    start_seq_id,
                                    end_seq_id,
                                    &mut self.buffer,
                                    chunk,
                                    &mut self.local_state_store,
                                ).await?;
                                Ok(None)
                            }
                            // TODO(kwannoel): This should be written to the logstore,
                            // it will not bypass like barrier.
                            Message::Watermark(_watermark) => Ok(None),
                        }
                    }
                }
            }
        }
    }
}

// Read methods
impl<S: StateStore, LS: LocalStateStore> SyncedKvLogStore<S, LS> {
    #[allow(clippy::too_many_arguments)]
    async fn try_next_item(
        table_id: TableId,
        read_metrics: &KvLogStoreReadMetrics,
        serde: &LogStoreRowSerde,
        truncation_offset: &mut Option<ReaderTruncationOffsetType>,

        // state
        log_store_state: &mut Option<StateStoreStream<S>>,
        read_flushed_chunk_future: &mut Option<ReadFlushedChunkFuture>,
        state_store: S,
        buffer: &mut Mutex<SyncedLogStoreBuffer>,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        // 1. read state store
        if let Some(chunk) = Self::try_next_state_store_item(log_store_state).await? {
            return Ok(Some(chunk));
        }

        // 2. read existing flushed chunk future
        if let Some(chunk) = Self::try_next_flushed_chunk_future(read_flushed_chunk_future).await? {
            return Ok(Some(chunk));
        }

        // 3. read buffer
        if let Some(chunk) = Self::try_next_buffer_item(
            truncation_offset,
            read_flushed_chunk_future,
            serde,
            state_store,
            buffer,
            table_id,
            read_metrics,
        )
        .await?
        {
            return Ok(Some(chunk));
        }
        Ok(None)
    }

    async fn try_next_state_store_item(
        state_store_stream_opt: &mut Option<StateStoreStream<S>>,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        if let Some(state_store_stream) = state_store_stream_opt {
            match state_store_stream
                .try_next()
                .instrument_await("try_next item")
                .await?
            {
                Some((_epoch, item)) => match item {
                    KvLogStoreItem::StreamChunk(chunk) => Ok(Some(chunk)),
                    KvLogStoreItem::Barrier { .. } => Ok(None),
                },
                None => {
                    *state_store_stream_opt = None;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    async fn try_next_flushed_chunk_future(
        flushed_chunk_future: &mut Option<ReadFlushedChunkFuture>,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        if let Some(future) = flushed_chunk_future {
            match future.await {
                Ok((_, chunk, _)) => {
                    *flushed_chunk_future = None;
                    Ok(Some(chunk))
                }
                Err(_) => {
                    // TODO: log + propagate error
                    *flushed_chunk_future = None;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    async fn try_next_buffer_item(
        truncation_offset: &mut Option<ReaderTruncationOffsetType>,
        read_flushed_chunk_future: &mut Option<ReadFlushedChunkFuture>,
        serde: &LogStoreRowSerde,
        state_store: impl StateStoreRead,
        buffer: &mut Mutex<SyncedLogStoreBuffer>,
        table_id: TableId,
        read_metrics: &KvLogStoreReadMetrics,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        let Some((item_epoch, item)) = buffer.lock().pop_front() else {
            return Ok(None);
        };
        match item {
            LogStoreBufferItem::StreamChunk {
                chunk, end_seq_id, ..
            } => {
                truncation_offset.replace((item_epoch, Some(end_seq_id)));
                Ok(Some(chunk))
            }
            LogStoreBufferItem::Flushed {
                vnode_bitmap,
                start_seq_id,
                end_seq_id,
                chunk_id,
            } => {
                truncation_offset.replace((item_epoch, Some(end_seq_id)));
                let serde = serde.clone();
                let read_metrics = read_metrics.clone();
                let read_flushed_chunk_fut = read_flushed_chunk(
                    serde,
                    state_store,
                    vnode_bitmap,
                    chunk_id,
                    start_seq_id,
                    end_seq_id,
                    item_epoch,
                    table_id,
                    read_metrics,
                )
                .boxed();
                *read_flushed_chunk_future = Some(read_flushed_chunk_fut);
                Self::try_next_flushed_chunk_future(read_flushed_chunk_future).await
            }
            LogStoreBufferItem::Barrier { next_epoch, .. } => {
                // FIXME(kwannoel): Is `next_epoch` correct for truncation??
                truncation_offset.replace((next_epoch, None));
                Ok(None)
            }
            LogStoreBufferItem::UpdateVnodes(_) => Ok(None),
        }
    }
}

// Write methods
impl<S: StateStore, LS: LocalStateStore> SyncedKvLogStore<S, LS> {
    async fn write_barrier(
        state_store: &mut LS,
        serde: &LogStoreRowSerde,
        barrier: Barrier,
        metrics: &mut KvLogStoreMetrics,
        truncation_offset: Option<ReaderTruncationOffsetType>,
        seq_id: &mut SeqIdType,
        buffer: &mut Mutex<SyncedLogStoreBuffer>,
    ) -> StreamExecutorResult<()> {
        let epoch = state_store.epoch();
        let mut flush_info = FlushInfo::new();

        // FIXME(kwannoel): Handle paused stream.
        for vnode in serde.vnodes().iter_vnodes() {
            let (key, value) = serde.serialize_barrier(epoch, vnode, barrier.is_checkpoint());
            flush_info.flush_one(key.estimated_size() + value.estimated_size());
            state_store.insert(key, value, None)?;
        }

        // FIXME(kwannoel): Flush all unflushed chunks
        // As an optimization we can also change it into flushed items instead.
        // This will reduce memory consumption of logstore.

        flush_info.report(metrics);

        // Apply truncation
        let watermark = truncation_offset.map(|truncation_offset| {
            VnodeWatermark::new(
                serde.vnodes().clone(),
                serde.serialize_truncation_offset_watermark(truncation_offset),
            )
        });
        state_store.flush().await?;
        let watermark = watermark.into_iter().collect_vec();
        state_store.seal_current_epoch(
            barrier.epoch.curr,
            SealCurrentEpochOptions {
                table_watermarks: Some((WatermarkDirection::Ascending, watermark)),
                switch_op_consistency_level: None,
            },
        );

        // Add to buffer
        let mut buffer = buffer.lock();
        buffer.buffer.push_back((
            epoch,
            LogStoreBufferItem::Barrier {
                is_checkpoint: barrier.is_checkpoint(),
                next_epoch: barrier.epoch.curr,
            },
        ));
        buffer.next_chunk_id = 0;
        buffer.update_unconsumed_buffer_metrics();

        *seq_id = FIRST_SEQ_ID;
        Ok(())
    }

    async fn write_chunk(
        metrics: &KvLogStoreMetrics,
        serde: &LogStoreRowSerde,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        buffer: &mut Mutex<SyncedLogStoreBuffer>,
        chunk: StreamChunk,
        state_store: &mut LS,
    ) -> StreamExecutorResult<()> {
        let chunk_to_flush = {
            let mut buffer = buffer.lock();
            buffer.add_or_flush_chunk(start_seq_id, end_seq_id, chunk, state_store)
        };
        match chunk_to_flush {
            None => {}
            Some(chunk_to_flush) => {
                let new_vnode_bitmap = flush_chunk(
                    metrics,
                    serde,
                    start_seq_id,
                    end_seq_id,
                    state_store,
                    chunk_to_flush,
                )
                .await?;
                {
                    let mut buffer = buffer.lock();
                    buffer.add_flushed_item_to_buffer(
                        start_seq_id,
                        end_seq_id,
                        new_vnode_bitmap,
                        state_store.epoch(),
                    );
                }
            }
        }
        Ok(())
    }
}

struct SyncedLogStoreBuffer {
    buffer: VecDeque<(u64, LogStoreBufferItem)>,
    max_size: usize,
    next_chunk_id: ChunkId,
    metrics: KvLogStoreMetrics,
}

async fn flush_chunk(
    metrics: &KvLogStoreMetrics,
    serde: &LogStoreRowSerde,
    start_seq_id: SeqIdType,
    end_seq_id: SeqIdType,
    state_store: &mut impl LocalStateStore,
    chunk: StreamChunk,
) -> StreamExecutorResult<Bitmap> {
    let epoch = state_store.epoch();
    let mut vnode_bitmap_builder = BitmapBuilder::zeroed(serde.vnodes().len());
    let mut flush_info = FlushInfo::new();
    for (i, (op, row)) in chunk.rows().enumerate() {
        let seq_id = start_seq_id + (i as SeqIdType);
        assert!(seq_id <= end_seq_id);
        let (vnode, key, value) = serde.serialize_data_row(epoch, seq_id, op, row);
        vnode_bitmap_builder.set(vnode.to_index(), true);
        flush_info.flush_one(key.estimated_size() + value.estimated_size());
        state_store.insert(key, value, None)?;
    }
    flush_info.report(metrics);
    state_store.flush().await?;

    Ok(vnode_bitmap_builder.finish())
}

impl SyncedLogStoreBuffer {
    fn add_or_flush_chunk(
        &mut self,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        chunk: StreamChunk,
        state_store: &mut impl LocalStateStore,
    ) -> Option<StreamChunk> {
        let current_size = self.buffer.len();
        let chunk_size = chunk.cardinality();
        let epoch = state_store.epoch();

        let should_flush_chunk = current_size + chunk_size >= self.max_size;
        if should_flush_chunk {
            Some(chunk)
        } else {
            self.add_chunk_to_buffer(chunk, start_seq_id, end_seq_id, epoch);
            None
        }
    }

    /// After flushing a chunk, we will preserve a `FlushedItem` inside the buffer.
    /// This doesn't contain any data, but it contains the metadata to read the flushed chunk.
    fn add_flushed_item_to_buffer(
        &mut self,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        new_vnode_bitmap: Bitmap,
        epoch: u64,
    ) {
        if let Some((
            item_epoch,
            LogStoreBufferItem::Flushed {
                end_seq_id: prev_end_seq_id,
                vnode_bitmap,
                ..
            },
        )) = self.buffer.front_mut()
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
        }
        // FIXME(kwannoel): Seems these metrics are updated _after_ the flush info is reported.
        self.update_unconsumed_buffer_metrics();
    }

    fn add_chunk_to_buffer(
        &mut self,
        chunk: StreamChunk,
        start_seq_id: SeqIdType,
        end_seq_id: SeqIdType,
        epoch: u64,
    ) {
        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;
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

    fn is_full(&self) -> bool {
        self.buffer.len() >= self.max_size
    }

    fn pop_front(&mut self) -> Option<(u64, LogStoreBufferItem)> {
        self.buffer.pop_front()
    }

    fn update_unconsumed_buffer_metrics(&self) {
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
                LogStoreBufferItem::UpdateVnodes(_) => {}
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