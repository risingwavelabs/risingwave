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
//!    It will write stream messages to the log store buffer. e.g. Message::Barrier, Message::Chunk, ...
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
//!    - If it's a flushed chunk reference (LogStoreBufferItem::Flushed),
//!      we will read the corresponding chunks in the log store.
//!      This is done by constructing a `flushed_chunk_future` which will read the log store
//!      using the `seq_id`.
//!
//! TODO(kwannoel):
//! - [] Add metrics
//! - [] Add tests

use std::pin::Pin;

use await_tree::InstrumentAwait;
use futures::future::BoxFuture;
use futures_async_stream::try_stream;
use parking_lot::Mutex;
use risingwave_common::array::StreamChunk;
use risingwave_connector::sink::log_store::{ChunkId, LogStoreReadItem, LogStoreResult, TruncateOffset};
use risingwave_storage::StateStore;
use tokio::select;
use tokio_stream::StreamExt;

use super::reader::timeout_auto_rebuild::TimeoutAutoRebuildIter;
use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::serde::{KvLogStoreItem, LogStoreItemMergeStream};
use crate::executor::{
    Barrier, BoxedMessageStream, Message, StreamExecutorError, StreamExecutorResult,
};

type StateStoreStream<S> = Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>;
type ReadFlushedChunkFuture = BoxFuture<'static, LogStoreResult<(ChunkId, StreamChunk, u64)>>;

struct LogStoreState<S: StateStore> {
    state_store_stream: Option<StateStoreStream<S>>,
    flushed_chunk_future: Option<ReadFlushedChunkFuture>,
    buffer: Mutex<SyncedLogStoreBuffer>,
}

struct SyncedKvLogStore<S: StateStore> {
    state: LogStoreState<S>,
    upstream: BoxedMessageStream,
}

// Top-level interface:
// - constructor
// - stream interface
impl<S: StateStore> SyncedKvLogStore<S> {
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
            logstore_item = Self::try_next_item(&mut self.state_store_stream, &mut self.buffer) => {
                let logstore_item = logstore_item?;
                Ok(logstore_item.map(Message::Chunk))
            }
            // read from upstream
            upstream_item = self.upstream.next() => {
                match upstream_item {
                    None => Ok(None),
                    Some(upstream_item) => {
                        match upstream_item? {
                            Message::Barrier(barrier) => {
                                self.write_barrier(barrier.clone()).await?;
                                Ok(Some(Message::Barrier(barrier)))
                            }
                            Message::Chunk(chunk) => {
                                self.write_chunk(chunk).await?;
                                Ok(None)
                            }
                            _ => Ok(None),
                        }
                    }
                }
            }
        }
    }
}

// Poll upstream
impl<S: StateStore> SyncedKvLogStore<S> {
    async fn poll_upstream(
        upstream: &mut BoxedMessageStream,
    ) -> StreamExecutorResult<Option<Barrier>> {
        todo!()
    }
}

// Read methods
impl<S: StateStore> SyncedKvLogStore<S> {
    async fn try_next_item(
        state_store_stream: &mut Option<StateStoreStream<S>>,
        buffer: &mut Mutex<SyncedLogStoreBuffer>,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        // First try to read from the state store stream
        if let Some(chunk) = Self::try_next_state_store_item(state_store_stream).await? {
            return Ok(Some(chunk));
        }
        // Then try to read from the buffer
        if let Some(chunk) = Self::try_next_buffer_item(buffer).await? {
            return Ok(Some(chunk));
        }
        return Ok(None);
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
                Some((epoch, item)) => match item {
                    KvLogStoreItem::StreamChunk(chunk) => Ok(Some(chunk)),
                    KvLogStoreItem::Barrier { is_checkpoint } => Ok(None),
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

    async fn try_next_buffer_item(
        buffer: &mut Mutex<SyncedLogStoreBuffer>,
    ) -> StreamExecutorResult<Option<StreamChunk>> {
        if let Some(LogStoreBufferItem::StreamChunk { chunk, .. }) = buffer.lock().pop() {
            Ok(Some(chunk))
        } else {
            Ok(None)
        }
    }
}

// Write methods
impl<S: StateStore> SyncedKvLogStore<S> {
    async fn write_barrier(&mut self, barrier: Barrier) -> StreamExecutorResult<()> {
        // Write a barrier to the state store
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()> {
        if self.buffer.lock().is_full() {
            self.flush_buffer().await?;
        }
        // Try to write to the buffer
        // If it is full, flush everything in the buffer to the state store.
        Ok(())
    }

    async fn flush_buffer(&mut self) -> StreamExecutorResult<()> {
        // Flush everything in the buffer to the state store.
        Ok(())
    }
}

struct SyncedLogStoreBuffer {
    buffer: Vec<LogStoreBufferItem>,
    max_size: usize,
}

impl SyncedLogStoreBuffer {
    pub fn is_full(&self) -> bool {
        self.buffer.len() >= self.max_size
    }

    pub fn pop(&mut self) -> Option<LogStoreBufferItem> {
        self.buffer.pop()
    }
}
