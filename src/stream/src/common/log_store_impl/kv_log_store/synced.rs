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

use std::pin::Pin;

use await_tree::InstrumentAwait;
use futures_async_stream::try_stream;
use parking_lot::Mutex;
use risingwave_common::array::StreamChunk;
use risingwave_connector::sink::log_store::{LogStoreReadItem, TruncateOffset};
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

struct SyncedKvLogStore<S: StateStore> {
    state_store_stream: Option<StateStoreStream<S>>,
    buffer: Mutex<SyncedLogStoreBuffer>,
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
