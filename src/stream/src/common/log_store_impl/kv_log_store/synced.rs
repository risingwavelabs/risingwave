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
use risingwave_common::array::StreamChunk;
use risingwave_connector::sink::log_store::{LogStoreReadItem, TruncateOffset};
use risingwave_storage::StateStore;
use tokio::select;
use tokio_stream::StreamExt;

use super::reader::timeout_auto_rebuild::TimeoutAutoRebuildIter;
use crate::common::log_store_impl::kv_log_store::buffer::LogStoreBufferItem;
use crate::common::log_store_impl::kv_log_store::serde::{KvLogStoreItem, LogStoreItemMergeStream};
use crate::executor::{BoxedMessageStream, Message, StreamExecutorError, StreamExecutorResult};

struct SyncedKvLogStore<S: StateStore> {
    state_store_stream: Option<Pin<Box<LogStoreItemMergeStream<TimeoutAutoRebuildIter<S>>>>>,
    buffer: SyncedLogStoreBuffer,
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
            // Read from upstream
            // Block until write succeeds
            c = self.poll_upstream() => Ok(Some(c?)),
        }
    }
}

// Poll upstream
impl<S: StateStore> SyncedKvLogStore<S> {
    async fn poll_upstream(&mut self) -> StreamExecutorResult<Message> {
        todo!()
    }
}

// Read methods
impl<S: StateStore> SyncedKvLogStore<S> {
    async fn try_next_item(&mut self) -> StreamExecutorResult<Option<StreamChunk>> {
        // First try to read from the state store stream
        if let Some(chunk) = self.try_next_state_store_item().await? {
            return Ok(Some(chunk));
        }
        // Then try to read from the buffer
        if let Some(chunk) = self.try_next_buffer_item().await? {
            return Ok(Some(chunk));
        }
        return Ok(None);
    }

    async fn try_next_state_store_item(&mut self) -> StreamExecutorResult<Option<StreamChunk>> {
        if let Some(state_store_stream) = &mut self.state_store_stream {
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
                    self.state_store_stream = None;
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    async fn try_next_buffer_item(&mut self) -> StreamExecutorResult<Option<StreamChunk>> {
        if let Some(LogStoreBufferItem::StreamChunk { chunk, .. }) = self.buffer.pop() {
            Ok(Some(chunk))
        } else {
            Ok(None)
        }
    }
}

// Write methods
impl<S: StateStore> SyncedKvLogStore<S> {
    async fn write_barrier(&mut self, is_checkpoint: bool) -> StreamExecutorResult<()> {
        // Write a barrier to the state store
        Ok(())
    }

    async fn write_chunk(&mut self, chunk: StreamChunk) -> StreamExecutorResult<()> {
        if self.buffer.is_full() {
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
