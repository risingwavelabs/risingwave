// Copyright 2023 RisingWave Labs
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use fail::fail_point;
use risingwave_common::cache::CachePriority;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{MonitoredStreamingReader, ObjectError};

use super::{Block, BlockCache, BlockMeta};
use crate::hummock::{BlockHolder, HummockResult};

#[async_trait::async_trait]
pub trait BlockStream: Send + Sync + 'static {
    /// Reads the next block from the stream and returns it. Returns `None` if there are no blocks
    /// left to read.
    async fn next_block(&mut self) -> HummockResult<Option<BlockHolder>>;
    fn next_block_index(&self) -> usize;
}

/// An iterator that reads the blocks of an SST step by step from a given stream of bytes.
pub struct BlockDataStream {
    buf_reader: MonitoredStreamingReader,

    /// The index of the next block. Note that `block_idx` is relative to the start index of the
    /// stream (and is compatible with `block_size_vec`); it is not relative to the corresponding
    /// SST. That is, if streaming starts at block 2 of a given SST `T`, then `block_idx = 0`
    /// refers to the third block of `T`.
    block_idx: usize,

    /// The sizes of each block which the stream reads. The first number states the compressed size
    /// in the stream. The second number is the block's uncompressed size.  Note that the list does
    /// not contain the size of blocks which precede the first streamed block. That is, if
    /// streaming starts at block 2 of a given SST, then the list does not contain information
    /// about block 0 and block 1.
    block_metas: Vec<BlockMeta>,

    buf: Bytes,

    buff_offset: usize,
}

impl BlockDataStream {
    /// Constructs a new `BlockStream` object that reads from the given `byte_stream` and interprets
    /// the data as blocks of the SST described in `sst_meta`, starting at block `block_index`.
    ///
    /// If `block_index >= sst_meta.block_metas.len()`, then `BlockStream` will not read any data
    /// from `byte_stream`.
    pub fn new(
        // The stream that provides raw data.
        byte_stream: MonitoredStreamingReader,
        // Meta data of the SST that is streamed.
        block_metas: Vec<BlockMeta>,
    ) -> Self {
        Self {
            buf_reader: byte_stream,
            block_idx: 0,
            block_metas,
            buf: Bytes::default(),
            buff_offset: 0,
        }
    }

    /// Reads the next block from the stream and returns it. Returns `None` if there are no blocks
    /// left to read.
    pub async fn next_block_impl(&mut self) -> HummockResult<Option<(Bytes, usize)>> {
        if self.block_idx >= self.block_metas.len() {
            return Ok(None);
        }

        let block_meta = &self.block_metas[self.block_idx];
        fail_point!("stream_read_err", |_| Err(ObjectError::internal(
            "stream read error"
        )
        .into()));
        let uncompressed_size = block_meta.uncompressed_size as usize;
        let end = self.buff_offset + block_meta.len as usize;
        let data = if end > self.buf.len() {
            let current_block = self.read_next_buf(block_meta.len as usize).await?;
            self.buff_offset = 0;
            current_block
        } else {
            let data = self.buf.slice(self.buff_offset..end);
            self.buff_offset = end;
            data
        };

        self.block_idx += 1;
        Ok(Some((data, uncompressed_size)))
    }

    async fn read_next_buf(&mut self, read_size: usize) -> HummockResult<Bytes> {
        let mut read_buf = BytesMut::with_capacity(read_size);
        let start_pos = if self.buff_offset < self.buf.len() {
            read_buf.extend_from_slice(&self.buf[self.buff_offset..]);
            self.buf.len() - self.buff_offset
        } else {
            0
        };
        let mut rest = read_size - start_pos;
        while rest > 0 {
            let next_packet = self
                .buf_reader
                .read_bytes()
                .await
                .unwrap_or_else(|| Err(ObjectError::internal("read unexpected EOF")))?;
            let read_len = std::cmp::min(next_packet.len(), rest);
            read_buf.extend_from_slice(&next_packet[..read_len]);
            rest -= read_len;
            if rest == 0 {
                self.buf = next_packet.slice(read_len..);
                return Ok(read_buf.freeze());
            }
        }
        self.buf = Bytes::default();
        Ok(read_buf.freeze())
    }

    pub fn next_block_index(&self) -> usize {
        self.block_idx
    }

    pub async fn next_block(&mut self) -> HummockResult<Option<Box<Block>>> {
        match self.next_block_impl().await? {
            None => Ok(None),
            Some((buf, uncompressed_size)) => {
                Ok(Some(Box::new(Block::decode(buf, uncompressed_size)?)))
            }
        }
    }
}

/// An iterator that reads the blocks of an SST step by step from a given stream of bytes optimize for batch query.
/// Because all data in batch query shall be put into block-cache, we can not assign a reference of Bytes for each block.
/// We must copy them into new memory.
pub struct LongConnectionBlockStream {
    inner: BlockDataStream,
    cache: BlockCache,
    object_id: HummockSstableObjectId,
    start_block_index: usize,
    memory_usage: usize,
    /// To avoid high frequently query cost too much memory.
    prefetch_buffer_usage: Arc<AtomicUsize>,
}

impl LongConnectionBlockStream {
    pub fn new(
        inner: BlockDataStream,
        cache: BlockCache,
        object_id: HummockSstableObjectId,
        start_block_index: usize,
        memory_usage: usize,
        prefetch_buffer_usage: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            inner,
            cache,
            object_id,
            start_block_index,
            memory_usage,
            prefetch_buffer_usage,
        }
    }
}

#[async_trait::async_trait]
impl BlockStream for LongConnectionBlockStream {
    fn next_block_index(&self) -> usize {
        self.inner.next_block_index() + self.start_block_index
    }

    async fn next_block(&mut self) -> HummockResult<Option<BlockHolder>> {
        let index = self.next_block_index();
        let block = match self.inner.next_block_impl().await? {
            Some((buf, uncompressed_size)) => {
                Box::new(Block::decode_with_copy(buf, uncompressed_size, true)?)
            }
            None => return Ok(None),
        };
        self.cache.insert(
            self.object_id,
            index as u64,
            // Here we can clone this block just because the data in `Block` is `Bytes`, so we only increase the reference, and would not really cost memory.
            Box::new(block.as_ref().clone()),
            CachePriority::Low,
        );
        Ok(Some(BlockHolder::from_owned_block(block)))
    }
}

impl Drop for LongConnectionBlockStream {
    fn drop(&mut self) {
        self.prefetch_buffer_usage
            .fetch_sub(self.memory_usage, Ordering::SeqCst);
    }
}

pub struct PrefetchBlockStream {
    blocks: VecDeque<BlockHolder>,
    block_index: usize,
}

impl PrefetchBlockStream {
    pub fn new(blocks: VecDeque<BlockHolder>, block_index: usize) -> Self {
        Self {
            blocks,
            block_index,
        }
    }
}

#[async_trait::async_trait]
impl BlockStream for PrefetchBlockStream {
    fn next_block_index(&self) -> usize {
        self.block_index
    }

    async fn next_block(&mut self) -> HummockResult<Option<BlockHolder>> {
        if let Some(block) = self.blocks.pop_front() {
            self.block_index += 1;
            return Ok(Some(block));
        }
        Ok(None)
    }
}
