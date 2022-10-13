// Copyright 2022 Singularity Data
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

use std::sync::Arc;

use bytes::Bytes;
use risingwave_object_store::object::ObjectError;
use risingwave_pb::hummock::SstableInfo;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::hummock::{
    Block, BlockHolder, HummockError, HummockResult, MemoryLimiter, Sstable, SstableMeta,
};
use crate::monitor::{MemoryCollector, StoreLocalStatistic};

pub struct CompactorSstableStore {
    sstable_store: SstableStoreRef,
    memory_limiter: Arc<MemoryLimiter>,
}

pub type CompactorSstableStoreRef = Arc<CompactorSstableStore>;

impl CompactorSstableStore {
    pub fn new(sstable_store: SstableStoreRef, memory_limiter: Arc<MemoryLimiter>) -> Self {
        Self {
            sstable_store,
            memory_limiter,
        }
    }

    pub async fn sstable(
        &self,
        sst: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        self.sstable_store.sstable(sst, stats).await
    }

    pub async fn get_stream(
        &self,
        sst: &Sstable,
        block_index: Option<usize>,
    ) -> HummockResult<BlockStream> {
        let start_pos = match block_index {
            None => None,
            Some(index) => {
                let block_meta = sst
                    .meta
                    .block_metas
                    .get(index)
                    .ok_or_else(HummockError::invalid_block)?;

                Some(block_meta.offset as usize)
            }
        };

        let data_path = self.sstable_store.get_sst_data_path(sst.id);
        let store = self.sstable_store.store().clone();

        Ok(BlockStream::new(
            store
                .streaming_read(&data_path, start_pos)
                .await
                .map_err(HummockError::object_io_error)?,
            block_index.unwrap_or(0),
            &sst.meta,
        ))
    }
}

pub struct CompactorMemoryCollector {
    uploading_memory_limiter: Arc<MemoryLimiter>,
    sstable_store: CompactorSstableStoreRef,
}

impl CompactorMemoryCollector {
    pub fn new(
        uploading_memory_limiter: Arc<MemoryLimiter>,
        sstable_store: CompactorSstableStoreRef,
    ) -> Self {
        Self {
            uploading_memory_limiter,
            sstable_store,
        }
    }
}

impl MemoryCollector for CompactorMemoryCollector {
    fn get_meta_memory_usage(&self) -> u64 {
        self.sstable_store.sstable_store.get_meta_memory_usage()
    }

    fn get_data_memory_usage(&self) -> u64 {
        self.sstable_store.memory_limiter.get_memory_usage()
    }

    fn get_uploading_memory_usage(&self) -> u64 {
        self.uploading_memory_limiter.get_memory_usage()
    }
}

/// An iterator that reads the blocks of an SST step by step from a given stream of bytes.
pub struct BlockStream {
    /// The stream that provides raw data.
    byte_stream: Box<dyn AsyncRead + Unpin + Send + Sync>,

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
    block_size_vec: Vec<(usize, usize)>,
}

impl BlockStream {
    /// Constructs a new `BlockStream` object that reads from the given `byte_stream` and interprets
    /// the data as blocks of the SST described in `sst_meta`, starting at block `block_index`.
    ///
    /// If `block_index >= sst_meta.block_metas.len()`, then `BlockStream` will not read any data
    /// from `byte_stream`.
    fn new(
        // The stream that provides raw data.
        byte_stream: Box<dyn AsyncRead + Unpin + Send + Sync>,

        // Index of the SST's block where the stream starts.
        block_index: usize,

        // Meta data of the SST that is streamed.
        sst_meta: &SstableMeta,
    ) -> Self {
        let metas = &sst_meta.block_metas;

        // Avoids panicking if `block_index` is too large.
        let block_index = std::cmp::min(block_index, metas.len());

        let mut block_len_vec = Vec::with_capacity(metas.len() - block_index);
        sst_meta.block_metas[block_index..]
            .iter()
            .for_each(|b_meta| {
                block_len_vec.push((b_meta.len as usize, b_meta.uncompressed_size as usize))
            });

        Self {
            byte_stream,
            block_idx: 0,
            block_size_vec: block_len_vec,
        }
    }

    /// Reads the next block from the stream and returns it. Returns `None` if there are no blocks
    /// left to read.
    pub async fn next(&mut self) -> HummockResult<Option<BlockHolder>> {
        if self.block_idx >= self.block_size_vec.len() {
            return Ok(None);
        }

        let (block_stream_size, block_full_size) =
            *self.block_size_vec.get(self.block_idx).unwrap();
        let mut buffer = vec![0; block_stream_size];

        let bytes_read = self
            .byte_stream
            .read_exact(&mut buffer[..])
            .await
            .map_err(|e| HummockError::object_io_error(ObjectError::internal(e)))?;

        if bytes_read != block_stream_size {
            return Err(HummockError::object_io_error(ObjectError::internal(
                format!(
                    "unexpected number of bytes: expected: {} read: {}",
                    block_stream_size, bytes_read
                ),
            )));
        }

        let boxed_block = Box::new(Block::decode(Bytes::from(buffer), block_full_size)?);
        self.block_idx += 1;

        Ok(Some(BlockHolder::from_owned_block(boxed_block)))
    }
}

// ToDo: Unit tests.
