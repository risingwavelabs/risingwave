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

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_object_store::object::BlockLocation;

use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{
    Block, CachePolicy, HummockError, HummockResult, MemoryLimiter, Sstable, SstableMeta,
    SstableStoreWrite, BlockStream,
};
use crate::monitor::{MemoryCollector, StoreLocalStatistic};

pub struct SstableBlocks {
    block_data: Bytes,
    offset: usize,
    offset_index: usize,
    start_index: usize,
    end_index: usize,
    block_size: Vec<usize>,
    _tracker: MemoryTracker,
}

impl SstableBlocks {
    pub fn next(&mut self) -> Option<(usize, Box<Block>)> {
        if self.offset_index >= self.end_index {
            return None;
        }
        let idx = self.offset_index;
        let next_offset = self.offset + self.block_size[idx - self.start_index];
        let block = match Block::decode(&self.block_data[self.offset..next_offset]) {
            Ok(block) => Box::new(block),
            Err(_) => return None,
        };
        self.offset = next_offset;
        self.offset_index += 1;
        Some((idx, block))
    }

    pub fn end_index(&self) -> usize {
        self.end_index
    }
}

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
        sst_id: HummockSstableId,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        self.sstable_store.sstable(sst_id, stats).await
    }

    pub async fn scan(
        &self,
        sst: &Sstable,
        start_index: usize,
        end_index: usize,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<SstableBlocks> {
        stats.cache_data_block_total += 1;
        stats.cache_data_block_miss += 1;
        let store = self.sstable_store.store().clone();
        let stats_ptr = stats.remote_io_time.clone();
        let data_path = self.sstable_store.get_sst_data_path(sst.id);
        let block_meta = sst
            .meta
            .block_metas
            .get(start_index)
            .ok_or_else(HummockError::invalid_block)?;
        let start_offset = block_meta.offset as usize;
        let mut block_loc = BlockLocation {
            offset: start_offset,
            size: 0,
        };
        for block_meta in &sst.meta.block_metas[start_index..end_index] {
            block_loc.size += block_meta.len as usize;
        }
        let tracker = self
            .memory_limiter
            .require_memory(block_loc.size as u64)
            .await
            .unwrap();
        let now = Instant::now();
        let block_data = store
            .read(&data_path, Some(block_loc))
            .await
            .map_err(HummockError::object_io_error)?;
        let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
        stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
        Ok(SstableBlocks {
            block_data,
            offset: 0,
            offset_index: start_index,
            start_index,
            end_index,
            block_size: sst.meta.block_metas[start_index..end_index]
                .iter()
                .map(|meta| meta.len as usize)
                .collect_vec(),
            _tracker: tracker,
        })
    }

    /// Loads the blocks (their data) in the specified range of the specified SST from cache.
    pub async fn scan_cache(
        &self,
        sst: &Sstable,
        start_index: usize,
        end_index: usize,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<SstableBlocks> {
        unimplemented!();
    }

    pub async fn get_stream(
        &self,
        sst: &Sstable,
        block_index: Option<usize>,
    ) -> HummockResult<BlockStream> {
        // ToDo: What about `StoreLocalStatistic`?
        self.sstable_store.get_block_stream(sst, block_index).await
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

    fn get_total_memory_usage(&self) -> u64 {
        self.uploading_memory_limiter.get_memory_usage()
            + self.sstable_store.memory_limiter.get_memory_usage()
    }
}

#[async_trait::async_trait]
impl SstableStoreWrite for CompactorSstableStore {
    async fn put_sst(
        &self,
        sst_id: HummockSstableId,
        meta: SstableMeta,
        data: Bytes,
        _policy: CachePolicy,
    ) -> HummockResult<()> {
        // TODO: fill cache for L0
        self.sstable_store
            .put_sst(sst_id, meta, data, CachePolicy::NotFill)
            .await
    }
}
