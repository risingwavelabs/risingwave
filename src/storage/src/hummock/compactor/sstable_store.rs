use std::collections::LinkedList;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_object_store::object::{BlockLocation, ObjectStore};

use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{
    Block, CachePolicy, HummockError, HummockResult, MemoryLimiter, Sstable,
    SstableMeta, SstableStoreWrite,
};
use crate::monitor::{MemoryCollector, StoreLocalStatistic};

pub struct SstableBlocks {
    pub blocks: LinkedList<(usize, Arc<Block>)>,
    _tracker: MemoryTracker,
}

impl SstableBlocks {
    pub fn next(&mut self) -> Option<(usize, Arc<Block>)> {
        self.blocks.pop_front()
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
        let mut tracker = self
            .memory_limiter
            .require_memory(block_loc.size as u64)
            .await
            .unwrap();
        let now = Instant::now();
        let block_data = store
            .read(&data_path, Some(block_loc))
            .await
            .map_err(HummockError::object_io_error)?;
        let mut blocks = LinkedList::new();
        let mut charge = 0;
        for idx in start_index..end_index {
            let start_offset = sst.meta.block_metas[idx].offset as usize - start_offset;
            let end_offset = start_offset + sst.meta.block_metas[idx].len as usize;
            let block = Block::decode(&block_data[start_offset..end_offset])?;
            charge += block.raw_data().len();
            blocks.push_back((idx, Arc::new(block)));
        }
        drop(block_data);
        tracker.increase_memory(charge as u64).await;
        let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
        stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
        Ok(SstableBlocks {
            blocks,
            _tracker: tracker,
        })
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
