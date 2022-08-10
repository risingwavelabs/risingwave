use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use risingwave_common::cache::{CachableEntry, LruCache};
use risingwave_hummock_sdk::HummockSstableId;
use risingwave_object_store::object::ObjectStore;

use crate::hummock::sstable_store::{SstableStoreRef, TableHolder};
use crate::hummock::utils::MemoryTracker;
use crate::hummock::{
    Block, BlockMeta, CachePolicy, HummockError, HummockResult, MemoryLimiter, Sstable, SstableMeta,
};
use crate::monitor::{MemoryCollector, StoreLocalStatistic};

pub struct SstableBlocks {
    pub blocks: Vec<Arc<Block>>,
    _tracker: MemoryTracker,
}

pub type DataHolder = CachableEntry<HummockSstableId, SstableBlocks>;

pub struct CompactorSstableStore {
    sstable_store: SstableStoreRef,
    data_cache: Arc<LruCache<HummockSstableId, SstableBlocks>>,
    memory_limiter: Arc<MemoryLimiter>,
}

pub type CompactorSstableStoreRef = Arc<CompactorSstableStore>;

impl CompactorSstableStore {
    pub fn new(
        sstable_store: SstableStoreRef,
        memory_limiter: Arc<MemoryLimiter>,
        data_cache_capacity: usize,
    ) -> Self {
        let data_cache = Arc::new(LruCache::new(0, data_cache_capacity));
        Self {
            sstable_store,
            data_cache,
            memory_limiter,
        }
    }

    pub async fn put_sst(
        &self,
        sst_id: HummockSstableId,
        meta: SstableMeta,
        data: Bytes,
        policy: CachePolicy,
    ) -> HummockResult<()> {
        if let CachePolicy::Fill = policy {
            let blocks = decode_block(&meta.block_metas, &data)?;
            let charge = blocks.iter().map(|block| block.data().len()).sum::<usize>();
            if let Some(tracker) = self.memory_limiter.try_require_memory(charge as u64) {
                self.data_cache.insert(
                    sst_id,
                    sst_id,
                    charge,
                    SstableBlocks {
                        blocks,
                        _tracker: tracker,
                    },
                );
            }
        }
        self.sstable_store
            .put_sst(sst_id, meta, data, CachePolicy::Fill)
            .await
    }

    pub async fn sstable(
        &self,
        sst_id: HummockSstableId,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        self.sstable_store.sstable(sst_id, stats).await
    }

    pub async fn load_data(
        &self,
        sst: &Sstable,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<DataHolder> {
        stats.cache_data_block_total += 1;
        let entry = self
            .data_cache
            .lookup_with_request_dedup::<_, HummockError, _>(sst.id, sst.id, || {
                let store = self.sstable_store.store().clone();
                stats.cache_data_block_miss += 1;
                let stats_ptr = stats.remote_io_time.clone();
                let data_path = self.sstable_store.get_sst_data_path(sst.id);
                let block_metas = sst.meta.block_metas.clone();
                let charge = sst.meta.estimated_size as u64;
                let memory_controller = self.memory_limiter.clone();
                async move {
                    let tracker = memory_controller.require_memory(charge).await.unwrap();
                    let now = Instant::now();
                    let block_data = store
                        .read(&data_path, None)
                        .await
                        .map_err(HummockError::object_io_error)?;
                    let blocks = decode_block(&block_metas, &block_data)?;
                    let charge = blocks.iter().map(|block| block.data().len()).sum::<usize>();
                    let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
                    stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
                    Ok((
                        SstableBlocks {
                            blocks,
                            _tracker: tracker,
                        },
                        charge,
                    ))
                }
            })
            .await
            .map_err(|e| {
                HummockError::other(format!(
                    "meta cache lookup request dedup get cancel: {:?}",
                    e,
                ))
            })??;
        Ok(entry)
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
        self.sstable_store.data_cache.get_memory_usage() as u64
    }

    fn get_total_memory_usage(&self) -> u64 {
        self.uploading_memory_limiter.get_memory_usage()
            + self.sstable_store.memory_limiter.get_memory_usage()
    }
}
fn decode_block(block_metas: &[BlockMeta], data: &Bytes) -> HummockResult<Vec<Arc<Block>>> {
    let mut blocks = Vec::with_capacity(block_metas.len());
    for block_meta in block_metas {
        let end_offset = (block_meta.offset + block_meta.len) as usize;
        let block = Block::decode(&data[block_meta.offset as usize..end_offset])?;
        blocks.push(Arc::new(block));
    }
    Ok(blocks)
}
