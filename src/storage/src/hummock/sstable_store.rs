// Copyright 2024 RisingWave Labs
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
use std::clone::Clone;
use std::collections::VecDeque;
use std::future::Future;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use ahash::RandomState;
use await_tree::InstrumentAwait;
use bytes::Bytes;
use fail::fail_point;
use foyer::memory::{
    Cache, CacheContext, CacheEntry, CacheEventListener, EntryState, Key, LfuCacheConfig,
    LruCacheConfig, LruConfig, S3FifoCacheConfig, Value,
};
use futures::{future, StreamExt};
use itertools::Itertools;
use risingwave_common::config::{EvictionConfig, StorageMemoryConfig};
use risingwave_hummock_sdk::{HummockSstableObjectId, OBJECT_SUFFIX};
use risingwave_hummock_trace::TracedCachePolicy;
use risingwave_object_store::object::{
    ObjectError, ObjectMetadataIter, ObjectStoreRef, ObjectStreamingUploader,
};
use risingwave_pb::hummock::SstableInfo;
use thiserror_ext::AsReport;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use zstd::zstd_safe::WriteBuf;

use super::utils::MemoryTracker;
use super::{
    Block, BlockCache, BlockCacheConfig, BlockMeta, BlockResponse, CachedBlock, CachedSstable,
    FileCache, RecentFilter, Sstable, SstableBlockIndex, SstableMeta, SstableWriter,
};
use crate::hummock::block_stream::{
    BlockDataStream, BlockStream, MemoryUsageTracker, PrefetchBlockStream,
};
use crate::hummock::file_cache::preclude::*;
use crate::hummock::multi_builder::UploadJoinHandle;
use crate::hummock::{BlockHolder, HummockError, HummockResult, MemoryLimiter};
use crate::monitor::{HummockStateStoreMetrics, MemoryCollector, StoreLocalStatistic};

pub type TableHolder = CacheEntry<HummockSstableObjectId, Box<Sstable>, MetaCacheEventListener>;

// TODO: Define policy based on use cases (read / compaction / ...).
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum CachePolicy {
    /// Disable read cache and not fill the cache afterwards.
    Disable,
    /// Try reading the cache and fill the cache afterwards.
    Fill(CacheContext),
    /// Fill file cache only.
    FillFileCache,
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

impl Default for CachePolicy {
    fn default() -> Self {
        CachePolicy::Fill(CacheContext::Default)
    }
}

impl From<TracedCachePolicy> for CachePolicy {
    fn from(policy: TracedCachePolicy) -> Self {
        match policy {
            TracedCachePolicy::Disable => Self::Disable,
            TracedCachePolicy::Fill(priority) => Self::Fill(priority.into()),
            TracedCachePolicy::FileFileCache => Self::FillFileCache,
            TracedCachePolicy::NotFill => Self::NotFill,
        }
    }
}

impl From<CachePolicy> for TracedCachePolicy {
    fn from(policy: CachePolicy) -> Self {
        match policy {
            CachePolicy::Disable => Self::Disable,
            CachePolicy::FillFileCache => Self::FileFileCache,
            CachePolicy::Fill(priority) => Self::Fill(priority.into()),
            CachePolicy::NotFill => Self::NotFill,
        }
    }
}

#[derive(Debug)]
pub struct BlockCacheEventListener {
    data_file_cache: FileCache<SstableBlockIndex, CachedBlock>,
    metrics: Arc<HummockStateStoreMetrics>,
}

impl BlockCacheEventListener {
    pub fn new(
        data_file_cache: FileCache<SstableBlockIndex, CachedBlock>,
        metrics: Arc<HummockStateStoreMetrics>,
    ) -> Self {
        Self {
            data_file_cache,
            metrics,
        }
    }
}

impl CacheEventListener<(HummockSstableObjectId, u64), Box<Block>> for BlockCacheEventListener {
    fn on_release(
        &self,
        key: (HummockSstableObjectId, u64),
        value: Box<Block>,
        _context: CacheContext,
        _charges: usize,
    ) {
        let key = SstableBlockIndex {
            sst_id: key.0,
            block_idx: key.1,
        };
        self.metrics
            .block_efficiency_histogram
            .with_label_values(&[&value.table_id().to_string()])
            .observe(value.efficiency());
        // temporarily avoid spawn task while task drop with madsim
        // FYI: https://github.com/madsim-rs/madsim/issues/182
        #[cfg(not(madsim))]
        self.data_file_cache
            .insert_if_not_exists_async(key, CachedBlock::Loaded { block: value });
    }
}

pub struct MetaCacheEventListener(FileCache<HummockSstableObjectId, CachedSstable>);

impl From<FileCache<HummockSstableObjectId, CachedSstable>> for MetaCacheEventListener {
    fn from(value: FileCache<HummockSstableObjectId, CachedSstable>) -> Self {
        Self(value)
    }
}

impl CacheEventListener<HummockSstableObjectId, Box<Sstable>> for MetaCacheEventListener {
    fn on_release(
        &self,
        key: HummockSstableObjectId,
        value: Box<Sstable>,
        _context: CacheContext,
        _charges: usize,
    ) {
        // temporarily avoid spawn task while task drop with madsim
        // FYI: https://github.com/madsim-rs/madsim/issues/182
        #[cfg(not(madsim))]
        self.0.insert_if_not_exists_async(key, value.into());
    }
}

pub enum CachedOrShared<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, Box<V>>,
{
    Cached(CacheEntry<K, Box<V>, L>),
    Shared(Arc<V>),
}

impl<K, V, L> Deref for CachedOrShared<K, V, L>
where
    K: Key,
    V: Value,
    L: CacheEventListener<K, Box<V>>,
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        match self {
            CachedOrShared::Cached(entry) => entry,
            CachedOrShared::Shared(v) => v,
        }
    }
}

pub struct SstableStoreConfig {
    pub store: ObjectStoreRef,
    pub path: String,
    pub block_cache_capacity: usize,
    pub block_cache_shard_num: usize,
    pub block_cache_eviction: EvictionConfig,
    pub meta_cache_capacity: usize,
    pub meta_cache_shard_num: usize,
    pub meta_cache_eviction: EvictionConfig,
    pub prefetch_buffer_capacity: usize,
    pub max_prefetch_block_number: usize,
    pub data_file_cache: FileCache<SstableBlockIndex, CachedBlock>,
    pub meta_file_cache: FileCache<HummockSstableObjectId, CachedSstable>,
    pub recent_filter: Option<Arc<RecentFilter<(HummockSstableObjectId, usize)>>>,
    pub state_store_metrics: Arc<HummockStateStoreMetrics>,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    block_cache: BlockCache,
    // TODO(MrCroxx): use no hash random state
    meta_cache: Arc<Cache<HummockSstableObjectId, Box<Sstable>, MetaCacheEventListener>>,

    data_file_cache: FileCache<SstableBlockIndex, CachedBlock>,
    meta_file_cache: FileCache<HummockSstableObjectId, CachedSstable>,
    /// Recent filter for `(sst_obj_id, blk_idx)`.
    ///
    /// `blk_idx == USIZE::MAX` stands for `sst_obj_id` only entry.
    recent_filter: Option<Arc<RecentFilter<(HummockSstableObjectId, usize)>>>,
    prefetch_buffer_usage: Arc<AtomicUsize>,
    prefetch_buffer_capacity: usize,
    max_prefetch_block_number: usize,
}

impl SstableStore {
    pub fn new(config: SstableStoreConfig) -> Self {
        // TODO: We should validate path early. Otherwise object store won't report invalid path
        // error until first write attempt.

        let block_cache = BlockCache::new(BlockCacheConfig {
            capacity: config.block_cache_capacity,
            shard_num: config.block_cache_shard_num,
            eviction: config.block_cache_eviction,
            listener: BlockCacheEventListener::new(
                config.data_file_cache.clone(),
                config.state_store_metrics.clone(),
            ),
        });

        // TODO(MrCroxx): reuse BlockCacheConfig here?
        let meta_cache = {
            let capacity = config.meta_cache_capacity;
            let shards = config.meta_cache_shard_num;
            let object_pool_capacity = config.meta_cache_shard_num * 1024;
            let hash_builder = RandomState::default();
            let event_listener = MetaCacheEventListener::from(config.meta_file_cache.clone());
            match config.meta_cache_eviction {
                EvictionConfig::Lru(eviction_config) => Cache::lru(LruCacheConfig {
                    capacity,
                    shards,
                    eviction_config,
                    object_pool_capacity,
                    hash_builder,
                    event_listener,
                }),
                EvictionConfig::Lfu(eviction_config) => Cache::lfu(LfuCacheConfig {
                    capacity,
                    shards,
                    eviction_config,
                    object_pool_capacity,
                    hash_builder,
                    event_listener,
                }),
                EvictionConfig::S3Fifo(eviction_config) => Cache::s3fifo(S3FifoCacheConfig {
                    capacity,
                    shards,
                    eviction_config,
                    object_pool_capacity,
                    hash_builder,
                    event_listener,
                }),
            }
        };
        let meta_cache = Arc::new(meta_cache);

        Self {
            path: config.path,
            store: config.store,
            block_cache,
            meta_cache,

            data_file_cache: config.data_file_cache,
            meta_file_cache: config.meta_file_cache,

            recent_filter: config.recent_filter,
            prefetch_buffer_usage: Arc::new(AtomicUsize::new(0)),
            prefetch_buffer_capacity: config.prefetch_buffer_capacity,
            max_prefetch_block_number: config.max_prefetch_block_number,
        }
    }

    /// For compactor, we do not need a high concurrency load for cache. Instead, we need the cache
    ///  can be evict more effective.
    pub fn for_compactor(
        store: ObjectStoreRef,
        path: String,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
    ) -> Self {
        let meta_cache = Arc::new(Cache::lru(LruCacheConfig {
            capacity: meta_cache_capacity,
            shards: 1,
            eviction_config: LruConfig {
                high_priority_pool_ratio: 0.0,
            },
            object_pool_capacity: 1024,
            hash_builder: RandomState::default(),
            event_listener: FileCache::none().into(),
        }));
        Self {
            path,
            store,
            block_cache: BlockCache::new(BlockCacheConfig {
                capacity: block_cache_capacity,
                shard_num: 1,
                eviction: EvictionConfig::Lru(LruConfig {
                    high_priority_pool_ratio: 0.0,
                }),
                listener: BlockCacheEventListener::new(
                    FileCache::none(),
                    Arc::new(HummockStateStoreMetrics::unused()),
                ),
            }),
            meta_cache,
            data_file_cache: FileCache::none(),
            meta_file_cache: FileCache::none(),
            prefetch_buffer_usage: Arc::new(AtomicUsize::new(0)),
            prefetch_buffer_capacity: block_cache_capacity,
            max_prefetch_block_number: 16, /* compactor won't use this parameter, so just assign a default value. */
            recent_filter: None,
        }
    }

    pub async fn delete(&self, object_id: HummockSstableObjectId) -> HummockResult<()> {
        // Data
        self.store
            .delete(self.get_sst_data_path(object_id).as_str())
            .await?;
        self.meta_cache.remove(&object_id);
        self.meta_file_cache
            .remove(&object_id)
            .map_err(HummockError::file_cache)?;
        Ok(())
    }

    /// Deletes all SSTs specified in the given list of IDs from storage and cache.
    pub async fn delete_list(
        &self,
        object_id_list: &[HummockSstableObjectId],
    ) -> HummockResult<()> {
        let mut paths = Vec::with_capacity(object_id_list.len() * 2);

        for &object_id in object_id_list {
            paths.push(self.get_sst_data_path(object_id));
        }
        // Delete from storage.
        self.store.delete_objects(&paths).await?;

        // Delete from cache.
        for &object_id in object_id_list {
            self.meta_cache.remove(&object_id);
            self.meta_file_cache
                .remove(&object_id)
                .map_err(HummockError::file_cache)?;
        }

        Ok(())
    }

    pub fn delete_cache(&self, object_id: HummockSstableObjectId) {
        self.meta_cache.remove(&object_id);
        if let Err(e) = self.meta_file_cache.remove(&object_id) {
            tracing::warn!(error = %e.as_report(), "meta file cache remove error");
        }
    }

    async fn put_sst_data(
        &self,
        object_id: HummockSstableObjectId,
        data: Bytes,
    ) -> HummockResult<()> {
        let data_path = self.get_sst_data_path(object_id);
        self.store
            .upload(&data_path, data)
            .await
            .map_err(Into::into)
    }

    pub async fn prefetch_blocks(
        &self,
        sst: &Sstable,
        block_index: usize,
        end_index: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<Box<dyn BlockStream>> {
        let object_id = sst.id;
        if self.prefetch_buffer_usage.load(Ordering::Acquire) > self.prefetch_buffer_capacity {
            let block = self.get(sst, block_index, policy, stats).await?;
            return Ok(Box::new(PrefetchBlockStream::new(
                VecDeque::from([block]),
                block_index,
                None,
            )));
        }
        stats.cache_data_block_total += 1;
        if let Some(block) = self.block_cache.get(object_id, block_index as u64) {
            return Ok(Box::new(PrefetchBlockStream::new(
                VecDeque::from([block]),
                block_index,
                None,
            )));
        }
        let end_index = std::cmp::min(end_index, block_index + self.max_prefetch_block_number);
        let mut end_index = std::cmp::min(end_index, sst.meta.block_metas.len());
        let start_offset = sst.meta.block_metas[block_index].offset as usize;
        let mut min_hit_index = end_index;
        let mut hit_count = 0;
        for idx in block_index..end_index {
            if self.block_cache.exists_block(object_id, idx as u64) {
                if min_hit_index > idx && idx > block_index {
                    min_hit_index = idx;
                }
                hit_count += 1;
            }
        }

        if hit_count * 3 >= (end_index - block_index) || min_hit_index * 2 > block_index + end_index
        {
            end_index = min_hit_index;
        }
        stats.cache_data_prefetch_count += 1;
        stats.cache_data_prefetch_block_count += (end_index - block_index) as u64;
        let end_offset = start_offset
            + sst.meta.block_metas[block_index..end_index]
                .iter()
                .map(|meta| meta.len as usize)
                .sum::<usize>();
        let data_path = self.get_sst_data_path(object_id);
        let memory_usage = end_offset - start_offset;
        let tracker = MemoryUsageTracker::new(self.prefetch_buffer_usage.clone(), memory_usage);
        let span: await_tree::Span = format!("Prefetch SST-{}", object_id).into();
        let store = self.store.clone();
        let join_handle = tokio::spawn(async move {
            store
                .read(&data_path, start_offset..end_offset)
                .verbose_instrument_await(span)
                .await
        });
        let buf = match join_handle.await {
            Ok(Ok(data)) => data,
            Ok(Err(e)) => {
                tracing::error!(
                    "prefetch meet error when read {}..{} from sst-{} ({})",
                    start_offset,
                    end_offset,
                    object_id,
                    sst.meta.estimated_size,
                );
                return Err(e.into());
            }
            Err(_) => {
                return Err(HummockError::other("cancel by other thread"));
            }
        };
        let mut offset = 0;
        let mut blocks = VecDeque::default();
        for idx in block_index..end_index {
            let end = offset + sst.meta.block_metas[idx].len as usize;
            if end > buf.len() {
                return Err(ObjectError::internal("read unexpected EOF").into());
            }
            // copy again to avoid holding a large data in memory.
            let block = Block::decode_with_copy(
                buf.slice(offset..end),
                sst.meta.block_metas[idx].uncompressed_size as usize,
                true,
            )?;
            let holder = if let CachePolicy::Fill(priority) = policy {
                let cache_priority = if idx == block_index {
                    priority
                } else {
                    CacheContext::LruPriorityLow
                };
                self.block_cache
                    .insert(object_id, idx as u64, Box::new(block), cache_priority)
            } else {
                BlockHolder::from_owned_block(Box::new(block))
            };

            blocks.push_back(holder);
            offset = end;
        }
        Ok(Box::new(PrefetchBlockStream::new(
            blocks,
            block_index,
            Some(tracker),
        )))
    }

    pub async fn get_block_response(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockResponse> {
        let object_id = sst.id;
        let (range, uncompressed_capacity) = sst.calculate_block_info(block_index);

        stats.cache_data_block_total += 1;
        let file_size = sst.meta.estimated_size;
        let mut fetch_block = || {
            let file_cache = self.data_file_cache.clone();
            stats.cache_data_block_miss += 1;
            let data_path = self.get_sst_data_path(object_id);
            let store = self.store.clone();
            let use_file_cache = !matches!(policy, CachePolicy::Disable);
            let range = range.clone();

            async move {
                let key = SstableBlockIndex {
                    sst_id: object_id,
                    block_idx: block_index as u64,
                };
                if use_file_cache
                    && let Some(block) = file_cache
                        .lookup(&key)
                        .await
                        .map_err(HummockError::file_cache)?
                {
                    let block = block.try_into_block()?;
                    return Ok(block);
                }

                let block_data = match store
                    .read(&data_path, range.clone())
                    .verbose_instrument_await("get_block_response")
                    .await
                {
                    Ok(data) => data,
                    Err(e) => {
                        tracing::error!(
                            "get_block_response meet error when read {:?} from sst-{}, total length: {}",
                            range,
                            object_id,
                            file_size
                        );
                        return Err(HummockError::from(e));
                    }
                };
                let block = Box::new(Block::decode(block_data, uncompressed_capacity)?);

                Ok(block)
            }
        };

        let disable_cache: fn() -> bool = || {
            fail_point!("disable_block_cache", |_| true);
            false
        };

        let policy = if disable_cache() {
            CachePolicy::Disable
        } else {
            policy
        };

        if let Some(filter) = self.recent_filter.as_ref() {
            filter.extend([(object_id, usize::MAX), (object_id, block_index)]);
        }

        match policy {
            CachePolicy::Fill(priority) => Ok(self.block_cache.get_or_insert_with(
                object_id,
                block_index as u64,
                priority,
                fetch_block,
            )),
            CachePolicy::FillFileCache => {
                let block = fetch_block().await?;
                self.data_file_cache.insert_async(
                    SstableBlockIndex {
                        sst_id: object_id,
                        block_idx: block_index as u64,
                    },
                    CachedBlock::Loaded {
                        block: block.clone(),
                    },
                );
                Ok(BlockResponse::Block(BlockHolder::from_owned_block(block)))
            }
            CachePolicy::NotFill => match self.block_cache.get(object_id, block_index as u64) {
                Some(block) => Ok(BlockResponse::Block(block)),
                None => fetch_block()
                    .await
                    .map(BlockHolder::from_owned_block)
                    .map(BlockResponse::Block),
            },
            CachePolicy::Disable => fetch_block()
                .await
                .map(BlockHolder::from_owned_block)
                .map(BlockResponse::Block),
        }
    }

    pub async fn get(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        match self
            .get_block_response(sst, block_index, policy, stats)
            .await
        {
            Ok(block_response) => block_response.wait().await,
            Err(err) => Err(err),
        }
    }

    pub fn get_sst_data_path(&self, object_id: HummockSstableObjectId) -> String {
        let obj_prefix = self.store.get_object_prefix(object_id);
        format!(
            "{}/{}{}.{}",
            self.path, obj_prefix, object_id, OBJECT_SUFFIX
        )
    }

    pub fn get_object_id_from_path(path: &str) -> HummockSstableObjectId {
        let split = path.split(&['/', '.']).collect_vec();
        assert!(split.len() > 2);
        assert_eq!(split[split.len() - 1], OBJECT_SUFFIX);
        split[split.len() - 2]
            .parse::<HummockSstableObjectId>()
            .expect("valid sst id")
    }

    pub fn store(&self) -> ObjectStoreRef {
        self.store.clone()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear_block_cache(&self) {
        self.block_cache.clear();
        if let Err(e) = self.data_file_cache.clear() {
            tracing::warn!(error = %e.as_report(), "data file cache clear error");
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear_meta_cache(&self) {
        self.meta_cache.clear();
        if let Err(e) = self.meta_file_cache.clear() {
            tracing::warn!(error = %e.as_report(), "meta file cache clear error");
        }
    }

    pub async fn sstable_cached(
        &self,
        sst_obj_id: HummockSstableObjectId,
    ) -> HummockResult<
        Option<CachedOrShared<HummockSstableObjectId, Sstable, MetaCacheEventListener>>,
    > {
        if let Some(sst) = self.meta_cache.get(&sst_obj_id) {
            return Ok(Some(CachedOrShared::Cached(sst)));
        }

        if let Some(sst) = self
            .meta_file_cache
            .lookup(&sst_obj_id)
            .await
            .map_err(HummockError::file_cache)?
        {
            return Ok(Some(CachedOrShared::Shared(sst.into_inner())));
        }

        Ok(None)
    }

    /// Returns `table_holder`
    pub fn sstable(
        &self,
        sst: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> impl Future<Output = HummockResult<TableHolder>> + Send + 'static {
        let object_id = sst.get_object_id();
        let entry = self.meta_cache.entry(object_id, || {
            let meta_file_cache = self.meta_file_cache.clone();
            let store = self.store.clone();
            let meta_path = self.get_sst_data_path(object_id);
            let stats_ptr = stats.remote_io_time.clone();
            let range = sst.meta_offset as usize..sst.file_size as usize;
            async move {
                if let Some(sst) = meta_file_cache
                    .lookup(&object_id)
                    .await
                    .map_err(HummockError::file_cache)?
                {
                    // TODO(MrCroxx): Make meta cache receives Arc<Sstable> to reduce copy?
                    let sst: Box<Sstable> = sst.into();
                    let charge = sst.estimate_size();
                    return Ok((sst, charge, CacheContext::Default));
                }

                let now = Instant::now();
                let buf = store.read(&meta_path, range).await?;
                let meta = SstableMeta::decode(&buf[..])?;

                let sst = Sstable::new(object_id, meta);
                let charge = sst.estimate_size();
                let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
                stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
                Ok((Box::new(sst), charge, CacheContext::Default))
            }
        });

        if matches! { entry.state(), EntryState::Wait | EntryState::Miss } {
            stats.cache_meta_block_miss += 1;
        }

        stats.cache_meta_block_total += 1;

        entry
    }

    pub async fn list_object_metadata_from_object_store(
        &self,
    ) -> HummockResult<ObjectMetadataIter> {
        let raw_iter = self.store.list(&format!("{}/", self.path)).await?;
        let iter = raw_iter.filter(|r| match r {
            Ok(i) => future::ready(i.key.ends_with(&format!(".{}", OBJECT_SUFFIX))),
            Err(_) => future::ready(true),
        });
        Ok(Box::pin(iter))
    }

    pub fn create_sst_writer(
        self: Arc<Self>,
        object_id: HummockSstableObjectId,
        options: SstableWriterOptions,
    ) -> BatchUploadWriter {
        BatchUploadWriter::new(object_id, self, options)
    }

    pub fn insert_meta_cache(&self, object_id: HummockSstableObjectId, meta: SstableMeta) {
        let sst = Sstable::new(object_id, meta);
        let charge = sst.estimate_size();
        self.meta_cache.insert_with_context(
            object_id,
            Box::new(sst),
            charge,
            CacheContext::Default,
        );
    }

    pub fn insert_block_cache(
        &self,
        object_id: HummockSstableObjectId,
        block_index: u64,
        block: Box<Block>,
    ) {
        if let Some(filter) = self.recent_filter.as_ref() {
            filter.extend([(object_id, usize::MAX), (object_id, block_index as usize)]);
        }
        self.block_cache
            .insert(object_id, block_index, block, CacheContext::Default);
    }

    pub fn get_meta_memory_usage(&self) -> u64 {
        self.meta_cache.usage() as u64
    }

    pub fn get_prefetch_memory_usage(&self) -> usize {
        self.prefetch_buffer_usage.load(Ordering::Acquire)
    }

    pub async fn get_stream_for_blocks(
        &self,
        object_id: HummockSstableObjectId,
        metas: &[BlockMeta],
    ) -> HummockResult<BlockDataStream> {
        fail_point!("get_stream_err");
        let data_path = self.get_sst_data_path(object_id);
        let store = self.store().clone();
        let block_meta = &metas[0];
        let start_pos = block_meta.offset as usize;
        let end_pos = metas.iter().map(|meta| meta.len as usize).sum::<usize>() + start_pos;
        let range = start_pos..end_pos;
        // spawn to tokio pool because the object-storage sdk may not be safe to cancel.
        let ret = tokio::spawn(async move { store.streaming_read(&data_path, range).await }).await;

        let reader = match ret {
            Ok(Ok(reader)) => reader,
            Ok(Err(e)) => return Err(HummockError::from(e)),
            Err(e) => {
                return Err(HummockError::other(format!(
                    "failed to get result, this read request may be canceled: {}",
                    e.as_report()
                )))
            }
        };
        Ok(BlockDataStream::new(reader, metas.to_vec()))
    }

    pub fn data_recent_filter(
        &self,
    ) -> Option<&Arc<RecentFilter<(HummockSstableObjectId, usize)>>> {
        self.recent_filter.as_ref()
    }

    pub fn data_file_cache(&self) -> &FileCache<SstableBlockIndex, CachedBlock> {
        &self.data_file_cache
    }

    pub fn data_cache(&self) -> &BlockCache {
        &self.block_cache
    }
}

pub type SstableStoreRef = Arc<SstableStore>;

pub struct HummockMemoryCollector {
    sstable_store: SstableStoreRef,
    limiter: Arc<MemoryLimiter>,
    storage_memory_config: StorageMemoryConfig,
}

impl HummockMemoryCollector {
    pub fn new(
        sstable_store: SstableStoreRef,
        limiter: Arc<MemoryLimiter>,
        storage_memory_config: StorageMemoryConfig,
    ) -> Self {
        Self {
            sstable_store,
            limiter,
            storage_memory_config,
        }
    }
}

impl MemoryCollector for HummockMemoryCollector {
    fn get_meta_memory_usage(&self) -> u64 {
        self.sstable_store.get_meta_memory_usage()
    }

    fn get_data_memory_usage(&self) -> u64 {
        self.sstable_store.block_cache.size() as u64
    }

    fn get_uploading_memory_usage(&self) -> u64 {
        self.limiter.get_memory_usage()
    }

    fn get_prefetch_memory_usage(&self) -> usize {
        self.sstable_store.get_prefetch_memory_usage()
    }

    fn get_meta_cache_memory_usage_ratio(&self) -> f64 {
        self.sstable_store.get_meta_memory_usage() as f64
            / (self.storage_memory_config.meta_cache_capacity_mb * 1024 * 1024) as f64
    }

    fn get_block_cache_memory_usage_ratio(&self) -> f64 {
        self.sstable_store.block_cache.size() as f64
            / (self.storage_memory_config.block_cache_capacity_mb * 1024 * 1024) as f64
    }

    fn get_shared_buffer_usage_ratio(&self) -> f64 {
        self.limiter.get_memory_usage() as f64
            / (self.storage_memory_config.shared_buffer_capacity_mb * 1024 * 1024) as f64
    }
}

pub struct SstableWriterOptions {
    /// Total length of SST data.
    pub capacity_hint: Option<usize>,
    pub tracker: Option<MemoryTracker>,
    pub policy: CachePolicy,
}

impl Default for SstableWriterOptions {
    fn default() -> Self {
        Self {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::NotFill,
        }
    }
}
#[async_trait::async_trait]
pub trait SstableWriterFactory: Send {
    type Writer: SstableWriter<Output = UploadJoinHandle>;

    async fn create_sst_writer(
        &mut self,
        object_id: HummockSstableObjectId,
        options: SstableWriterOptions,
    ) -> HummockResult<Self::Writer>;
}

pub struct BatchSstableWriterFactory {
    sstable_store: SstableStoreRef,
}

impl BatchSstableWriterFactory {
    pub fn new(sstable_store: SstableStoreRef) -> Self {
        BatchSstableWriterFactory { sstable_store }
    }
}

#[async_trait::async_trait]
impl SstableWriterFactory for BatchSstableWriterFactory {
    type Writer = BatchUploadWriter;

    async fn create_sst_writer(
        &mut self,
        object_id: HummockSstableObjectId,
        options: SstableWriterOptions,
    ) -> HummockResult<Self::Writer> {
        Ok(BatchUploadWriter::new(
            object_id,
            self.sstable_store.clone(),
            options,
        ))
    }
}

/// Buffer SST data and upload it as a whole on `finish`.
/// The upload is finished when the returned `JoinHandle` is joined.
pub struct BatchUploadWriter {
    object_id: HummockSstableObjectId,
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
    buf: Vec<u8>,
    block_info: Vec<Block>,
    tracker: Option<MemoryTracker>,
}

impl BatchUploadWriter {
    pub fn new(
        object_id: HummockSstableObjectId,
        sstable_store: Arc<SstableStore>,
        options: SstableWriterOptions,
    ) -> Self {
        Self {
            object_id,
            sstable_store,
            policy: options.policy,
            buf: Vec::with_capacity(options.capacity_hint.unwrap_or(0)),
            block_info: Vec::new(),
            tracker: options.tracker,
        }
    }
}

#[async_trait::async_trait]
impl SstableWriter for BatchUploadWriter {
    type Output = JoinHandle<HummockResult<()>>;

    async fn write_block(&mut self, block: &[u8], meta: &BlockMeta) -> HummockResult<()> {
        self.buf.extend_from_slice(block);
        if let CachePolicy::Fill(_) = self.policy {
            self.block_info.push(Block::decode(
                Bytes::from(block.to_vec()),
                meta.uncompressed_size as usize,
            )?);
        }
        Ok(())
    }

    async fn write_block_bytes(&mut self, block: Bytes, meta: &BlockMeta) -> HummockResult<()> {
        self.buf.extend_from_slice(&block);
        if let CachePolicy::Fill(_) = self.policy {
            self.block_info
                .push(Block::decode(block, meta.uncompressed_size as usize)?);
        }
        Ok(())
    }

    async fn finish(mut self, meta: SstableMeta) -> HummockResult<Self::Output> {
        fail_point!("data_upload_err");
        let join_handle = tokio::spawn(async move {
            meta.encode_to(&mut self.buf);
            let data = Bytes::from(self.buf);
            let _tracker = self.tracker.map(|mut t| {
                if !t.try_increase_memory(data.capacity() as u64) {
                    tracing::debug!("failed to allocate increase memory for data file, sst object id: {}, file size: {}",
                                    self.object_id, data.capacity());
                }
                t
            });

            // Upload data to object store.
            self.sstable_store
                .clone()
                .put_sst_data(self.object_id, data)
                .await?;
            self.sstable_store.insert_meta_cache(self.object_id, meta);

            // Only update recent filter with sst obj id is okay here, for l0 is only filter by sst obj id with recent filter.
            if let Some(filter) = self.sstable_store.recent_filter.as_ref() {
                filter.insert((self.object_id, usize::MAX));
            }

            // Add block cache.
            if let CachePolicy::Fill(fill_cache_priority) = self.policy {
                // The `block_info` may be empty when there is only range-tombstones, because we
                //  store them in meta-block.
                for (block_idx, block) in self.block_info.into_iter().enumerate() {
                    self.sstable_store.block_cache.insert(
                        self.object_id,
                        block_idx as u64,
                        Box::new(block),
                        fill_cache_priority,
                    );
                }
            }
            Ok(())
        });
        Ok(join_handle)
    }

    fn data_len(&self) -> usize {
        self.buf.len()
    }
}

pub struct StreamingUploadWriter {
    object_id: HummockSstableObjectId,
    sstable_store: SstableStoreRef,
    policy: CachePolicy,
    /// Data are uploaded block by block, except for the size footer.
    object_uploader: ObjectStreamingUploader,
    /// Compressed blocks to refill block or meta cache. Keep the uncompressed capacity for decode.
    blocks: Vec<Block>,
    data_len: usize,
    tracker: Option<MemoryTracker>,
}

impl StreamingUploadWriter {
    pub fn new(
        object_id: HummockSstableObjectId,
        sstable_store: SstableStoreRef,
        object_uploader: ObjectStreamingUploader,
        options: SstableWriterOptions,
    ) -> Self {
        Self {
            object_id,
            sstable_store,
            policy: options.policy,
            object_uploader,
            blocks: Vec::new(),
            data_len: 0,
            tracker: options.tracker,
        }
    }
}

pub enum UnifiedSstableWriter {
    StreamingSstableWriter(StreamingUploadWriter),
    BatchSstableWriter(BatchUploadWriter),
}

#[async_trait::async_trait]
impl SstableWriter for StreamingUploadWriter {
    type Output = JoinHandle<HummockResult<()>>;

    async fn write_block(&mut self, block_data: &[u8], meta: &BlockMeta) -> HummockResult<()> {
        self.data_len += block_data.len();
        let block_data = Bytes::from(block_data.to_vec());
        if let CachePolicy::Fill(_) = self.policy {
            let block = Block::decode(block_data.clone(), meta.uncompressed_size as usize)?;
            self.blocks.push(block);
        }
        self.object_uploader
            .write_bytes(block_data)
            .await
            .map_err(Into::into)
    }

    async fn write_block_bytes(&mut self, block: Bytes, meta: &BlockMeta) -> HummockResult<()> {
        self.data_len += block.len();
        if let CachePolicy::Fill(_) = self.policy {
            let block = Block::decode(block.clone(), meta.uncompressed_size as usize)?;
            self.blocks.push(block);
        }
        self.object_uploader
            .write_bytes(block)
            .await
            .map_err(Into::into)
    }

    async fn finish(mut self, meta: SstableMeta) -> HummockResult<UploadJoinHandle> {
        let metadata = Bytes::from(meta.encode_to_bytes());

        self.object_uploader.write_bytes(metadata).await?;
        let join_handle = tokio::spawn(async move {
            let uploader_memory_usage = self.object_uploader.get_memory_usage();
            let _tracker = self.tracker.map(|mut t| {
                    if !t.try_increase_memory(uploader_memory_usage) {
                        tracing::debug!("failed to allocate increase memory for data file, sst object id: {}, file size: {}",
                                        self.object_id, uploader_memory_usage);
                    }
                    t
                });

            assert!(!meta.block_metas.is_empty() || !meta.monotonic_tombstone_events.is_empty());

            // Upload data to object store.
            self.object_uploader.finish().await?;
            // Add meta cache.
            self.sstable_store.insert_meta_cache(self.object_id, meta);

            // Add block cache.
            if let CachePolicy::Fill(fill_high_priority_cache) = self.policy
                && !self.blocks.is_empty()
            {
                for (block_idx, block) in self.blocks.into_iter().enumerate() {
                    self.sstable_store.block_cache.insert(
                        self.object_id,
                        block_idx as u64,
                        Box::new(block),
                        fill_high_priority_cache,
                    );
                }
            }
            Ok(())
        });
        Ok(join_handle)
    }

    fn data_len(&self) -> usize {
        self.data_len
    }
}

pub struct StreamingSstableWriterFactory {
    sstable_store: SstableStoreRef,
}

impl StreamingSstableWriterFactory {
    pub fn new(sstable_store: SstableStoreRef) -> Self {
        StreamingSstableWriterFactory { sstable_store }
    }
}
pub struct UnifiedSstableWriterFactory {
    sstable_store: SstableStoreRef,
}

impl UnifiedSstableWriterFactory {
    pub fn new(sstable_store: SstableStoreRef) -> Self {
        UnifiedSstableWriterFactory { sstable_store }
    }
}

#[async_trait::async_trait]
impl SstableWriterFactory for UnifiedSstableWriterFactory {
    type Writer = UnifiedSstableWriter;

    async fn create_sst_writer(
        &mut self,
        object_id: HummockSstableObjectId,
        options: SstableWriterOptions,
    ) -> HummockResult<Self::Writer> {
        if self.sstable_store.store().support_streaming_upload() {
            let path = self.sstable_store.get_sst_data_path(object_id);
            let uploader = self.sstable_store.store.streaming_upload(&path).await?;
            let streaming_uploader_writer = StreamingUploadWriter::new(
                object_id,
                self.sstable_store.clone(),
                uploader,
                options,
            );

            Ok(UnifiedSstableWriter::StreamingSstableWriter(
                streaming_uploader_writer,
            ))
        } else {
            let batch_uploader_writer =
                BatchUploadWriter::new(object_id, self.sstable_store.clone(), options);
            Ok(UnifiedSstableWriter::BatchSstableWriter(
                batch_uploader_writer,
            ))
        }
    }
}

#[async_trait::async_trait]
impl SstableWriterFactory for StreamingSstableWriterFactory {
    type Writer = StreamingUploadWriter;

    async fn create_sst_writer(
        &mut self,
        object_id: HummockSstableObjectId,
        options: SstableWriterOptions,
    ) -> HummockResult<Self::Writer> {
        let path = self.sstable_store.get_sst_data_path(object_id);
        let uploader = self.sstable_store.store.streaming_upload(&path).await?;
        Ok(StreamingUploadWriter::new(
            object_id,
            self.sstable_store.clone(),
            uploader,
            options,
        ))
    }
}

#[async_trait::async_trait]
impl SstableWriter for UnifiedSstableWriter {
    type Output = JoinHandle<HummockResult<()>>;

    async fn write_block(&mut self, block_data: &[u8], meta: &BlockMeta) -> HummockResult<()> {
        match self {
            UnifiedSstableWriter::StreamingSstableWriter(stream) => {
                stream.write_block(block_data, meta).await
            }
            UnifiedSstableWriter::BatchSstableWriter(batch) => {
                batch.write_block(block_data, meta).await
            }
        }
    }

    async fn write_block_bytes(&mut self, block: Bytes, meta: &BlockMeta) -> HummockResult<()> {
        match self {
            UnifiedSstableWriter::StreamingSstableWriter(stream) => {
                stream.write_block_bytes(block, meta).await
            }
            UnifiedSstableWriter::BatchSstableWriter(batch) => {
                batch.write_block_bytes(block, meta).await
            }
        }
    }

    async fn finish(self, meta: SstableMeta) -> HummockResult<UploadJoinHandle> {
        match self {
            UnifiedSstableWriter::StreamingSstableWriter(stream) => stream.finish(meta).await,
            UnifiedSstableWriter::BatchSstableWriter(batch) => batch.finish(meta).await,
        }
    }

    fn data_len(&self) -> usize {
        match self {
            UnifiedSstableWriter::StreamingSstableWriter(stream) => stream.data_len(),
            UnifiedSstableWriter::BatchSstableWriter(batch) => batch.data_len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;

    use risingwave_hummock_sdk::HummockSstableObjectId;
    use risingwave_pb::hummock::SstableInfo;

    use super::{SstableStoreRef, SstableWriterOptions};
    use crate::hummock::iterator::test_utils::{iterator_test_key_of, mock_sstable_store};
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::sstable::SstableIteratorReadOptions;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_data, put_sst,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{CachePolicy, SstableIterator, SstableMeta, SstableStore};
    use crate::monitor::StoreLocalStatistic;

    const SST_ID: HummockSstableObjectId = 1;

    fn get_hummock_value(x: usize) -> HummockValue<Vec<u8>> {
        HummockValue::put(format!("overlapped_new_{}", x).as_bytes().to_vec())
    }

    async fn validate_sst(
        sstable_store: SstableStoreRef,
        info: &SstableInfo,
        mut meta: SstableMeta,
        x_range: Range<usize>,
    ) {
        let mut stats = StoreLocalStatistic::default();
        let holder = sstable_store.sstable(info, &mut stats).await.unwrap();
        std::mem::take(&mut meta.bloom_filter);
        assert_eq!(holder.meta, meta);
        let holder = sstable_store.sstable(info, &mut stats).await.unwrap();
        assert_eq!(holder.meta, meta);
        let mut iter = SstableIterator::new(
            holder,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );
        iter.rewind().await.unwrap();
        for i in x_range {
            let key = iter.key();
            let value = iter.value();
            assert_eq!(key, iterator_test_key_of(i).to_ref());
            assert_eq!(value, get_hummock_value(i).as_slice());
            iter.next().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_batch_upload() {
        let sstable_store = mock_sstable_store();
        let x_range = 0..100;
        let (data, meta) = gen_test_sstable_data(
            default_builder_opt_for_test(),
            x_range
                .clone()
                .map(|x| (iterator_test_key_of(x), get_hummock_value(x))),
        )
        .await;
        let writer_opts = SstableWriterOptions {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::Disable,
        };
        let info = put_sst(
            SST_ID,
            data.clone(),
            meta.clone(),
            sstable_store.clone(),
            writer_opts,
        )
        .await
        .unwrap();

        validate_sst(sstable_store, &info, meta, x_range).await;
    }

    #[tokio::test]
    async fn test_streaming_upload() {
        // Generate test data.
        let sstable_store = mock_sstable_store();
        let x_range = 0..100;
        let (data, meta) = gen_test_sstable_data(
            default_builder_opt_for_test(),
            x_range
                .clone()
                .map(|x| (iterator_test_key_of(x), get_hummock_value(x))),
        )
        .await;
        let writer_opts = SstableWriterOptions {
            capacity_hint: None,
            tracker: None,
            policy: CachePolicy::Disable,
        };
        let info = put_sst(
            SST_ID,
            data.clone(),
            meta.clone(),
            sstable_store.clone(),
            writer_opts,
        )
        .await
        .unwrap();

        validate_sst(sstable_store, &info, meta, x_range).await;
    }

    #[test]
    fn test_basic() {
        let sstable_store = mock_sstable_store();
        let object_id = 123;
        let data_path = sstable_store.get_sst_data_path(object_id);
        assert_eq!(data_path, "test/123.data");
        assert_eq!(SstableStore::get_object_id_from_path(&data_path), object_id);
    }
}
