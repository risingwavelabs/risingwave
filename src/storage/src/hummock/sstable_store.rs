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
use std::clone::Clone;
use std::collections::VecDeque;
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::{Bytes, BytesMut};
use fail::fail_point;
use futures::{future, StreamExt};
use itertools::Itertools;
use risingwave_common::cache::{CachePriority, LookupResponse, LruCacheEventListener};
use risingwave_common::config::StorageMemoryConfig;
use risingwave_hummock_sdk::{HummockSstableObjectId, OBJECT_SUFFIX};
use risingwave_hummock_trace::TracedCachePolicy;
use risingwave_object_store::object::{
    MonitoredStreamingReader, ObjectError, ObjectMetadataIter, ObjectStoreRef,
    ObjectStreamingUploader,
};
use risingwave_pb::hummock::SstableInfo;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use zstd::zstd_safe::WriteBuf;

use super::utils::MemoryTracker;
use super::{
    Block, BlockCache, BlockMeta, BlockResponse, CachedBlock, FileCache, RecentFilter, Sstable,
    SstableBlockIndex, SstableMeta, SstableWriter,
};
use crate::hummock::file_cache::preclude::*;
use crate::hummock::multi_builder::UploadJoinHandle;
use crate::hummock::{
    BlockHolder, CacheableEntry, HummockError, HummockResult, LruCache, MemoryLimiter,
};
use crate::monitor::{MemoryCollector, StoreLocalStatistic};

const MAX_META_CACHE_SHARD_BITS: usize = 2;
const MAX_CACHE_SHARD_BITS: usize = 6; // It means that there will be 64 shards lru-cache to avoid lock conflict.
const MIN_BUFFER_SIZE_PER_SHARD: usize = 256 * 1024 * 1024; // 256MB

pub type TableHolder = CacheableEntry<HummockSstableObjectId, Box<Sstable>>;

// TODO: Define policy based on use cases (read / compaction / ...).
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum CachePolicy {
    /// Disable read cache and not fill the cache afterwards.
    Disable,
    /// Try reading the cache and fill the cache afterwards.
    Fill(CachePriority),
    /// Fill file cache only.
    FillFileCache,
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

impl Default for CachePolicy {
    fn default() -> Self {
        CachePolicy::Fill(CachePriority::High)
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

struct BlockCacheEventListener {
    data_file_cache: FileCache<SstableBlockIndex, CachedBlock>,
}

impl LruCacheEventListener for BlockCacheEventListener {
    type K = (u64, u64);
    type T = Box<Block>;

    fn on_release(&self, key: Self::K, value: Self::T) {
        let key = SstableBlockIndex {
            sst_id: key.0,
            block_idx: key.1,
        };
        // temporarily avoid spawn task while task drop with madsim
        // FYI: https://github.com/madsim-rs/madsim/issues/182
        #[cfg(not(madsim))]
        self.data_file_cache
            .insert_if_not_exists_async(key, CachedBlock::Loaded { block: value });
    }
}

struct MetaCacheEventListener(FileCache<HummockSstableObjectId, Box<Sstable>>);

impl LruCacheEventListener for MetaCacheEventListener {
    type K = HummockSstableObjectId;
    type T = Box<Sstable>;

    fn on_release(&self, key: Self::K, value: Self::T) {
        // temporarily avoid spawn task while task drop with madsim
        // FYI: https://github.com/madsim-rs/madsim/issues/182
        #[cfg(not(madsim))]
        self.0.insert_if_not_exists_async(key, value);
    }
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    block_cache: BlockCache,
    meta_cache: Arc<LruCache<HummockSstableObjectId, Box<Sstable>>>,

    data_file_cache: FileCache<SstableBlockIndex, CachedBlock>,
    meta_file_cache: FileCache<HummockSstableObjectId, Box<Sstable>>,

    recent_filter: Option<Arc<RecentFilter<HummockSstableObjectId>>>,
    pending_streaming_loading: Arc<AtomicUsize>,
    large_query_memory_usage: usize,
}

impl SstableStore {
    pub fn new(
        store: ObjectStoreRef,
        path: String,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
        high_priority_ratio: usize,
        large_query_memory_usage: usize,
        data_file_cache: FileCache<SstableBlockIndex, CachedBlock>,
        meta_file_cache: FileCache<HummockSstableObjectId, Box<Sstable>>,
        recent_filter: Option<Arc<RecentFilter<HummockSstableObjectId>>>,
    ) -> Self {
        // TODO: We should validate path early. Otherwise object store won't report invalid path
        // error until first write attempt.
        let mut shard_bits = MAX_META_CACHE_SHARD_BITS;
        while (meta_cache_capacity >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0 {
            shard_bits -= 1;
        }
        let block_cache_listener = Arc::new(BlockCacheEventListener {
            data_file_cache: data_file_cache.clone(),
        });
        let meta_cache_listener = Arc::new(MetaCacheEventListener(meta_file_cache.clone()));
        Self {
            path,
            store,
            block_cache: BlockCache::with_event_listener(
                block_cache_capacity,
                MAX_CACHE_SHARD_BITS,
                high_priority_ratio,
                block_cache_listener,
            ),
            meta_cache: Arc::new(LruCache::with_event_listener(
                shard_bits,
                meta_cache_capacity,
                0,
                meta_cache_listener,
            )),

            data_file_cache,
            meta_file_cache,

            recent_filter,
            pending_streaming_loading: Arc::new(AtomicUsize::new(0)),
            large_query_memory_usage,
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
        let meta_cache = Arc::new(LruCache::new(0, meta_cache_capacity, 0));
        Self {
            path,
            store,
            block_cache: BlockCache::new(block_cache_capacity, 0, 0),
            meta_cache,
            data_file_cache: FileCache::none(),
            meta_file_cache: FileCache::none(),
            pending_streaming_loading: Arc::new(AtomicUsize::new(0)),
            large_query_memory_usage: block_cache_capacity,
            recent_filter: None,
        }
    }

    pub async fn delete(&self, object_id: HummockSstableObjectId) -> HummockResult<()> {
        // Data
        self.store
            .delete(self.get_sst_data_path(object_id).as_str())
            .await?;
        self.meta_cache.erase(object_id, &object_id);
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
            self.meta_cache.erase(object_id, &object_id);
            self.meta_file_cache
                .remove(&object_id)
                .map_err(HummockError::file_cache)?;
        }

        Ok(())
    }

    pub fn delete_cache(&self, object_id: HummockSstableObjectId) {
        self.meta_cache.erase(object_id, &object_id);
        if let Err(e) = self.meta_file_cache.remove(&object_id) {
            tracing::warn!("meta file cache remove error: {}", e);
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
            .map_err(HummockError::object_io_error)
    }

    pub async fn get_stream(
        &self,
        sst: &Sstable,
        start_index: usize,
        mut end_index: usize,
    ) -> HummockResult<Option<BatchBlockStream>> {
        let object_id = sst.id;
        if self.block_cache.exists_block(object_id, start_index as u64) {
            return Ok(None);
        }
        let start_offset = sst.meta.block_metas[start_index].offset as usize;
        let mut end_offset = start_offset;
        for idx in start_index..end_index {
            if self.block_cache.exists_block(object_id, idx as u64) {
                end_index = idx;
                break;
            }
            end_offset += sst.meta.block_metas[idx].len as usize;
        }
        let pending_data = self.pending_streaming_loading.load(Ordering::SeqCst);
        if end_offset == start_offset || pending_data > self.large_query_memory_usage {
            return Ok(None);
        }
        let data_path = self.get_sst_data_path(object_id);
        let memory_usage = std::cmp::min(end_offset - start_offset, self.store.recv_buffer_size());
        self.pending_streaming_loading
            .fetch_add(memory_usage, Ordering::SeqCst);
        let reader = self
            .store
            .streaming_read(&data_path, start_offset..end_offset)
            .await?;
        let block_metas = sst.meta.block_metas[start_index..end_index].to_vec();
        Ok(Some(BatchBlockStream::new(
            reader,
            self.block_cache.clone(),
            object_id,
            start_index,
            memory_usage,
            block_metas,
            self.pending_streaming_loading.clone(),
        )))
    }

    pub async fn get_with_prefetch(
        &self,
        sst: &Sstable,
        block_index: usize,
        max_prefetch_block_count: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        const MAX_PREFETCH_BLOCK: usize = 16;
        let object_id = sst.id;
        if let Some(block) = self.block_cache.get(object_id, block_index as u64) {
            return Ok(block);
        }
        let max_prefetch_block_count = std::cmp::min(max_prefetch_block_count, MAX_PREFETCH_BLOCK);
        let mut end_index = std::cmp::min(
            block_index + max_prefetch_block_count,
            sst.meta.block_metas.len(),
        );
        let start_offset = sst.meta.block_metas[block_index].offset as usize;
        let mut end_offset = start_offset;
        for idx in block_index..end_index {
            if self.block_cache.exists_block(object_id, idx as u64) {
                end_index = idx;
                break;
            }
            end_offset += sst.meta.block_metas[idx].len as usize;
        }
        if end_index == block_index {
            let resp = self
                .get_block_response(sst, block_index, policy, stats)
                .await?;
            return resp.wait().await;
        }
        let data_path = self.get_sst_data_path(object_id);
        let buf = self
            .store
            .read(&data_path, start_offset..end_offset)
            .await?;
        let mut offset = 0;
        let mut first_holder = None;
        for idx in block_index..end_index {
            let end = offset + sst.meta.block_metas[idx].len as usize;
            if end > buf.len() {
                return Err(ObjectError::internal("read unexpected EOF").into());
            }
            // copy again to avoid holding a large data in memory.
            let block = Block::decode(
                Bytes::copy_from_slice(&buf[offset..end]),
                sst.meta.block_metas[idx].uncompressed_size as usize,
            )?;
            let holder = self.block_cache.insert(
                object_id,
                (block_index + idx) as u64,
                Box::new(block),
                CachePriority::Low,
            );
            if block_index == idx {
                first_holder = Some(holder);
            }
            offset = end;
        }
        Ok(first_holder.unwrap())
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
                    let block = block.into_inner();
                    return Ok(block);
                }

                let block_data = store.read(&data_path, range).await?;
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
            filter.insert(object_id);
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
            tracing::warn!("data file cache clear error: {}", e);
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear_meta_cache(&self) {
        self.meta_cache.clear();
        if let Err(e) = self.meta_file_cache.clear() {
            tracing::warn!("meta file cache clear error: {}", e);
        }
    }

    /// Returns `table_holder`
    pub fn sstable(
        &self,
        sst: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> impl Future<Output = HummockResult<TableHolder>> + Send + 'static {
        let object_id = sst.get_object_id();
        let lookup_response = self
            .meta_cache
            .lookup_with_request_dedup::<_, HummockError, _>(
                object_id,
                object_id,
                CachePriority::High,
                || {
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
                            let charge = sst.estimate_size();
                            return Ok((sst, charge));
                        }

                        let now = Instant::now();
                        let buf = store
                            .read(&meta_path, range)
                            .await
                            .map_err(HummockError::object_io_error)?;
                        let meta = SstableMeta::decode(&buf[..])?;

                        let sst = Sstable::new(object_id, meta);
                        let charge = sst.estimate_size();
                        let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
                        stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
                        Ok((Box::new(sst), charge))
                    }
                },
            );
        match &lookup_response {
            LookupResponse::Miss(_) | LookupResponse::WaitPendingRequest(_) => {
                stats.cache_meta_block_miss += 1;
            }
            _ => (),
        }
        stats.cache_meta_block_total += 1;
        lookup_response.verbose_instrument_await("sstable")
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
        self.meta_cache.insert(
            object_id,
            object_id,
            charge,
            Box::new(sst),
            CachePriority::High,
        );
    }

    pub fn insert_block_cache(
        &self,
        object_id: HummockSstableObjectId,
        block_index: u64,
        block: Box<Block>,
    ) {
        if let Some(filter) = self.recent_filter.as_ref() {
            filter.insert(object_id);
        }
        self.block_cache
            .insert(object_id, block_index, block, CachePriority::High);
    }

    pub fn get_meta_memory_usage(&self) -> u64 {
        self.meta_cache.get_memory_usage() as u64
    }

    pub async fn get_stream_by_position(
        &self,
        object_id: HummockSstableObjectId,
        block_index: usize,
        metas: &[BlockMeta],
    ) -> HummockResult<BlockStream> {
        fail_point!("get_stream_err");
        let data_path = self.get_sst_data_path(object_id);
        let store = self.store().clone();
        let block_meta = metas
            .get(block_index)
            .ok_or_else(HummockError::invalid_block)?;
        let start_pos = block_meta.offset as usize;
        let end_pos = metas[block_index..]
            .iter()
            .map(|meta| meta.len as usize)
            .sum::<usize>()
            + start_pos;
        let range = start_pos..end_pos;

        Ok(BlockStream::new(
            store
                .streaming_read(&data_path, range)
                .await
                .map_err(HummockError::object_io_error)?,
            block_index,
            metas,
        ))
    }

    pub fn data_recent_filter(&self) -> Option<&Arc<RecentFilter<HummockSstableObjectId>>> {
        self.recent_filter.as_ref()
    }

    pub fn data_file_cache(&self) -> &FileCache<SstableBlockIndex, CachedBlock> {
        &self.data_file_cache
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

            if let Some(filter) = self.sstable_store.recent_filter.as_ref() {
                filter.insert(self.object_id);
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
            .map_err(HummockError::object_io_error)
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
            .map_err(HummockError::object_io_error)
    }

    async fn finish(mut self, meta: SstableMeta) -> HummockResult<UploadJoinHandle> {
        let meta_data = Bytes::from(meta.encode_to_bytes());

        self.object_uploader
            .write_bytes(meta_data)
            .await
            .map_err(HummockError::object_io_error)?;
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
            self.object_uploader
                .finish()
                .await
                .map_err(HummockError::object_io_error)?;
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

pub struct BlockStreamReader {
    /// The stream that provides raw data.
    byte_stream: MonitoredStreamingReader,
}

impl BlockStreamReader {
    fn new(byte_stream: MonitoredStreamingReader) -> Self {
        Self { byte_stream }
    }

    async fn next_block(
        &mut self,
        buf: Bytes,
        offset: usize,
        read_size: usize,
    ) -> HummockResult<(Bytes, Bytes)> {
        let mut read_buf = BytesMut::with_capacity(read_size);
        let start_pos = if offset < buf.len() {
            read_buf.extend_from_slice(&buf[offset..]);
            buf.len() - offset
        } else {
            0
        };
        let mut rest = read_size - start_pos;
        while rest > 0 {
            let next_packet = self
                .byte_stream
                .read_bytes()
                .await
                .unwrap_or_else(|| Err(ObjectError::internal("read unexpected EOF")))?;
            let read_len = std::cmp::min(next_packet.len(), rest);
            read_buf.extend_from_slice(&next_packet[..read_len]);
            rest -= read_len;
            if rest == 0 {
                return Ok((read_buf.freeze(), next_packet.slice(read_len..)));
            }
        }
        Ok((read_buf.freeze(), Bytes::default()))
    }
}

/// An iterator that reads the blocks of an SST step by step from a given stream of bytes.
pub struct BlockStream {
    reader: BlockStreamReader,

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

impl BlockStream {
    /// Constructs a new `BlockStream` object that reads from the given `byte_stream` and interprets
    /// the data as blocks of the SST described in `sst_meta`, starting at block `block_index`.
    ///
    /// If `block_index >= sst_meta.block_metas.len()`, then `BlockStream` will not read any data
    /// from `byte_stream`.
    fn new(
        // The stream that provides raw data.
        byte_stream: MonitoredStreamingReader,

        // Index of the SST's block where the stream starts.
        block_index: usize,

        // Meta data of the SST that is streamed.
        metas: &[BlockMeta],
    ) -> Self {
        // Avoids panicking if `block_index` is too large.
        let block_index = std::cmp::min(block_index, metas.len());

        Self {
            reader: BlockStreamReader::new(byte_stream),
            block_idx: 0,
            block_metas: metas[block_index..].to_vec(),
            buf: Bytes::default(),
            buff_offset: 0,
        }
    }

    /// Reads the next block from the stream and returns it. Returns `None` if there are no blocks
    /// left to read.
    pub async fn next(&mut self) -> HummockResult<Option<(Bytes, BlockMeta)>> {
        if self.block_idx >= self.block_metas.len() {
            return Ok(None);
        }

        let block_meta = &self.block_metas[self.block_idx];
        fail_point!("stream_read_err", |_| Err(HummockError::object_io_error(
            ObjectError::internal("stream read error")
        )));
        let end = self.buff_offset + block_meta.len as usize;
        let data = if end > self.buf.len() {
            let (current_block, buf) = self
                .reader
                .next_block(self.buf.clone(), self.buff_offset, block_meta.len as usize)
                .await?;
            self.buff_offset = 0;
            self.buf = buf;
            current_block
        } else {
            let data = self.buf.slice(self.buff_offset..end);
            self.buff_offset = end;
            data
        };

        self.block_idx += 1;
        Ok(Some((data, block_meta.clone())))
    }

    pub async fn next_block(&mut self) -> HummockResult<Option<Box<Block>>> {
        match self.next().await? {
            None => Ok(None),
            Some((buf, meta)) => Ok(Some(Box::new(Block::decode(
                buf,
                meta.uncompressed_size as usize,
            )?))),
        }
    }
}

/// An iterator that reads the blocks of an SST step by step from a given stream of bytes optimize for batch query.
/// Because all data in batch query shall be put into block-cache, we can not assign a reference of Bytes for each block.
/// We must copy them into new memory.
pub struct BatchBlockStream {
    reader: BlockStreamReader,

    block_idx: usize,

    block_metas: Vec<BlockMeta>,

    buf: Bytes,

    buff_offset: usize,

    blocks: VecDeque<BlockHolder>,

    cache: BlockCache,

    object_id: HummockSstableObjectId,

    start_block_index: usize,
    memory_usage: usize,
    /// To avoid high frequently query cost too much memory.
    pending_streaming_loading: Arc<AtomicUsize>,
}

impl BatchBlockStream {
    fn new(
        byte_stream: MonitoredStreamingReader,
        cache: BlockCache,
        object_id: HummockSstableObjectId,
        start_block_index: usize,
        memory_usage: usize,
        block_metas: Vec<BlockMeta>,
        pending_streaming_loading: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            reader: BlockStreamReader::new(byte_stream),
            block_idx: 0,
            block_metas,
            buf: Bytes::default(),
            buff_offset: 0,
            object_id,
            memory_usage,
            cache,
            blocks: VecDeque::default(),
            start_block_index,
            pending_streaming_loading,
        }
    }

    /// Reads the next block from the stream and returns it. Returns `None` if there are no blocks
    /// left to read.
    pub async fn next_block(&mut self) -> HummockResult<Option<BlockHolder>> {
        if self.block_idx >= self.block_metas.len() {
            return Ok(None);
        }

        let block_meta = &self.block_metas[self.block_idx];
        fail_point!("stream_batch_read_err", |_| Err(
            HummockError::object_io_error(ObjectError::internal("stream read error"))
        ));
        if let Some(block) = self.blocks.pop_front() {
            self.block_idx += 1;
            return Ok(Some(block));
        }
        let (block, buf) = self
            .reader
            .next_block(self.buf.clone(), self.buff_offset, block_meta.len as usize)
            .await?;
        self.buf = buf;
        self.buff_offset = 0;
        let block = Block::decode(
            block,
            self.block_metas[self.block_idx].uncompressed_size as usize,
        )?;
        let holder = self.cache.insert(
            self.object_id,
            (self.start_block_index + self.block_idx) as u64,
            Box::new(block),
            CachePriority::Low,
        );
        let mut block_idx = self.block_idx + 1;
        let mut buff_offset = self.buff_offset;
        while block_idx < self.block_metas.len() {
            let end = buff_offset + self.block_metas[block_idx].len as usize;
            if end > self.buf.len() {
                break;
            }
            // copy again to avoid hold a large bytes reference in block-cache.
            let block = Block::decode(
                Bytes::copy_from_slice(&self.buf[buff_offset..end]),
                self.block_metas[block_idx].uncompressed_size as usize,
            )?;
            let next_holder = self.cache.insert(
                self.object_id,
                (self.start_block_index + block_idx) as u64,
                Box::new(block),
                CachePriority::Low,
            );
            self.blocks.push_back(next_holder);
            buff_offset = end;
            block_idx += 1;
        }

        self.buff_offset = buff_offset;
        self.block_idx += 1;
        Ok(Some(holder))
    }

    pub fn next_block_index(&self) -> usize {
        self.block_idx + self.start_block_index
    }
}

impl Drop for BatchBlockStream {
    fn drop(&mut self) {
        self.pending_streaming_loading
            .fetch_sub(self.memory_usage, Ordering::SeqCst);
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
        assert_eq!(holder.value().meta, meta);
        let holder = sstable_store.sstable(info, &mut stats).await.unwrap();
        assert_eq!(holder.value().meta, meta);
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
