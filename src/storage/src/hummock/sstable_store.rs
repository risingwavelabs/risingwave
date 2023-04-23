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
use std::sync::atomic::Ordering;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::{Buf, BufMut, Bytes};
use fail::fail_point;
use itertools::Itertools;
use risingwave_common::cache::{CachePriority, LookupResponse, LruCacheEventListener};
use risingwave_hummock_sdk::{HummockSstableObjectId, OBJECT_SUFFIX};
use risingwave_object_store::object::{
    BlockLocation, MonitoredStreamingReader, ObjectError, ObjectMetadata, ObjectStoreRef,
    ObjectStreamingUploader,
};
use risingwave_pb::hummock::SstableInfo;
use tokio::task::JoinHandle;
use zstd::zstd_safe::WriteBuf;

use super::utils::MemoryTracker;
use super::{
    Block, BlockCache, BlockMeta, BlockResponse, Sstable, SstableMeta, SstableWriter, TieredCache,
    TieredCacheKey, TieredCacheValue,
};
use crate::hummock::multi_builder::UploadJoinHandle;
use crate::hummock::{
    BlockHolder, CacheableEntry, HummockError, HummockResult, LruCache, MemoryLimiter,
};
use crate::monitor::{MemoryCollector, StoreLocalStatistic};

const MAX_META_CACHE_SHARD_BITS: usize = 2;
const MAX_CACHE_SHARD_BITS: usize = 6; // It means that there will be 64 shards lru-cache to avoid lock conflict.
const MIN_BUFFER_SIZE_PER_SHARD: usize = 256 * 1024 * 1024; // 256MB

pub type TableHolder = CacheableEntry<HummockSstableObjectId, Box<Sstable>>;

// BEGIN section for tiered cache

impl TieredCacheKey for (HummockSstableObjectId, u64) {
    fn encoded_len() -> usize {
        16
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_u64(self.0);
        buf.put_u64(self.1);
    }

    fn decode(mut buf: &[u8]) -> Self {
        let object_id = buf.get_u64();
        let block_idx = buf.get_u64();
        (object_id, block_idx)
    }
}

impl TieredCacheValue for Box<Block> {
    fn len(&self) -> usize {
        self.raw_data().len()
    }

    fn encoded_len(&self) -> usize {
        self.raw_data().len()
    }

    fn encode(&self, mut buf: &mut [u8]) {
        buf.put_slice(self.raw_data());
    }

    fn decode(buf: Vec<u8>) -> Self {
        Box::new(Block::decode_from_raw(Bytes::from(buf)))
    }
}

pub struct BlockCacheEventListener {
    tiered_cache: TieredCache<(HummockSstableObjectId, u64), Box<Block>>,
}

impl LruCacheEventListener for BlockCacheEventListener {
    type K = (HummockSstableObjectId, u64);
    type T = Box<Block>;

    fn on_release(&self, key: Self::K, value: Self::T) {
        // TODO(MrCroxx): handle error?
        self.tiered_cache.insert(key, value).unwrap();
    }
}

// END section for tiered cache

// TODO: Define policy based on use cases (read / compaction / ...).
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum CachePolicy {
    /// Disable read cache and not fill the cache afterwards.
    Disable,
    /// Try reading the cache and fill the cache afterwards.
    Fill(CachePriority),
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

impl Default for CachePolicy {
    fn default() -> Self {
        CachePolicy::Fill(CachePriority::High)
    }
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    block_cache: BlockCache,
    meta_cache: Arc<LruCache<HummockSstableObjectId, Box<Sstable>>>,
    tiered_cache: TieredCache<(HummockSstableObjectId, u64), Box<Block>>,
}

impl SstableStore {
    pub fn new(
        store: ObjectStoreRef,
        path: String,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
        high_priority_ratio: usize,
        tiered_cache: TieredCache<(HummockSstableObjectId, u64), Box<Block>>,
    ) -> Self {
        // TODO: We should validate path early. Otherwise object store won't report invalid path
        // error until first write attempt.
        let mut shard_bits = MAX_META_CACHE_SHARD_BITS;
        while (meta_cache_capacity >> shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && shard_bits > 0 {
            shard_bits -= 1;
        }
        let meta_cache = Arc::new(LruCache::new(shard_bits, meta_cache_capacity, 0));
        let listener = Arc::new(BlockCacheEventListener {
            tiered_cache: tiered_cache.clone(),
        });

        Self {
            path,
            store,
            block_cache: BlockCache::with_event_listener(
                block_cache_capacity,
                MAX_CACHE_SHARD_BITS,
                high_priority_ratio,
                listener,
            ),
            meta_cache,
            tiered_cache,
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
        let tiered_cache = TieredCache::none();
        Self {
            path,
            store,
            block_cache: BlockCache::new(block_cache_capacity, 0, 0),
            meta_cache,
            tiered_cache,
        }
    }

    pub async fn delete(&self, object_id: HummockSstableObjectId) -> HummockResult<()> {
        // Data
        self.store
            .delete(self.get_sst_data_path(object_id).as_str())
            .await?;
        self.meta_cache.erase(object_id, &object_id);
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
        }

        Ok(())
    }

    pub fn delete_cache(&self, object_id: HummockSstableObjectId) {
        self.meta_cache.erase(object_id, &object_id);
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

    pub async fn get_block_response(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockResponse> {
        let object_id = sst.id;
        let (block_loc, uncompressed_capacity) = sst.calculate_block_info(block_index);

        stats.cache_data_block_total += 1;
        let mut fetch_block = || {
            let tiered_cache = self.tiered_cache.clone();
            stats.cache_data_block_miss += 1;
            let data_path = self.get_sst_data_path(object_id);
            let store = self.store.clone();
            let use_tiered_cache = !matches!(policy, CachePolicy::Disable);

            async move {
                if use_tiered_cache && let Some(holder) = tiered_cache
                    .get(&(object_id, block_index as u64))
                    .await
                    .map_err(HummockError::tiered_cache)?
                {
                    // TODO(MrCroxx): `into_owned()` may perform buffer copy, eliminate it later.
                    return Ok(holder.into_owned());
                }

                let block_data = store.read(&data_path, Some(block_loc)).await?;
                let block = Block::decode(block_data, uncompressed_capacity)?;
                Ok(Box::new(block))
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

        match policy {
            CachePolicy::Fill(priority) => Ok(self.block_cache.get_or_insert_with(
                object_id,
                block_index as u64,
                priority,
                fetch_block,
            )),
            CachePolicy::NotFill => match self.block_cache.get(object_id, block_index as u64) {
                Some(block) => Ok(BlockResponse::Block(block)),
                None => match self
                    .tiered_cache
                    .get(&(object_id, block_index as u64))
                    .await
                    .map_err(HummockError::tiered_cache)?
                {
                    Some(holder) => Ok(BlockResponse::Block(BlockHolder::from_tiered_cache(
                        holder.into_inner(),
                    ))),
                    None => fetch_block()
                        .await
                        .map(BlockHolder::from_owned_block)
                        .map(BlockResponse::Block),
                },
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
        let obj_prefix = self.store.get_object_prefix(object_id, true);
        format!(
            "{}/{}{}.{}",
            self.path, obj_prefix, object_id, OBJECT_SUFFIX
        )
    }

    pub fn get_object_id_from_path(&self, path: &str) -> HummockSstableObjectId {
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

    pub fn get_meta_cache(&self) -> Arc<LruCache<HummockSstableObjectId, Box<Sstable>>> {
        self.meta_cache.clone()
    }

    pub fn get_block_cache(&self) -> BlockCache {
        self.block_cache.clone()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear_block_cache(&self) {
        self.block_cache.clear();
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear_meta_cache(&self) {
        self.meta_cache.clear();
    }

    /// Returns `table_holder`, `local_cache_meta_block_miss` (1 if cache miss) and
    /// `local_cache_meta_block_unhit` (1 if not cache hit).
    pub async fn sstable_syncable(
        &self,
        sst: &SstableInfo,
        stats: &StoreLocalStatistic,
    ) -> HummockResult<(TableHolder, u64, u64)> {
        let mut local_cache_meta_block_miss = 0;
        let mut local_cache_meta_block_unhit = 0;
        let object_id = sst.get_object_id();
        let lookup_response = self
            .meta_cache
            .lookup_with_request_dedup::<_, HummockError, _>(
                object_id,
                object_id,
                CachePriority::High,
                || {
                    let store = self.store.clone();
                    let meta_path = self.get_sst_data_path(object_id);
                    local_cache_meta_block_miss += 1;
                    let stats_ptr = stats.remote_io_time.clone();
                    let loc = BlockLocation {
                        offset: sst.meta_offset as usize,
                        size: (sst.file_size - sst.meta_offset) as usize,
                    };
                    async move {
                        let now = minstant::Instant::now();
                        let buf = store
                            .read(&meta_path, Some(loc))
                            .await
                            .map_err(HummockError::object_io_error)?;
                        let meta = SstableMeta::decode(&mut &buf[..])?;
                        let sst = Sstable::new(object_id, meta);
                        let charge = sst.estimate_size();
                        let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
                        stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
                        Ok((Box::new(sst), charge))
                    }
                },
            );
        if !matches!(lookup_response, LookupResponse::Cached(..)) {
            local_cache_meta_block_unhit += 1;
        }
        let result = lookup_response
            .verbose_instrument_await("meta_cache_lookup")
            .await;
        result.map(|table_holder| {
            (
                table_holder,
                local_cache_meta_block_miss,
                local_cache_meta_block_unhit,
            )
        })
    }

    pub async fn sstable(
        &self,
        sst: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        self.sstable_syncable(sst, stats).await.map(
            |(table_holder, local_cache_meta_block_miss, ..)| {
                stats.apply_meta_fetch(local_cache_meta_block_miss);
                table_holder
            },
        )
    }

    pub async fn list_ssts_from_object_store(&self) -> HummockResult<Vec<ObjectMetadata>> {
        self.store
            .list(&format!("{}/", self.path))
            .await
            .map(|v| {
                v.into_iter()
                    .filter(|m| m.key.ends_with(&format!(".{}", OBJECT_SUFFIX)))
                    .collect()
            })
            .map_err(HummockError::object_io_error)
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
        self.block_cache
            .insert(object_id, block_index, block, CachePriority::High);
    }

    pub fn get_meta_memory_usage(&self) -> u64 {
        self.meta_cache.get_memory_usage() as u64
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

        let data_path = self.get_sst_data_path(sst.id);
        let store = self.store().clone();

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

pub type SstableStoreRef = Arc<SstableStore>;

pub struct HummockMemoryCollector {
    sstable_store: SstableStoreRef,
    limiter: Arc<MemoryLimiter>,
}

impl HummockMemoryCollector {
    pub fn new(sstable_store: SstableStoreRef, limiter: Arc<MemoryLimiter>) -> Self {
        Self {
            sstable_store,
            limiter,
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
}

pub struct SstableWriterOptions {
    /// Total length of SST data.
    pub capacity_hint: Option<usize>,
    pub tracker: Option<MemoryTracker>,
    pub policy: CachePolicy,
}
#[async_trait::async_trait]
pub trait SstableWriterFactory: Send + Sync {
    type Writer: SstableWriter<Output = UploadJoinHandle>;

    async fn create_sst_writer(
        &self,
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
        &self,
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

            // Upload data to object store.
            self.object_uploader
                .finish()
                .await
                .map_err(HummockError::object_io_error)?;
            self.sstable_store.insert_meta_cache(self.object_id, meta);

            // Add block cache.
            if let CachePolicy::Fill(fill_high_priority_cache) = self.policy {
                debug_assert!(!self.blocks.is_empty());
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
        &self,
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

/// An iterator that reads the blocks of an SST step by step from a given stream of bytes.
pub struct BlockStream {
    /// The stream that provides raw data.
    byte_stream: MonitoredStreamingReader,

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
        byte_stream: MonitoredStreamingReader,

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
    pub async fn next(&mut self) -> HummockResult<Option<Box<Block>>> {
        if self.block_idx >= self.block_size_vec.len() {
            return Ok(None);
        }

        let (block_stream_size, block_full_size) =
            *self.block_size_vec.get(self.block_idx).unwrap();
        let mut buffer = vec![0; block_stream_size];

        let bytes_read = self
            .byte_stream
            .read_bytes(&mut buffer[..])
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

        Ok(Some(boxed_block))
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
    use crate::hummock::{CachePolicy, SstableIterator, SstableMeta};
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
        assert_eq!(sstable_store.get_object_id_from_path(&data_path), object_id);
    }
}
