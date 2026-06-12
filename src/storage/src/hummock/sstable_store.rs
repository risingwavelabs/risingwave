// Copyright 2022 RisingWave Labs
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
use std::ops::Deref;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use fail::fail_point;
use foyer::{
    Cache, CacheBuilder, CacheEntry, EventListener, Hint, HybridCache, HybridCacheBuilder,
    HybridCacheEntry, HybridCacheProperties,
};
use futures::{FutureExt, StreamExt, future};
use prost::Message;
use risingwave_hummock_sdk::key::UserKeyRangeRef;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::vector_index::{HnswGraphFileInfo, VectorFileInfo};
use risingwave_hummock_sdk::{
    HummockHnswGraphFileId, HummockObjectId, HummockRawObjectId, HummockSstableObjectId,
    HummockVectorFileId, SST_OBJECT_SUFFIX,
};
use risingwave_hummock_trace::TracedCachePolicy;
use risingwave_object_store::object::{
    ObjectError, ObjectMetadataIter, ObjectResult, ObjectStoreRef, ObjectStreamingUploader,
};
use risingwave_pb::hummock::PbHnswGraph;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::time::Instant;

use super::{
    BatchUploadWriter, Block, BlockMeta, BlockResponse, MetaPartitionIndex, MetaShard,
    MetaShardDesc, PARTITIONED_META_VERSION, PartitionedSstableMeta, RecentFilter, Sstable,
    SstableWriterOptions, XorFilterReader, decode_meta_footer_version,
};
use crate::hummock::block_stream::{
    BlockDataStream, BlockStream, MemoryUsageTracker, PrefetchBlockStream,
};
use crate::hummock::none::NoneRecentFilter;
use crate::hummock::vector::file::{VectorBlock, VectorBlockMeta, VectorFileMeta};
use crate::hummock::vector::monitor::VectorStoreCacheStats;
use crate::hummock::{BlockEntry, BlockHolder, HummockError, HummockResult, RecentFilterTrait};
use crate::monitor::{HummockStateStoreMetrics, StoreLocalStatistic};

macro_rules! impl_vector_index_meta_file {
    ($($type_name:ident),+) => {
        pub enum HummockVectorIndexMetaFile {
            $(
                $type_name(Pin<Box<$type_name>>),
            )+
        }

        $(
            impl From<$type_name> for HummockVectorIndexMetaFile {
                fn from(v: $type_name) -> Self {
                    Self::$type_name(Box::pin(v))
                }
            }

            unsafe impl Send for VectorMetaFileHolder<$type_name> {}

            impl VectorMetaFileHolder<$type_name> {
                fn try_from_entry(
                    entry: CacheEntry<HummockRawObjectId, HummockVectorIndexMetaFile>,
                    object_id: HummockRawObjectId
                ) -> HummockResult<Self> {
                    let HummockVectorIndexMetaFile::$type_name(file_meta) = &*entry else {
                        return Err(HummockError::decode_error(format!(
                            "expect {} for object {}",
                            stringify!($type_name),
                            object_id
                        )));
                    };
                    let ptr = file_meta.as_ref().get_ref() as *const _;
                    Ok(VectorMetaFileHolder {
                        _cache_entry: entry,
                        ptr,
                    })
                }
            }
        )+
    };
}

impl_vector_index_meta_file!(VectorFileMeta, PbHnswGraph);

pub struct VectorMetaFileHolder<T> {
    _cache_entry: CacheEntry<HummockRawObjectId, HummockVectorIndexMetaFile>,
    ptr: *const T,
}

impl<T> Deref for VectorMetaFileHolder<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        // SAFETY: VectorFileHolder is exposed only as immutable, and `VectorFileMeta` is pinned via box
        unsafe { &*self.ptr }
    }
}

pub type TableHolder = Box<Sstable>;

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetaShardCacheKey {
    sst_id: HummockSstableObjectId,
    shard_idx: u32,
    first_block_idx: u32,
    offset: u64,
    len: u32,
    checksum: u64,
    filter_type: u32,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum HummockMetaCacheKey {
    MetaIndex(HummockSstableObjectId),
    MetaShard(MetaShardCacheKey),
}

impl HummockMetaCacheKey {
    pub fn estimate_size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
}

#[derive(Serialize, Deserialize)]
enum SerdeHummockMetaCacheEntry {
    MetaIndex(Box<PartitionedSstableMeta>),
    MetaShard(Box<MetaShard>),
}

#[derive(Clone)]
pub enum HummockMetaCacheEntry {
    MetaIndex(Box<PartitionedSstableMeta>),
    MetaShard {
        shard: Box<MetaShard>,
        filter_reader: Box<XorFilterReader>,
        skip_filter_in_serde: bool,
    },
}

impl Serialize for HummockMetaCacheEntry {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::MetaIndex(meta_index) => {
                SerdeHummockMetaCacheEntry::MetaIndex(meta_index.clone()).serialize(serializer)
            }
            Self::MetaShard {
                shard,
                filter_reader,
                skip_filter_in_serde,
            } => {
                let mut shard = shard.clone();
                if !*skip_filter_in_serde {
                    shard.filter = filter_reader.encode_to_bytes();
                }
                SerdeHummockMetaCacheEntry::MetaShard(shard).serialize(serializer)
            }
        }
    }
}

impl<'de> Deserialize<'de> for HummockMetaCacheEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(
            match SerdeHummockMetaCacheEntry::deserialize(deserializer)? {
                SerdeHummockMetaCacheEntry::MetaIndex(meta_index) => Self::MetaIndex(meta_index),
                SerdeHummockMetaCacheEntry::MetaShard(mut shard) => {
                    let filter = std::mem::take(&mut shard.filter);
                    let filter_reader = XorFilterReader::new(&filter, &shard.block_metas);
                    Self::MetaShard {
                        filter_reader: Box::new(filter_reader),
                        shard,
                        skip_filter_in_serde: false,
                    }
                }
            },
        )
    }
}

impl HummockMetaCacheEntry {
    pub fn estimate_size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                Self::MetaIndex(meta_index) => meta_index.estimate_size(),
                Self::MetaShard {
                    shard,
                    filter_reader,
                    ..
                } => {
                    std::mem::size_of::<MetaShard>()
                        + shard.estimated_heap_size()
                        + std::mem::size_of::<XorFilterReader>()
                        + filter_reader.estimated_heap_size()
                }
            }
    }
}

pub fn estimate_meta_cache_entry_size(
    key: &HummockMetaCacheKey,
    value: &HummockMetaCacheEntry,
) -> usize {
    key.estimate_size() + value.estimate_size()
}

#[derive(Clone)]
pub struct PartitionedSstableMetaHolder {
    entry: HybridCacheEntry<HummockMetaCacheKey, HummockMetaCacheEntry>,
}

impl PartitionedSstableMetaHolder {
    fn try_from_entry(
        entry: HybridCacheEntry<HummockMetaCacheKey, HummockMetaCacheEntry>,
        object_id: HummockSstableObjectId,
    ) -> HummockResult<Self> {
        let HummockMetaCacheEntry::MetaIndex(_) = &*entry else {
            return Err(HummockError::decode_error(format!(
                "expect partitioned meta index cache entry for object {}",
                object_id
            )));
        };
        Ok(Self { entry })
    }

    fn source(&self) -> foyer::Source {
        self.entry.source()
    }
}

impl Deref for PartitionedSstableMetaHolder {
    type Target = PartitionedSstableMeta;

    fn deref(&self) -> &Self::Target {
        let HummockMetaCacheEntry::MetaIndex(meta_index) = &*self.entry else {
            unreachable!("PartitionedSstableMetaHolder always stores meta index entries");
        };
        meta_index
    }
}

impl AsRef<PartitionedSstableMeta> for PartitionedSstableMetaHolder {
    fn as_ref(&self) -> &PartitionedSstableMeta {
        self
    }
}

impl std::fmt::Debug for PartitionedSstableMetaHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

#[derive(Clone)]
pub struct MetaShardHolder {
    entry: HybridCacheEntry<HummockMetaCacheKey, HummockMetaCacheEntry>,
}

impl MetaShardHolder {
    fn try_from_entry(
        entry: HybridCacheEntry<HummockMetaCacheKey, HummockMetaCacheEntry>,
        object_id: HummockSstableObjectId,
        shard_idx: u32,
    ) -> HummockResult<Self> {
        let HummockMetaCacheEntry::MetaShard { .. } = &*entry else {
            return Err(HummockError::decode_error(format!(
                "expect meta shard cache entry for object {} shard {}",
                object_id, shard_idx
            )));
        };
        Ok(Self { entry })
    }
}

impl Deref for MetaShardHolder {
    type Target = MetaShard;

    fn deref(&self) -> &Self::Target {
        let HummockMetaCacheEntry::MetaShard { shard, .. } = &*self.entry else {
            unreachable!("MetaShardHolder always stores meta shard entries");
        };
        shard
    }
}

impl MetaShardHolder {
    pub fn to_meta_shard(&self) -> MetaShard {
        let HummockMetaCacheEntry::MetaShard {
            shard,
            filter_reader,
            ..
        } = &*self.entry
        else {
            unreachable!("MetaShardHolder always stores meta shard entries");
        };
        let mut shard = (**shard).clone();
        shard.filter = filter_reader.encode_to_bytes();
        shard
    }

    pub fn may_match(&self, user_key_range: &UserKeyRangeRef<'_>, hash: u64) -> bool {
        let HummockMetaCacheEntry::MetaShard {
            shard,
            filter_reader,
            ..
        } = &*self.entry
        else {
            unreachable!("MetaShardHolder always stores meta shard entries");
        };
        filter_reader.may_match(&shard.block_metas, user_key_range, hash)
    }
}

impl std::fmt::Debug for MetaShardHolder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.deref().fmt(f)
    }
}

pub type VectorBlockHolder = CacheEntry<(HummockVectorFileId, usize), Box<VectorBlock>>;

pub type VectorFileHolder = VectorMetaFileHolder<VectorFileMeta>;
pub type HnswGraphFileHolder = VectorMetaFileHolder<PbHnswGraph>;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub struct SstableBlockIndex {
    pub sst_id: HummockSstableObjectId,
    pub block_idx: u64,
}

pub struct BlockCacheEventListener {
    metrics: Arc<HummockStateStoreMetrics>,
}

impl BlockCacheEventListener {
    pub fn new(metrics: Arc<HummockStateStoreMetrics>) -> Self {
        Self { metrics }
    }
}

impl EventListener for BlockCacheEventListener {
    type Key = SstableBlockIndex;
    type Value = Box<Block>;

    fn on_leave(&self, _reason: foyer::Event, _key: &Self::Key, value: &Self::Value)
    where
        Self::Key: foyer::Key,
        Self::Value: foyer::Value,
    {
        self.metrics
            .block_efficiency_histogram
            .observe(value.efficiency());
    }
}

// TODO: Define policy based on use cases (read / compaction / ...).
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum CachePolicy {
    /// Disable read cache and not fill the cache afterwards.
    Disable,
    /// Try reading the cache and fill the cache afterwards.
    Fill(Hint),
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

impl Default for CachePolicy {
    fn default() -> Self {
        CachePolicy::Fill(Hint::Normal)
    }
}

impl From<TracedCachePolicy> for CachePolicy {
    fn from(policy: TracedCachePolicy) -> Self {
        match policy {
            TracedCachePolicy::Disable => Self::Disable,
            TracedCachePolicy::Fill(priority) => Self::Fill(priority.into()),
            TracedCachePolicy::NotFill => Self::NotFill,
        }
    }
}

impl From<CachePolicy> for TracedCachePolicy {
    fn from(policy: CachePolicy) -> Self {
        match policy {
            CachePolicy::Disable => Self::Disable,
            CachePolicy::Fill(priority) => Self::Fill(priority.into()),
            CachePolicy::NotFill => Self::NotFill,
        }
    }
}

fn record_data_block_object_read(
    stats: &mut StoreLocalStatistic,
    policy: CachePolicy,
    read_count: u64,
    read_bytes: u64,
) {
    if read_count == 0 {
        return;
    }
    match policy {
        CachePolicy::Fill(_) => {
            stats.sst_store_data_block_fill_read_count += read_count;
            stats.sst_store_data_block_fill_read_bytes += read_bytes;
        }
        CachePolicy::NotFill => {
            stats.sst_store_data_block_not_fill_read_count += read_count;
            stats.sst_store_data_block_not_fill_read_bytes += read_bytes;
        }
        CachePolicy::Disable => {
            stats.sst_store_data_block_disable_read_count += read_count;
            stats.sst_store_data_block_disable_read_bytes += read_bytes;
        }
    }
}

#[derive(Clone, Default)]
struct ObjectReadRecorder {
    count: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
}

impl ObjectReadRecorder {
    fn record(&self, bytes: u64) {
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64) {
        (
            self.count.load(Ordering::Relaxed),
            self.bytes.load(Ordering::Relaxed),
        )
    }
}

pub struct SstableStoreConfig {
    pub store: ObjectStoreRef,
    pub path: String,

    pub prefetch_buffer_capacity: usize,
    pub max_prefetch_block_number: usize,
    pub recent_filter: Arc<RecentFilter<(HummockSstableObjectId, usize)>>,
    pub state_store_metrics: Arc<HummockStateStoreMetrics>,
    pub use_new_object_prefix_strategy: bool,
    pub skip_bloom_filter_in_serde: bool,

    pub meta_cache: HybridCache<HummockMetaCacheKey, HummockMetaCacheEntry>,
    pub block_cache: HybridCache<SstableBlockIndex, Box<Block>>,

    pub vector_meta_cache: Cache<HummockRawObjectId, HummockVectorIndexMetaFile>,
    pub vector_block_cache: Cache<(HummockVectorFileId, usize), Box<VectorBlock>>,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,

    meta_cache: HybridCache<HummockMetaCacheKey, HummockMetaCacheEntry>,
    block_cache: HybridCache<SstableBlockIndex, Box<Block>>,
    pub vector_meta_cache: Cache<HummockRawObjectId, HummockVectorIndexMetaFile>,
    pub vector_block_cache: Cache<(HummockVectorFileId, usize), Box<VectorBlock>>,

    /// Recent filter for `(sst_obj_id, blk_idx)`.
    ///
    /// `blk_idx == USIZE::MAX` stands for `sst_obj_id` only entry.
    recent_filter: Arc<RecentFilter<(HummockSstableObjectId, usize)>>,
    prefetch_buffer_usage: Arc<AtomicUsize>,
    prefetch_buffer_capacity: usize,
    max_prefetch_block_number: usize,
    /// Whether the object store is divided into prefixes depends on two factors:
    ///   1. The specific object store type.
    ///   2. Whether the existing cluster is a new cluster.
    ///
    /// The value of `use_new_object_prefix_strategy` is determined by the `use_new_object_prefix_strategy` field in the system parameters.
    /// For a new cluster, `use_new_object_prefix_strategy` is set to True.
    /// For an old cluster, `use_new_object_prefix_strategy` is set to False.
    /// The final decision of whether to divide prefixes is based on this field and the specific object store type, this approach is implemented to ensure backward compatibility.
    use_new_object_prefix_strategy: bool,

    /// sst serde happens when a sst meta is written to meta disk cache.
    /// Excluding the SST filter from serde can reduce the meta disk cache entry size
    /// and reduce disk IO throughput at the cost of making the SST filter useless.
    skip_bloom_filter_in_serde: bool,
}

impl SstableStore {
    pub fn new(config: SstableStoreConfig) -> Self {
        // TODO: We should validate path early. Otherwise object store won't report invalid path
        // error until first write attempt.

        Self {
            path: config.path,
            store: config.store,

            meta_cache: config.meta_cache,
            block_cache: config.block_cache,
            vector_meta_cache: config.vector_meta_cache,
            vector_block_cache: config.vector_block_cache,

            recent_filter: config.recent_filter,
            prefetch_buffer_usage: Arc::new(AtomicUsize::new(0)),
            prefetch_buffer_capacity: config.prefetch_buffer_capacity,
            max_prefetch_block_number: config.max_prefetch_block_number,
            use_new_object_prefix_strategy: config.use_new_object_prefix_strategy,
            skip_bloom_filter_in_serde: config.skip_bloom_filter_in_serde,
        }
    }

    /// For compactor, we do not need a high concurrency load for cache. Instead, we need the cache
    ///  can be evict more effective.
    #[expect(clippy::borrowed_box)]
    pub async fn for_compactor(
        store: ObjectStoreRef,
        path: String,
        block_cache_capacity: usize,
        meta_cache_capacity: usize,
        use_new_object_prefix_strategy: bool,
    ) -> HummockResult<Self> {
        let meta_cache = HybridCacheBuilder::new()
            .memory(meta_cache_capacity)
            .with_shards(1)
            .with_weighter(estimate_meta_cache_entry_size)
            .storage()
            .build()
            .await
            .map_err(HummockError::foyer_error)?;

        let block_cache = HybridCacheBuilder::new()
            .memory(block_cache_capacity)
            .with_shards(1)
            .with_weighter(|_: &SstableBlockIndex, value: &Box<Block>| {
                std::mem::size_of::<SstableBlockIndex>() + value.estimated_memory_weight()
            })
            .storage()
            .build()
            .await
            .map_err(HummockError::foyer_error)?;

        Ok(Self {
            path,
            store,

            prefetch_buffer_usage: Arc::new(AtomicUsize::new(0)),
            prefetch_buffer_capacity: block_cache_capacity,
            max_prefetch_block_number: 16, /* compactor won't use this parameter, so just assign a default value. */
            recent_filter: Arc::new(NoneRecentFilter::default().into()),
            use_new_object_prefix_strategy,
            skip_bloom_filter_in_serde: false,

            meta_cache,
            block_cache,
            vector_meta_cache: CacheBuilder::new(1 << 10).build(),
            vector_block_cache: CacheBuilder::new(1 << 10).build(),
        })
    }

    pub async fn delete(&self, object_id: HummockSstableObjectId) -> HummockResult<()> {
        self.store
            .delete(self.get_sst_data_path(object_id).as_str())
            .await?;
        self.meta_cache
            .clear()
            .await
            .map_err(HummockError::foyer_error)?;
        // TODO(MrCroxx): support group remove in foyer.
        Ok(())
    }

    pub async fn delete_cache(&self, _object_id: HummockSstableObjectId) -> HummockResult<()> {
        self.meta_cache
            .clear()
            .await
            .map_err(HummockError::foyer_error)?;
        Ok(())
    }

    pub(crate) async fn put_sst_data(
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
        let end_index = std::cmp::min(end_index, sst.meta.block_metas.len());
        self.prefetch_blocks_by_block_metas(
            sst.id,
            sst.meta.estimated_size,
            block_index,
            &sst.meta.block_metas[block_index..end_index],
            policy,
            stats,
        )
        .await
    }

    pub fn max_prefetch_block_number(&self) -> usize {
        self.max_prefetch_block_number
    }

    pub async fn prefetch_blocks_by_block_metas(
        &self,
        object_id: HummockSstableObjectId,
        file_size: u32,
        block_index: usize,
        block_metas: &[BlockMeta],
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<Box<dyn BlockStream>> {
        if block_metas.is_empty() {
            return Ok(Box::new(PrefetchBlockStream::new(
                VecDeque::new(),
                block_index,
                None,
            )));
        }

        if self.prefetch_buffer_usage.load(Ordering::Acquire) > self.prefetch_buffer_capacity {
            let block = self
                .get_by_block_meta(
                    object_id,
                    file_size,
                    block_index,
                    &block_metas[0],
                    policy,
                    stats,
                )
                .await?;
            return Ok(Box::new(PrefetchBlockStream::new(
                VecDeque::from([block]),
                block_index,
                None,
            )));
        }
        if let Some(entry) = self
            .block_cache
            .get(&SstableBlockIndex {
                sst_id: object_id,
                block_idx: block_index as _,
            })
            .await
            .map_err(HummockError::foyer_error)?
        {
            stats.cache_data_block_total += 1;
            if entry.source() == foyer::Source::Outer {
                stats.cache_data_block_miss += 1;
            }
            let block = BlockHolder::from_hybrid_cache_entry(entry);
            return Ok(Box::new(PrefetchBlockStream::new(
                VecDeque::from([block]),
                block_index,
                None,
            )));
        }
        let mut prefetch_block_count =
            std::cmp::min(block_metas.len(), self.max_prefetch_block_number);
        let start_offset = block_metas[0].offset as usize;
        let mut min_hit_offset = prefetch_block_count;
        let mut hit_count = 0;
        for idx in block_index..block_index + prefetch_block_count {
            if self.block_cache.contains(&SstableBlockIndex {
                sst_id: object_id,
                block_idx: idx as _,
            }) {
                let hit_offset = idx - block_index;
                if min_hit_offset > hit_offset && hit_offset > 0 {
                    min_hit_offset = hit_offset;
                }
                hit_count += 1;
            }
        }

        if hit_count * 3 >= prefetch_block_count || min_hit_offset * 2 > prefetch_block_count {
            prefetch_block_count = min_hit_offset;
        }
        stats.cache_data_prefetch_count += 1;
        stats.cache_data_prefetch_block_count += prefetch_block_count as u64;
        let end_offset = start_offset
            + block_metas[..prefetch_block_count]
                .iter()
                .map(|meta| meta.len as usize)
                .sum::<usize>();
        let data_path = self.get_sst_data_path(object_id);
        let memory_usage = end_offset - start_offset;
        let tracker = MemoryUsageTracker::new(self.prefetch_buffer_usage.clone(), memory_usage);
        let span = await_tree::span!("Prefetch SST-{}", object_id).verbose();
        let store = self.store.clone();
        let join_handle = tokio::spawn(async move {
            store
                .read(&data_path, start_offset..end_offset)
                .instrument_await(span)
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
                    file_size,
                );
                return Err(e.into());
            }
            Err(_) => {
                return Err(HummockError::other("cancel by other thread"));
            }
        };
        stats.sst_store_data_prefetch_read_count += 1;
        stats.sst_store_data_prefetch_read_bytes += (end_offset - start_offset) as u64;
        let mut offset = 0;
        let mut blocks = VecDeque::default();
        for (offset_in_prefetch, block_meta) in
            block_metas[..prefetch_block_count].iter().enumerate()
        {
            let idx = block_index + offset_in_prefetch;
            let end = offset + block_meta.len as usize;
            if end > buf.len() {
                return Err(ObjectError::internal("read unexpected EOF").into());
            }
            // copy again to avoid holding a large data in memory.
            let block = Block::decode_with_copy(
                buf.slice(offset..end),
                block_meta.uncompressed_size as usize,
                true,
            )?;
            let holder = if let CachePolicy::Fill(hint) = policy {
                let hint = if idx == block_index { hint } else { Hint::Low };
                let entry = self.block_cache.insert_with_properties(
                    SstableBlockIndex {
                        sst_id: object_id,
                        block_idx: idx as _,
                    },
                    Box::new(block),
                    HybridCacheProperties::default().with_hint(hint),
                );
                BlockHolder::from_hybrid_cache_entry(entry)
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
    ) -> HummockResult<BlockResponse> {
        self.get_block_response_inner(sst, block_index, policy, None)
            .await
    }

    async fn get_block_response_inner(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
        read_recorder: Option<ObjectReadRecorder>,
    ) -> HummockResult<BlockResponse> {
        let object_id = sst.id;
        let (range, uncompressed_capacity) = sst.calculate_block_info(block_index);
        let store = self.store.clone();

        let file_size = sst.meta.estimated_size;
        let data_path = Arc::new(self.get_sst_data_path(object_id));

        let disable_cache: fn() -> bool = || {
            fail_point!("disable_block_cache", |_| true);
            false
        };

        let policy = if disable_cache() {
            CachePolicy::Disable
        } else {
            policy
        };

        let idx = SstableBlockIndex {
            sst_id: object_id,
            block_idx: block_index as _,
        };

        self.recent_filter
            .extend([(object_id, usize::MAX), (object_id, block_index)]);

        // future: fetch block if hybrid cache miss
        let fetch_block = async move {
            let block_data = match store
                .read(&data_path, range.clone())
                .instrument_await("get_block_response".verbose())
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
            if let Some(read_recorder) = &read_recorder {
                read_recorder.record(block_data.len() as u64);
            }
            let block = Box::new(Block::decode(block_data, uncompressed_capacity)?);
            Ok(block)
        };

        match policy {
            CachePolicy::Fill(hint) => {
                let properties = HybridCacheProperties::default().with_hint(hint);
                let fetch = self.block_cache.get_or_fetch(&idx, || {
                    fetch_block.map(|res| res.map(|block| (block, properties)))
                });
                Ok(BlockResponse::Fetch(fetch))
            }
            CachePolicy::NotFill => {
                match self
                    .block_cache
                    .get(&idx)
                    .await
                    .map_err(HummockError::foyer_error)?
                {
                    Some(entry) => Ok(BlockResponse::Block(BlockHolder::from_hybrid_cache_entry(
                        entry,
                    ))),
                    _ => {
                        let block = fetch_block.await?;
                        Ok(BlockResponse::Block(BlockHolder::from_owned_block(block)))
                    }
                }
            }
            CachePolicy::Disable => {
                let block = fetch_block.await?;
                Ok(BlockResponse::Block(BlockHolder::from_owned_block(block)))
            }
        }
    }

    pub async fn get(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        let read_recorder = ObjectReadRecorder::default();
        let block_response = self
            .get_block_response_inner(sst, block_index, policy, Some(read_recorder.clone()))
            .await?;
        let block_holder = block_response.wait().await?;
        let (read_count, read_bytes) = read_recorder.snapshot();
        record_data_block_object_read(stats, policy, read_count, read_bytes);
        stats.cache_data_block_total += 1;
        if let BlockEntry::HybridCache(entry) = block_holder.entry()
            && entry.source() == foyer::Source::Outer
        {
            stats.cache_data_block_miss += 1;
        }
        Ok(block_holder)
    }

    pub async fn get_by_block_meta(
        &self,
        object_id: HummockSstableObjectId,
        file_size: u32,
        block_index: usize,
        block_meta: &BlockMeta,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        let range =
            block_meta.offset as usize..block_meta.offset as usize + block_meta.len as usize;
        let uncompressed_capacity = block_meta.uncompressed_size as usize;
        let store = self.store.clone();
        let data_path = Arc::new(self.get_sst_data_path(object_id));

        let disable_cache: fn() -> bool = || {
            fail_point!("disable_block_cache", |_| true);
            false
        };

        let policy = if disable_cache() {
            CachePolicy::Disable
        } else {
            policy
        };

        let idx = SstableBlockIndex {
            sst_id: object_id,
            block_idx: block_index as _,
        };

        self.recent_filter
            .extend([(object_id, usize::MAX), (object_id, block_index)]);

        let read_recorder = ObjectReadRecorder::default();
        let read_recorder_for_fetch = read_recorder.clone();
        let fetch_block = async move {
            let block_data = match store
                .read(&data_path, range.clone())
                .instrument_await("get_block_response".verbose())
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
            read_recorder_for_fetch.record(block_data.len() as u64);
            let block = Box::new(Block::decode(block_data, uncompressed_capacity)?);
            Ok(block)
        };

        let block_holder = match policy {
            CachePolicy::Fill(hint) => {
                let properties = HybridCacheProperties::default().with_hint(hint);
                let fetch = self.block_cache.get_or_fetch(&idx, || {
                    fetch_block.map(|res| res.map(|block| (block, properties)))
                });
                BlockResponse::Fetch(fetch).wait().await?
            }
            CachePolicy::NotFill => {
                match self
                    .block_cache
                    .get(&idx)
                    .await
                    .map_err(HummockError::foyer_error)?
                {
                    Some(entry) => BlockHolder::from_hybrid_cache_entry(entry),
                    _ => BlockHolder::from_owned_block(fetch_block.await?),
                }
            }
            CachePolicy::Disable => BlockHolder::from_owned_block(fetch_block.await?),
        };

        let (read_count, read_bytes) = read_recorder.snapshot();
        record_data_block_object_read(stats, policy, read_count, read_bytes);
        stats.cache_data_block_total += 1;
        if let BlockEntry::HybridCache(entry) = block_holder.entry()
            && entry.source() == foyer::Source::Outer
        {
            stats.cache_data_block_miss += 1;
        }
        Ok(block_holder)
    }

    pub async fn get_meta_shard(
        &self,
        object_id: HummockSstableObjectId,
        filter_type: u32,
        desc: &MetaShardDesc,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<MetaShard> {
        Ok(self
            .get_meta_shard_holder(object_id, filter_type, desc, stats)
            .await?
            .to_meta_shard())
    }

    pub async fn get_meta_shard_holder(
        &self,
        object_id: HummockSstableObjectId,
        filter_type: u32,
        desc: &MetaShardDesc,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<MetaShardHolder> {
        let cache_key = HummockMetaCacheKey::MetaShard(MetaShardCacheKey {
            sst_id: object_id,
            shard_idx: desc.shard_idx,
            first_block_idx: desc.first_block_idx,
            offset: desc.offset,
            len: desc.len,
            checksum: desc.checksum,
            filter_type,
        });
        stats.cache_meta_block_total += 1;
        stats.partitioned_meta_shard_cache_total += 1;

        let store = self.store.clone();
        let data_path = self.get_sst_data_path(object_id);
        let range = desc.offset as usize..desc.offset as usize + desc.len as usize;
        let checksum = desc.checksum;
        let shard_idx = desc.shard_idx;
        let first_block_idx = desc.first_block_idx;
        let expected_block_count = desc.block_count;
        let expected_smallest_key = desc.smallest_key.clone();
        let stats_ptr = stats.remote_io_time.clone();
        let read_recorder = ObjectReadRecorder::default();
        let read_recorder_for_fetch = read_recorder.clone();
        let skip_filter_in_serde = self.skip_bloom_filter_in_serde;

        let entry = self
            .meta_cache
            .get_or_fetch(&cache_key, || async move {
                let now = Instant::now();
                let buf = store
                    .read(&data_path, range)
                    .instrument_await("get_meta_shard_response".verbose())
                    .await?;
                read_recorder_for_fetch.record(buf.len() as u64);
                super::xxhash64_verify(&buf[..], checksum)?;
                let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
                stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
                let shard = MetaShard::decode_body(
                    shard_idx,
                    first_block_idx,
                    expected_block_count,
                    &expected_smallest_key,
                    filter_type,
                    &buf[..],
                )?;
                let mut shard = shard;
                let filter = std::mem::take(&mut shard.filter);
                let filter_reader = XorFilterReader::new(&filter, &shard.block_metas);
                Ok::<_, anyhow::Error>((
                    HummockMetaCacheEntry::MetaShard {
                        filter_reader: Box::new(filter_reader),
                        shard: Box::new(shard),
                        skip_filter_in_serde,
                    },
                    HybridCacheProperties::default().with_hint(Hint::Normal),
                ))
            })
            .await
            .map_err(HummockError::foyer_error)?;

        if entry.source() == foyer::Source::Outer {
            stats.cache_meta_block_miss += 1;
            stats.partitioned_meta_shard_cache_miss += 1;
        }
        let (read_count, read_bytes) = read_recorder.snapshot();
        if read_count > 0 {
            stats.sst_store_meta_shard_read_count += read_count;
            stats.sst_store_meta_shard_read_bytes += read_bytes;
            stats.partitioned_meta_shard_read_bytes += read_bytes;
        }
        MetaShardHolder::try_from_entry(entry, object_id, shard_idx)
    }

    pub async fn get_partitioned_block_metas(
        &self,
        sstable: &PartitionedSstableMeta,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<Vec<BlockMeta>> {
        let index = &sstable.index;
        let Some(first_shard) = index.shards.first() else {
            return Ok(vec![]);
        };
        let last_shard = index
            .shards
            .last()
            .expect("non-empty index shards should have last shard");
        let range_start = first_shard.offset as usize;
        let range_end = last_shard.offset as usize + last_shard.len as usize;
        let now = Instant::now();
        let buf = self
            .store
            .read(&self.get_sst_data_path(sstable.id), range_start..range_end)
            .instrument_await("get_partitioned_block_metas_response".verbose())
            .await?;
        let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
        stats
            .remote_io_time
            .fetch_add(add as u64, Ordering::Relaxed);
        stats.sst_store_meta_shard_read_count += 1;
        stats.sst_store_meta_shard_read_bytes += buf.len() as u64;
        stats.partitioned_meta_shard_read_bytes += buf.len() as u64;

        let mut block_metas = Vec::with_capacity(index.block_count as usize);
        for desc in &index.shards {
            let shard_start = desc.offset as usize;
            let shard_len = desc.len as usize;
            let relative_start = shard_start.checked_sub(range_start).ok_or_else(|| {
                HummockError::decode_error(format!(
                    "partitioned meta shard {} offset {} precedes read range {}",
                    desc.shard_idx, shard_start, range_start
                ))
            })?;
            let relative_end = relative_start.checked_add(shard_len).ok_or_else(|| {
                HummockError::decode_error(format!(
                    "partitioned meta shard {} range overflow",
                    desc.shard_idx
                ))
            })?;
            let shard_body = buf.get(relative_start..relative_end).ok_or_else(|| {
                HummockError::decode_error(format!(
                    "partitioned meta shard {} range {}..{} exceeds full meta read {}",
                    desc.shard_idx,
                    relative_start,
                    relative_end,
                    buf.len()
                ))
            })?;
            super::xxhash64_verify(shard_body, desc.checksum)?;
            block_metas.extend(MetaShard::decode_block_metas_body(
                desc.shard_idx,
                desc.block_count,
                &desc.smallest_key,
                shard_body,
            )?);
        }
        Ok(block_metas)
    }

    pub async fn get_partitioned_meta_shards(
        &self,
        sstable: &PartitionedSstableMeta,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<Vec<MetaShard>> {
        let index = &sstable.index;

        let mut shards = Vec::with_capacity(index.shard_count as usize);
        for desc in &index.shards {
            let shard = self
                .get_meta_shard(sstable.id, index.filter_type, desc, stats)
                .await?;
            shards.push(shard);
        }
        Ok(shards)
    }

    pub async fn get_vector_file_meta(
        &self,
        vector_file: &VectorFileInfo,
        stats: &mut VectorStoreCacheStats,
    ) -> HummockResult<VectorFileHolder> {
        let store = self.store.clone();
        let path = self.get_object_data_path(HummockObjectId::VectorFile(vector_file.object_id));
        let meta_offset = vector_file.meta_offset;
        let entry = self
            .vector_meta_cache
            .get_or_fetch(&vector_file.object_id.as_raw(), || async move {
                let encoded_footer = store.read(&path, meta_offset..).await?;
                let meta = VectorFileMeta::decode_footer(&encoded_footer)?;
                Ok::<_, anyhow::Error>(HummockVectorIndexMetaFile::from(meta))
            })
            .await?;
        stats.file_meta_total += 1;
        if entry.source() == foyer::Source::Outer {
            stats.file_meta_miss += 1;
        }
        VectorFileHolder::try_from_entry(entry, vector_file.object_id.as_raw())
    }

    pub async fn get_vector_block(
        &self,
        vector_file: &VectorFileInfo,
        block_idx: usize,
        block_meta: &VectorBlockMeta,
        stats: &mut VectorStoreCacheStats,
    ) -> HummockResult<VectorBlockHolder> {
        let store = self.store.clone();
        let path = self.get_object_data_path(HummockObjectId::VectorFile(vector_file.object_id));
        let start_offset = block_meta.offset;
        let end_offset = start_offset + block_meta.block_size;
        let entry = self
            .vector_block_cache
            .get_or_fetch(&(vector_file.object_id, block_idx), || async move {
                let encoded_block = store.read(&path, start_offset..end_offset).await?;
                let block = VectorBlock::decode(&encoded_block)?;
                Ok::<_, anyhow::Error>(Box::new(block))
            })
            .await
            .map_err(HummockError::foyer_error)?;

        stats.file_block_total += 1;
        if entry.source() == foyer::Source::Outer {
            stats.file_block_miss += 1;
        }
        Ok(entry)
    }

    pub fn insert_vector_cache(
        &self,
        object_id: HummockVectorFileId,
        meta: VectorFileMeta,
        blocks: Vec<VectorBlock>,
    ) {
        self.vector_meta_cache
            .insert(object_id.as_raw(), meta.into());
        for (idx, block) in blocks.into_iter().enumerate() {
            self.vector_block_cache
                .insert((object_id, idx), Box::new(block));
        }
    }

    pub fn insert_hnsw_graph_cache(&self, object_id: HummockHnswGraphFileId, graph: PbHnswGraph) {
        self.vector_meta_cache
            .insert(object_id.as_raw(), graph.into());
    }

    pub async fn get_hnsw_graph(
        &self,
        graph_file: &HnswGraphFileInfo,
        stats: &mut VectorStoreCacheStats,
    ) -> HummockResult<HnswGraphFileHolder> {
        let store = self.store.clone();
        let graph_file_path =
            self.get_object_data_path(HummockObjectId::HnswGraphFile(graph_file.object_id));
        let entry = self
            .vector_meta_cache
            .get_or_fetch(&graph_file.object_id.as_raw(), || async move {
                let encoded_graph = store.read(&graph_file_path, ..).await?;
                let graph = PbHnswGraph::decode(encoded_graph.as_ref())?;
                Ok::<_, anyhow::Error>(HummockVectorIndexMetaFile::from(graph))
            })
            .await
            .map_err(HummockError::foyer_error)?;
        stats.hnsw_graph_total += 1;
        if entry.source() == foyer::Source::Outer {
            stats.hnsw_graph_miss += 1;
        }
        HnswGraphFileHolder::try_from_entry(entry, graph_file.object_id.as_raw())
    }

    pub fn get_sst_data_path(&self, object_id: impl Into<HummockSstableObjectId>) -> String {
        self.get_object_data_path(HummockObjectId::Sstable(object_id.into()))
    }

    pub fn get_object_data_path(&self, object_id: HummockObjectId) -> String {
        let obj_prefix = self.store.get_object_prefix(
            object_id.as_raw().as_raw_id(),
            self.use_new_object_prefix_strategy,
        );
        risingwave_hummock_sdk::get_object_data_path(&obj_prefix, &self.path, object_id)
    }

    pub fn get_object_id_from_path(path: &str) -> HummockObjectId {
        risingwave_hummock_sdk::get_object_id_from_path(path)
    }

    pub fn store(&self) -> ObjectStoreRef {
        self.store.clone()
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn clear_block_cache(&self) -> HummockResult<()> {
        self.block_cache
            .clear()
            .await
            .map_err(HummockError::foyer_error)
    }

    #[cfg(any(test, feature = "test"))]
    pub async fn clear_meta_cache(&self) -> HummockResult<()> {
        self.meta_cache
            .clear()
            .await
            .map_err(HummockError::foyer_error)
    }

    pub async fn meta_index_cached(
        &self,
        sst_obj_id: HummockSstableObjectId,
    ) -> HummockResult<Option<PartitionedSstableMetaHolder>> {
        let entry = self
            .meta_cache
            .get(&HummockMetaCacheKey::MetaIndex(sst_obj_id))
            .await
            .map_err(HummockError::foyer_error)?;
        entry
            .map(|entry| PartitionedSstableMetaHolder::try_from_entry(entry, sst_obj_id))
            .transpose()
    }

    pub async fn meta_index(
        &self,
        sstable_info_ref: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<PartitionedSstableMetaHolder> {
        let object_id = sstable_info_ref.object_id;
        let store = self.store.clone();
        let meta_path = self.get_sst_data_path(object_id);
        let stats_ptr = stats.remote_io_time.clone();
        let range = sstable_info_ref.meta_offset as usize..;
        let read_recorder = ObjectReadRecorder::default();
        let read_recorder_for_fetch = read_recorder.clone();

        let cache_key = HummockMetaCacheKey::MetaIndex(object_id);
        let fetch = self.meta_cache.get_or_fetch(&cache_key, || async move {
            let now = Instant::now();
            let buf = store
                .read(&meta_path, range)
                .instrument_await("get_meta_response".verbose())
                .await?;
            read_recorder_for_fetch.record(buf.len() as u64);
            let version = decode_meta_footer_version(&buf[..])?;
            if version != PARTITIONED_META_VERSION {
                return Err(HummockError::invalid_format_version(version).into());
            }
            let index = MetaPartitionIndex::decode(&buf[..])?;
            let meta_index = PartitionedSstableMeta::new(object_id, index);
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
            Ok::<_, anyhow::Error>((
                HummockMetaCacheEntry::MetaIndex(Box::new(meta_index)),
                HybridCacheProperties::default().with_hint(Hint::Normal),
            ))
        });

        stats.cache_meta_block_total += 1;
        let entry = fetch
            .instrument_await("fetch_meta".verbose())
            .await
            .map_err(HummockError::foyer_error);
        if let Ok(ref entry) = entry
            && entry.source() == foyer::Source::Outer
        {
            stats.cache_meta_block_miss += 1;
        }
        let (meta_read_count, meta_read_bytes) = read_recorder.snapshot();
        let entry =
            entry.and_then(|entry| PartitionedSstableMetaHolder::try_from_entry(entry, object_id));
        if meta_read_count > 0 && entry.is_ok() {
            stats.sst_store_meta_index_read_count += meta_read_count;
            stats.sst_store_meta_index_read_bytes += meta_read_bytes;
        }
        if let Ok(ref entry) = entry {
            stats.partitioned_meta_index_cache_total += 1;
            if entry.source() == foyer::Source::Outer {
                stats.partitioned_meta_index_cache_miss += 1;
            }
            if meta_read_count > 0 {
                stats.partitioned_meta_index_read_bytes += meta_read_bytes;
            }
        }
        entry
    }

    /// Disabled fast-compaction full-meta entrypoint.
    pub async fn sstable(
        &self,
        _sstable_info_ref: &SstableInfo,
        _stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        std::future::ready(()).await;
        Err(HummockError::other(
            "fast compaction is disabled for v3 meta benchmark",
        ))
    }

    pub async fn list_sst_object_metadata_from_object_store(
        &self,
        prefix: Option<String>,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> HummockResult<ObjectMetadataIter> {
        let list_path = format!("{}/{}", self.path, prefix.unwrap_or("".into()));
        let raw_iter = self.store.list(&list_path, start_after, limit).await?;
        let iter = raw_iter.filter(|r| match r {
            Ok(i) => future::ready(i.key.ends_with(&format!(".{}", SST_OBJECT_SUFFIX))),
            Err(_) => future::ready(true),
        });
        Ok(Box::pin(iter))
    }

    pub fn create_sst_writer(
        self: Arc<Self>,
        object_id: impl Into<HummockSstableObjectId>,
        options: SstableWriterOptions,
    ) -> BatchUploadWriter {
        BatchUploadWriter::new(object_id, self, options)
    }

    pub fn insert_partitioned_meta_cache(
        &self,
        object_id: HummockSstableObjectId,
        partitioned_index: MetaPartitionIndex,
        meta_shards: Vec<MetaShard>,
    ) -> HummockResult<()> {
        let encoded_index = partitioned_index.encode_to_bytes();
        let partitioned_index = MetaPartitionIndex::decode(&encoded_index)?;
        let meta_index = PartitionedSstableMeta::new(object_id, partitioned_index.clone());
        self.meta_cache.insert_with_properties(
            HummockMetaCacheKey::MetaIndex(object_id),
            HummockMetaCacheEntry::MetaIndex(Box::new(meta_index)),
            HybridCacheProperties::default().with_hint(Hint::Normal),
        );
        for desc in &partitioned_index.shards {
            let Some(shard) = meta_shards.get(desc.shard_idx as usize) else {
                return Err(HummockError::decode_error(format!(
                    "missing partitioned meta shard {} for object {}",
                    desc.shard_idx, object_id
                )));
            };
            if shard.shard_idx != desc.shard_idx {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard order mismatch for object {}: desc {} shard {}",
                    object_id, desc.shard_idx, shard.shard_idx
                )));
            }
            let shard_body = shard.encode_body_to_bytes();
            if shard_body.len() != desc.len as usize {
                return Err(HummockError::decode_error(format!(
                    "partitioned meta shard {} length mismatch for object {}: desc {} body {}",
                    desc.shard_idx,
                    object_id,
                    desc.len,
                    shard_body.len()
                )));
            }
            super::xxhash64_verify(&shard_body, desc.checksum)?;
            let shard = MetaShard::decode_body(
                desc.shard_idx,
                desc.first_block_idx,
                desc.block_count,
                &desc.smallest_key,
                partitioned_index.filter_type,
                &shard_body,
            )?;
            let mut shard = shard;
            let filter = std::mem::take(&mut shard.filter);
            let filter_reader = XorFilterReader::new(&filter, &shard.block_metas);
            self.meta_cache.insert_with_properties(
                HummockMetaCacheKey::MetaShard(MetaShardCacheKey {
                    sst_id: object_id,
                    shard_idx: desc.shard_idx,
                    first_block_idx: desc.first_block_idx,
                    offset: desc.offset,
                    len: desc.len,
                    checksum: desc.checksum,
                    filter_type: partitioned_index.filter_type,
                }),
                HummockMetaCacheEntry::MetaShard {
                    filter_reader: Box::new(filter_reader),
                    shard: Box::new(shard),
                    skip_filter_in_serde: self.skip_bloom_filter_in_serde,
                },
                HybridCacheProperties::default().with_hint(Hint::Normal),
            );
        }
        Ok(())
    }

    pub fn insert_block_cache(
        &self,
        object_id: HummockSstableObjectId,
        block_index: u64,
        block: Box<Block>,
    ) {
        self.block_cache.insert(
            SstableBlockIndex {
                sst_id: object_id,
                block_idx: block_index,
            },
            block,
        );
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
        let store = self.store();
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
                )));
            }
        };
        Ok(BlockDataStream::new(reader, metas.to_vec()))
    }

    pub fn meta_cache(&self) -> &HybridCache<HummockMetaCacheKey, HummockMetaCacheEntry> {
        &self.meta_cache
    }

    pub fn block_cache(&self) -> &HybridCache<SstableBlockIndex, Box<Block>> {
        &self.block_cache
    }

    pub fn recent_filter(&self) -> &Arc<RecentFilter<(HummockSstableObjectId, usize)>> {
        &self.recent_filter
    }

    pub async fn create_streaming_uploader(
        &self,
        path: &str,
    ) -> ObjectResult<ObjectStreamingUploader> {
        self.store.streaming_upload(path).await
    }
}

pub type SstableStoreRef = Arc<SstableStore>;
#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;

    use risingwave_hummock_sdk::sstable_info::SstableInfo;
    use risingwave_hummock_sdk::{HummockObjectId, HummockSstableObjectId};

    use super::{
        HummockMetaCacheEntry, HummockMetaCacheKey, MetaShardCacheKey, SstableStoreRef,
        SstableWriterOptions, estimate_meta_cache_entry_size,
    };
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::iterator::test_utils::{iterator_test_key_of, mock_sstable_store};
    use crate::hummock::sstable::SstableIteratorReadOptions;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable, gen_test_sstable_data, put_sst,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{
        BlockMeta, CachePolicy, FilterBuilder, MetaShard, SstableIterator, SstableMeta,
        SstableStore, Xor16FilterBuilder, XorFilterReader,
    };
    use crate::monitor::StoreLocalStatistic;

    const SST_ID: u64 = 1;

    fn get_hummock_value(x: usize) -> HummockValue<Vec<u8>> {
        HummockValue::put(format!("overlapped_new_{}", x).as_bytes().to_vec())
    }

    fn test_meta_shard_cache_entry(skip_filter_in_serde: bool) -> HummockMetaCacheEntry {
        let mut filter_builder = Xor16FilterBuilder::new(1);
        filter_builder.add_key(b"test-key", 1);
        let filter = filter_builder.finish(None);
        assert!(!filter.is_empty());

        let mut shard = MetaShard {
            shard_idx: 0,
            first_block_idx: 0,
            block_metas: vec![BlockMeta::default()],
            filter_type: 1,
            filter,
        };
        let filter = std::mem::take(&mut shard.filter);
        let filter_reader = XorFilterReader::new(&filter, &shard.block_metas);
        HummockMetaCacheEntry::MetaShard {
            shard: Box::new(shard),
            filter_reader: Box::new(filter_reader),
            skip_filter_in_serde,
        }
    }

    #[test]
    fn test_meta_cache_entry_size_accounts_for_typed_key() {
        let entry = test_meta_shard_cache_entry(false);
        let index_key = HummockMetaCacheKey::MetaIndex(HummockSstableObjectId::new(SST_ID));
        let shard_key = HummockMetaCacheKey::MetaShard(MetaShardCacheKey {
            sst_id: HummockSstableObjectId::new(SST_ID),
            shard_idx: 1,
            first_block_idx: 2,
            offset: 3,
            len: 4,
            checksum: 5,
            filter_type: 6,
        });

        assert_eq!(
            index_key.estimate_size(),
            std::mem::size_of::<HummockMetaCacheKey>()
        );
        assert_eq!(
            shard_key.estimate_size(),
            std::mem::size_of::<HummockMetaCacheKey>()
        );
        assert!(shard_key.estimate_size() > std::mem::size_of::<HummockSstableObjectId>());
        assert_eq!(
            estimate_meta_cache_entry_size(&shard_key, &entry),
            shard_key.estimate_size() + entry.estimate_size()
        );
    }

    #[test]
    fn test_meta_shard_cache_entry_serde_aligns_sstable_filter_cache() {
        let entry = test_meta_shard_cache_entry(false);
        let HummockMetaCacheEntry::MetaShard {
            shard,
            filter_reader,
            ..
        } = &entry
        else {
            unreachable!();
        };
        assert!(shard.filter.is_empty());
        let expected_filter = filter_reader.encode_to_bytes();
        assert!(!expected_filter.is_empty());

        let buf = bincode::serialize(&entry).unwrap();
        let decoded: HummockMetaCacheEntry = bincode::deserialize(&buf).unwrap();
        let HummockMetaCacheEntry::MetaShard {
            shard,
            filter_reader,
            ..
        } = decoded
        else {
            unreachable!();
        };
        assert!(shard.filter.is_empty());
        assert_eq!(filter_reader.encode_to_bytes(), expected_filter);
    }

    #[test]
    fn test_meta_shard_cache_entry_skip_filter_serde() {
        let entry = test_meta_shard_cache_entry(true);
        let buf = bincode::serialize(&entry).unwrap();
        let decoded: HummockMetaCacheEntry = bincode::deserialize(&buf).unwrap();
        let HummockMetaCacheEntry::MetaShard {
            shard,
            filter_reader,
            ..
        } = decoded
        else {
            unreachable!();
        };
        assert!(shard.filter.is_empty());
        assert!(filter_reader.is_empty());
    }

    #[tokio::test]
    async fn test_get_partitioned_block_metas_reads_all_shards_once() {
        let sstable_store = mock_sstable_store().await;
        let mut opts = default_builder_opt_for_test();
        opts.block_capacity = 256;
        opts.partitioned_meta_block_count = 1;
        let (holder, _) = gen_test_sstable(
            opts,
            SST_ID,
            (0..100).map(|x| (iterator_test_key_of(x), get_hummock_value(x))),
            sstable_store.clone(),
        )
        .await;
        assert!(holder.index.shard_count > 1);

        let mut stats = StoreLocalStatistic::default();
        let block_metas = sstable_store
            .get_partitioned_block_metas(&holder, &mut stats)
            .await
            .unwrap();

        assert_eq!(block_metas.len(), holder.index.block_count as usize);
        assert_eq!(stats.sst_store_meta_shard_read_count, 1);
        assert_eq!(
            stats.partitioned_meta_shard_read_bytes,
            stats.sst_store_meta_shard_read_bytes
        );
    }

    async fn validate_sst(
        sstable_store: SstableStoreRef,
        info: &SstableInfo,
        meta: SstableMeta,
        x_range: Range<usize>,
    ) {
        let mut stats = StoreLocalStatistic::default();
        let holder = sstable_store.meta_index(info, &mut stats).await.unwrap();
        assert_eq!(holder.index.estimated_size, meta.estimated_size);
        assert_eq!(holder.index.key_count, meta.key_count);
        assert_eq!(holder.index.smallest_key, meta.smallest_key);
        assert_eq!(holder.index.largest_key, meta.largest_key);
        assert_eq!(holder.index.block_count as usize, meta.block_metas.len());
        let holder = sstable_store.meta_index(info, &mut stats).await.unwrap();
        assert_eq!(holder.index.estimated_size, meta.estimated_size);
        assert_eq!(holder.index.key_count, meta.key_count);
        assert_eq!(holder.index.smallest_key, meta.smallest_key);
        assert_eq!(holder.index.largest_key, meta.largest_key);
        assert_eq!(holder.index.block_count as usize, meta.block_metas.len());
        let mut iter = SstableIterator::new(
            holder,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
            info,
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
        let sstable_store = mock_sstable_store().await;
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
            vec![0],
        )
        .await
        .unwrap();

        validate_sst(sstable_store, &info, meta, x_range).await;
    }

    #[tokio::test]
    async fn test_streaming_upload() {
        // Generate test data.
        let sstable_store = mock_sstable_store().await;
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
            vec![0],
        )
        .await
        .unwrap();

        validate_sst(sstable_store, &info, meta, x_range).await;
    }

    #[tokio::test]
    async fn test_basic() {
        let sstable_store = mock_sstable_store().await;
        let object_id = 123;
        let data_path = sstable_store.get_sst_data_path(object_id);
        assert_eq!(data_path, "test/123.data");
        assert_eq!(
            SstableStore::get_object_id_from_path(&data_path),
            HummockObjectId::Sstable(object_id.into())
        );
    }
}
