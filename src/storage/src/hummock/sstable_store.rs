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
use std::sync::atomic::{AtomicUsize, Ordering};

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use fail::fail_point;
use foyer::{
    Cache, CacheBuilder, CacheEntry, EventListener, Hint, HybridCache, HybridCacheBuilder,
    HybridCacheEntry, HybridCacheProperties,
};
use futures::{FutureExt, StreamExt, future};
use prost::Message;
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
use tracing::Instrument;

use super::{
    BatchUploadWriter, Block, BlockMeta, BlockResponse, RecentFilter, Sstable, SstableMeta,
    SstableWriterOptions,
};
use crate::hummock::block_stream::{BlockDataStream, MemoryUsageTracker};
use crate::hummock::none::NoneRecentFilter;
use crate::hummock::vector::file::{VectorBlock, VectorBlockMeta, VectorFileMeta};
use crate::hummock::vector::monitor::VectorStoreCacheStats;
use crate::hummock::{BlockEntry, BlockHolder, HummockError, HummockResult, RecentFilterTrait};
use crate::monitor::{HummockStateStoreMetrics, StoreLocalStatistic};

mod cold_range;
mod cold_range_observation;
pub(crate) use cold_range::ColdRangePrefetch;
use cold_range_observation::{BufferedRangeGuard, BufferedRanges};

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

/// Meta-cache value with the existing bincode format and allocation-free filter sizing.
///
/// A separate type lets this cache override `Code::estimated_size` without changing
/// `Sstable`'s serde implementation or conflicting with Foyer's serde blanket impl.
#[derive(Debug)]
pub struct SstableMetaCacheValue(Box<Sstable>);

impl From<Sstable> for SstableMetaCacheValue {
    fn from(sstable: Sstable) -> Self {
        Self(Box::new(sstable))
    }
}

impl Deref for SstableMetaCacheValue {
    type Target = Sstable;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<Sstable> for SstableMetaCacheValue {
    fn as_ref(&self) -> &Sstable {
        &self.0
    }
}

impl foyer::Code for SstableMetaCacheValue {
    fn encode(&self, writer: &mut impl std::io::Write) -> foyer::Result<()> {
        bincode::serialize_into(writer, &self.0).map_err(foyer::Error::bincode_error)
    }

    fn decode(reader: &mut impl std::io::Read) -> foyer::Result<Self> {
        bincode::deserialize_from(reader)
            .map(Self)
            .map_err(foyer::Error::bincode_error)
    }

    fn estimated_size(&self) -> usize {
        self.0.serde_encoded_size()
    }
}

pub type TableHolder = HybridCacheEntry<HummockSstableObjectId, SstableMetaCacheValue>;

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

pub struct SstableStoreConfig {
    pub store: ObjectStoreRef,
    pub path: String,

    pub prefetch_buffer_capacity: usize,
    pub max_prefetch_block_number: usize,
    pub scan_prefetch: risingwave_common::config::storage::ScanPrefetchConfig,
    pub read_batch: risingwave_common::config::storage::ReadBatchConfig,
    pub recent_filter: Arc<RecentFilter<(HummockSstableObjectId, usize)>>,
    pub state_store_metrics: Arc<HummockStateStoreMetrics>,
    pub use_new_object_prefix_strategy: bool,
    pub skip_bloom_filter_in_serde: bool,

    pub meta_cache: HybridCache<HummockSstableObjectId, SstableMetaCacheValue>,
    pub block_cache: HybridCache<SstableBlockIndex, Box<Block>>,

    pub vector_meta_cache: Cache<HummockRawObjectId, HummockVectorIndexMetaFile>,
    pub vector_block_cache: Cache<(HummockVectorFileId, usize), Box<VectorBlock>>,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,

    meta_cache: HybridCache<HummockSstableObjectId, SstableMetaCacheValue>,
    block_cache: HybridCache<SstableBlockIndex, Box<Block>>,
    pub vector_meta_cache: Cache<HummockRawObjectId, HummockVectorIndexMetaFile>,
    pub vector_block_cache: Cache<(HummockVectorFileId, usize), Box<VectorBlock>>,

    /// Recent filter for `(sst_obj_id, blk_idx)`.
    ///
    /// `blk_idx == USIZE::MAX` stands for `sst_obj_id` only entry.
    recent_filter: Arc<RecentFilter<(HummockSstableObjectId, usize)>>,
    prefetch_buffer_usage: Arc<AtomicUsize>,
    buffered_cold_ranges: Arc<BufferedRanges>,
    prefetch_buffer_capacity: usize,
    max_prefetch_block_number: usize,
    pub(crate) scan_prefetch: risingwave_common::config::storage::ScanPrefetchConfig,
    pub(crate) read_batch: risingwave_common::config::storage::ReadBatchConfig,
    pub(crate) read_metrics: Arc<HummockStateStoreMetrics>,
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
        tracing::info!(scan_prefetch = ?config.scan_prefetch, read_batch = ?config.read_batch, "storage read controls");
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
            buffered_cold_ranges: Arc::default(),
            prefetch_buffer_capacity: config.prefetch_buffer_capacity,
            max_prefetch_block_number: config.max_prefetch_block_number,
            scan_prefetch: config.scan_prefetch,
            read_batch: config.read_batch,
            read_metrics: config.state_store_metrics,
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
        read_metrics: Arc<HummockStateStoreMetrics>,
    ) -> HummockResult<Self> {
        let meta_cache = HybridCacheBuilder::new()
            .memory(meta_cache_capacity)
            .with_shards(1)
            .with_weighter(
                |_: &HummockSstableObjectId, value: &SstableMetaCacheValue| {
                    std::mem::size_of::<HummockSstableObjectId>()
                        + value.estimated_meta_cache_memory_weight()
                },
            )
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
            buffered_cold_ranges: Arc::default(),
            prefetch_buffer_capacity: block_cache_capacity,
            max_prefetch_block_number: 16, /* compactor won't use this parameter, so just assign a default value. */
            scan_prefetch: Default::default(),
            read_batch: Default::default(),
            read_metrics,
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
        self.meta_cache.remove(&object_id);
        // TODO(MrCroxx): support group remove in foyer.
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

    pub async fn get_block_response(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
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
        let span = tracing::debug_span!(target: "state_store_read_batch", "block_read",
            object_id = %object_id, block = block_index, start = range.start, end = range.end);
        let fetch_range = range.clone();
        let fetch_block = || {
            let outer_fetch = self.read_metrics.block_cache_outer_fetch.clone();
            let buffered_ranges = self.buffered_cold_ranges.clone();
            let read_metrics = self.read_metrics.clone();
            async move {
            outer_fetch.inc();
            // Only a polled external fallback counts: cache hits and Foyer followers
            // never reach this point. Scheduler reuse can still avoid a physical GET.
            if buffered_ranges.contains(object_id, &fetch_range) {
                let policy = match policy {
                    CachePolicy::Fill(_) => "fill",
                    CachePolicy::NotFill => "not_fill",
                    CachePolicy::Disable => "disable",
                };
                read_metrics.cold_range_buffered_overlap_reads.with_label_values(&[policy]).inc();
                read_metrics.cold_range_buffered_overlap_bytes.with_label_values(&[policy]).inc_by(fetch_range.len() as u64);
            }
            let block_data = match store
                .read(&data_path, fetch_range.clone())
                .instrument_await("get_block_response".verbose())
                .await
            {
                Ok(data) => data,
                Err(e) => {
                    tracing::error!(
                        "get_block_response meet error when read {:?} from sst-{}, total length: {}",
                        fetch_range,
                        object_id,
                        file_size
                    );
                    return Err(HummockError::from(e));
                }
            };
            let block = Box::new(Block::decode(block_data, uncompressed_capacity)?);
            Ok(block)
        }.instrument(span.clone())
        };

        match policy {
            CachePolicy::Fill(hint) => {
                let properties = HybridCacheProperties::default().with_hint(hint);
                let fetch = self.block_cache.get_or_fetch(&idx, || {
                    fetch_block().map(|res| res.map(|block| (block, properties)))
                });
                if fetch.need_await() {
                    // Foyer owns shared producers. Pending responses can also resolve
                    // from disk or join another caller without issuing external IO.
                    self.read_metrics.block_cache_pending.inc();
                } else {
                    self.read_metrics.block_cache_memory_hit.inc();
                }
                tracing::debug!(target: "state_store_read_batch", parent: &span, object_id = %object_id,
                    block = block_index, memory_hit = !fetch.need_await(), "block cache request registered");
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
                        let block = fetch_block().await?;
                        Ok(BlockResponse::Block(BlockHolder::from_owned_block(block)))
                    }
                }
            }
            CachePolicy::Disable => {
                let block = fetch_block().await?;
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
        let block_response = self.get_block_response(sst, block_index, policy).await?;
        let block_holder = block_response.wait().await?;
        stats.cache_data_block_total += 1;
        if let BlockEntry::HybridCache(entry) = block_holder.entry()
            && entry.source() == foyer::Source::Outer
        {
            stats.cache_data_block_miss += 1;
        }
        Ok(block_holder)
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

    pub async fn sstable_cached(
        &self,
        sst_obj_id: HummockSstableObjectId,
    ) -> HummockResult<Option<TableHolder>> {
        self.meta_cache
            .get(&sst_obj_id)
            .await
            .map_err(HummockError::foyer_error)
    }

    /// Returns `table_holder`
    pub async fn sstable(
        &self,
        sstable_info_ref: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<TableHolder> {
        let object_id = sstable_info_ref.object_id;
        let store = self.store.clone();
        let meta_path = self.get_sst_data_path(object_id);
        let stats_ptr = stats.remote_io_time.clone();
        let range = sstable_info_ref.meta_offset as usize..;
        let skip_bloom_filter_in_serde = self.skip_bloom_filter_in_serde;

        let fetch = self.meta_cache.get_or_fetch(&object_id, || async move {
            let now = Instant::now();
            let buf = store
                .read(&meta_path, range)
                .instrument_await("get_meta_response".verbose())
                .await?;
            let sst = Sstable::from_meta_bytes(object_id, &buf, skip_bloom_filter_in_serde)?;
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
            Ok::<_, anyhow::Error>(SstableMetaCacheValue::from(sst))
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
        entry
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

    pub fn insert_meta_cache(&self, object_id: HummockSstableObjectId, meta: SstableMeta) {
        let sst = Sstable::new(object_id, meta, self.skip_bloom_filter_in_serde);
        self.meta_cache
            .insert(object_id, SstableMetaCacheValue::from(sst));
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
        Ok(BlockDataStream::new(reader, metas))
    }

    pub fn meta_cache(&self) -> &HybridCache<HummockSstableObjectId, SstableMetaCacheValue> {
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

    use risingwave_hummock_sdk::HummockObjectId;
    use risingwave_hummock_sdk::sstable_info::SstableInfo;

    use super::{SstableStoreRef, SstableWriterOptions};
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::iterator::test_utils::{iterator_test_key_of, mock_sstable_store};
    use crate::hummock::sstable::SstableIteratorReadOptions;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_default_test_sstable, gen_test_sstable_data, put_sst,
        test_key_of,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{CachePolicy, SstableIterator, SstableMeta, SstableStore};
    use crate::monitor::StoreLocalStatistic;

    const SST_ID: u64 = 1;

    #[tokio::test]
    async fn parked_backfill_iterator_allows_independent_cold_reads() {
        use std::time::Duration;

        use risingwave_hummock_sdk::key::FullKey;
        use risingwave_object_store::object::{
            InMemObjectStore, ObjectStore, ObjectStoreConfig, ObjectStoreImpl,
        };

        use super::SstableBlockIndex;
        use crate::hummock::iterator::test_utils::mock_sstable_store_with_object_store;
        use crate::monitor::ObjectStoreMetrics;
        use crate::store::PrefetchMode;

        let registry = prometheus::Registry::new();
        let backend = InMemObjectStore::for_test();
        let mut config = ObjectStoreConfig::default();
        config.read_scheduler.max_admitted_groups = 1.try_into().unwrap();
        config.read_scheduler.max_inflight_reads = 1.try_into().unwrap();
        let object = ObjectStoreImpl::InMem(backend.clone().monitored(
            Arc::new(ObjectStoreMetrics::new(&registry)),
            Arc::new(config),
        ))
        .with_read_coalescing();
        let store = mock_sstable_store_with_object_store(Arc::new(object)).await;
        let (sst, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), SST_ID, store.clone()).await;
        let key = |idx: usize| FullKey::decode(&sst.meta.block_metas[idx].smallest_key);
        let wait_for_depth = async |depth: f64| {
            loop {
                if registry
                    .gather()
                    .iter()
                    .find(|family| family.name() == "object_store_read_scheduler_waiting")
                    .is_some_and(|family| family.get_metric()[0].get_gauge().value() == depth)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        };

        let first = 2;
        let second = sst.meta.block_metas.len() - 3;
        assert!(second > first + store.max_prefetch_block_number);
        for prefetch in [PrefetchMode::Disabled, PrefetchMode::ColdRanges] {
            store.clear_block_cache().await.unwrap();
            let options = Arc::new(SstableIteratorReadOptions {
                prefetch,
                ..Default::default()
            });
            let mut a = SstableIterator::new(sst.clone(), store.clone(), options.clone(), &info);
            let mut b = SstableIterator::new(sst.clone(), store.clone(), options, &info);
            tokio::time::timeout(Duration::from_secs(5), async {
                let path = store.get_sst_data_path(sst.id);
                backend.set_read_delay_for_test(Duration::from_secs(60));
                let mut busy = Box::pin(store.store.read(&path, 0..1));
                assert!(futures::poll!(&mut busy).is_pending());

                // Each iterator gets one poll to start its Foyer fetch or cold-range task.
                let mut parked = Box::pin(a.seek(key(first)));
                assert!(futures::poll!(&mut parked).is_pending());
                wait_for_depth(1.0).await;
                let mut next = Box::pin(b.seek(key(second)));
                assert!(futures::poll!(&mut next).is_pending());
                wait_for_depth(2.0).await;

                backend.set_read_delay_for_test(Duration::ZERO);
                drop(busy);
                next.await.unwrap();
                // Observe completion before polling A again. ColdRanges decodes on consumption.
                let block = SstableBlockIndex {
                    sst_id: sst.id,
                    block_idx: first as u64,
                };
                let (bytes, _) = sst.calculate_block_info(first);
                let finished = || {
                    if prefetch == PrefetchMode::ColdRanges {
                        store.buffered_cold_ranges.contains(sst.id, &bytes)
                    } else {
                        store.block_cache.memory().contains(&block)
                    }
                };
                while !finished() {
                    tokio::task::yield_now().await;
                }
                parked.await.unwrap();
                assert_eq!(a.key(), key(first));
                assert_eq!(b.key(), key(second));
            })
            .await
            .unwrap_or_else(|_| {
                panic!("parked iterator blocked an independent cold read with {prefetch:?}")
            });
        }
        store.block_cache.close().await.unwrap();
    }

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
    async fn test_empty_prefetch_falls_back_to_block_get() {
        let mut sstable_store = mock_sstable_store().await;
        // Direct SstableStoreConfig construction can bypass the serde nonzero check.
        Arc::get_mut(&mut sstable_store)
            .unwrap()
            .max_prefetch_block_number = 0;
        let (sstable, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        sstable_store.clear_block_cache().await.unwrap();
        let mut iter = SstableIterator::new(
            sstable,
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions {
                prefetch: crate::store::PrefetchMode::ColdRanges,
                cache_policy: CachePolicy::Disable,
                ..Default::default()
            }),
            &info,
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), iter.rewind())
            .await
            .expect("empty prefetch must not cause an unbounded refill loop")
            .unwrap();
        assert_eq!(iter.key(), test_key_of(0).to_ref());
        let mut stats = StoreLocalStatistic::default();
        iter.collect_local_statistic(&mut stats);
        assert_eq!(stats.cache_data_prefetch_count, 0);
        assert_eq!(stats.cache_data_block_total, 1);
        assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
    }

    #[tokio::test]
    async fn test_prefetch_failure_falls_back_and_releases_tracker() {
        use crate::store::PrefetchMode;

        for (truncate, mode, policy) in [
            (true, PrefetchMode::ColdRanges, CachePolicy::default()),
            (false, PrefetchMode::ColdRanges, CachePolicy::default()),
            (true, PrefetchMode::ColdRanges, CachePolicy::NotFill),
            (false, PrefetchMode::ColdRanges, CachePolicy::NotFill),
            (true, PrefetchMode::ColdRanges, CachePolicy::Disable),
            (false, PrefetchMode::ColdRanges, CachePolicy::Disable),
        ] {
            let sstable_store = mock_sstable_store().await;
            let (sstable, info) =
                gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                    .await;
            assert!(sstable.meta.block_metas.len() > 1);
            sstable_store.clear_block_cache().await.unwrap();
            let path = sstable_store.get_sst_data_path(sstable.id);
            let mut data = sstable_store.store.read(&path, ..).await.unwrap().to_vec();
            let second_offset = sstable.meta.block_metas[1].offset as usize;
            if truncate {
                // A multi-block read fails, but reading block 0 still succeeds.
                data.truncate(second_offset);
            } else {
                // Decoding block 1 fails its checksum after block 0 was already decoded.
                data[second_offset] ^= 1;
            }
            sstable_store
                .store
                .upload(&path, data.into())
                .await
                .unwrap();
            let mut iter = SstableIterator::new(
                sstable.clone(),
                sstable_store.clone(),
                Arc::new(SstableIteratorReadOptions {
                    prefetch: mode,
                    cache_policy: policy,
                    ..Default::default()
                }),
                &info,
            );
            iter.rewind().await.unwrap();
            assert_eq!(iter.key(), test_key_of(0).to_ref());
            let lazy_head = mode == PrefetchMode::ColdRanges && !truncate;
            assert_eq!(sstable_store.get_prefetch_memory_usage() > 0, lazy_head);
            let mut stats = StoreLocalStatistic::default();
            iter.collect_local_statistic(&mut stats);
            assert_eq!(stats.cache_data_prefetch_count, 1);
            // A valid ColdRanges head no longer falls back because a later block is corrupt.
            assert_eq!(stats.cache_data_block_total, u64::from(!lazy_head));
            if lazy_head {
                // Advance through the valid head to exercise failure of a buffered neighbor,
                // not just a fresh seek. The iterator must release that window on failure.
                loop {
                    assert!(iter.is_valid());
                    if iter.next().await.is_err() {
                        break;
                    }
                }
                assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
            }
            // If the single-block read also fails, the error must still reach the caller.
            let second_key = risingwave_hummock_sdk::key::FullKey::decode(
                &sstable.meta.block_metas[1].smallest_key,
            );
            assert!(iter.seek(second_key).await.is_err());
            assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
        }
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

    #[tokio::test]
    async fn test_clear_file_cache_and_refetch() {
        let sstable_store = mock_sstable_store().await;
        let x_range = 0..10;
        let (data, meta) = gen_test_sstable_data(
            default_builder_opt_for_test(),
            x_range.map(|x| (iterator_test_key_of(x), get_hummock_value(x))),
        )
        .await;
        let info = put_sst(
            SST_ID,
            data,
            meta,
            sstable_store.clone(),
            SstableWriterOptions {
                capacity_hint: None,
                tracker: None,
                policy: CachePolicy::Fill(foyer::Hint::Normal),
            },
            vec![0],
        )
        .await
        .unwrap();

        let mut stats = StoreLocalStatistic::default();
        assert!(
            sstable_store
                .sstable_cached(info.object_id)
                .await
                .unwrap()
                .is_some()
        );
        let sstable = sstable_store.sstable(&info, &mut stats).await.unwrap();
        sstable_store
            .get(
                &sstable,
                0,
                CachePolicy::Fill(foyer::Hint::Normal),
                &mut stats,
            )
            .await
            .unwrap();
        assert!(
            sstable_store
                .block_cache()
                .get(&super::SstableBlockIndex {
                    sst_id: info.object_id,
                    block_idx: 0,
                })
                .await
                .unwrap()
                .is_some()
        );

        sstable_store.clear_meta_cache().await.unwrap();
        assert!(
            sstable_store
                .sstable_cached(info.object_id)
                .await
                .unwrap()
                .is_none()
        );
        sstable_store.sstable(&info, &mut stats).await.unwrap();
        assert!(
            sstable_store
                .sstable_cached(info.object_id)
                .await
                .unwrap()
                .is_some()
        );

        sstable_store.clear_block_cache().await.unwrap();
        assert!(
            sstable_store
                .block_cache()
                .get(&super::SstableBlockIndex {
                    sst_id: info.object_id,
                    block_idx: 0,
                })
                .await
                .unwrap()
                .is_none()
        );
        sstable_store
            .get(
                &sstable,
                0,
                CachePolicy::Fill(foyer::Hint::Normal),
                &mut stats,
            )
            .await
            .unwrap();
        assert!(
            sstable_store
                .block_cache()
                .get(&super::SstableBlockIndex {
                    sst_id: info.object_id,
                    block_idx: 0,
                })
                .await
                .unwrap()
                .is_some()
        );
    }
}
