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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

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
    ObjectError, ObjectMetadataIter, ObjectRangeBounds, ObjectResult, ObjectStoreRef,
    ObjectStreamingUploader,
};
use risingwave_pb::hummock::PbHnswGraph;
use serde::{Deserialize, Serialize};
use thiserror_ext::AsReport;
use tokio::time::Instant;

use super::{
    BatchUploadWriter, Block, BlockMeta, BlockResponse, RecentFilter, Sstable, SstableMeta,
    SstableWriterOptions,
};
use crate::hummock::block_stream::{
    BlockDataStream, BlockStream, MemoryUsageTracker, PrefetchBlockStream,
};
use crate::hummock::none::NoneRecentFilter;
use crate::hummock::pin_cache::{PinCache, PinCacheReadHandle};
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

pub type TableHolder = HybridCacheEntry<HummockSstableObjectId, Box<Sstable>>;

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

#[derive(Clone, Copy, Eq, PartialEq)]
enum PinCacheBlockSource {
    Memory,
    Local,
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
    pub recent_filter: Arc<RecentFilter<(HummockSstableObjectId, usize)>>,
    pub state_store_metrics: Arc<HummockStateStoreMetrics>,
    pub use_new_object_prefix_strategy: bool,
    pub skip_bloom_filter_in_serde: bool,

    pub meta_cache: HybridCache<HummockSstableObjectId, Box<Sstable>>,
    pub block_cache: HybridCache<SstableBlockIndex, Box<Block>>,

    pub vector_meta_cache: Cache<HummockRawObjectId, HummockVectorIndexMetaFile>,
    pub vector_block_cache: Cache<(HummockVectorFileId, usize), Box<VectorBlock>>,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,
    pin_cache: OnceLock<Arc<PinCache>>,

    meta_cache: HybridCache<HummockSstableObjectId, Box<Sstable>>,
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
            pin_cache: OnceLock::new(),

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
            .with_weighter(|_: &HummockSstableObjectId, value: &Box<Sstable>| {
                std::mem::size_of::<HummockSstableObjectId>()
                    + value.estimated_meta_cache_memory_weight()
            })
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
            pin_cache: OnceLock::new(),

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
        self.meta_cache.remove(&object_id);
        // TODO(MrCroxx): support group remove in foyer.
        Ok(())
    }

    pub(crate) fn set_pin_cache(&self, pin_cache: Arc<PinCache>) {
        assert!(
            self.pin_cache.set(pin_cache).is_ok(),
            "pin cache must only be initialized once"
        );
    }

    pub(crate) fn pin_cache(&self) -> Option<&Arc<PinCache>> {
        self.pin_cache.get()
    }

    pub(crate) async fn pin_sst(&self, object_id: HummockSstableObjectId) -> HummockResult<()> {
        let Some(pin_cache) = self.pin_cache.get() else {
            return Ok(());
        };
        pin_cache
            .pin_sst(
                self.store.clone(),
                self.get_sst_data_path(object_id),
                object_id,
            )
            .await
            .map_err(Into::into)
    }

    fn pinned_sst(&self, object_id: HummockSstableObjectId) -> Option<PinCacheReadHandle> {
        self.pin_cache
            .get()
            .and_then(|pin_cache| pin_cache.get(object_id))
    }

    async fn read_sst_meta(
        pinned_sst: Option<&PinCacheReadHandle>,
        remote_store: ObjectStoreRef,
        remote_path: String,
        object_id: HummockSstableObjectId,
        range: impl ObjectRangeBounds,
    ) -> HummockResult<SstableMeta> {
        if let Some(pinned_sst) = pinned_sst {
            let result = pinned_sst
                .read(range.clone())
                .await
                .map_err(HummockError::from)
                .and_then(|data| SstableMeta::decode(&data));
            match result {
                Ok(meta) => return Ok(meta),
                Err(error) => {
                    pinned_sst.invalidate();
                    tracing::warn!(
                        object_id = object_id.as_raw_id(),
                        error = %error.as_report(),
                        "failed to read or decode pinned SST metadata; falling back to remote object store"
                    );
                }
            }
        }
        let data = remote_store.read(&remote_path, range).await?;
        SstableMeta::decode(&data)
    }

    pub fn delete_cache(&self, object_id: HummockSstableObjectId) -> HummockResult<()> {
        self.meta_cache.remove(&object_id);
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

    async fn single_foyer_block_stream(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<Box<dyn BlockStream>> {
        let block = self
            .get_block_response_from_foyer(sst, block_index, policy)
            .await?
            .wait()
            .await?;
        stats.cache_data_block_total += 1;
        if let BlockEntry::HybridCache(entry) = block.entry()
            && entry.source() == foyer::Source::Outer
        {
            stats.cache_data_block_miss += 1;
        }
        Ok(Box::new(PrefetchBlockStream::new(
            VecDeque::from([block]),
            block_index,
            None,
        )))
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
        let pinned_sst = self.pinned_sst(object_id);
        let pin_cache_desired = self
            .pin_cache
            .get()
            .is_some_and(|pin_cache| pin_cache.is_desired(object_id));
        let pin_cache_candidate = pin_cache_desired || pinned_sst.is_some();
        let first_block = SstableBlockIndex {
            sst_id: object_id,
            block_idx: block_index as _,
        };
        let cached_entry = if pinned_sst.is_some() {
            match policy {
                CachePolicy::Disable => None,
                CachePolicy::Fill(_) | CachePolicy::NotFill => {
                    self.block_cache.memory().get(&first_block)
                }
            }
        } else {
            self.block_cache
                .get(&first_block)
                .await
                .map_err(HummockError::foyer_error)?
        };
        if let Some(entry) = cached_entry {
            if pinned_sst.is_some() {
                stats.pin_cache_data_block_total += 1;
                stats.pin_cache_data_block_hit += 1;
                stats.pin_cache_data_block_memory_hit += 1;
            } else if pin_cache_candidate {
                stats.pin_cache_data_block_total += 1;
            }
            stats.cache_data_block_total += 1;
            if pinned_sst.is_none() && entry.source() == foyer::Source::Outer {
                stats.cache_data_block_miss += 1;
            }
            let block = BlockHolder::from_hybrid_cache_entry(entry);
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
            let block = SstableBlockIndex {
                sst_id: object_id,
                block_idx: idx as _,
            };
            let contains = if pinned_sst.is_some() {
                policy != CachePolicy::Disable && self.block_cache.memory().contains(&block)
            } else {
                self.block_cache.contains(&block)
            };
            if contains {
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
        let prefetch_block_count = (end_index - block_index) as u64;
        stats.cache_data_prefetch_block_count += prefetch_block_count;
        if pin_cache_candidate {
            stats.pin_cache_data_block_total += prefetch_block_count;
        }
        let end_offset = start_offset
            + sst.meta.block_metas[block_index..end_index]
                .iter()
                .map(|meta| meta.len as usize)
                .sum::<usize>();
        let data_path = self.get_sst_data_path(object_id);
        let memory_usage = end_offset - start_offset;
        let tracker = MemoryUsageTracker::new(self.prefetch_buffer_usage.clone(), memory_usage);
        let span = await_tree::span!("Prefetch SST-{}", object_id).verbose();
        let store = self.store.clone();
        let read_route = pinned_sst.clone();
        let join_handle = tokio::spawn(async move {
            let range = start_offset..end_offset;
            if let Some(pinned_sst) = read_route {
                return pinned_sst.read(range).instrument_await(span).await;
            }
            store.read(&data_path, range).instrument_await(span).await
        });
        let buf = match join_handle.await {
            Ok(Ok(data)) => data,
            Ok(Err(error)) => {
                if pinned_sst.is_some() {
                    tracing::warn!(
                        object_id = object_id.as_raw_id(),
                        error = %error.as_report(),
                        "failed to prefetch pinned SST; falling back to Foyer"
                    );
                    return self
                        .single_foyer_block_stream(sst, block_index, policy, stats)
                        .await;
                }
                tracing::error!(
                    "prefetch meet error when read {}..{} from sst-{} ({})",
                    start_offset,
                    end_offset,
                    object_id,
                    sst.meta.estimated_size,
                );
                return Err(error.into());
            }
            Err(_) => {
                return Err(HummockError::other("cancel by other thread"));
            }
        };
        let mut offset = 0;
        let mut blocks = VecDeque::default();
        for idx in block_index..end_index {
            let end = offset + sst.meta.block_metas[idx].len as usize;
            let decoded = if end <= buf.len() {
                // Copy again to avoid holding a large buffer in one block.
                Block::decode_with_copy(
                    buf.slice(offset..end),
                    sst.meta.block_metas[idx].uncompressed_size as usize,
                    true,
                )
            } else {
                Err(ObjectError::internal("read unexpected EOF").into())
            };
            let block = match decoded {
                Ok(block) => block,
                Err(error) => {
                    if let Some(route) = &pinned_sst {
                        route.invalidate();
                        tracing::warn!(
                            object_id = object_id.as_raw_id(),
                            error = %error.as_report(),
                            "failed to decode pinned SST prefetch; falling back to Foyer"
                        );
                        return self
                            .single_foyer_block_stream(sst, block_index, policy, stats)
                            .await;
                    }
                    return Err(error);
                }
            };
            let holder = if let CachePolicy::Fill(hint) = policy {
                let hint = if idx == block_index { hint } else { Hint::Low };
                let block_index = SstableBlockIndex {
                    sst_id: object_id,
                    block_idx: idx as _,
                };
                let properties = HybridCacheProperties::default().with_hint(hint);
                let entry = if pinned_sst.is_some() {
                    self.block_cache.memory().insert_with_properties(
                        block_index,
                        Box::new(block),
                        properties,
                    )
                } else {
                    self.block_cache.insert_with_properties(
                        block_index,
                        Box::new(block),
                        properties,
                    )
                };
                BlockHolder::from_hybrid_cache_entry(entry)
            } else {
                BlockHolder::from_owned_block(Box::new(block))
            };

            blocks.push_back(holder);
            offset = end;
        }
        if pinned_sst.is_some() {
            stats.pin_cache_data_block_hit += prefetch_block_count;
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
        self.get_block_response_with_pin_cache_hit(sst, block_index, policy)
            .await
            .map(|(response, _)| response)
    }

    async fn get_block_response_with_pin_cache_hit(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
    ) -> HummockResult<(BlockResponse, Option<PinCacheBlockSource>)> {
        let object_id = sst.id;
        let (range, uncompressed_capacity) = sst.calculate_block_info(block_index);
        if let Some(pinned_sst) = self.pinned_sst(object_id) {
            let idx = SstableBlockIndex {
                sst_id: object_id,
                block_idx: block_index as _,
            };
            if policy != CachePolicy::Disable
                && let Some(entry) = self.block_cache.memory().get(&idx)
            {
                return Ok((
                    BlockResponse::Block(BlockHolder::from_hybrid_cache_entry(entry)),
                    Some(PinCacheBlockSource::Memory),
                ));
            }

            let result = match policy {
                CachePolicy::Fill(hint) => {
                    let read_route = pinned_sst.clone();
                    let properties = HybridCacheProperties::default().with_hint(hint);
                    self.block_cache
                        .memory()
                        .get_or_fetch(&idx, move || async move {
                            let data = read_route
                                .read(range)
                                .instrument_await("get_pinned_block_response".verbose())
                                .await
                                .map_err(HummockError::from)?;
                            let block = Box::new(Block::decode(data, uncompressed_capacity)?);
                            Ok::<_, anyhow::Error>((block, properties))
                        })
                        .await
                        .map(BlockHolder::from_hybrid_cache_entry)
                        .map_err(HummockError::foyer_error)
                }
                CachePolicy::NotFill | CachePolicy::Disable => pinned_sst
                    .read(range)
                    .instrument_await("get_pinned_block_response".verbose())
                    .await
                    .map_err(HummockError::from)
                    .and_then(|data| Block::decode(data, uncompressed_capacity))
                    .map(|block| BlockHolder::from_owned_block(Box::new(block))),
            };
            match result {
                Ok(block) => {
                    return Ok((
                        BlockResponse::Block(block),
                        Some(PinCacheBlockSource::Local),
                    ));
                }
                Err(error) => {
                    pinned_sst.invalidate();
                    tracing::warn!(
                        object_id = object_id.as_raw_id(),
                        error = %error.as_report(),
                        "failed to read or decode pinned SST block; falling back to Foyer"
                    );
                }
            }
        }

        self.get_block_response_from_foyer(sst, block_index, policy)
            .await
            .map(|response| (response, None))
    }

    async fn get_block_response_from_foyer(
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
        let pin_cache_desired = self
            .pin_cache
            .get()
            .is_some_and(|pin_cache| pin_cache.is_desired(sst.id));
        let (block_response, pin_cache_source) = self
            .get_block_response_with_pin_cache_hit(sst, block_index, policy)
            .await?;
        let block_holder = block_response.wait().await?;
        if pin_cache_desired || pin_cache_source.is_some() {
            stats.pin_cache_data_block_total += 1;
        }
        if pin_cache_source.is_some() {
            stats.pin_cache_data_block_hit += 1;
        }
        if pin_cache_source == Some(PinCacheBlockSource::Memory) {
            stats.pin_cache_data_block_memory_hit += 1;
        }
        stats.cache_data_block_total += 1;
        if pin_cache_source.is_none()
            && let BlockEntry::HybridCache(entry) = block_holder.entry()
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
    ) -> HummockResult<Option<HybridCacheEntry<HummockSstableObjectId, Box<Sstable>>>> {
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
        let pinned_sst = self.pinned_sst(object_id);
        let meta_path = self.get_sst_data_path(object_id);
        let stats_ptr = stats.remote_io_time.clone();
        let range = sstable_info_ref.meta_offset as usize..;
        let skip_bloom_filter_in_serde = self.skip_bloom_filter_in_serde;

        let fetch = self.meta_cache.get_or_fetch(&object_id, || async move {
            let now = Instant::now();
            let meta = Self::read_sst_meta(pinned_sst.as_ref(), store, meta_path, object_id, range)
                .instrument_await("get_meta_response".verbose())
                .await?;

            let sst = Sstable::new(object_id, meta, skip_bloom_filter_in_serde);
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
            Ok::<_, anyhow::Error>(Box::new(sst))
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
        self.meta_cache.insert(object_id, Box::new(sst));
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
        let pinned_sst = self.pinned_sst(object_id);
        let block_meta = &metas[0];
        let start_pos = block_meta.offset as usize;
        let end_pos = metas.iter().map(|meta| meta.len as usize).sum::<usize>() + start_pos;
        let range = start_pos..end_pos;
        // spawn to tokio pool because the object-storage sdk may not be safe to cancel.
        let ret = tokio::spawn(async move {
            if let Some(pinned_sst) = pinned_sst {
                match pinned_sst.streaming_read(range.clone()).await {
                    Ok(reader) => return Ok((reader, Some(pinned_sst))),
                    Err(error) => tracing::warn!(
                        object_id = object_id.as_raw_id(),
                        error = %error.as_report(),
                        "failed to stream pinned SST; falling back to remote object store"
                    ),
                }
            }
            store
                .streaming_read(&data_path, range)
                .await
                .map(|reader| (reader, None))
        })
        .await;

        let (reader, local_route) = match ret {
            Ok(Ok(reader)) => reader,
            Ok(Err(e)) => return Err(HummockError::from(e)),
            Err(e) => {
                return Err(HummockError::other(format!(
                    "failed to get result, this read request may be canceled: {}",
                    e.as_report()
                )));
            }
        };
        Ok(BlockDataStream::new_with_local_route(
            reader,
            metas.to_vec(),
            local_route,
        ))
    }

    pub fn meta_cache(&self) -> &HybridCache<HummockSstableObjectId, Box<Sstable>> {
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
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Arc;

    use bytes::Bytes;
    use futures::StreamExt;
    use risingwave_hummock_sdk::HummockObjectId;
    use risingwave_hummock_sdk::sstable_info::SstableInfo;

    use super::{SstableBlockIndex, SstableStoreRef, SstableWriterOptions};
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::iterator::test_utils::{iterator_test_key_of, mock_sstable_store};
    use crate::hummock::pin_cache::PinCache;
    use crate::hummock::sstable::SstableIteratorReadOptions;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable_data, put_sst,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{CachePolicy, SstableIterator, SstableMeta, SstableStore};
    use crate::monitor::StoreLocalStatistic;

    const SST_ID: u64 = 1;

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
    async fn test_pin_meta_route_is_fixed_and_decode_failure_falls_back() {
        let sstable_store = mock_sstable_store().await;
        let local_store = mock_sstable_store().await.store();
        let pin_cache = PinCache::new(local_store.clone(), u64::MAX);
        sstable_store.set_pin_cache(pin_cache.clone());
        let object_id = SST_ID.into();
        let path = sstable_store.get_sst_data_path(object_id);
        let remote_store = sstable_store.store();
        let local_meta = SstableMeta {
            key_count: 1,
            ..Default::default()
        };
        let remote_meta = SstableMeta {
            key_count: 2,
            ..local_meta.clone()
        };
        let local_bytes = Bytes::from(local_meta.encode_to_bytes());
        let remote_bytes = Bytes::from(remote_meta.encode_to_bytes());
        assert_eq!(local_bytes.len(), remote_bytes.len());
        remote_store
            .upload(&path, local_bytes.clone())
            .await
            .unwrap();
        pin_cache.replace_desired_objects(HashMap::from([(object_id, local_bytes.len() as u64)]));
        let unpublished_route = sstable_store.pinned_sst(object_id);
        assert!(unpublished_route.is_none());
        pin_cache
            .pin_sst(remote_store.clone(), path.clone(), object_id)
            .await
            .unwrap();
        remote_store.upload(&path, remote_bytes).await.unwrap();

        // Publication during a Foyer miss must not redirect its remote fetch to LocalFS.
        assert_eq!(
            SstableStore::read_sst_meta(
                unpublished_route.as_ref(),
                remote_store.clone(),
                path.clone(),
                object_id,
                ..,
            )
            .await
            .unwrap()
            .key_count,
            2
        );
        let published_route = sstable_store.pinned_sst(object_id).unwrap();
        assert_eq!(
            SstableStore::read_sst_meta(
                Some(&published_route),
                remote_store.clone(),
                path.clone(),
                object_id,
                ..,
            )
            .await
            .unwrap()
            .key_count,
            1
        );

        let local_path = local_store
            .list("", None, None)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .unwrap()
            .key;
        local_store
            .upload(&local_path, Bytes::from(vec![0; local_bytes.len()]))
            .await
            .unwrap();
        assert_eq!(
            SstableStore::read_sst_meta(Some(&published_route), remote_store, path, object_id, ..,)
                .await
                .unwrap()
                .key_count,
            2
        );
        assert!(pin_cache.get(object_id).is_none());
    }

    #[tokio::test]
    async fn test_pin_prefetch_failure_falls_back_to_foyer() {
        for corrupt in [false, true] {
            assert_pin_prefetch_falls_back_to_foyer(corrupt).await;
        }
    }

    async fn assert_pin_prefetch_falls_back_to_foyer(corrupt: bool) {
        let sstable_store = mock_sstable_store().await;
        let x_range = 0..100;
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
                policy: CachePolicy::Disable,
            },
            vec![0],
        )
        .await
        .unwrap();
        let mut stats = StoreLocalStatistic::default();
        let sst = sstable_store.sstable(&info, &mut stats).await.unwrap();

        let local_store = mock_sstable_store().await.store();
        let pin_cache = PinCache::new(local_store.clone(), u64::MAX);
        sstable_store.set_pin_cache(pin_cache.clone());
        let remote_path = sstable_store.get_sst_data_path(info.object_id);
        let remote_size = sstable_store
            .store()
            .metadata(&remote_path)
            .await
            .unwrap()
            .total_size as u64;
        pin_cache.replace_desired_objects(HashMap::from([(info.object_id, remote_size)]));
        pin_cache
            .pin_sst(sstable_store.store(), remote_path.clone(), info.object_id)
            .await
            .unwrap();

        let mut pin_get_stats = StoreLocalStatistic::default();
        sstable_store
            .get(&sst, 0, CachePolicy::default(), &mut pin_get_stats)
            .await
            .unwrap();
        assert_eq!(pin_get_stats.pin_cache_data_block_total, 1);
        assert_eq!(pin_get_stats.pin_cache_data_block_hit, 1);
        assert_eq!(pin_get_stats.pin_cache_data_block_memory_hit, 0);

        let mut pin_memory_get_stats = StoreLocalStatistic::default();
        sstable_store
            .get(&sst, 0, CachePolicy::NotFill, &mut pin_memory_get_stats)
            .await
            .unwrap();
        assert_eq!(pin_memory_get_stats.pin_cache_data_block_total, 1);
        assert_eq!(pin_memory_get_stats.pin_cache_data_block_hit, 1);
        assert_eq!(pin_memory_get_stats.pin_cache_data_block_memory_hit, 1);

        sstable_store.block_cache().memory().clear();

        let mut pin_prefetch_stats = StoreLocalStatistic::default();
        let mut pin_stream = sstable_store
            .prefetch_blocks(
                &sst,
                0,
                sst.block_count(),
                CachePolicy::default(),
                &mut pin_prefetch_stats,
            )
            .await
            .unwrap();
        assert!(pin_stream.next_block().await.unwrap().is_some());
        assert!(pin_prefetch_stats.pin_cache_data_block_total > 0);
        assert_eq!(
            pin_prefetch_stats.pin_cache_data_block_hit,
            pin_prefetch_stats.pin_cache_data_block_total
        );
        assert_eq!(pin_prefetch_stats.pin_cache_data_block_memory_hit, 0);
        assert!(
            sstable_store
                .block_cache()
                .memory()
                .contains(&SstableBlockIndex {
                    sst_id: info.object_id,
                    block_idx: 0,
                })
        );

        sstable_store.block_cache().memory().clear();

        let mut local_objects = local_store.list("", None, None).await.unwrap();
        let local_path = local_objects.next().await.unwrap().unwrap().key;
        if corrupt {
            local_store
                .upload(&local_path, Bytes::from(vec![0; remote_size as usize]))
                .await
                .unwrap();
        } else {
            local_store.delete(&local_path).await.unwrap();
        }
        let mut stream = sstable_store
            .prefetch_blocks(&sst, 0, sst.block_count(), CachePolicy::NotFill, &mut stats)
            .await
            .unwrap();
        assert!(stream.next_block().await.unwrap().is_some());
        assert!(pin_cache.get(info.object_id).is_none());
        assert!(stats.pin_cache_data_block_total > 0);
        assert_eq!(stats.pin_cache_data_block_hit, 0);
    }
}
