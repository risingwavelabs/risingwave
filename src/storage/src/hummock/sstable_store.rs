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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use await_tree::InstrumentAwait;
use bytes::Bytes;
use fail::fail_point;
use foyer::{
    CacheHint, Engine, EventListener, FetchState, HybridCache, HybridCacheBuilder, HybridCacheEntry,
};
use futures::{future, StreamExt};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::{HummockSstableObjectId, OBJECT_SUFFIX};
use risingwave_hummock_trace::TracedCachePolicy;
use risingwave_object_store::object::{
    ObjectError, ObjectMetadataIter, ObjectResult, ObjectStoreRef, ObjectStreamingUploader,
};
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
use crate::hummock::{BlockHolder, HummockError, HummockResult};
use crate::monitor::{HummockStateStoreMetrics, StoreLocalStatistic};

pub type TableHolder = HybridCacheEntry<HummockSstableObjectId, Box<Sstable>>;

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
    Fill(CacheHint),
    /// Read the cache but not fill the cache afterwards.
    NotFill,
}

impl Default for CachePolicy {
    fn default() -> Self {
        CachePolicy::Fill(CacheHint::Normal)
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
    pub recent_filter: Option<Arc<RecentFilter<(HummockSstableObjectId, usize)>>>,
    pub state_store_metrics: Arc<HummockStateStoreMetrics>,
    pub use_new_object_prefix_strategy: bool,

    pub meta_cache: HybridCache<HummockSstableObjectId, Box<Sstable>>,
    pub block_cache: HybridCache<SstableBlockIndex, Box<Block>>,
}

pub struct SstableStore {
    path: String,
    store: ObjectStoreRef,

    meta_cache: HybridCache<HummockSstableObjectId, Box<Sstable>>,
    block_cache: HybridCache<SstableBlockIndex, Box<Block>>,

    /// Recent filter for `(sst_obj_id, blk_idx)`.
    ///
    /// `blk_idx == USIZE::MAX` stands for `sst_obj_id` only entry.
    recent_filter: Option<Arc<RecentFilter<(HummockSstableObjectId, usize)>>>,
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

            recent_filter: config.recent_filter,
            prefetch_buffer_usage: Arc::new(AtomicUsize::new(0)),
            prefetch_buffer_capacity: config.prefetch_buffer_capacity,
            max_prefetch_block_number: config.max_prefetch_block_number,
            use_new_object_prefix_strategy: config.use_new_object_prefix_strategy,
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
                u64::BITS as usize / 8 + value.estimate_size()
            })
            .storage(Engine::Large)
            .build()
            .await
            .map_err(HummockError::foyer_error)?;

        let block_cache = HybridCacheBuilder::new()
            .memory(block_cache_capacity)
            .with_shards(1)
            .with_weighter(|_: &SstableBlockIndex, value: &Box<Block>| {
                // FIXME(MrCroxx): Calculate block weight more accurately.
                u64::BITS as usize * 2 / 8 + value.raw().len()
            })
            .storage(Engine::Large)
            .build()
            .await
            .map_err(HummockError::foyer_error)?;

        Ok(Self {
            path,
            store,

            prefetch_buffer_usage: Arc::new(AtomicUsize::new(0)),
            prefetch_buffer_capacity: block_cache_capacity,
            max_prefetch_block_number: 16, /* compactor won't use this parameter, so just assign a default value. */
            recent_filter: None,
            use_new_object_prefix_strategy,

            meta_cache,
            block_cache,
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
        if let Some(entry) = self
            .block_cache
            .get(&SstableBlockIndex {
                sst_id: object_id,
                block_idx: block_index as _,
            })
            .await
            .map_err(HummockError::foyer_error)?
        {
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
            if self.block_cache.contains(&SstableBlockIndex {
                sst_id: object_id,
                block_idx: idx as _,
            }) {
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
                    CacheHint::Low
                };
                let entry = self.block_cache.insert_with_hint(
                    SstableBlockIndex {
                        sst_id: object_id,
                        block_idx: idx as _,
                    },
                    Box::new(block),
                    cache_priority,
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
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockResponse> {
        let object_id = sst.id;
        let (range, uncompressed_capacity) = sst.calculate_block_info(block_index);
        let store = self.store.clone();

        stats.cache_data_block_total += 1;
        let file_size = sst.meta.estimated_size;
        let data_path = self.get_sst_data_path(object_id);

        let disable_cache: fn() -> bool = || {
            fail_point!("disable_block_cache", |_| true);
            false
        };

        let policy = if disable_cache() {
            CachePolicy::Disable
        } else {
            policy
        };

        // future: fetch block if hybrid cache miss
        let fetch_block = move || {
            let range = range.clone();

            async move {
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
                        return Err(anyhow::Error::from(HummockError::from(e)));
                    }
                };
                let block = Box::new(
                    Block::decode(block_data, uncompressed_capacity)
                        .map_err(anyhow::Error::from)?,
                );
                Ok(block)
            }
        };

        if let Some(filter) = self.recent_filter.as_ref() {
            filter.extend([(object_id, usize::MAX), (object_id, block_index)]);
        }

        match policy {
            CachePolicy::Fill(context) => {
                let entry = self.block_cache.fetch_with_hint(
                    SstableBlockIndex {
                        sst_id: object_id,
                        block_idx: block_index as _,
                    },
                    context,
                    fetch_block,
                );
                if matches!(entry.state(), FetchState::Miss) {
                    stats.cache_data_block_miss += 1;
                }
                Ok(BlockResponse::Entry(entry))
            }
            CachePolicy::NotFill => {
                if let Some(entry) = self
                    .block_cache
                    .get(&SstableBlockIndex {
                        sst_id: object_id,
                        block_idx: block_index as _,
                    })
                    .await
                    .map_err(HummockError::foyer_error)?
                {
                    Ok(BlockResponse::Block(BlockHolder::from_hybrid_cache_entry(
                        entry,
                    )))
                } else {
                    let block = fetch_block().await.map_err(HummockError::foyer_error)?;
                    Ok(BlockResponse::Block(BlockHolder::from_owned_block(block)))
                }
            }
            CachePolicy::Disable => {
                let block = fetch_block().await.map_err(HummockError::foyer_error)?;
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
        match self
            .get_block_response(sst, block_index, policy, stats)
            .await
        {
            Ok(block_response) => block_response.wait().await,
            Err(err) => Err(err),
        }
    }

    pub fn get_sst_data_path(&self, object_id: HummockSstableObjectId) -> String {
        let obj_prefix = self
            .store
            .get_object_prefix(object_id, self.use_new_object_prefix_strategy);
        risingwave_hummock_sdk::get_sst_data_path(&obj_prefix, &self.path, object_id)
    }

    pub fn get_object_id_from_path(path: &str) -> HummockSstableObjectId {
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
    pub fn sstable(
        &self,
        sstable_info_ref: &SstableInfo,
        stats: &mut StoreLocalStatistic,
    ) -> impl Future<Output = HummockResult<TableHolder>> + Send + 'static {
        let object_id = sstable_info_ref.object_id;

        let entry = self.meta_cache.fetch(object_id, || {
            let store = self.store.clone();
            let meta_path = self.get_sst_data_path(object_id);
            let stats_ptr = stats.remote_io_time.clone();
            let range = sstable_info_ref.meta_offset as usize..;
            async move {
                let now = Instant::now();
                let buf = store.read(&meta_path, range).await?;
                let meta = SstableMeta::decode(&buf[..])?;

                let sst = Sstable::new(object_id, meta);
                let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
                stats_ptr.fetch_add(add as u64, Ordering::Relaxed);
                Ok(Box::new(sst))
            }
        });

        if matches! { entry.state(), FetchState::Wait | FetchState::Miss } {
            stats.cache_meta_block_miss += 1;
        }

        stats.cache_meta_block_total += 1;

        async move { entry.await.map_err(HummockError::foyer_error) }
    }

    pub async fn list_object_metadata_from_object_store(
        &self,
        prefix: Option<String>,
        start_after: Option<String>,
        limit: Option<usize>,
    ) -> HummockResult<ObjectMetadataIter> {
        let list_path = format!("{}/{}", self.path, prefix.unwrap_or("".into()));
        let raw_iter = self.store.list(&list_path, start_after, limit).await?;
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

    pub fn get_meta_memory_usage(&self) -> u64 {
        self.meta_cache.memory().usage() as _
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

    pub fn meta_cache(&self) -> &HybridCache<HummockSstableObjectId, Box<Sstable>> {
        &self.meta_cache
    }

    pub fn block_cache(&self) -> &HybridCache<SstableBlockIndex, Box<Block>> {
        &self.block_cache
    }

    pub fn recent_filter(&self) -> Option<&Arc<RecentFilter<(HummockSstableObjectId, usize)>>> {
        self.recent_filter.as_ref()
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
    use risingwave_hummock_sdk::HummockSstableObjectId;

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
        assert_eq!(SstableStore::get_object_id_from_path(&data_path), object_id);
    }
}
