// Copyright 2026 RisingWave Labs
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

//! SST data reads through published Pin files and the ordinary Foyer cache.
//! Refill and publication remain outside the read path.

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use await_tree::{InstrumentAwait, SpanExt};
use bytes::Bytes;
use fail::fail_point;
use foyer::{Hint, HybridCacheProperties, Location};
use futures::FutureExt;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{ObjectError, ObjectResult};
use thiserror_ext::AsReport;

use super::{CachePolicy, SstableBlockIndex, SstableStore};
use crate::hummock::block_cache::HybridCachedBlockEntry;
use crate::hummock::block_stream::{MemoryUsageTracker, PrefetchBlockStream};
use crate::hummock::pin_cache::PinCacheReadHandle;
use crate::hummock::{
    Block, BlockEntry, BlockHolder, BlockResponse, HummockError, HummockResult, RecentFilterTrait,
    Sstable,
};
use crate::monitor::StoreLocalStatistic;

impl SstableStore {
    /// Selects the source now; I/O starts when the returned future is polled.
    pub fn get<'a>(
        &'a self,
        sst: &'a Sstable,
        block_index: usize,
        policy: CachePolicy,
        stats: &'a mut StoreLocalStatistic,
    ) -> impl Future<Output = HummockResult<BlockHolder>> + Send + 'a {
        self.reader(sst.id)
            .read_block(sst, block_index, policy, stats)
    }

    /// Buffers consecutive decoded blocks starting at `block_index`, up to `end_index` (exclusive).
    /// Cache hits, the memory budget, and the prefetch limit may shorten the returned sequence.
    /// The source is selected now; I/O and decoding run when the future is polled,
    /// and all errors are returned before the buffered blocks are consumed.
    pub fn prefetch_blocks<'a>(
        &'a self,
        sst: &'a Sstable,
        block_index: usize,
        end_index: usize,
        policy: CachePolicy,
        stats: &'a mut StoreLocalStatistic,
    ) -> impl Future<Output = HummockResult<Box<PrefetchBlockStream>>> + Send + 'a {
        self.reader(sst.id)
            .prefetch_blocks(sst, block_index..end_index, policy, stats)
    }

    fn reader(&self, object_id: HummockSstableObjectId) -> SstReader<'_> {
        let pin_cache = self.pin_cache();
        let source = match pin_cache.and_then(|cache| cache.get(object_id)) {
            Some(file) => ReadSource::Pin(file),
            None => ReadSource::Foyer {
                // Registration contributes to accounting, but only a publication selects Pin.
                pin_cache_candidate: pin_cache.is_some_and(|cache| cache.is_registered(object_id)),
            },
        };
        SstReader {
            store: self,
            object_id,
            source,
        }
    }

    /// Reads a selected Pin publication without consulting or filling Foyer's disk cache.
    pub(super) async fn get_pinned_block(
        &self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
        pinned_sst: PinCacheReadHandle,
    ) -> HummockResult<BlockHolder> {
        let disable_cache: fn() -> bool = || {
            fail_point!("disable_block_cache", |_| true);
            false
        };
        let policy = if disable_cache() {
            CachePolicy::Disable
        } else {
            policy
        };
        self.recent_filter
            .extend([(sst.id, usize::MAX), (sst.id, block_index)]);
        let idx = SstableBlockIndex {
            sst_id: sst.id,
            block_idx: block_index as _,
        };
        if policy != CachePolicy::Disable
            && let Some(entry) = self.block_cache.memory().get(&idx)
        {
            return Ok(BlockHolder::from_hybrid_cache_entry(entry));
        }
        let (range, uncompressed_capacity) = sst.calculate_block_info(block_index);
        let fetch_block = move |read_handle: PinCacheReadHandle| async move {
            let data = read_handle
                .read(range)
                .instrument_await("get_pinned_block".verbose())
                .await?;
            let mut block = Block::decode(data, uncompressed_capacity)
                .inspect_err(|_| read_handle.invalidate())?;
            block.loaded_from_pin_cache = true;
            Ok::<_, HummockError>(Box::new(block))
        };
        match policy {
            CachePolicy::Fill(hint) => {
                let entry = pinned_sst
                    .get_or_fetch(block_index, || {
                        let read_handle = pinned_sst.clone();
                        let memory = self.block_cache.memory().clone();
                        async move {
                            // Another fetch may have filled memory after the initial lookup.
                            if let Some(entry) = memory.get(&idx) {
                                return Ok(entry);
                            }
                            let block = fetch_block(read_handle).await?;
                            Ok(memory.insert_with_properties(
                                idx,
                                block,
                                HybridCacheProperties::default()
                                    .with_hint(hint)
                                    .with_location(Location::InMem),
                            ))
                        }
                    })
                    .await?;
                Ok(BlockHolder::from_hybrid_cache_entry(entry))
            }
            CachePolicy::NotFill | CachePolicy::Disable => Ok(BlockHolder::from_owned_block(
                fetch_block(pinned_sst).await?,
            )),
        }
    }

    async fn get_foyer_block_response(
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

    // Foyer waiters can receive a Pin insertion, so attribute the result by its provenance.
    fn record_block_read(
        block: &BlockHolder,
        pin_selected: bool,
        pin_cache_candidate: bool,
        stats: &mut StoreLocalStatistic,
    ) {
        let source = match block.entry() {
            BlockEntry::HybridCache(entry) => Some(entry.source()),
            _ => None,
        };
        let pin_memory_hit = pin_selected && source == Some(foyer::Source::Memory);
        let pin_local_hit =
            block.loaded_from_pin_cache && matches!(source, None | Some(foyer::Source::Outer));
        if pin_cache_candidate || pin_local_hit {
            stats.pin_cache_data_block_total += 1;
        }
        if pin_memory_hit || pin_local_hit {
            stats.pin_cache_data_block_hit += 1;
        }
        if pin_memory_hit {
            stats.pin_cache_data_block_memory_hit += 1;
        }
        stats.cache_data_block_total += 1;
        if source == Some(foyer::Source::Outer) && !block.loaded_from_pin_cache {
            stats.cache_data_block_miss += 1;
        }
    }
}

struct SstReader<'a> {
    store: &'a SstableStore,
    object_id: HummockSstableObjectId,
    source: ReadSource,
}

enum ReadSource {
    Pin(PinCacheReadHandle),
    Foyer { pin_cache_candidate: bool },
}

impl SstReader<'_> {
    async fn read_block(
        self,
        sst: &Sstable,
        block_index: usize,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        let (pin_selected, pin_cache_candidate) = self.accounting();
        if let ReadSource::Pin(file) = self.source {
            match self
                .store
                .get_pinned_block(sst, block_index, policy, file)
                .await
            {
                Ok(block) => {
                    SstableStore::record_block_read(
                        &block,
                        pin_selected,
                        pin_cache_candidate,
                        stats,
                    );
                    return Ok(block);
                }
                Err(error) => tracing::warn!(
                    object_id = self.object_id.as_raw_id(),
                    error = %error.as_report(),
                    "failed to read or decode pinned SST block; falling back to Foyer"
                ),
            }
        }
        let block = self
            .store
            .get_foyer_block_response(sst, block_index, policy)
            .await?
            .wait()
            .await?;
        // Keep the initial selection for accounting even after a Foyer fallback.
        SstableStore::record_block_read(&block, pin_selected, pin_cache_candidate, stats);
        Ok(block)
    }

    fn accounting(&self) -> (bool, bool) {
        match self.source {
            ReadSource::Pin(_) => (true, true),
            ReadSource::Foyer {
                pin_cache_candidate,
            } => (false, pin_cache_candidate),
        }
    }

    async fn prefetch_blocks(
        self,
        sst: &Sstable,
        indices: Range<usize>,
        policy: CachePolicy,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<Box<PrefetchBlockStream>> {
        if self.store.prefetch_buffer_usage.load(Ordering::Acquire)
            > self.store.prefetch_buffer_capacity
        {
            let block = self.read_block(sst, indices.start, policy, stats).await?;
            return Ok(Box::new(PrefetchBlockStream::new(
                VecDeque::from([block]),
                indices.start,
                None,
            )));
        }
        let (pin_selected, pin_cache_candidate) = self.accounting();
        let object_id = self.object_id;
        let Range {
            start: block_index,
            end: end_index,
        } = indices;
        let first_block = SstableBlockIndex {
            sst_id: object_id,
            block_idx: block_index as _,
        };
        if policy != CachePolicy::Disable {
            let entry = self.cached(&first_block).await?;
            if let Some(entry) = entry {
                let block = BlockHolder::from_hybrid_cache_entry(entry);
                SstableStore::record_block_read(&block, pin_selected, pin_cache_candidate, stats);
                return Ok(Box::new(PrefetchBlockStream::new(
                    VecDeque::from([block]),
                    block_index,
                    None,
                )));
            }
        }
        let end_index = std::cmp::min(
            end_index,
            block_index.saturating_add(self.store.max_prefetch_block_number),
        );
        let mut end_index = std::cmp::min(end_index, sst.meta.block_metas.len());
        let start_offset = sst.meta.block_metas[block_index].offset as usize;
        if policy != CachePolicy::Disable {
            let mut min_hit_index = end_index;
            let mut hit_count = 0;
            for idx in block_index..end_index {
                let key = SstableBlockIndex {
                    sst_id: object_id,
                    block_idx: idx as _,
                };
                if self.contains(&key) {
                    if min_hit_index > idx && idx > block_index {
                        min_hit_index = idx;
                    }
                    hit_count += 1;
                }
            }
            if hit_count * 3 >= (end_index - block_index)
                || min_hit_index * 2 > block_index + end_index
            {
                end_index = min_hit_index;
            }
        }
        let prefetch_block_count = (end_index - block_index) as u64;
        stats.cache_data_prefetch_count += 1;
        stats.cache_data_prefetch_block_count += prefetch_block_count;
        let block_metas = &sst.meta.block_metas[block_index..end_index];
        let end_offset = start_offset
            + block_metas
                .iter()
                .map(|meta| meta.len as usize)
                .sum::<usize>();
        let tracker = MemoryUsageTracker::new(
            self.store.prefetch_buffer_usage.clone(),
            end_offset - start_offset,
        );
        let buf = self
            .spawn_read(start_offset..end_offset)
            .await
            .map_err(|_| HummockError::other("cancel by other thread"))?
            .inspect_err(|_| {
                tracing::error!(
                    "prefetch meet error when read {}..{} from sst-{} ({})",
                    start_offset,
                    end_offset,
                    object_id,
                    sst.meta.estimated_size,
                );
            })?;
        let mut offset = 0;
        let mut blocks = VecDeque::default();
        for (idx, meta) in (block_index..end_index).zip_eq_fast(block_metas) {
            let end = offset + meta.len as usize;
            let decoded = if end <= buf.len() {
                // Copy again to avoid holding a large buffer in one block.
                Block::decode_with_copy(
                    buf.slice(offset..end),
                    meta.uncompressed_size as usize,
                    true,
                )
            } else {
                Err(ObjectError::internal("read unexpected EOF").into())
            };
            let key = SstableBlockIndex {
                sst_id: object_id,
                block_idx: idx as _,
            };
            let fill_hint = match policy {
                CachePolicy::Fill(hint) => Some(if idx == block_index { hint } else { Hint::Low }),
                CachePolicy::NotFill | CachePolicy::Disable => None,
            };
            // Finish each block immediately: a later decode failure keeps earlier admissions.
            let holder = self.finish_block(key, decoded, fill_hint)?;
            blocks.push_back(holder);
            offset = end;
        }
        // Failed batches are accounted for by the subsequent single-block fallback.
        if pin_cache_candidate {
            stats.pin_cache_data_block_total += prefetch_block_count;
        }
        if pin_selected {
            stats.pin_cache_data_block_hit += prefetch_block_count;
        }
        Ok(Box::new(PrefetchBlockStream::new(
            blocks,
            block_index,
            Some(tracker),
        )))
    }

    async fn cached(
        &self,
        key: &SstableBlockIndex,
    ) -> HummockResult<Option<HybridCachedBlockEntry>> {
        match self.source {
            ReadSource::Pin(_) => Ok(self.store.block_cache.memory().get(key)),
            ReadSource::Foyer { .. } => self
                .store
                .block_cache
                .get(key)
                .await
                .map_err(HummockError::foyer_error),
        }
    }

    fn contains(&self, key: &SstableBlockIndex) -> bool {
        match self.source {
            ReadSource::Pin(_) => self.store.block_cache.memory().contains(key),
            ReadSource::Foyer { .. } => self.store.block_cache.contains(key),
        }
    }

    // The producer owns its resources and can outlive the caller and this reader.
    fn spawn_read(&self, range: Range<usize>) -> tokio::task::JoinHandle<ObjectResult<Bytes>> {
        let span = await_tree::span!("Prefetch SST-{}", self.object_id).verbose();
        match &self.source {
            ReadSource::Pin(file) => {
                let file = file.clone();
                tokio::spawn(async move { file.read(range).await }.instrument_await(span))
            }
            ReadSource::Foyer { .. } => {
                let path = self.store.get_sst_data_path(self.object_id);
                let remote = self.store.store.clone();
                tokio::spawn(async move { remote.read(&path, range).await }.instrument_await(span))
            }
        }
    }

    fn finish_block(
        &self,
        key: SstableBlockIndex,
        decoded: HummockResult<Block>,
        fill_hint: Option<Hint>,
    ) -> HummockResult<BlockHolder> {
        let block = match &self.source {
            ReadSource::Pin(file) => {
                let mut block = decoded.inspect_err(|_| file.invalidate())?;
                block.loaded_from_pin_cache = true;
                block
            }
            ReadSource::Foyer { .. } => decoded?,
        };
        let block = Box::new(block);
        let Some(hint) = fill_hint else {
            return Ok(BlockHolder::from_owned_block(block));
        };
        let properties = HybridCacheProperties::default().with_hint(hint);
        let entry = match self.source {
            ReadSource::Pin(_) => self.store.block_cache.memory().insert_with_properties(
                key,
                block,
                properties.with_location(Location::InMem),
            ),
            ReadSource::Foyer { .. } => self
                .store
                .block_cache
                .insert_with_properties(key, block, properties),
        };
        Ok(BlockHolder::from_hybrid_cache_entry(entry))
    }
}
