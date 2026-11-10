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

use std::collections::VecDeque;
use std::ops::Bound::*;
use std::sync::Arc;

use await_tree::{InstrumentAwait, SpanExt};
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use sync_point::sync_point;
use thiserror_ext::AsReport;
use tracing::Instrument;

use super::super::{HummockResult, HummockValue};
use crate::hummock::block_stream::{PrefetchBlockStream, PrefetchLookup};
use crate::hummock::iterator::{Forward, HummockIterator, ValueMeta};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::{BlockEntry, BlockIterator, BlockResponse, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;
use crate::store::PrefetchMode;

pub trait SstableIteratorType: HummockIterator + 'static {
    fn create(
        sstable: TableHolder,
        sstable_store: SstableStoreRef,
        read_options: Arc<SstableIteratorReadOptions>,
        sstable_info_ref: &SstableInfo,
    ) -> Self;
}

/// Iterates on a sstable.
pub struct SstableIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    preload_stream: Option<Box<PrefetchBlockStream>>,
    /// Reference to the sst
    pub sst: TableHolder,
    preload_end_block_idx: usize,
    /// End of the last blockwise window. Advancing within it does not issue more lookahead.
    read_ahead_end: usize,
    /// Registered neighbor responses, starting at `read_ahead_end - read_ahead.len()`.
    /// Keep the Foyer handles until consumption instead of looking the blocks up again.
    read_ahead: VecDeque<HummockResult<BlockResponse>>,

    sstable_store: SstableStoreRef,
    stats: StoreLocalStatistic,
    options: Arc<SstableIteratorReadOptions>,

    // The readable block window is first restricted by table IDs and then by `scan_end_user_key`.
    // Included/Excluded bounds set its exclusive block end; outer iterators still filter keys
    // precisely inside the boundary block.
    block_start_idx_inclusive: usize,
    block_end_idx_exclusive: usize,
}

impl SstableIterator {
    pub fn new(
        sstable: TableHolder,
        sstable_store: SstableStoreRef,
        options: Arc<SstableIteratorReadOptions>,
        sstable_info_ref: &SstableInfo,
    ) -> Self {
        assert!(
            !sstable_info_ref.table_ids.is_empty(),
            "SstableIterator: SST {} (object {}) has empty table_ids",
            sstable_info_ref.sst_id,
            sstable_info_ref.object_id,
        );
        let read_table_id_range = if let Some(read_table_id) = options.read_table_id
            && sstable_info_ref
                .table_ids
                .binary_search(&read_table_id)
                .is_ok()
        {
            (read_table_id, read_table_id)
        } else {
            (
                *sstable_info_ref.table_ids.first().unwrap(),
                *sstable_info_ref.table_ids.last().unwrap(),
            )
        };
        assert!(
            read_table_id_range.0 <= read_table_id_range.1,
            "invalid table id range {} - {}",
            read_table_id_range.0,
            read_table_id_range.1
        );
        let block_meta_count = sstable.meta.block_metas.len();
        assert!(block_meta_count > 0);
        let first_table_id = sstable.meta.block_metas[0].table_id();
        let last_table_id = sstable.meta.block_metas[block_meta_count - 1].table_id();
        assert!(
            first_table_id <= read_table_id_range.0,
            "table id {} not found table_ids in block_meta {:?}",
            read_table_id_range.0,
            sstable
                .meta
                .block_metas
                .iter()
                .map(|meta| meta.table_id())
                .collect::<Vec<_>>()
        );
        assert!(
            last_table_id >= read_table_id_range.1,
            "table id {} not found table_ids in block_meta {:?}",
            read_table_id_range.1,
            sstable
                .meta
                .block_metas
                .iter()
                .map(|meta| meta.table_id())
                .collect::<Vec<_>>()
        );

        // SST blocks are ordered by full key, whose first component is the table ID.
        // Keep full-table-range reads constant-time; only search when clipping is needed.
        let block_start_idx_inclusive = if first_table_id == read_table_id_range.0 {
            0
        } else {
            sstable
                .meta
                .block_metas
                .partition_point(|meta| meta.table_id() < read_table_id_range.0)
        };
        // We assume that the table id read must exist in the sstable, otherwise it is a fatal error.
        assert!(
            block_start_idx_inclusive < block_meta_count,
            "table id {} not found table_ids in block_meta {:?}",
            read_table_id_range.0,
            sstable
                .meta
                .block_metas
                .iter()
                .map(|meta| meta.table_id())
                .collect::<Vec<_>>()
        );

        let mut block_end_idx_exclusive = if last_table_id == read_table_id_range.1 {
            block_meta_count
        } else {
            block_start_idx_inclusive
                + sstable.meta.block_metas[block_start_idx_inclusive..]
                    .partition_point(|meta| meta.table_id() <= read_table_id_range.1)
        };
        assert!(
            block_end_idx_exclusive > block_start_idx_inclusive,
            "block_end_idx_exclusive {} <= block_start_idx_inclusive {} block_meta_count {}",
            block_end_idx_exclusive,
            block_start_idx_inclusive,
            block_meta_count
        );

        if let Some(end_bound) = options.scan_end_user_key.as_ref() {
            let block_metas =
                &sstable.meta.block_metas[block_start_idx_inclusive..block_end_idx_exclusive];
            let range_end_idx_exclusive = match end_bound {
                Unbounded => block_metas.len(),
                Included(end_key) => block_metas.partition_point(|block_meta| {
                    FullKey::decode(&block_meta.smallest_key).user_key <= end_key.as_ref()
                }),
                Excluded(end_key) => block_metas.partition_point(|block_meta| {
                    FullKey::decode(&block_meta.smallest_key).user_key < end_key.as_ref()
                }),
            };
            block_end_idx_exclusive = block_start_idx_inclusive + range_end_idx_exclusive;
        }

        Self {
            block_iter: None,
            cur_idx: 0,
            preload_stream: None,
            sst: sstable,
            sstable_store,
            stats: StoreLocalStatistic::default(),
            options,
            preload_end_block_idx: 0,
            read_ahead_end: 0,
            read_ahead: VecDeque::new(),
            block_start_idx_inclusive,
            block_end_idx_exclusive,
        }
    }

    fn discard_read_ahead(&mut self) {
        if !self.read_ahead.is_empty() {
            self.sstable_store
                .read_metrics
                .prefetch_discarded
                .inc_by(self.read_ahead.len() as u64);
            let start = self.read_ahead_end - self.read_ahead.len();
            let bytes: u64 = self.sst.meta.block_metas[start..self.read_ahead_end]
                .iter()
                .map(|meta| u64::from(meta.len))
                .sum();
            self.sstable_store
                .read_metrics
                .prefetch_discarded_bytes
                .inc_by(bytes);
            self.read_ahead.clear();
        }
    }

    fn init_block_prefetch_range(&mut self, start_idx: usize) {
        assert!(
            start_idx >= self.block_start_idx_inclusive && start_idx < self.block_end_idx_exclusive
        );

        // Discard accounting uses the old window indices, so release before resetting them.
        self.discard_read_ahead();
        self.preload_end_block_idx = 0;
        self.read_ahead_end = 0;
        if self.options.prefetch != PrefetchMode::Range {
            return;
        }

        // Prefetch never extends beyond the block window already clipped by the scan hard bound.
        if start_idx + 1 < self.block_end_idx_exclusive {
            self.preload_end_block_idx = self.block_end_idx_exclusive;
        }
    }

    /// Seeks to a block, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(
        &mut self,
        idx: usize,
        seek_key: Option<FullKey<&[u8]>>,
    ) -> HummockResult<()> {
        tracing::debug!(
            target: "events::storage::sstable::block_seek",
            "table iterator seek: sstable_object_id = {}, block_id = {}",
            self.sst.id,
            idx,
        );

        // When all data are in block cache, it is highly possible that this iterator will stay on a
        // worker thread for a full time. Therefore, we use tokio's unstable API consume_budget to
        // do cooperative scheduling.
        tokio::task::consume_budget().await;

        if idx >= self.block_end_idx_exclusive {
            self.block_iter = None;
            self.discard_read_ahead();
            return Ok(());
        }
        let mut prefetched_block = None;
        let should_prefetch = match self
            .preload_stream
            .as_mut()
            .map(|stream| stream.take_block(idx))
        {
            Some(PrefetchLookup::Hit(block)) => {
                prefetched_block = Some(block);
                false
            }
            // Preserve the queue for subsequent forward reads after a backward seek.
            Some(PrefetchLookup::BeforeStart) => false,
            Some(PrefetchLookup::Exhausted) | None => {
                // Release the old tracker before checking the prefetch memory budget again.
                self.preload_stream = None;
                true
            }
        };

        // Try at most once per seek. Even an empty prefetch result must reach the block-get
        // fallback instead of repeatedly recreating a stream without making progress.
        if should_prefetch && idx + 1 < self.preload_end_block_idx {
            match self.prefetch_blocks(idx).await {
                Ok(mut stream) => {
                    if let PrefetchLookup::Hit(block) = stream.take_block(idx) {
                        prefetched_block = Some(block);
                        self.preload_stream = Some(stream);
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "failed to prefetch data, fall back to block get");
                }
            }
        }
        if prefetched_block.is_none() && self.options.prefetch == PrefetchMode::Blockwise {
            if idx >= self.read_ahead_end
                && let Some(register) = self.register_read_ahead(idx)
            {
                register.await;
            }
            if !self.read_ahead.is_empty() && idx == self.read_ahead_end - self.read_ahead.len() {
                self.sstable_store.read_metrics.prefetch_consumed.inc();
                self.sstable_store
                    .read_metrics
                    .prefetch_consumed_bytes
                    .inc_by(u64::from(self.sst.meta.block_metas[idx].len));
                let result = match self.read_ahead.pop_front().unwrap() {
                    Ok(response) => {
                        let ready = match &response {
                            BlockResponse::Fetch(fetch) => !fetch.need_await(),
                            BlockResponse::Block(_) => true,
                        };
                        if ready {
                            self.sstable_store.read_metrics.prefetch_consume_ready.inc();
                        } else {
                            self.sstable_store
                                .read_metrics
                                .prefetch_consume_pending
                                .inc();
                        }
                        let _timer = self
                            .sstable_store
                            .read_metrics
                            .blockwise_prefetch_wait_duration
                            .start_timer();
                        response.wait().await
                    }
                    Err(error) => Err(error),
                };
                match result {
                    Ok(block) => {
                        self.stats.cache_data_block_total += 1;
                        if let BlockEntry::HybridCache(entry) = block.entry()
                            && entry.source() == foyer::Source::Outer
                        {
                            self.stats.cache_data_block_miss += 1;
                        }
                        self.sstable_store.read_metrics.prefetch_consume_ok.inc();
                        prefetched_block = Some(block);
                    }
                    Err(error) => {
                        self.sstable_store.read_metrics.prefetch_consume_error.inc();
                        tracing::warn!(error = %error.as_report(),
                            "failed to read ahead block, fall back to block get");
                    }
                }
            }
        }
        let block = match prefetched_block {
            Some(block) => block,
            None => match self
                .sstable_store
                .get(&self.sst, idx, self.options.cache_policy, &mut self.stats)
                .await
            {
                Ok(block) => block,
                Err(error) => {
                    self.discard_read_ahead();
                    return Err(error);
                }
            },
        };
        self.block_iter = Some(BlockIterator::new(block));
        let block_iter = self.block_iter.as_mut().unwrap();
        if let Some(key) = seek_key {
            block_iter.seek(key);
        } else {
            block_iter.seek_to_first();
        }

        self.cur_idx = idx;

        Ok(())
    }

    // Keep construction of the large Range refill future off the common seek stack too.
    // The caller checks whether a refill is needed before reaching this allocation.
    fn prefetch_blocks(
        &mut self,
        idx: usize,
    ) -> impl Future<Output = HummockResult<Box<PrefetchBlockStream>>> + Send + '_ {
        Box::pin(
            self.sstable_store
                .prefetch_blocks(
                    &self.sst,
                    idx,
                    self.preload_end_block_idx,
                    self.options.cache_policy,
                    &mut self.stats,
                )
                .instrument_await("prefetch_blocks".verbose()),
        )
    }

    /// Select the window synchronously; allocate a registration future only for real lookahead.
    /// Polling that future registers Foyer responses. The iterator retains the responses, while
    /// Foyer continues to own any started producer shared with other readers.
    fn register_read_ahead(&mut self, idx: usize) -> Option<impl Future<Output = ()> + Send + '_> {
        use crate::hummock::CachePolicy;

        if !matches!(self.options.cache_policy, CachePolicy::Fill(_)) {
            self.sstable_store
                .read_metrics
                .prefetch_policy_skipped
                .inc();
            return None;
        }
        self.read_ahead_end = idx + 1;
        let config = self.sstable_store.blockwise_prefetch;
        let metas = &self.sst.meta.block_metas;
        let mut bytes = metas[idx].len.max(metas[idx].uncompressed_size) as usize;
        let mut end = idx + 1;
        let mut limit =
            if idx.saturating_add(config.max_blocks.get()) < self.block_end_idx_exclusive {
                "blocks"
            } else {
                "range"
            };
        for meta in metas
            .iter()
            .take(
                idx.saturating_add(config.max_blocks.get())
                    .min(self.block_end_idx_exclusive),
            )
            .skip(idx + 1)
        {
            let next_bytes = bytes.saturating_add(meta.len.max(meta.uncompressed_size) as usize);
            if next_bytes > config.max_bytes.get() {
                limit = "bytes";
                break;
            }
            bytes = next_bytes;
            end += 1;
        }
        self.sstable_store
            .read_metrics
            .blockwise_prefetch_window_blocks
            .with_label_values(&[limit])
            .observe((end - idx) as f64);
        self.sstable_store
            .read_metrics
            .blockwise_prefetch_window_bytes
            .observe(bytes as f64);
        if end == idx + 1 {
            return None;
        }
        // Keep registration's async state out of ordinary seek futures. Only a new window
        // pays for this box, not rejected lookahead or consumption of existing responses.
        Some(Box::pin(async move {
            let span = tracing::trace_span!(target: "state_store_read_batch", "block_read_ahead",
                object_id = %self.sst.id, head = idx, scan_end = self.block_end_idx_exclusive);
            for next in idx + 1..end {
                self.read_ahead.push_back(
                    self.sstable_store
                        .get_block_response(&self.sst, next, CachePolicy::Fill(foyer::Hint::Low))
                        .instrument(span.clone())
                        .await,
                );
                let result = self.read_ahead.back().unwrap();
                match result {
                    Ok(BlockResponse::Fetch(fetch)) if !fetch.need_await() => {
                        &self.sstable_store.read_metrics.prefetch_registered_ready
                    }
                    Ok(_) => &self.sstable_store.read_metrics.prefetch_registered_pending,
                    Err(_) => &self.sstable_store.read_metrics.prefetch_registration_error,
                }
                .inc();
                self.sstable_store
                    .read_metrics
                    .prefetch_registered_bytes
                    .inc_by(u64::from(self.sst.meta.block_metas[next].len));
                self.read_ahead_end = next + 1;
            }
            tracing::trace!(target: "state_store_read_batch", object_id = %self.sst.id,
                head = idx, end = self.read_ahead_end, scan_end = self.block_end_idx_exclusive,
                "blockwise read-ahead window");
        }))
    }

    fn calculate_block_idx_by_key(&self, key: FullKey<&[u8]>) -> usize {
        self.block_start_idx_inclusive
            + self.sst.meta.block_metas
                [self.block_start_idx_inclusive..self.block_end_idx_exclusive]
                .partition_point(|block_meta| {
                    // compare by version comparator
                    // Note: we are comparing against the `smallest_key` of the `block`, thus the
                    // partition point should be `prev(<=)` instead of `<`.
                    FullKey::decode(&block_meta.smallest_key).le(&key)
                })
                .saturating_sub(1) // considering the boundary of 0
    }
}

impl Drop for SstableIterator {
    fn drop(&mut self) {
        self.discard_read_ahead();
    }
}

impl HummockIterator for SstableIterator {
    type Direction = Forward;

    async fn next(&mut self) -> HummockResult<()> {
        self.stats.total_key_count += 1;
        let block_iter = self.block_iter.as_mut().expect("no block iter");
        if !block_iter.try_next() {
            // seek to next block
            self.seek_idx(self.cur_idx + 1, None).await?;
        }

        Ok(())
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.block_iter.as_ref().expect("no block iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let raw_value = self.block_iter.as_ref().expect("no block iter").value();

        HummockValue::from_slice(raw_value).expect("decode error")
    }

    fn is_valid(&self) -> bool {
        self.block_iter.as_ref().is_some_and(|i| i.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        if self.block_start_idx_inclusive >= self.block_end_idx_exclusive {
            self.block_iter = None;
            self.discard_read_ahead();
            return Ok(());
        }
        self.init_block_prefetch_range(self.block_start_idx_inclusive);
        // seek_idx will update the current block iter state
        self.seek_idx(self.block_start_idx_inclusive, None).await?;
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        if self.block_start_idx_inclusive >= self.block_end_idx_exclusive {
            self.block_iter = None;
            self.discard_read_ahead();
            return Ok(());
        }
        let block_idx = self.calculate_block_idx_by_key(key);
        self.init_block_prefetch_range(block_idx);

        self.seek_idx(block_idx, Some(key)).await?;
        if !self.is_valid() {
            // seek to next block
            sync_point!("SSTABLE_ITERATOR::SEEK::BEFORE_NEXT_BLOCK");
            self.seek_idx(block_idx + 1, None).await?;
        }
        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats);
    }

    fn value_meta(&self) -> ValueMeta {
        ValueMeta {
            object_id: Some(self.sst.id),
            block_id: Some(self.cur_idx as _),
        }
    }
}

impl SstableIteratorType for SstableIterator {
    fn create(
        sstable: TableHolder,
        sstable_store: SstableStoreRef,
        options: Arc<SstableIteratorReadOptions>,
        sstable_info_ref: &SstableInfo,
    ) -> Self {
        SstableIterator::new(sstable, sstable_store, options, sstable_info_ref)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::Bound;

    use bytes::Bytes;
    use foyer::Hint;
    use itertools::Itertools;
    use rand::prelude::*;
    use rand::rng as thread_rng;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::EpochWithGap;
    use risingwave_hummock_sdk::key::{TableKey, UserKey};
    use risingwave_hummock_sdk::sstable_info::{SstableInfo, SstableInfoInner};

    use super::*;
    use crate::assert_bytes_eq;
    use crate::hummock::CachePolicy;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        TEST_KEYS_COUNT, default_builder_opt_for_test, gen_default_test_sstable,
        gen_test_sstable_info, gen_test_sstable_with_table_ids, test_key_of, test_value_of,
    };

    #[tokio::test]
    async fn test_blockwise_config_limits_and_handle_accounting() {
        for (blocks, bytes, expected, reason) in [
            (2, usize::MAX, 2, "blocks"),
            (8, 1, 1, "bytes"),
            (8, usize::MAX, 3, "range"),
            (1, usize::MAX, 1, "blocks"),
        ] {
            let mut store = mock_sstable_store().await;
            let inner = Arc::get_mut(&mut store).unwrap();
            inner.blockwise_prefetch.max_blocks = blocks.try_into().unwrap();
            inner.blockwise_prefetch.max_bytes = bytes.try_into().unwrap();
            inner.read_metrics = Arc::new(crate::monitor::HummockStateStoreMetrics::new(
                &prometheus::Registry::new(),
                risingwave_common::config::MetricLevel::Info,
            ));
            let (sst, info) =
                gen_default_test_sstable(default_builder_opt_for_test(), 10, store.clone()).await;
            let options = Arc::new(SstableIteratorReadOptions {
                prefetch: PrefetchMode::Blockwise,
                scan_end_user_key: Some(Excluded(
                    FullKey::decode(&sst.meta.block_metas[3].smallest_key)
                        .user_key
                        .copy_into(),
                )),
                ..Default::default()
            });
            let mut iter = SstableIterator::new(sst, store.clone(), options, &info);
            iter.rewind().await.unwrap();
            assert_eq!(iter.read_ahead_end, expected);
            assert_eq!(iter.read_ahead.len(), expected - 1);
            assert_eq!(
                store
                    .read_metrics
                    .blockwise_prefetch_window_blocks
                    .with_label_values(&[reason])
                    .get_sample_sum(),
                expected as f64
            );
            // A seek abandons the old neighbors; Drop abandons the replacement window.
            iter.rewind().await.unwrap();
            drop(iter);
            let events = &store.read_metrics.blockwise_prefetch_events;
            let registered = events.with_label_values(&["registered_ready"]).get()
                + events.with_label_values(&["registered_pending"]).get()
                + events.with_label_values(&["registration_error"]).get();
            assert_eq!(registered, 2 * (expected - 1) as u64);
            assert_eq!(events.with_label_values(&["discarded"]).get(), registered);
        }
    }

    #[tokio::test]
    async fn test_blockwise_prefetch_window_and_single_flight() {
        use std::time::Duration;

        use foyer::HybridCacheProperties;
        use tokio::sync::Notify;

        use crate::hummock::iterator::test_utils::mock_sstable_store_with_recent_filter;
        use crate::hummock::simple::SimpleRecentFilter;
        use crate::hummock::{Block, HummockError, RecentFilterTrait, SstableBlockIndex};

        let recent: Arc<crate::hummock::RecentFilter<_>> =
            Arc::new(SimpleRecentFilter::new(2, Duration::from_secs(3600)).into());
        let store = mock_sstable_store_with_recent_filter(recent.clone()).await;
        let (sst, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 10, store.clone()).await;
        assert!(sst.meta.block_metas.len() > 7);
        store.clear_block_cache().await.unwrap();
        let block_key = |idx| SstableBlockIndex {
            sst_id: sst.id,
            block_idx: idx,
        };
        let path = store.get_sst_data_path(sst.id);
        let data = store.store().read(&path, ..).await.unwrap();

        // A B C D E F G: C is cached; E belongs to a pre-existing slow reader.
        drop(
            store
                .get(
                    &sst,
                    2,
                    CachePolicy::default(),
                    &mut StoreLocalStatistic::default(),
                )
                .await
                .unwrap(),
        );
        let (range, capacity) = sst.calculate_block_info(4);
        let block_e = Box::new(Block::decode(data.slice(range), capacity).unwrap());
        let release = Arc::new(Notify::new());
        let existing = store.block_cache().get_or_fetch(&block_key(4), {
            let release = release.clone();
            move || async move {
                release.notified().await;
                Ok::<_, HummockError>((block_e, HybridCacheProperties::default()))
            }
        });
        // Rereading C or E remotely now fails decoding. The real cache and single-flight
        // must supply them; a contiguous range reader cannot substitute for this path.
        let mut damaged = data.to_vec();
        for idx in [2, 4] {
            damaged[sst.meta.block_metas[idx].offset as usize] ^= 1;
        }
        store.store().upload(&path, damaged.into()).await.unwrap();
        let options = Arc::new(SstableIteratorReadOptions {
            prefetch: PrefetchMode::Blockwise,
            scan_end_user_key: Some(Excluded(
                FullKey::decode(&sst.meta.block_metas[7].smallest_key)
                    .user_key
                    .copy_into(),
            )),
            ..Default::default()
        });
        let mut iter = SstableIterator::new(sst.clone(), store.clone(), options.clone(), &info);
        iter.rewind().await.unwrap();
        assert_eq!(iter.read_ahead_end, 3);
        // The observer and read-ahead window each hold C. Remove it from the cache so a
        // second lookup at consumption would read the corrupted backing block and fail.
        assert_eq!(
            store
                .block_cache()
                .memory()
                .get(&block_key(2))
                .unwrap()
                .refs(),
            2
        );
        store.block_cache().remove(&block_key(2));
        assert!(recent.contains(&(sst.id, 1)));
        assert!(!recent.contains(&(sst.id, 3)));
        // Consuming A and B does not slide the window ahead of consumption.
        while iter.cur_idx < 2 {
            iter.next().await.unwrap();
        }
        assert_eq!(iter.read_ahead_end, 3);
        assert!(!recent.contains(&(sst.id, 3)));
        tokio::time::timeout(Duration::from_secs(5), async {
            while iter.cur_idx < 3 {
                iter.next().await.unwrap();
            }
        })
        .await
        .expect("lookahead must not wait for E before returning D");
        assert_eq!(iter.read_ahead_end, 6);
        assert!(recent.contains(&(sst.id, 4)));
        assert!(recent.contains(&(sst.id, 5)));
        assert!(!recent.contains(&(sst.id, 6)));
        assert!(iter.preload_stream.is_none());
        assert_eq!(store.get_prefetch_memory_usage(), 0);

        // Abandoning the iterator must not cancel the shared E producer or retain neighbors.
        drop(iter);
        release.notify_one();
        drop(
            tokio::time::timeout(Duration::from_secs(5), existing)
                .await
                .unwrap()
                .unwrap(),
        );
        // C was removed from the cache above. Restore its backing bytes for the next scan.
        store.store().upload(&path, data).await.unwrap();
        let mut iter = SstableIterator::new(sst.clone(), store.clone(), options, &info);
        iter.rewind().await.unwrap();
        while iter.is_valid() {
            iter.next().await.unwrap();
        }
        assert_eq!(iter.read_ahead_end, 7);
        assert!(!recent.contains(&(sst.id, 7)));
        drop(iter);
        for idx in 0..7 {
            assert_eq!(
                store
                    .block_cache()
                    .memory()
                    .get(&block_key(idx))
                    .unwrap()
                    .refs(),
                1
            );
        }
        store.clear_block_cache().await.unwrap();
        assert_eq!(store.block_cache().memory().entries(), 0);
    }

    #[tokio::test]
    async fn test_blockwise_prefetch_failure_and_seek_release() {
        use foyer::HybridCacheProperties;
        use tokio::sync::Notify;

        use crate::hummock::{Block, HummockError, SstableBlockIndex};

        let store = mock_sstable_store().await;
        let (sst, info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 10, store.clone()).await;
        store.clear_block_cache().await.unwrap();
        let key = |idx| SstableBlockIndex {
            sst_id: sst.id,
            block_idx: idx,
        };
        drop(
            store
                .get(
                    &sst,
                    2,
                    CachePolicy::default(),
                    &mut StoreLocalStatistic::default(),
                )
                .await
                .unwrap(),
        );
        let release = Arc::new(Notify::new());
        let failed = store.block_cache().get_or_fetch(&key(1), {
            let release = release.clone();
            move || async move {
                release.notified().await;
                Err::<(Box<Block>, HybridCacheProperties), _>(HummockError::other(
                    "injected speculative failure",
                ))
            }
        });
        let mut iter = SstableIterator::new(
            sst.clone(),
            store.clone(),
            Arc::new(SstableIteratorReadOptions {
                prefetch: PrefetchMode::Blockwise,
                ..Default::default()
            }),
            &info,
        );
        iter.rewind().await.unwrap();
        release.notify_one();
        assert!(failed.await.is_err());
        // B's registered response failed. Demand must retry through the normal cache path.
        while iter.cur_idx < 1 {
            iter.next().await.unwrap();
        }
        assert_eq!(store.block_cache().memory().get(&key(2)).unwrap().refs(), 2);
        // Seeking to D abandons the retained C handle before registering the new window.
        iter.seek(FullKey::decode(&sst.meta.block_metas[3].smallest_key))
            .await
            .unwrap();
        assert_eq!(store.block_cache().memory().get(&key(2)).unwrap().refs(), 1);
    }

    #[tokio::test]
    async fn test_blockwise_prefetch_respects_bound_and_cache_policy() {
        use std::time::Duration;

        use crate::hummock::RecentFilterTrait;
        use crate::hummock::iterator::test_utils::mock_sstable_store_with_recent_filter;
        use crate::hummock::simple::SimpleRecentFilter;

        for (policy, end, requested_end) in [
            (CachePolicy::default(), 1, 1),
            (CachePolicy::default(), 2, 2),
            (CachePolicy::NotFill, 3, 1),
            (CachePolicy::Disable, 3, 1),
        ] {
            let recent: Arc<crate::hummock::RecentFilter<_>> =
                Arc::new(SimpleRecentFilter::new(2, Duration::from_secs(3600)).into());
            let store = mock_sstable_store_with_recent_filter(recent.clone()).await;
            let (sst, info) =
                gen_default_test_sstable(default_builder_opt_for_test(), 10, store.clone()).await;
            store.clear_block_cache().await.unwrap();
            let options = Arc::new(SstableIteratorReadOptions {
                prefetch: PrefetchMode::Blockwise,
                cache_policy: policy,
                scan_end_user_key: Some(Excluded(
                    FullKey::decode(&sst.meta.block_metas[end].smallest_key)
                        .user_key
                        .copy_into(),
                )),
                ..Default::default()
            });
            let mut iter = SstableIterator::new(sst.clone(), store, options, &info);
            iter.rewind().await.unwrap();
            for idx in 0..4 {
                assert_eq!(recent.contains(&(sst.id, idx)), idx < requested_end);
            }
            assert!(iter.preload_stream.is_none());
        }
    }

    async fn inner_test_forward_iterator(
        sstable_store: SstableStoreRef,
        handle: TableHolder,
        sstable_info: SstableInfo,
    ) {
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        let mut sstable_iter = SstableIterator::create(
            handle,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
            &sstable_info,
        );
        let mut cnt = 0;
        sstable_iter.rewind().await.unwrap();

        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_eq!(key, test_key_of(cnt).to_ref());
            assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
            cnt += 1;
            sstable_iter.next().await.unwrap();
        }

        assert_eq!(cnt, TEST_KEYS_COUNT);
    }

    #[tokio::test]
    async fn test_table_iterator() {
        // Build remote sstable
        let sstable_store = mock_sstable_store().await;
        let (sstable, sstable_info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        assert!(sstable.meta.block_metas.len() > 10);

        inner_test_forward_iterator(sstable_store.clone(), sstable, sstable_info).await;
    }

    #[tokio::test]
    async fn test_table_seek() {
        let sstable_store = mock_sstable_store().await;
        let (sstable, sstable_info) =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        assert!(sstable.meta.block_metas.len() > 10);
        let mut sstable_iter = SstableIterator::create(
            sstable,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
            &sstable_info,
        );
        let mut all_key_to_test = (0..TEST_KEYS_COUNT).collect_vec();
        let mut rng = thread_rng();
        all_key_to_test.shuffle(&mut rng);

        // We seek and access all the keys in random order
        for i in all_key_to_test {
            sstable_iter.seek(test_key_of(i).to_ref()).await.unwrap();
            // sstable_iter.next().await.unwrap();
            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(i).to_ref());
        }

        // Seek to key #500 and start iterating.
        sstable_iter.seek(test_key_of(500).to_ref()).await.unwrap();
        for i in 500..TEST_KEYS_COUNT {
            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(i).to_ref());
            sstable_iter.next().await.unwrap();
        }
        assert!(!sstable_iter.is_valid());

        // Seek to < first key
        let smallest_key = FullKey::for_test(
            TableId::default(),
            [
                VirtualNode::ZERO.to_be_bytes().as_slice(),
                format!("key_aaaa_{:05}", 0).as_bytes(),
            ]
            .concat(),
            test_epoch(233),
        );
        sstable_iter.seek(smallest_key.to_ref()).await.unwrap();
        let key = sstable_iter.key();
        assert_eq!(key, test_key_of(0).to_ref());

        // Seek to > last key
        let largest_key = FullKey::for_test(
            TableId::default(),
            [
                VirtualNode::ZERO.to_be_bytes().as_slice(),
                format!("key_zzzz_{:05}", 0).as_bytes(),
            ]
            .concat(),
            test_epoch(233),
        );
        sstable_iter.seek(largest_key.to_ref()).await.unwrap();
        assert!(!sstable_iter.is_valid());

        // Seek to non-existing key
        for idx in 1..TEST_KEYS_COUNT {
            // Seek to the previous key of each existing key. e.g.,
            // Our key space is `key_test_00000`, `key_test_00002`, `key_test_00004`, ...
            // And we seek to `key_test_00001` (will produce `key_test_00002`), `key_test_00003`
            // (will produce `key_test_00004`).
            sstable_iter
                .seek(
                    FullKey::for_test(
                        TableId::default(),
                        [
                            VirtualNode::ZERO.to_be_bytes().as_slice(),
                            format!("key_test_{:05}", idx * 2 - 1).as_bytes(),
                        ]
                        .concat(),
                        0,
                    )
                    .to_ref(),
                )
                .await
                .unwrap();

            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(idx).to_ref());
            sstable_iter.next().await.unwrap();
        }
        assert!(!sstable_iter.is_valid());
    }

    #[tokio::test]
    async fn test_prefetch_table_read() {
        let sstable_store = mock_sstable_store().await;
        // when upload data is successful, but upload meta is fail and delete is fail
        let kv_iter =
            (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i))));
        let sst_info = gen_test_sstable_info(
            default_builder_opt_for_test(),
            0,
            kv_iter,
            sstable_store.clone(),
        )
        .await;

        let end_key = test_key_of(TEST_KEYS_COUNT);
        let uk = UserKey::new(
            end_key.user_key.table_id,
            TableKey(Bytes::from(end_key.user_key.table_key.0)),
        );
        let options = Arc::new(SstableIteratorReadOptions {
            cache_policy: CachePolicy::Fill(Hint::Normal),
            read_table_id: None,
            scan_end_user_key: Some(Bound::Included(uk.clone())),
            prefetch: crate::store::PrefetchMode::Range,
        });
        let mut stats = StoreLocalStatistic::default();
        let mut sstable_iter = SstableIterator::create(
            sstable_store.sstable(&sst_info, &mut stats).await.unwrap(),
            sstable_store.clone(),
            options.clone(),
            &sst_info,
        );
        let mut cnt = 1000;
        sstable_iter.seek(test_key_of(cnt).to_ref()).await.unwrap();
        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_eq!(
                key,
                test_key_of(cnt).to_ref(),
                "fail at {}, get key :{:?}",
                cnt,
                String::from_utf8(key.user_key.table_key.key_part().to_vec()).unwrap()
            );
            assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
            cnt += 1;
            sstable_iter.next().await.unwrap();
        }
        assert_eq!(cnt, TEST_KEYS_COUNT);
        let mut sstable_iter = SstableIterator::create(
            sstable_store.sstable(&sst_info, &mut stats).await.unwrap(),
            sstable_store,
            options.clone(),
            &sst_info,
        );

        // Cache hits produce a singleton prefetch stream. Seeking past its end must
        // fetch the requested block instead of repeatedly polling the exhausted stream.
        assert!(
            sstable_iter.calculate_block_idx_by_key(test_key_of(TEST_KEYS_COUNT / 2).to_ref())
                > sstable_iter.calculate_block_idx_by_key(test_key_of(1000).to_ref()) + 1
        );
        for idx in [1000, TEST_KEYS_COUNT / 2, TEST_KEYS_COUNT - 1] {
            sstable_iter.seek(test_key_of(idx).to_ref()).await.unwrap();
            assert!(sstable_iter.is_valid());
            assert_eq!(sstable_iter.key(), test_key_of(idx).to_ref());
            assert_bytes_eq!(
                sstable_iter.value().into_user_value().unwrap(),
                test_value_of(idx)
            );
        }

        let mut cnt = 1000;
        sstable_iter.seek(test_key_of(cnt).to_ref()).await.unwrap();
        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_eq!(key, test_key_of(cnt).to_ref());
            assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
            cnt += 1;
            sstable_iter.next().await.unwrap();
        }
        assert_eq!(cnt, TEST_KEYS_COUNT);
    }

    #[tokio::test]
    async fn test_prefetch_seek_preserves_remaining_blocks() {
        let sstable_store = mock_sstable_store().await;
        let mut builder_options = default_builder_opt_for_test();
        builder_options.block_capacity = 128;
        let info = gen_test_sstable_info(
            builder_options,
            0,
            (0..64).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
            sstable_store.clone(),
        )
        .await;
        let sstable = sstable_store
            .sstable(&info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        assert!(sstable.meta.block_metas.len() > 16);
        let mut iter = SstableIterator::new(
            sstable.clone(),
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions {
                prefetch: crate::store::PrefetchMode::Range,
                cache_policy: CachePolicy::Disable,
                ..Default::default()
            }),
            &info,
        );
        let mut reference = SstableIterator::new(
            sstable.clone(),
            sstable_store.clone(),
            Arc::new(SstableIteratorReadOptions {
                prefetch: crate::store::PrefetchMode::Disabled,
                cache_policy: CachePolicy::Disable,
                ..Default::default()
            }),
            &info,
        );
        let last = sstable.meta.block_metas.len() - 1;
        // Initial prefetch, forward skip, backward seek, reuse, final-block fallback,
        // rewind after exhaustion, and reread of an already consumed block.
        for (idx, prefetches, gets) in [
            (0, 1, 0),
            (2, 1, 0),
            (1, 1, 1),
            (3, 1, 1),
            (last, 1, 2),
            (0, 2, 2),
            (0, 2, 3),
        ] {
            let key = FullKey::decode(&sstable.meta.block_metas[idx].smallest_key);
            iter.seek(key).await.unwrap();
            reference.seek(key).await.unwrap();
            assert_eq!(iter.key(), reference.key(), "block {idx}");
            assert_bytes_eq!(
                iter.value().into_user_value().unwrap(),
                reference.value().into_user_value().unwrap()
            );
            assert_eq!(
                iter.stats.cache_data_prefetch_count, prefetches,
                "block {idx}"
            );
            assert_eq!(iter.stats.cache_data_block_total, gets, "block {idx}");
            if idx == last {
                assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
            } else {
                assert!(sstable_store.get_prefetch_memory_usage() > 0);
            }
        }
        drop(iter);
        assert_eq!(sstable_store.get_prefetch_memory_usage(), 0);
    }

    #[tokio::test]
    async fn test_scan_end_with_prefetch_on_or_off() {
        let sstable_store = mock_sstable_store().await;
        let mut builder_options = default_builder_opt_for_test();
        builder_options.block_capacity = 128;

        let test_user_key = |table_id, idx| {
            UserKey::new(
                TableId::new(table_id),
                TableKey(Bytes::from(
                    [
                        VirtualNode::ZERO.to_be_bytes().as_slice(),
                        format!("key_{idx:05}").as_bytes(),
                    ]
                    .concat(),
                )),
            )
        };
        let test_key = |table_id, idx| FullKey {
            user_key: test_user_key(table_id, idx),
            epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
        };
        let kv_pairs = (1..=3).flat_map(|table_id| {
            (0..8).map(move |idx| {
                (
                    test_key(table_id, idx),
                    HummockValue::put(Bytes::from(test_value_of(idx))),
                )
            })
        });
        let (sstable, sstable_info) = gen_test_sstable_with_table_ids(
            builder_options,
            10,
            kv_pairs,
            sstable_store.clone(),
            vec![1, 2, 3],
        )
        .await;

        let table_2_block_start = sstable
            .meta
            .block_metas
            .partition_point(|block_meta| block_meta.table_id() < TableId::new(2));
        let table_3_block_start = sstable
            .meta
            .block_metas
            .partition_point(|block_meta| block_meta.table_id() < TableId::new(3));
        assert!(table_2_block_start + 2 < table_3_block_start);

        let table_3_start = UserKey::new(TableId::new(3), TableKey(Bytes::from_static(b"")));
        for (case, prefetch) in [("prefetch off", false), ("prefetch on", true)] {
            let options = Arc::new(SstableIteratorReadOptions {
                cache_policy: CachePolicy::Disable,
                read_table_id: None,
                scan_end_user_key: Some(Bound::Excluded(table_3_start.clone())),
                prefetch: if prefetch {
                    PrefetchMode::Range
                } else {
                    PrefetchMode::Disabled
                },
            });
            let mut sstable_iter = SstableIterator::create(
                sstable.clone(),
                sstable_store.clone(),
                options,
                &sstable_info,
            );

            assert_eq!(sstable_iter.block_end_idx_exclusive, table_3_block_start);
            sstable_iter.seek(test_key(2, 0).to_ref()).await.unwrap();
            if prefetch {
                assert_eq!(
                    sstable_iter.preload_end_block_idx, sstable_iter.block_end_idx_exclusive,
                    "{case}"
                );
            } else {
                assert_eq!(sstable_iter.preload_end_block_idx, 0, "{case}");
            }
            let mut key_count = 0;
            while sstable_iter.is_valid() {
                assert_eq!(
                    sstable_iter.key().user_key.table_id,
                    TableId::new(2),
                    "{case}"
                );
                key_count += 1;
                sstable_iter.next().await.unwrap();
            }
            assert_eq!(key_count, 8, "{case}");

            let mut stats = StoreLocalStatistic::default();
            sstable_iter.collect_local_statistic(&mut stats);
            assert_eq!(
                stats.cache_data_block_total + stats.cache_data_prefetch_block_count,
                (table_3_block_start - table_2_block_start) as u64,
                "{case}"
            );
            if prefetch {
                assert!(stats.cache_data_prefetch_block_count > 0, "{case}");
            } else {
                assert_eq!(stats.cache_data_prefetch_block_count, 0, "{case}");
            }
        }

        let options = Arc::new(SstableIteratorReadOptions {
            cache_policy: CachePolicy::Disable,
            read_table_id: Some(TableId::new(2)),
            scan_end_user_key: None,
            prefetch: crate::store::PrefetchMode::Disabled,
        });
        let mut sstable_iter = SstableIterator::create(
            sstable.clone(),
            sstable_store.clone(),
            options,
            &sstable_info,
        );
        assert_eq!(sstable_iter.block_start_idx_inclusive, table_2_block_start);
        assert_eq!(sstable_iter.block_end_idx_exclusive, table_3_block_start);
        sstable_iter.rewind().await.unwrap();
        assert!(sstable_iter.is_valid());
        while sstable_iter.is_valid() {
            assert_eq!(sstable_iter.key().user_key.table_id, TableId::new(2));
            sstable_iter.next().await.unwrap();
        }

        for missing_table_id in [0, 4] {
            let options = Arc::new(SstableIteratorReadOptions {
                read_table_id: Some(TableId::new(missing_table_id)),
                ..Default::default()
            });
            let mut sstable_iter = SstableIterator::create(
                sstable.clone(),
                sstable_store.clone(),
                options,
                &sstable_info,
            );
            sstable_iter.rewind().await.unwrap();
            for table_id in 1..=3 {
                for idx in 0..8 {
                    assert!(sstable_iter.is_valid());
                    assert_eq!(sstable_iter.key(), test_key(table_id, idx).to_ref());
                    sstable_iter.next().await.unwrap();
                }
            }
            assert!(!sstable_iter.is_valid());
        }

        let mut table_2_sstable_info = sstable_info.get_inner();
        table_2_sstable_info.table_ids = vec![TableId::new(2)];
        let table_2_sstable_info = SstableInfo::from(table_2_sstable_info);
        let table_2_start: UserKey<Bytes> =
            FullKey::decode(&sstable.meta.block_metas[table_2_block_start].smallest_key)
                .user_key
                .copy_into();
        let options = Arc::new(SstableIteratorReadOptions {
            cache_policy: CachePolicy::Disable,
            read_table_id: None,
            scan_end_user_key: Some(Bound::Excluded(table_2_start)),
            prefetch: crate::store::PrefetchMode::Disabled,
        });
        let mut sstable_iter =
            SstableIterator::create(sstable, sstable_store, options, &table_2_sstable_info);

        assert_eq!(
            sstable_iter.block_start_idx_inclusive,
            sstable_iter.block_end_idx_exclusive
        );
        sstable_iter.seek(test_key(2, 0).to_ref()).await.unwrap();
        assert!(!sstable_iter.is_valid());
        let mut stats = StoreLocalStatistic::default();
        sstable_iter.collect_local_statistic(&mut stats);
        assert_eq!(stats.cache_data_block_total, 0);
        assert_eq!(stats.cache_data_prefetch_block_count, 0);
    }

    #[tokio::test]
    async fn test_read_table_id_range() {
        {
            let sstable_store = mock_sstable_store().await;
            let (sstable, sstable_info) =
                gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                    .await;
            let mut sstable_iter = SstableIterator::create(
                sstable,
                sstable_store.clone(),
                Arc::new(SstableIteratorReadOptions::default()),
                &sstable_info,
            );
            sstable_iter.rewind().await.unwrap();
            assert!(sstable_iter.is_valid());
            assert_eq!(sstable_iter.key(), test_key_of(0).to_ref());
        }

        {
            let sstable_store = mock_sstable_store().await;
            // test key_range right
            let k1 = {
                let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
                table_key.extend_from_slice(format!("key_test_{:05}", 1).as_bytes());
                let uk = UserKey::for_test(TableId::from(1), table_key);
                FullKey {
                    user_key: uk,
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                }
            };

            let k2 = {
                let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
                table_key.extend_from_slice(format!("key_test_{:05}", 2).as_bytes());
                let uk = UserKey::for_test(TableId::from(2), table_key);
                FullKey {
                    user_key: uk,
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                }
            };

            let k3 = {
                let mut table_key = VirtualNode::ZERO.to_be_bytes().to_vec();
                table_key.extend_from_slice(format!("key_test_{:05}", 3).as_bytes());
                let uk = UserKey::for_test(TableId::from(3), table_key);
                FullKey {
                    user_key: uk,
                    epoch_with_gap: EpochWithGap::new_from_epoch(test_epoch(1)),
                }
            };

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;
                let mut sstable_iter = SstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![1.into(), 2.into(), 3.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k1.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(3, cnt);
                assert_eq!(last_key, k3.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = SstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![1.into(), 2.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k1.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(2, cnt);
                assert_eq!(last_key, k2.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = SstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![2.into(), 3.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k2.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(2, cnt);
                assert_eq!(last_key, k3.clone());
            }

            {
                let kv_pairs = vec![
                    (k1.clone(), HummockValue::put(test_value_of(1))),
                    (k2.clone(), HummockValue::put(test_value_of(2))),
                    (k3.clone(), HummockValue::put(test_value_of(3))),
                ];

                let (sstable, _sstable_info) = gen_test_sstable_with_table_ids(
                    default_builder_opt_for_test(),
                    10,
                    kv_pairs.into_iter(),
                    sstable_store.clone(),
                    vec![1, 2, 3],
                )
                .await;

                let mut sstable_iter = SstableIterator::create(
                    sstable,
                    sstable_store.clone(),
                    Arc::new(SstableIteratorReadOptions::default()),
                    &SstableInfo::from(SstableInfoInner {
                        table_ids: vec![2.into()],
                        ..Default::default()
                    }),
                );
                sstable_iter.rewind().await.unwrap();
                assert!(sstable_iter.is_valid());
                assert!(sstable_iter.key().eq(&k2.to_ref()));

                let mut cnt = 0;
                let mut last_key = k1.clone();
                while sstable_iter.is_valid() {
                    last_key = sstable_iter.key().to_vec();
                    cnt += 1;
                    sstable_iter.next().await.unwrap();
                }

                assert_eq!(1, cnt);
                assert_eq!(last_key, k2.clone());
            }
        }
    }
}
