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

use std::cmp::Ordering::{Equal, Less};
use std::collections::VecDeque;
use std::future::Future;
use std::ops::Bound::*;
use std::sync::Arc;

use risingwave_hummock_sdk::key::FullKey;

use super::super::{HummockResult, HummockValue};
use super::Sstable;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::{
    BlockHolder, BlockIterator, BlockResponse, CachePolicy, SstableStore, SstableStoreRef,
    TableHolder,
};
use crate::monitor::StoreLocalStatistic;

pub trait SstableIteratorType: HummockIterator + 'static {
    fn create(
        sstable: TableHolder,
        sstable_store: SstableStoreRef,
        read_options: Arc<SstableIteratorReadOptions>,
    ) -> Self;
}

/// Prefetching may increase the memory footprint of the CN process because the prefetched blocks
/// cannot be evicted.
enum BlockFetcher {
    Simple(SimpleFetchContext),
    Prefetch(PrefetchContext),
}

impl BlockFetcher {
    async fn get_block(
        &mut self,
        sst: &Sstable,
        block_idx: usize,
        sstable_store: &SstableStore,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        match self {
            BlockFetcher::Simple(context) => {
                sstable_store
                    .get(sst, block_idx, context.cache_policy, stats)
                    .await
            }
            BlockFetcher::Prefetch(context) => {
                context
                    .get_block(sst, block_idx, sstable_store, stats)
                    .await
            }
        }
    }
}

struct SimpleFetchContext {
    cache_policy: CachePolicy,
}

struct PrefetchContext {
    prefetched_blocks: VecDeque<(usize, BlockResponse)>,

    /// block[cur_idx..=dest_idx] will definitely be visited in the future.
    dest_idx: usize,

    cache_policy: CachePolicy,
}

const DEFAULT_PREFETCH_BLOCK_NUM: usize = 1;

impl PrefetchContext {
    fn new(dest_idx: usize, cache_policy: CachePolicy) -> Self {
        Self {
            prefetched_blocks: VecDeque::with_capacity(DEFAULT_PREFETCH_BLOCK_NUM + 1),
            dest_idx,
            cache_policy,
        }
    }

    async fn get_block(
        &mut self,
        sst: &Sstable,
        idx: usize,
        sstable_store: &SstableStore,
        stats: &mut StoreLocalStatistic,
    ) -> HummockResult<BlockHolder> {
        let is_empty = if let Some((prefetched_idx, _)) = self.prefetched_blocks.front() {
            if *prefetched_idx == idx {
                false
            } else {
                tracing::warn!(target: "events::storage::sstable::block_seek", "prefetch mismatch: sstable_object_id = {}, block_id = {}, prefetched_block_id = {}", sst.id, idx, *prefetched_idx);
                self.prefetched_blocks.clear();
                true
            }
        } else {
            true
        };
        if is_empty {
            self.prefetched_blocks.push_back((
                idx,
                sstable_store
                    .get_block_response(sst, idx, self.cache_policy, stats)
                    .await?,
            ));
        }
        let block_response = self.prefetched_blocks.pop_front().unwrap().1;

        let next_prefetch_idx = self
            .prefetched_blocks
            .back()
            .map_or(idx, |(latest_idx, _)| *latest_idx)
            + 1;
        if next_prefetch_idx <= self.dest_idx {
            self.prefetched_blocks.push_back((
                next_prefetch_idx,
                sstable_store
                    .get_block_response(sst, next_prefetch_idx, self.cache_policy, stats)
                    .await?,
            ));
        }

        block_response.wait().await
    }
}

/// Iterates on a sstable.
pub struct SstableIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    /// simple or prefetch strategy
    block_fetcher: BlockFetcher,

    /// Reference to the sst
    pub sst: TableHolder,

    sstable_store: SstableStoreRef,
    stats: StoreLocalStatistic,
    options: Arc<SstableIteratorReadOptions>,
}

impl SstableIterator {
    pub fn new(
        sstable: TableHolder,
        sstable_store: SstableStoreRef,
        options: Arc<SstableIteratorReadOptions>,
    ) -> Self {
        Self {
            block_iter: None,
            cur_idx: 0,
            block_fetcher: BlockFetcher::Simple(SimpleFetchContext {
                cache_policy: options.cache_policy,
            }),
            sst: sstable,
            sstable_store,
            stats: StoreLocalStatistic::default(),
            options,
        }
    }

    fn init_block_fetcher(&mut self, start_idx: usize) {
        if let Some(bound) = self.options.must_iterated_end_user_key.as_ref() {
            let block_metas = &self.sst.value().meta.block_metas;
            let next_to_start_idx = start_idx + 1;
            if next_to_start_idx < block_metas.len() {
                let dest_idx = match bound {
                    Unbounded => block_metas.len() - 1, // will not overflow
                    Included(dest_key) => {
                        let dest_key = dest_key.as_ref();
                        if FullKey::decode(&block_metas[next_to_start_idx].smallest_key).user_key
                            > dest_key
                        {
                            start_idx
                        } else {
                            next_to_start_idx
                                + block_metas[(next_to_start_idx + 1)..].partition_point(
                                    |block_meta| {
                                        FullKey::decode(&block_meta.smallest_key).user_key
                                            <= dest_key
                                    },
                                )
                        }
                    }
                    Excluded(end_key) => {
                        let end_key = end_key.as_ref();
                        if FullKey::decode(&block_metas[next_to_start_idx].smallest_key).user_key
                            >= end_key
                        {
                            start_idx
                        } else {
                            next_to_start_idx
                                + block_metas[(next_to_start_idx + 1)..].partition_point(
                                    |block_meta| {
                                        FullKey::decode(&block_meta.smallest_key).user_key < end_key
                                    },
                                )
                        }
                    }
                };
                if start_idx < dest_idx {
                    self.block_fetcher = BlockFetcher::Prefetch(PrefetchContext::new(
                        dest_idx,
                        self.options.cache_policy,
                    ));
                }
            }
        }
    }

    /// Seeks to a block, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(
        &mut self,
        idx: usize,
        seek_key: Option<FullKey<&[u8]>>,
    ) -> HummockResult<()> {
        tracing::trace!(
            target: "events::storage::sstable::block_seek",
            "table iterator seek: sstable_object_id = {}, block_id = {}",
            self.sst.value().id,
            idx,
        );

        // When all data are in block cache, it is highly possible that this iterator will stay on a
        // worker thread for a full time. Therefore, we use tokio's unstable API consume_budget to
        // do cooperative scheduling.
        tokio::task::consume_budget().await;

        if idx >= self.sst.value().block_count() {
            self.block_iter = None;
        } else {
            let block = self
                .block_fetcher
                .get_block(self.sst.value(), idx, &self.sstable_store, &mut self.stats)
                .await?;
            let mut block_iter = BlockIterator::new(block);
            if let Some(key) = seek_key {
                block_iter.seek(key);
            } else {
                block_iter.seek_to_first();
            }

            self.block_iter = Some(block_iter);
            self.cur_idx = idx;
        }

        Ok(())
    }
}

impl HummockIterator for SstableIterator {
    type Direction = Forward;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        self.stats.total_key_count += 1;
        async move {
            let block_iter = self.block_iter.as_mut().expect("no block iter");
            if block_iter.try_next() {
                Ok(())
            } else {
                // seek to next block
                self.seek_idx(self.cur_idx + 1, None).await
            }
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.block_iter.as_ref().expect("no block iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let raw_value = self.block_iter.as_ref().expect("no block iter").value();

        HummockValue::from_slice(raw_value).expect("decode error")
    }

    fn is_valid(&self) -> bool {
        self.block_iter.as_ref().map_or(false, |i| i.is_valid())
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            self.init_block_fetcher(0);
            self.seek_idx(0, None).await
        }
    }

    fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> Self::SeekFuture<'a> {
        async move {
            let block_idx = self
                .sst
                .value()
                .meta
                .block_metas
                .partition_point(|block_meta| {
                    // compare by version comparator
                    // Note: we are comparing against the `smallest_key` of the `block`, thus the
                    // partition point should be `prev(<=)` instead of `<`.
                    let ord = FullKey::decode(&block_meta.smallest_key).cmp(&key);
                    ord == Less || ord == Equal
                })
                .saturating_sub(1); // considering the boundary of 0
            self.init_block_fetcher(block_idx);

            self.seek_idx(block_idx, Some(key)).await?;
            if !self.is_valid() {
                // seek to next block
                self.seek_idx(block_idx + 1, None).await?;
            }

            Ok(())
        }
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats);
    }
}

impl SstableIteratorType for SstableIterator {
    fn create(
        sstable: TableHolder,
        sstable_store: SstableStoreRef,
        options: Arc<SstableIteratorReadOptions>,
    ) -> Self {
        SstableIterator::new(sstable, sstable_store, options)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::prelude::*;
    use risingwave_common::catalog::TableId;

    use super::*;
    use crate::assert_bytes_eq;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        create_small_table_cache, default_builder_opt_for_test, gen_default_test_sstable,
        gen_test_sstable, test_key_of, test_value_of, TEST_KEYS_COUNT,
    };

    async fn inner_test_forward_iterator(sstable_store: SstableStoreRef, handle: TableHolder) {
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        let mut sstable_iter = SstableIterator::create(
            handle,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
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
        let sstable_store = mock_sstable_store();
        let sstable =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        assert!(sstable.meta.block_metas.len() > 10);

        let cache = create_small_table_cache();
        let handle = cache.insert(0, 0, 1, Box::new(sstable));
        inner_test_forward_iterator(sstable_store.clone(), handle).await;
    }

    #[tokio::test]
    async fn test_table_seek() {
        let sstable_store = mock_sstable_store();
        let sstable =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that sstable iterator test could cover more code
        // path.
        assert!(sstable.meta.block_metas.len() > 10);
        let cache = create_small_table_cache();
        let handle = cache.insert(0, 0, 1, Box::new(sstable));

        let mut sstable_iter = SstableIterator::create(
            handle,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
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
            format!("key_aaaa_{:05}", 0).as_bytes().to_vec(),
            233,
        );
        sstable_iter.seek(smallest_key.to_ref()).await.unwrap();
        let key = sstable_iter.key();
        assert_eq!(key, test_key_of(0).to_ref());

        // Seek to > last key
        let largest_key = FullKey::for_test(
            TableId::default(),
            format!("key_zzzz_{:05}", 0).as_bytes().to_vec(),
            233,
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
                        format!("key_test_{:05}", idx * 2 - 1).as_bytes().to_vec(),
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
        let sstable_store = mock_sstable_store();
        // when upload data is successful, but upload meta is fail and delete is fail
        let kv_iter =
            (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i))));
        let table = gen_test_sstable(
            default_builder_opt_for_test(),
            0,
            kv_iter,
            sstable_store.clone(),
        )
        .await;

        let mut stats = StoreLocalStatistic::default();
        let mut sstable_iter = SstableIterator::create(
            sstable_store
                .sstable(&table.get_sstable_info(), &mut stats)
                .await
                .unwrap(),
            sstable_store,
            Arc::new(SstableIteratorReadOptions {
                cache_policy: CachePolicy::Fill,
                must_iterated_end_user_key: None,
            }),
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
}
