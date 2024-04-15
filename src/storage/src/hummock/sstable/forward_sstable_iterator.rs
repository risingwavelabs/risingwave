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

use std::cmp::Ordering::{Equal, Less};
use std::ops::Bound::*;
use std::sync::Arc;

use await_tree::InstrumentAwait;
use risingwave_hummock_sdk::key::FullKey;
use thiserror_ext::AsReport;

use super::super::{HummockResult, HummockValue};
use crate::hummock::block_stream::BlockStream;
use crate::hummock::iterator::{Forward, HummockIterator, ValueMeta};
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::{BlockIterator, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;

pub trait SstableIteratorType: HummockIterator + 'static {
    fn create(
        sstable: TableHolder,
        sstable_store: SstableStoreRef,
        read_options: Arc<SstableIteratorReadOptions>,
    ) -> Self;
}

/// Iterates on a sstable.
pub struct SstableIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    preload_stream: Option<Box<dyn BlockStream>>,
    /// Reference to the sst
    pub sst: TableHolder,
    preload_end_block_idx: usize,
    preload_retry_times: usize,

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
            preload_stream: None,
            sst: sstable,
            sstable_store,
            stats: StoreLocalStatistic::default(),
            options,
            preload_end_block_idx: 0,
            preload_retry_times: 0,
        }
    }

    fn init_block_prefetch_range(&mut self, start_idx: usize) {
        self.preload_end_block_idx = 0;
        if let Some(bound) = self.options.must_iterated_end_user_key.as_ref() {
            let block_metas = &self.sst.meta.block_metas;
            let next_to_start_idx = start_idx + 1;
            if next_to_start_idx < block_metas.len() {
                let end_idx = match bound {
                    Unbounded => block_metas.len(),
                    Included(dest_key) => {
                        let dest_key = dest_key.as_ref();
                        block_metas.partition_point(|block_meta| {
                            FullKey::decode(&block_meta.smallest_key).user_key <= dest_key
                        })
                    }
                    Excluded(end_key) => {
                        let end_key = end_key.as_ref();
                        block_metas.partition_point(|block_meta| {
                            FullKey::decode(&block_meta.smallest_key).user_key < end_key
                        })
                    }
                };
                if start_idx + 1 < end_idx {
                    self.preload_end_block_idx = end_idx;
                }
            }
        }
    }

    pub(crate) fn sst(&self) -> &TableHolder {
        &self.sst
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

        let mut hit_cache = false;
        if idx >= self.sst.block_count() {
            self.block_iter = None;
            return Ok(());
        }
        // Maybe the previous preload stream breaks on some cached block, so here we can try to preload some data again
        if self.preload_stream.is_none() && idx + 1 < self.preload_end_block_idx {
            match self
                .sstable_store
                .prefetch_blocks(
                    &self.sst,
                    idx,
                    self.preload_end_block_idx,
                    self.options.cache_policy,
                    &mut self.stats,
                )
                .verbose_instrument_await("prefetch_blocks")
                .await
            {
                Ok(preload_stream) => self.preload_stream = Some(preload_stream),
                Err(e) => {
                    tracing::warn!(error = %e.as_report(), "failed to create stream for prefetch data, fall back to block get")
                }
            }
        }

        if self
            .preload_stream
            .as_ref()
            .map(|preload_stream| preload_stream.next_block_index() <= idx)
            .unwrap_or(false)
        {
            while let Some(preload_stream) = self.preload_stream.as_mut() {
                let mut ret = Ok(());
                while preload_stream.next_block_index() < idx {
                    if let Err(e) = preload_stream.next_block().await {
                        ret = Err(e);
                        break;
                    }
                }
                assert_eq!(preload_stream.next_block_index(), idx);
                if ret.is_ok() {
                    match preload_stream.next_block().await {
                        Ok(Some(block)) => {
                            hit_cache = true;
                            self.block_iter = Some(BlockIterator::new(block));
                            break;
                        }
                        Ok(None) => {
                            self.preload_stream.take();
                        }
                        Err(e) => {
                            self.preload_stream.take();
                            ret = Err(e);
                        }
                    }
                } else {
                    self.preload_stream.take();
                }
                if self.preload_stream.is_none() && idx + 1 < self.preload_end_block_idx {
                    if let Err(e) = ret {
                        tracing::warn!(error = %e.as_report(), "recreate stream because the connection to remote storage has closed");
                        if self.preload_retry_times >= self.options.max_preload_retry_times {
                            break;
                        }
                        self.preload_retry_times += 1;
                    }

                    match self
                        .sstable_store
                        .prefetch_blocks(
                            &self.sst,
                            idx,
                            self.preload_end_block_idx,
                            self.options.cache_policy,
                            &mut self.stats,
                        )
                        .verbose_instrument_await("prefetch_blocks")
                        .await
                    {
                        Ok(stream) => {
                            self.preload_stream = Some(stream);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e.as_report(), "failed to recreate stream meet IO error");
                            break;
                        }
                    }
                }
            }
        }
        if !hit_cache {
            let block = self
                .sstable_store
                .get(&self.sst, idx, self.options.cache_policy, &mut self.stats)
                .await?;
            self.block_iter = Some(BlockIterator::new(block));
        };
        let block_iter = self.block_iter.as_mut().unwrap();
        if let Some(key) = seek_key {
            block_iter.seek(key);
        } else {
            block_iter.seek_to_first();
        }

        self.cur_idx = idx;
        Ok(())
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
        self.block_iter.as_ref().map_or(false, |i| i.is_valid())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.init_block_prefetch_range(0);
        self.seek_idx(0, None).await?;
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        let block_idx = self
            .sst
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
        self.init_block_prefetch_range(block_idx);

        self.seek_idx(block_idx, Some(key)).await?;
        if !self.is_valid() {
            // seek to next block
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
    ) -> Self {
        SstableIterator::new(sstable, sstable_store, options)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::Bound;

    use bytes::Bytes;
    use foyer::memory::CacheContext;
    use itertools::Itertools;
    use rand::prelude::*;
    use risingwave_common::catalog::TableId;
    use risingwave_common::hash::VirtualNode;
    use risingwave_common::util::epoch::test_epoch;
    use risingwave_hummock_sdk::key::{TableKey, UserKey};

    use super::*;
    use crate::assert_bytes_eq;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_default_test_sstable, gen_test_sstable_info, test_key_of,
        test_value_of, TEST_KEYS_COUNT,
    };
    use crate::hummock::CachePolicy;

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

        inner_test_forward_iterator(sstable_store.clone(), sstable).await;
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
        let mut sstable_iter = SstableIterator::create(
            sstable,
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
        let sstable_store = mock_sstable_store();
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
            cache_policy: CachePolicy::Fill(CacheContext::Default),
            must_iterated_end_user_key: Some(Bound::Included(uk.clone())),
            max_preload_retry_times: 0,
            prefetch_for_large_query: false,
        });
        let mut stats = StoreLocalStatistic::default();
        let mut sstable_iter = SstableIterator::create(
            sstable_store.sstable(&sst_info, &mut stats).await.unwrap(),
            sstable_store.clone(),
            options.clone(),
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
        );
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
}
