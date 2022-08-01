// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering::{Equal, Less};
use std::sync::Arc;

use async_trait::async_trait;
use risingwave_hummock_sdk::VersionedComparator;

use super::super::{HummockResult, HummockValue};
use super::{SstableIteratorReadOptions, SstableIteratorType};
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::{BlockHolder, BlockIterator, BlockStream, SstableStoreRef, TableHolder};
use crate::monitor::StoreLocalStatistic;

/// Iterates on a table while downloading it.
pub struct SstableStreamIterator {
    /// Iterates over the blocks of the table.
    block_stream: Option<BlockStream>,

    /// Iterates over the KV-pairs of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    /// Reference to the sst
    pub sst: TableHolder,

    sstable_store: SstableStoreRef,
    stats: StoreLocalStatistic,
}

impl SstableStreamIterator {
    pub fn new(table: TableHolder, sstable_store: SstableStoreRef) -> Self {
        Self {
            block_stream: None,
            block_iter: None,
            cur_idx: 0,
            sst: table,
            sstable_store,
            stats: StoreLocalStatistic::default(),
        }
    }

    /// Seeks to a block, and then seeks to the first position where the key >= `start_key` (if
    /// given). If the the block does not contain such a key, the iterator continues to the first
    /// key of the next block.
    async fn init(&mut self, block_idx: usize, start_key: Option<&[u8]>) -> HummockResult<()> {
        // Enforce the use of new stream and iterators.
        self.block_stream = None;
        self.block_iter = None;
        self.cur_idx = block_idx;

        if self.cur_idx >= self.sst.value().block_count() {
            return Ok(());
        }

        let block_iter = self.load_block().await?;

        if let Some(key) = start_key {
            block_iter.seek(key);
        } else {
            block_iter.seek_to_first();
        }

        // If `key` is larger than anything stored in the current block, then `block_iter` searches
        // through the whole block and eventually ends in an invalid state. We therefore move to the
        // start of the next block.
        if !block_iter.is_valid() {
            self.next_block().await?;
        }

        Ok(())
    }

    /// Loads a new block, creates a new iterator for it, stores that iterator in `self.block_iter`,
    /// and returns a reference to it.
    ///
    /// If there is an active block stream the function returns the next block from that stream.
    /// Otherwise, the function loads the block with index `self.cur_idx`. Let `B` be that block. If
    /// `B` is already in memory, the function loads it from there. Otherwise, the function starts a
    /// new stream which starts with block `B`.
    async fn load_block(&mut self) -> HummockResult<&mut BlockIterator> {
        tracing::trace!(
            target: "events::storage::sstable::block_seek",
            "table iterator seek: table_id = {}, block_id = {}",
            self.sst.value().id,
            self.cur_idx,
        );

        // When all data are in block cache, it is highly possible that this iterator will stay on a
        // worker thread for a full time. Therefore, we use tokio's unstable API consume_budget to
        // do cooperative scheduling.
        tokio::task::consume_budget().await;

        // We use the block stream (i.e. download the next block) if we already started a download,
        // or if the desired block is not in memory.
        let block = if self.block_stream.is_some() || self.cur_idx >= self.sst.value().blocks.len()
        {
            // Start a new block stream if needed.
            if self.block_stream.is_none() {
                self.block_stream = Some(
                    self.sstable_store
                        .get_block_stream(
                            self.sst.value(),
                            Some(self.cur_idx),
                            // ToDo: What about parameters used before (CachePolicy,
                            // &StoreLocalStatistic)?
                        )
                        .await?,
                );
            }

            self.block_stream
                .as_mut()
                .expect("no block stream")
                .next()
                .await?
                .unwrap()
        } else {
            BlockHolder::from_ref_block(self.sst.value().blocks[self.cur_idx].clone())
        };

        self.block_iter = Some(BlockIterator::new(block));
        Ok(self.block_iter.as_mut().unwrap())
    }

    /// Continues the iterator at the next block.
    async fn next_block(&mut self) -> HummockResult<()> {
        self.cur_idx += 1;

        if self.cur_idx >= self.sst.value().block_count() {
            self.block_stream = None;
            self.block_iter = None;

            return Ok(());
        }

        // Load next block and move iterator to first key.
        self.load_block().await?.seek_to_first();

        Ok(())
    }
}

#[async_trait]
impl HummockIterator for SstableStreamIterator {
    type Direction = Forward;

    /// Moves to the next KV-pair in the table. Assumes that the current position is valid. Even if
    /// the next position is invalid, the function return `Ok(())`.
    async fn next(&mut self) -> HummockResult<()> {
        // We have to handle two internal iterators.
        //   `block_stream`: iterates over the blocks of the table.
        //     `block_iter`: iterates over the KV-pairs of the current block.
        // These iterators work in different ways.

        // BlockIterator (and Self) works as follows: After new(), we call seek(). That brings us
        // to the first element. Calling next() then brings us to the second element and does not
        // return anything.

        // BlockStream follows a different approach. After new(), we do not seek, instead next()
        // returns the first value.

        // If possible, we also use data from blocks already stored in memory before we start a new
        // BlockStream. However, once we created such a stream, we always use it.

        self.stats.scan_key_count += 1;

        // Unwrap internal iterator.
        let block_iter = self.block_iter.as_mut().expect("no block iterator");

        // Note that we assume that we are at a valid position.

        // Can we continue in current block?
        block_iter.next();
        if block_iter.is_valid() {
            Ok(())
        } else {
            // No, block is exhausted. We need to load the next block.
            self.next_block().await
        }
    }

    fn key(&self) -> &[u8] {
        self.block_iter.as_ref().expect("no block iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let raw_value = self.block_iter.as_ref().expect("no block iter").value();

        HummockValue::from_slice(raw_value).expect("decode error")
    }

    fn is_valid(&self) -> bool {
        self.block_iter.is_some() && self.block_iter.as_ref().unwrap().is_valid()
    }

    /// Resets the position of the iterator.
    ///
    /// ***Note:***
    /// Do not decide whether the position is valid or not by checking the returned error of this
    /// function. This function WILL NOT return an `Err` if invalid. You should check `is_valid`
    /// before starting iteration.
    async fn rewind(&mut self) -> HummockResult<()> {
        self.init(0, None).await
    }

    /// Resets iterator and seeks to the first position where the stored key >= provided key.
    ///
    /// ***Note:***
    /// Do not decide whether the position is valid or not by checking the returned error of this
    /// function. This function WON'T return an `Err` if invalid. You should check `is_valid` before
    /// starting iteration.
    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        let block_idx = self
            .sst
            .value()
            .meta
            .block_metas
            .partition_point(|block_meta| {
                // compare by version comparator
                // Note: we are comparing against the `smallest_key` of the `block`, thus the
                // partition point should be `prev(<=)` instead of `<`.

                // It would be better to compare based on the largest key of a block. However, we do
                // not have this information in the meta data. Since we can only compare against the
                // smallest key of a block and we want that all (search) keys within a block create
                // the same result, we use <= here (note that we are given a fixed key and search
                // over a set of min-keys). Subsequently, our search returns the index of the first
                // block for which we know that it does not contain our search key. We therefore
                // subtract 1 from the resulting index.

                let ord = VersionedComparator::compare_key(block_meta.smallest_key.as_slice(), key);
                ord == Less || ord == Equal
            })
            .saturating_sub(1); // considering the boundary of 0

        self.init(block_idx, Some(key)).await?;

        // Assume that our table contains two blocks `A: [k l m]` and `B: [s t u]` and that we
        // search for the key `p`. The search above then returns `A` (first `B` and then subtracts
        // 1). The `seek_idx()` of `SstableIterator` then searches over `A` for `p`. Since `A` does
        // not contain `p`, the search eventually reaches the end of `A` and leaves the iterator in
        // an invalid state. To compensate for that, `SstableIterator::seek()` then restarts the
        // search for the next block without a search key. We divert from that approach to avoid a
        // second call of `init()`. Instead, we implement `start_stream()` in such a way that it
        // handles this case without the need to start a second download.

        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats);
    }
}

impl SstableIteratorType for SstableStreamIterator {
    fn create(
        table: TableHolder,
        sstable_store: SstableStoreRef,
        _options_: Arc<SstableIteratorReadOptions>,
    ) -> Self {
        SstableStreamIterator::new(table, sstable_store)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use rand::prelude::*;
    use risingwave_hummock_sdk::key::key_with_epoch;

    use super::*;
    use crate::assert_bytes_eq;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::{
        create_small_table_cache, default_builder_opt_for_test, gen_default_test_sstable,
        gen_test_sstable_data, test_key_of, test_value_of, TEST_KEYS_COUNT,
    };
    use crate::hummock::{CachePolicy, Sstable};

    async fn inner_test_forward_iterator(sstable_store: SstableStoreRef, handle: TableHolder) {
        // We should have at least 10 blocks, so that table iterator test could cover more code
        // path.
        let mut sstable_iter = SstableStreamIterator::create(
            handle,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );
        let mut cnt = 0;
        sstable_iter.rewind().await.unwrap();

        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_bytes_eq!(key, test_key_of(cnt));
            assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
            cnt += 1;
            sstable_iter.next().await.unwrap();
        }

        assert_eq!(cnt, TEST_KEYS_COUNT);
    }

    #[tokio::test]
    async fn test_table_iterator() {
        // Build remote table
        let sstable_store = mock_sstable_store();
        let table =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that table iterator test could cover more code
        // path.
        assert!(table.meta.block_metas.len() > 10);

        let cache = create_small_table_cache();
        let handle = cache.insert(0, 0, 1, Box::new(table));
        inner_test_forward_iterator(sstable_store.clone(), handle).await;

        let kv_iter =
            (0..TEST_KEYS_COUNT).map(|i| (test_key_of(i), HummockValue::put(test_value_of(i))));
        let (data, meta, _) = gen_test_sstable_data(default_builder_opt_for_test(), kv_iter);
        let table = Sstable::new_with_data(0, meta, data).unwrap();
        let handle = cache.insert(0, 0, 1, Box::new(table));
        inner_test_forward_iterator(sstable_store, handle).await;
    }

    #[tokio::test]
    async fn test_table_seek() {
        let sstable_store = mock_sstable_store();
        let table =
            gen_default_test_sstable(default_builder_opt_for_test(), 0, sstable_store.clone())
                .await;
        // We should have at least 10 blocks, so that table iterator test could cover more code
        // path.
        assert!(table.meta.block_metas.len() > 10);
        let cache = create_small_table_cache();
        let handle = cache.insert(0, 0, 1, Box::new(table));

        let mut sstable_iter = SstableStreamIterator::create(
            handle,
            sstable_store,
            Arc::new(SstableIteratorReadOptions::default()),
        );
        let mut all_key_to_test = (0..TEST_KEYS_COUNT).collect_vec();
        let mut rng = thread_rng();
        all_key_to_test.shuffle(&mut rng);

        // We seek and access all the keys in random order
        for i in all_key_to_test {
            sstable_iter.seek(&test_key_of(i)).await.unwrap();
            // sstable_iter.next().await.unwrap();
            let key = sstable_iter.key();
            assert_bytes_eq!(key, test_key_of(i));
        }

        // Seek to key #500 and start iterating.
        sstable_iter.seek(&test_key_of(500)).await.unwrap();
        for i in 500..TEST_KEYS_COUNT {
            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(i));
            sstable_iter.next().await.unwrap();
        }
        assert!(!sstable_iter.is_valid());

        // Seek to < first key
        let smallest_key = key_with_epoch(format!("key_aaaa_{:05}", 0).as_bytes().to_vec(), 233);
        sstable_iter.seek(smallest_key.as_slice()).await.unwrap();
        let key = sstable_iter.key();
        assert_eq!(key, test_key_of(0));

        // Seek to > last key
        let largest_key = key_with_epoch(format!("key_zzzz_{:05}", 0).as_bytes().to_vec(), 233);
        sstable_iter.seek(largest_key.as_slice()).await.unwrap();
        assert!(!sstable_iter.is_valid());

        // Seek to non-existing key
        for idx in 1..TEST_KEYS_COUNT {
            // Seek to the previous key of each existing key. e.g.,
            // Our key space is `key_test_00000`, `key_test_00002`, `key_test_00004`, ...
            // And we seek to `key_test_00001` (will produce `key_test_00002`), `key_test_00003`
            // (will produce `key_test_00004`).
            sstable_iter
                .seek(
                    key_with_epoch(
                        format!("key_test_{:05}", idx * 2 - 1).as_bytes().to_vec(),
                        0,
                    )
                    .as_slice(),
                )
                .await
                .unwrap();

            let key = sstable_iter.key();
            assert_eq!(key, test_key_of(idx));
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
        let (data, meta, _) = gen_test_sstable_data(default_builder_opt_for_test(), kv_iter);
        let table = Sstable {
            id: 0,
            meta,
            blocks: vec![],
        };
        sstable_store
            .put(table, data, CachePolicy::NotFill)
            .await
            .unwrap();

        let mut stats = StoreLocalStatistic::default();
        let mut sstable_iter = SstableStreamIterator::create(
            sstable_store.sstable(0, &mut stats).await.unwrap(),
            sstable_store,
            Arc::new(SstableIteratorReadOptions { prefetch: true }),
        );
        let mut cnt = 0;
        sstable_iter.rewind().await.unwrap();
        while sstable_iter.is_valid() {
            let key = sstable_iter.key();
            let value = sstable_iter.value();
            assert_bytes_eq!(key, test_key_of(cnt));
            assert_bytes_eq!(value.into_user_value().unwrap(), test_value_of(cnt));
            cnt += 1;
            sstable_iter.next().await.unwrap();
        }
        assert_eq!(cnt, TEST_KEYS_COUNT);
    }
}
