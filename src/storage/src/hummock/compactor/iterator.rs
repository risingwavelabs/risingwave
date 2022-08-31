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

use std::cmp::Ordering;
use std::future::Future;

use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::compactor::sstable_store::SstableBlocks;
use crate::hummock::compactor::CompactorSstableStoreRef;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::sstable_store::TableHolder;
use crate::hummock::value::HummockValue;
use crate::hummock::{BlockHolder, BlockIterator, BlockStream, HummockResult};
use crate::monitor::StoreLocalStatistic;

/// Iterates over the KV-pairs of an SST which has its blocks already stored in memory.
pub struct SstablePrefetchIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    /// Reference to the sst
    sst: TableHolder,

    blocks: SstableBlocks,
}

impl SstablePrefetchIterator {
    pub fn new(sst: TableHolder, blocks: SstableBlocks) -> Self {
        Self {
            block_iter: None,
            cur_idx: 0,
            sst,
            blocks,
        }
    }

    /// Seeks to a block, and then seeks to the key if `seek_key` is given.
    fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        while let Some((next_idx, block)) = self.blocks.next() {
            if next_idx >= idx {
                let block = BlockHolder::from_owned_block(block);
                let mut block_iter = BlockIterator::new(block);
                if let Some(key) = seek_key {
                    block_iter.seek(key);
                } else {
                    block_iter.seek_to_first();
                }
                self.block_iter = Some(block_iter);
                self.cur_idx = idx;
                return Ok(());
            }
        }
        self.block_iter = None;
        Ok(())
    }

    pub fn next(&mut self) -> HummockResult<()> {
        let block_iter = self.block_iter.as_mut().expect("no block iter");
        block_iter.next();
        if block_iter.is_valid() {
            Ok(())
        } else {
            if let Some((idx, block)) = self.blocks.next() {
                assert_eq!(idx, self.cur_idx + 1);
                let mut block_iter = BlockIterator::new(BlockHolder::from_owned_block(block));
                block_iter.seek_to_first();
                self.block_iter = Some(block_iter);
                self.cur_idx += 1;
            } else {
                self.block_iter = None;
            }
            Ok(())
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
        self.block_iter.as_ref().map_or(false, |i| i.is_valid())
    }

    fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0, None)
    }

    fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        let block_idx = self
            .sst
            .value()
            .meta
            .block_metas
            .partition_point(|block_meta| {
                // compare by version comparator
                // Note: we are comparing against the `smallest_key` of the `block`, thus the
                // partition point should be `prev(<=)` instead of `<`.
                let ord = VersionedComparator::compare_key(block_meta.smallest_key.as_slice(), key);
                ord == Ordering::Less || ord == Ordering::Equal
            })
            .saturating_sub(1); // considering the boundary of 0
        self.seek_idx(block_idx, Some(key))?;
        if !self.is_valid() {
            // seek to next block
            self.seek_idx(block_idx + 1, None)?;
        }
        Ok(())
    }
}

/// Iterates over the KV-pairs of an SST while downloading it.
struct SstableStreamIterator {
    /// The downloading stream of the current block.
    block_stream: BlockStream,

    /// Iterates over the KV-pairs of the current block.
    block_iter: Option<BlockIterator>,

    /// The maximum number of remaining blocks that iterator will download and read.
    remaining_blocks: usize,

    /// Reference to the SST
    sst: TableHolder,
}

impl SstableStreamIterator {
    // We have to handle two internal iterators.
    //   `block_stream`: iterates over the blocks of the table.
    //     `block_iter`: iterates over the KV-pairs of the current block.
    // These iterators work in different ways.

    // BlockIterator works as follows: After new(), we call seek(). That brings us
    // to the first element. Calling next() then brings us to the second element and does not
    // return anything.

    // BlockStream follows a different approach. After new(), we do not seek, instead next()
    // returns the first value.

    /// Initialises a new [`SstableStreamIterator`] which iterates over the given SST using the
    /// given block stream. The stream reads at most `max_block_count` from the stream.
    pub fn new(sst: TableHolder, block_stream: BlockStream, max_block_count: usize) -> Self {
        Self {
            block_stream,
            block_iter: None,
            remaining_blocks: max_block_count,
            sst,
        }
    }

    /// Initialises the iterator by moving it to the first KV-pair in the stream's first block where
    /// key >= `start_key`. If that block does not contain such a KV-pair, the iterator continues to
    /// the first KV-pair of the next block. If `start_key` is not given, the iterator will move to
    /// the very first KV-pair of the stream's first block.
    pub async fn start(&mut self, start_key: Option<&[u8]>) -> HummockResult<()> {
        // Load first block.
        self.next_block().await?;

        // We assume that a block always contains at least one KV pair. Subsequently, if
        // `next_block()` loads a new block (i.e., `block_iter` is not `None`), then `block_iter` is
        // also valid and pointing on the block's first KV-pair.

        if let (Some(block_iter), Some(key)) = (self.block_iter.as_mut(), start_key) {
            // We now search for `key` in the current block. If `key` is larger than anything stored
            // in the current block, then `block_iter` searches through the whole block and
            // eventually ends in an invalid state. We therefore move to the start of the next
            // block.

            block_iter.seek(key);
            if !block_iter.is_valid() {
                self.next_block().await?;
            }
        }

        // Reached end of stream?
        if self.block_iter.is_none() {
            self.remaining_blocks = 0;
        }

        Ok(())
    }

    /// Loads a new block, creates a new iterator for it, and stores that iterator in
    /// `self.block_iter`. The created iterator points to the block's first KV-pair. If the end of
    /// the stream is reached or `self.remaining_blocks` is zero, then the function sets
    /// `self.block_iter` to `None`.
    async fn next_block(&mut self) -> HummockResult<()> {
        // Check if we want and if we can load the next block.
        if self.remaining_blocks > 0 && let Some(block) = self.block_stream.next().await? {
            self.remaining_blocks -= 1;
            self.block_iter = Some(BlockIterator::new(block));
            self.block_iter.as_mut().unwrap().seek_to_first();
        } else {
            self.remaining_blocks = 0;
            self.block_iter = None;
        }

        Ok(())
    }

    /// Moves to the next KV-pair in the table. Assumes that the current position is valid. Even if
    /// the next position is invalid, the function return `Ok(())`.
    ///
    /// Do not use `next()` to initialise the iterator (i.e. do not use it to find the first
    /// KV-pair). Instead, use `start()`. Afterwards, use `next()` to reach the second KV-pair and
    /// onwards.
    pub async fn next(&mut self) -> HummockResult<()> {
        // Ensure iterator is valid.
        if !self.is_valid() {
            return Ok(());
        }

        // Unwrap internal iterator.
        let block_iter = self.block_iter.as_mut().unwrap();

        // Can we continue in current block?
        block_iter.next();
        if !block_iter.is_valid() {
            // No, block is exhausted. We need to load the next block.
            self.next_block().await?;
        }

        Ok(())
    }

    fn key(&self) -> &[u8] {
        self.block_iter.as_ref().expect("no block iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let raw_value = self.block_iter.as_ref().expect("no block iter").value();
        HummockValue::from_slice(raw_value).expect("decode error")
    }

    fn is_valid(&self) -> bool {
        // True iff block_iter exists and is valid.
        self.block_iter.as_ref().map_or(false, |i| i.is_valid())
    }
}

/// Extension of default [`Option`] type so that we can use two different types of iterators.
enum SstIterOption {
    None,
    Prefetch(SstablePrefetchIterator),
    Stream(SstableStreamIterator),
}

impl SstIterOption {
    async fn next(&mut self) -> HummockResult<()> {
        match self {
            Self::None => panic!("no table iter"),
            Self::Prefetch(iter) => iter.next(),
            Self::Stream(iter) => iter.next().await,
        }
    }

    fn key(&self) -> &[u8] {
        match self {
            Self::None => panic!("no table iter"),
            Self::Prefetch(iter) => iter.key(),
            Self::Stream(iter) => iter.key(),
        }
    }

    fn value(&self) -> HummockValue<&[u8]> {
        match self {
            Self::None => panic!("no table iter"),
            Self::Prefetch(iter) => iter.value(),
            Self::Stream(iter) => iter.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            Self::None => false,
            Self::Prefetch(iter) => iter.is_valid(),
            Self::Stream(iter) => iter.is_valid(),
        }
    }
}

/// Iterates over the KV-pairs of a given list of SSTs. The key-ranges of these SSTs are assumed to
/// be consecutive and non-overlapping.
pub struct ConcatSstableIterator {
    key_range: KeyRange,

    /// The iterator of the current table.
    sstable_iter: SstIterOption,

    /// Current table index.
    cur_idx: usize,

    /// All non-overlapping tables.
    tables: Vec<SstableInfo>,

    sstable_store: CompactorSstableStoreRef,

    stats: StoreLocalStatistic,
}

impl ConcatSstableIterator {
    /// Caller should make sure that `tables` are non-overlapping,
    /// arranged in ascending order when it serves as a forward iterator,
    /// and arranged in descending order when it serves as a backward iterator.
    pub fn new(
        tables: Vec<SstableInfo>,
        key_range: KeyRange,
        sstable_store: CompactorSstableStoreRef,
    ) -> Self {
        Self {
            key_range,
            sstable_iter: SstIterOption::None,
            cur_idx: 0,
            tables,
            sstable_store,
            stats: StoreLocalStatistic::default(),
        }
    }

    /// Seeks to a table, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        self.sstable_iter = SstIterOption::None;

        if idx < self.tables.len() {
            let table = self
                .sstable_store
                .sstable(self.tables[idx].id, &mut self.stats)
                .await?;
            let block_metas = &table.value().meta.block_metas;
            let start_index = if self.key_range.left.is_empty() {
                0
            } else {
                block_metas
                    .partition_point(|block| {
                        VersionedComparator::compare_key(&block.smallest_key, &self.key_range.left)
                            != Ordering::Greater
                    })
                    .saturating_sub(1)
            };
            let end_index = if self.key_range.right.is_empty() {
                block_metas.len()
            } else {
                block_metas.partition_point(|block| {
                    VersionedComparator::compare_key(&block.smallest_key, &self.key_range.right)
                        != Ordering::Greater
                })
            };
            if end_index <= start_index {
                return Ok(());
            }
            let data = self
                .sstable_store
                .scan(table.value(), start_index, end_index, &mut self.stats)
                .await?;
            let mut sstable_iter = SstablePrefetchIterator::new(table, data);
            if let Some(key) = seek_key {
                sstable_iter.seek(key)?;
            } else {
                sstable_iter.rewind()?;
            }

            self.sstable_iter = Some(sstable_iter);
            self.cur_idx = idx;
        }
        Ok(())
    }
}

impl HummockIterator for ConcatSstableIterator {
    type Direction = Forward;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async {
            // Does just calling `next()` suffice?
            self.sstable_iter.next().await?;
            if self.sstable_iter.is_valid() {
                // Yes. Nothing else to do.
                return Ok(());
            }

            // No. Current SST-iterator is invalid. We need to load the next one.
            let (next_idx, min_key) = match &self.sstable_iter {
                SstIterOption::None => unreachable!(), // `next()` would have already panicked.
                SstIterOption::Prefetch(iter) => {
                    // If it was a Prefetch Iterator, we might be able to continue the rest of the
                    // SST with a Streaming Iterator.

                    let block_metas = &iter.sst.value().meta.block_metas;
                    let end_index = iter.blocks.end_index();

                    // Did the Prefetch Iterator process all blocks of the SST?
                    if end_index >= block_metas.len() {
                        // Yes, continue with next SST.
                        (self.cur_idx + 1, vec![])
                    } else {
                        // No, call `self.seek_idx()` with restart with the next block (will result
                        // in a Streaming Iterator).

                        // We need to clone here. The function call `self.seek_idx()` below borrows
                        // `self` as mutable. If we additionally pass in a borrowed key, we borrow
                        // from `self` twice (which then causes a compiler error).
                        let key = block_metas[end_index].smallest_key.clone();
                        (self.cur_idx + 1, key)
                    }
                }
                SstIterOption::Stream(iter) => {
                    // If it was a streaming iterator, we continue with the next SST.
                    (self.cur_idx + 1, vec![])
                }
            };

            self.seek_idx(next_idx, None).await
        }
    }

    fn key(&self) -> &[u8] {
        self.sstable_iter.key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.sstable_iter.value()
    }

    fn is_valid(&self) -> bool {
        self.sstable_iter.is_valid()
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async { self.seek_idx(0, None).await }
    }

    /// Resets the iterator and seeks to the first position where the stored key >= `key`.
    fn seek<'a>(&'a mut self, key: &'a [u8]) -> Self::SeekFuture<'a> {
        async {
            let table_idx = self.tables.partition_point(|table| {
                // We use the maximum key of an SST for the search. That avoids having to call
                // `seek_idx()` twice if determined SST does not contain `key`.

                // Assume we have two SSTs `A: [k l m]` and `B: [s t u]`, and that we search for the
                // key `p`. If we search using the min. key of an SST, our search would result in
                // `A`, we then search in `A`, receive an invalid iterator, and restart the search
                // with `B`. If we use the max. key instead, then the search results in `B` and the
                // created iterator is always valid.

                // Note that we need to use `<` instead of `<=` to ensure that all keys in an SST
                // (including its max. key) produce the same search result.
                let max_sst_key = &table.key_range.as_ref().unwrap().right;
                VersionedComparator::compare_key(max_sst_key, key) == Ordering::Less
            });

            self.seek_idx(table_idx, Some(key)).await
        }
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_hummock_sdk::key_range::KeyRange;

    use crate::hummock::compactor::ConcatSstableIterator;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::iterator::HummockIterator;
    use crate::hummock::test_utils::{
        default_builder_opt_for_test, gen_test_sstable, test_key_of, test_value_of, TEST_KEYS_COUNT,
    };
    use crate::hummock::value::HummockValue;
    use crate::hummock::{CompactorSstableStore, MemoryLimiter};

    #[tokio::test]
    async fn test_concat_iterator() {
        let sstable_store = mock_sstable_store();
        let mut table_infos = vec![];
        for sst_id in 0..3 {
            let start_index = sst_id * TEST_KEYS_COUNT;
            let end_index = (sst_id + 1) * TEST_KEYS_COUNT;
            let table = gen_test_sstable(
                default_builder_opt_for_test(),
                sst_id as u64,
                (start_index..end_index)
                    .map(|i| (test_key_of(i), HummockValue::put(test_value_of(i)))),
                sstable_store.clone(),
            )
            .await;
            table_infos.push(table.get_sstable_info());
        }
        let compact_store = Arc::new(CompactorSstableStore::new(
            sstable_store,
            MemoryLimiter::unlimit(),
        ));
        let start_index = 5000;
        let end_index = 25000;

        let kr = KeyRange::new(
            test_key_of(start_index).into(),
            test_key_of(end_index).into(),
        );
        let mut iter =
            ConcatSstableIterator::new(table_infos.clone(), kr.clone(), compact_store.clone());
        iter.seek(&kr.left).await.unwrap();

        for idx in start_index..end_index {
            let key = iter.key();
            let val = iter.value();
            assert_eq!(key, test_key_of(idx).as_slice(), "failed at {}", idx);
            assert_eq!(
                val.into_user_value().unwrap(),
                test_value_of(idx).as_slice()
            );
            iter.next().await.unwrap();
        }

        // seek non-overlap range
        let kr = KeyRange::new(test_key_of(30000).into(), test_key_of(40000).into());
        let mut iter =
            ConcatSstableIterator::new(table_infos.clone(), kr.clone(), compact_store.clone());
        iter.seek(&kr.left).await.unwrap();
        assert!(!iter.is_valid());
        let kr = KeyRange::new(test_key_of(start_index).into(), test_key_of(40000).into());
        let mut iter =
            ConcatSstableIterator::new(table_infos.clone(), kr.clone(), compact_store.clone());
        iter.seek(&kr.left).await.unwrap();
        for idx in start_index..30000 {
            let key = iter.key();
            let val = iter.value();
            assert_eq!(key, test_key_of(idx).as_slice(), "failed at {}", idx);
            assert_eq!(
                val.into_user_value().unwrap(),
                test_value_of(idx).as_slice()
            );
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());
    }
}
