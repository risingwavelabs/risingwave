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
use std::sync::atomic::AtomicU64;
use std::sync::{atomic, Arc};
use std::time::Instant;

use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::SstableInfo;

use super::sstable_store::BlockStream;
use crate::hummock::compactor::CompactorSstableStoreRef;
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::value::HummockValue;
use crate::hummock::{BlockHolder, BlockIterator, HummockResult};
use crate::monitor::StoreLocalStatistic;

/// Iterates over the KV-pairs of an SST while downloading it.
struct SstableStreamIterator {
    /// The downloading stream.
    block_stream: BlockStream,

    /// Iterates over the KV-pairs of the current block.
    block_iter: Option<BlockIterator>,

    /// The maximum number of remaining blocks that iterator will download and read.
    remaining_blocks: usize,

    /// Counts the time used for IO.
    stats_ptr: Arc<AtomicU64>,
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

    /// Initialises a new [`SstableStreamIterator`] which iterates over the given [`BlockStream`].
    /// The iterator reads at most `max_block_count` from the stream.
    pub fn new(
        block_stream: BlockStream,
        max_block_count: usize,
        stats: &StoreLocalStatistic,
    ) -> Self {
        Self {
            block_stream,
            block_iter: None,
            remaining_blocks: max_block_count,
            stats_ptr: stats.remote_io_time.clone(),
        }
    }

    /// Initialises the iterator by moving it to the first KV-pair in the stream's first block where
    /// key >= `seek_key`. If that block does not contain such a KV-pair, the iterator continues to
    /// the first KV-pair of the next block. If `seek_key` is not given, the iterator will move to
    /// the very first KV-pair of the stream's first block.
    pub async fn seek(&mut self, seek_key: Option<&[u8]>) -> HummockResult<()> {
        // Load first block.
        self.next_block().await?;

        // We assume that a block always contains at least one KV pair. Subsequently, if
        // `next_block()` loads a new block (i.e., `block_iter` is not `None`), then `block_iter` is
        // also valid and pointing on the block's first KV-pair.

        if let (Some(block_iter), Some(seek_key)) = (self.block_iter.as_mut(), seek_key) {
            block_iter.seek(seek_key);
            if !block_iter.is_valid() {
                // `seek_key` is larger than everything in the first block.
                self.next_block().await?;
            }
        }

        if self.block_iter.is_none() {
            // End of stream.
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
        if self.remaining_blocks > 0 && let Some(block) = self.download_next_block().await? {
            let mut block_iter = BlockIterator::new(block);
            block_iter.seek_to_first();

            self.remaining_blocks -= 1;
            self.block_iter = Some(block_iter);
        } else {
            self.remaining_blocks = 0;
            self.block_iter = None;
        }

        Ok(())
    }

    /// Wrapper function for `self.block_stream.next()` which allows us to measure the time needed.
    async fn download_next_block(&mut self) -> HummockResult<Option<BlockHolder>> {
        let now = Instant::now();
        let result = self.block_stream.next().await;
        let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
        self.stats_ptr
            .fetch_add(add as u64, atomic::Ordering::Relaxed);

        result
    }

    /// Moves to the next KV-pair in the table. Assumes that the current position is valid. Even if
    /// the next position is invalid, the function returns `Ok(())`.
    ///
    /// Do not use `next()` to initialise the iterator (i.e. do not use it to find the first
    /// KV-pair). Instead, use `seek()`. Afterwards, use `next()` to reach the second KV-pair and
    /// onwards.
    pub async fn next(&mut self) -> HummockResult<()> {
        if !self.is_valid() {
            return Ok(());
        }

        let block_iter = self.block_iter.as_mut().expect("no block iter");
        block_iter.next();
        if !block_iter.is_valid() {
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

/// Iterates over the KV-pairs of a given list of SSTs. The key-ranges of these SSTs are assumed to
/// be consecutive and non-overlapping.
pub struct ConcatSstableIterator {
    key_range: KeyRange,

    /// The iterator of the current table.
    sstable_iter: Option<SstableStreamIterator>,

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
            sstable_iter: None,
            cur_idx: 0,
            tables,
            sstable_store,
            stats: StoreLocalStatistic::default(),
        }
    }

    /// Resets the iterator, loads the specified SST, and seeks in that SST to `seek_key` if given.
    async fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        self.sstable_iter.take();
        let seek_key: Option<&[u8]> = match (seek_key, self.key_range.left.is_empty()) {
            (Some(seek_key), false) => {
                match VersionedComparator::compare_key(seek_key, &self.key_range.left) {
                    Ordering::Less | Ordering::Equal => Some(&self.key_range.left),
                    Ordering::Greater => Some(seek_key),
                }
            }
            (Some(seek_key), true) => Some(seek_key),
            (None, true) => None,
            (None, false) => Some(&self.key_range.left),
        };

        if idx < self.tables.len() {
            let table_info = &self.tables[idx];
            let table = self
                .sstable_store
                .sstable(table_info, &mut self.stats)
                .await?;
            let block_metas = &table.value().meta.block_metas;
            let start_index = match seek_key {
                None => 0,
                Some(seek_key) => {
                    // start_index points to the greatest block whose smallest_key <= seek_key.
                    block_metas
                        .partition_point(|block| {
                            VersionedComparator::compare_key(&block.smallest_key, seek_key)
                                != Ordering::Greater
                        })
                        .saturating_sub(1)
                }
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

            let stats_ptr = self.stats.remote_io_time.clone();
            let now = Instant::now();

            let block_stream = self
                .sstable_store
                .get_stream(table.value(), Some(start_index))
                .await?;

            // Determine time needed to open stream.
            let add = (now.elapsed().as_secs_f64() * 1000.0).ceil();
            stats_ptr.fetch_add(add as u64, atomic::Ordering::Relaxed);

            let mut sstable_iter =
                SstableStreamIterator::new(block_stream, end_index - start_index, &self.stats);
            sstable_iter.seek(seek_key).await?;

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
            let sstable_iter = self.sstable_iter.as_mut().expect("no table iter");

            // Does just calling `next()` suffice?
            sstable_iter.next().await?;
            if sstable_iter.is_valid() {
                Ok(())
            } else {
                // No, seek to next table.
                self.seek_idx(self.cur_idx + 1, None).await?;
                Ok(())
            }
        }
    }

    fn key(&self) -> &[u8] {
        self.sstable_iter.as_ref().expect("no table iter").key()
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.sstable_iter.as_ref().expect("no table iter").value()
    }

    fn is_valid(&self) -> bool {
        self.sstable_iter.as_ref().map_or(false, |i| i.is_valid())
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async { self.seek_idx(0, None).await }
    }

    /// Resets the iterator and seeks to the first position where the stored key >= `key`.
    fn seek<'a>(&'a mut self, key: &'a [u8]) -> Self::SeekFuture<'a> {
        async {
            let seek_key: &[u8] = if self.key_range.left.is_empty() {
                key
            } else {
                match VersionedComparator::compare_key(key, &self.key_range.left) {
                    Ordering::Less | Ordering::Equal => &self.key_range.left,
                    Ordering::Greater => key,
                }
            };
            let table_idx = self.tables.partition_point(|table| {
                // We use the maximum key of an SST for the search. That way, we guarantee that the
                // resulting SST contains either that key or the next-larger KV-pair. Subsequently,
                // we avoid calling `seek_idx()` twice if the determined SST does not contain `key`.

                // Note that we need to use `<` instead of `<=` to ensure that all keys in an SST
                // (including its max. key) produce the same search result.
                let max_sst_key = &table.key_range.as_ref().unwrap().right;
                VersionedComparator::compare_key(max_sst_key, seek_key) == Ordering::Less
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

    use risingwave_hummock_sdk::key::{next_key, prev_key};
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
            sstable_store.clone(),
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

        // Test seek. Result is dominated by given seek key rather than key range.
        let kr = KeyRange::new(test_key_of(0).into(), test_key_of(40000).into());
        let mut iter =
            ConcatSstableIterator::new(table_infos.clone(), kr.clone(), compact_store.clone());
        iter.seek(test_key_of(10000).as_slice()).await.unwrap();
        assert!(
            iter.is_valid() && iter.cur_idx == 1 && iter.key() == test_key_of(10000).as_slice()
        );
        iter.seek(test_key_of(10001).as_slice()).await.unwrap();
        assert!(
            iter.is_valid() && iter.cur_idx == 1 && iter.key() == test_key_of(10001).as_slice()
        );
        iter.seek(test_key_of(9999).as_slice()).await.unwrap();
        assert!(iter.is_valid() && iter.cur_idx == 0 && iter.key() == test_key_of(9999).as_slice());
        iter.seek(test_key_of(1).as_slice()).await.unwrap();
        assert!(iter.is_valid() && iter.cur_idx == 0 && iter.key() == test_key_of(1).as_slice());
        iter.seek(test_key_of(29999).as_slice()).await.unwrap();
        assert!(
            iter.is_valid() && iter.cur_idx == 2 && iter.key() == test_key_of(29999).as_slice()
        );
        iter.seek(test_key_of(30000).as_slice()).await.unwrap();
        assert!(!iter.is_valid());

        // Test seek. Result is dominated by key range rather than given seek key.
        let kr = KeyRange::new(test_key_of(6000).into(), test_key_of(16000).into());
        let mut iter =
            ConcatSstableIterator::new(table_infos.clone(), kr.clone(), compact_store.clone());
        iter.seek(test_key_of(17000).as_slice()).await.unwrap();
        assert!(!iter.is_valid());
        iter.seek(test_key_of(1).as_slice()).await.unwrap();
        assert!(iter.is_valid() && iter.cur_idx == 0 && iter.key() == kr.left);
    }

    #[tokio::test]
    async fn test_concat_iterator_seek_idx() {
        let sstable_store = mock_sstable_store();
        let mut table_infos = vec![];
        for sst_id in 0..3 {
            let start_index = sst_id * TEST_KEYS_COUNT + TEST_KEYS_COUNT / 2;
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
            sstable_store.clone(),
            MemoryLimiter::unlimit(),
        ));

        // Test seek_idx. Result is dominated by given seek key rather than key range.
        let kr = KeyRange::new(test_key_of(0).into(), test_key_of(40000).into());
        let mut iter =
            ConcatSstableIterator::new(table_infos.clone(), kr.clone(), compact_store.clone());
        let sst = sstable_store
            .sstable(&iter.tables[0], &mut iter.stats)
            .await
            .unwrap();
        let block_metas = &sst.value().meta.block_metas;
        let block_1_smallest_key = block_metas[1].smallest_key.clone();
        let block_2_smallest_key = block_metas[2].smallest_key.clone();
        // Use block_1_smallest_key as seek key and result in the first KV of block 1.
        let seek_key = block_1_smallest_key.clone();
        iter.seek_idx(0, Some(seek_key.as_slice())).await.unwrap();
        assert!(iter.is_valid() && iter.key() == block_1_smallest_key.as_slice());
        // Use prev_key(block_1_smallest_key) as seek key and result in the first KV of block 1.
        let seek_key = prev_key(block_1_smallest_key.as_slice());
        iter.seek_idx(0, Some(seek_key.as_slice())).await.unwrap();
        assert!(iter.is_valid() && iter.key() == block_1_smallest_key.as_slice());
        iter.next().await.unwrap();
        let block_1_second_key = iter.key().to_vec();
        // Use a big enough seek key and result in invalid iterator.
        let seek_key = test_key_of(30001);
        iter.seek_idx(0, Some(seek_key.as_slice())).await.unwrap();
        assert!(!iter.is_valid());

        // Test seek_idx. Result is dominated by key range rather than given seek key.
        let kr = KeyRange::new(
            next_key(&block_1_smallest_key).into(),
            prev_key(&block_2_smallest_key).into(),
        );
        let mut iter =
            ConcatSstableIterator::new(table_infos.clone(), kr.clone(), compact_store.clone());
        // Use block_2_smallest_key as seek key and result in invalid iterator.
        let seek_key = block_2_smallest_key.clone();
        assert!(seek_key.as_slice() > kr.right);
        iter.seek_idx(0, Some(seek_key.as_slice())).await.unwrap();
        assert!(!iter.is_valid());
        // Use a small enough seek key and result in the second KV of block 1.
        let seek_key = test_key_of(0);
        iter.seek_idx(0, Some(seek_key.as_slice())).await.unwrap();
        assert!(iter.is_valid() && iter.key() == block_1_second_key);
        // Use None seek key and result in the second KV of block 1.
        iter.seek_idx(0, None).await.unwrap();
        assert!(iter.is_valid() && iter.key() == block_1_second_key);
    }
}
