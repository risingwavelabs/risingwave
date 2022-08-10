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

use risingwave_hummock_sdk::VersionedComparator;
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::compactor::{CompactorSstableStoreRef, DataHolder};
use crate::hummock::iterator::{Forward, HummockIterator};
use crate::hummock::sstable_store::TableHolder;
use crate::hummock::value::HummockValue;
use crate::hummock::{BlockHolder, BlockIterator, HummockResult};
use crate::monitor::StoreLocalStatistic;

pub struct SstablePrefetchIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    /// Reference to the sst
    sst: TableHolder,

    data: DataHolder,
}

impl SstablePrefetchIterator {
    pub fn new(sst: TableHolder, data: DataHolder) -> Self {
        Self {
            block_iter: None,
            cur_idx: 0,
            sst,
            data,
        }
    }

    /// Seeks to a block, and then seeks to the key if `seek_key` is given.
    fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        if idx >= self.sst.value().block_count() {
            self.block_iter = None;
        } else {
            let block = BlockHolder::from_ref_block(self.data.value().blocks[idx].clone());
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

    pub fn next(&mut self) -> HummockResult<()> {
        let block_iter = self.block_iter.as_mut().expect("no block iter");
        block_iter.next();
        if block_iter.is_valid() {
            Ok(())
        } else {
            // seek to next block
            if self.cur_idx + 1 >= self.sst.value().block_count() {
                self.block_iter = None;
            } else {
                let block =
                    BlockHolder::from_ref_block(self.data.value().blocks[self.cur_idx + 1].clone());
                let mut block_iter = BlockIterator::new(block);
                block_iter.seek_to_first();
                self.block_iter = Some(block_iter);
                self.cur_idx += 1;
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

pub struct ConcatSstableIterator {
    /// The iterator of the current table.
    sstable_iter: Option<SstablePrefetchIterator>,

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
    pub fn new(tables: Vec<SstableInfo>, sstable_store: CompactorSstableStoreRef) -> Self {
        Self {
            sstable_iter: None,
            cur_idx: 0,
            tables,
            sstable_store,
            stats: StoreLocalStatistic::default(),
        }
    }

    /// Seeks to a table, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        if idx >= self.tables.len() {
            self.sstable_iter = None;
        } else {
            self.sstable_iter.take();
            let table = self
                .sstable_store
                .sstable(self.tables[idx].id, &mut self.stats)
                .await?;
            let data = self
                .sstable_store
                .load_data(table.value(), &mut self.stats)
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
            let sstable_iter = self.sstable_iter.as_mut().expect("no table iter");
            sstable_iter.next()?;

            if sstable_iter.is_valid() {
                Ok(())
            } else {
                // seek to next table
                self.seek_idx(self.cur_idx + 1, None).await
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

    fn seek<'a>(&'a mut self, key: &'a [u8]) -> Self::SeekFuture<'a> {
        async {
            let table_idx = self
                .tables
                .partition_point(|table| {
                    let ord = VersionedComparator::compare_key(
                        &table.key_range.as_ref().unwrap().left,
                        key,
                    );
                    ord == Ordering::Less || ord == Ordering::Equal
                })
                .saturating_sub(1); // considering the boundary of 0

            self.seek_idx(table_idx, Some(key)).await?;
            if !self.is_valid() {
                // Seek to next table
                self.seek_idx(table_idx + 1, None).await?;
            }
            Ok(())
        }
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats)
    }
}
