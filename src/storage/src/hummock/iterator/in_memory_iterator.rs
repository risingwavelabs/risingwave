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

use std::sync::Arc;

use risingwave_hummock_sdk::VersionedComparator;

use crate::hummock::iterator::{ConcatIteratorInner, Forward, HummockIterator, ReadOptions};
use crate::hummock::sstable_store::TableHolder;
use crate::hummock::table_accessor::StorageTableAcessor;
use crate::hummock::value::HummockValue;
use crate::hummock::{BlockHolder, BlockIterator, HummockResult, SSTableIteratorType};
use crate::monitor::StoreLocalStatistic;

pub struct InMemoryTableIterator {
    /// The iterator of the current block.
    block_iter: Option<BlockIterator>,

    /// Current block index.
    cur_idx: usize,

    /// Reference to the table
    table: TableHolder,

    stats: StoreLocalStatistic,
}

impl InMemoryTableIterator {
    pub fn new(table: TableHolder) -> Self {
        Self {
            block_iter: None,
            cur_idx: 0,
            table,
            stats: StoreLocalStatistic::default(),
        }
    }

    /// Seeks to a block, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(&mut self, idx: usize, seek_key: Option<&[u8]>) -> HummockResult<()> {
        if idx >= self.table.value().blocks.len() {
            self.block_iter = None;
        } else {
            let block = BlockHolder::from_ref_block(&self.table.value().blocks[idx]);
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

#[async_trait::async_trait]
impl HummockIterator for InMemoryTableIterator {
    type Direction = Forward;

    async fn next(&mut self) -> HummockResult<()> {
        self.stats.scan_key_count += 1;
        let block_iter = self.block_iter.as_mut().expect("no block iter");
        block_iter.next();

        if block_iter.is_valid() {
            Ok(())
        } else {
            // seek to next block
            self.seek_idx(self.cur_idx + 1, None).await
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

    /// Instead of setting idx to 0th block, a `BackwardSSTableIterator` rewinds to the last block
    /// in the table.
    async fn rewind(&mut self) -> HummockResult<()> {
        self.seek_idx(0, None).await
    }

    async fn seek(&mut self, key: &[u8]) -> HummockResult<()> {
        let block_idx = self
            .table
            .value()
            .meta
            .block_metas
            .partition_point(|block_meta| {
                // Compare by version comparator
                // Note: we are comparing against the `smallest_key` of the `block`, thus the
                // partition point should be `prev(<=)` instead of `<`.
                let ord = VersionedComparator::compare_key(block_meta.smallest_key.as_slice(), key);
                ord == std::cmp::Ordering::Less || ord == std::cmp::Ordering::Equal
            })
            .saturating_sub(1); // considering the boundary of 0

        self.seek_idx(block_idx, Some(key)).await?;
        if !self.is_valid() {
            // Seek to next block
            self.seek_idx(block_idx + 1, None).await?;
        }

        Ok(())
    }

    fn collect_local_statistic(&self, stats: &mut StoreLocalStatistic) {
        stats.add(&self.stats)
    }
}

impl SSTableIteratorType for InMemoryTableIterator {
    type Accessor = StorageTableAcessor;

    fn create(
        table: TableHolder,
        _sstable_store: Self::Accessor,
        _read_options: Arc<ReadOptions>,
    ) -> Self {
        InMemoryTableIterator::new(table)
    }
}

pub type BackwardMemoryConcatIterator = ConcatIteratorInner<InMemoryTableIterator>;
