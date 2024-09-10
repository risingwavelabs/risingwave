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

use std::future::Future;

use risingwave_hummock_sdk::key::{FullKey, PointRange, UserKey};
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::iterator::DeleteRangeIterator;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockResult, SstableDeleteRangeIterator};
use crate::monitor::StoreLocalStatistic;

pub struct ConcatDeleteRangeIterator {
    sstables: Vec<SstableInfo>,
    current: Option<SstableDeleteRangeIterator>,
    idx: usize,
    sstable_store: SstableStoreRef,
    stats: StoreLocalStatistic,
}

impl ConcatDeleteRangeIterator {
    pub fn new(sstables: Vec<SstableInfo>, sstable_store: SstableStoreRef) -> Self {
        Self {
            sstables,
            sstable_store,
            stats: StoreLocalStatistic::default(),
            idx: 0,
            current: None,
        }
    }

    async fn next_inner(&mut self) -> HummockResult<()> {
        if let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                if iter.is_last_range()
                    && self.idx + 1 < self.sstables.len()
                    && self.sstables[self.idx + 1].range_tombstone_count > 0
                    && iter
                        .next_extended_user_key()
                        .left_user_key
                        .eq(&FullKey::decode(&self.sstables[self.idx].key_range.right).user_key)
                {
                    // When the last range of the current sstable is equal to the first range of the
                    // next sstable, the `next` method would return two same `PointRange`. So we
                    // must skip one.
                    let exclusive_range_start = iter.next_extended_user_key().is_exclude_left_key;
                    let last_key_in_sst_start = iter
                        .next_extended_user_key()
                        .left_user_key
                        .eq(&FullKey::decode(&self.sstables[self.idx + 1].key_range.left).user_key);
                    iter.next().await?;
                    if !iter.is_valid() && last_key_in_sst_start {
                        self.seek_idx(self.idx + 1, None).await?;
                        let next_range = self.next_extended_user_key();
                        debug_assert!(self.is_valid());
                        if next_range.is_exclude_left_key == exclusive_range_start
                            && next_range
                                .left_user_key
                                .eq(&FullKey::decode(&self.sstables[self.idx].key_range.left)
                                    .user_key)
                        {
                            self.current.as_mut().unwrap().next().await?;
                        }
                        return Ok(());
                    }
                } else {
                    iter.next().await?;
                }
                let mut idx = self.idx;
                while idx + 1 < self.sstables.len() && !self.is_valid() {
                    self.seek_idx(idx + 1, None).await?;
                    idx += 1;
                }
            }
        }
        Ok(())
    }

    /// Seeks to a table, and then seeks to the key if `seek_key` is given.
    async fn seek_idx(
        &mut self,
        idx: usize,
        seek_key: Option<UserKey<&[u8]>>,
    ) -> HummockResult<()> {
        self.current.take();
        if idx < self.sstables.len() {
            if self.sstables[idx].range_tombstone_count == 0 {
                return Ok(());
            }
            let table = self
                .sstable_store
                .sstable(&self.sstables[idx], &mut self.stats)
                .await?;
            let mut sstable_iter = SstableDeleteRangeIterator::new(table);

            if let Some(key) = seek_key {
                sstable_iter.seek(key).await?;
            } else {
                sstable_iter.rewind().await?;
            }
            self.current = Some(sstable_iter);
            self.idx = idx;
        }
        Ok(())
    }
}

impl DeleteRangeIterator for ConcatDeleteRangeIterator {
    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next_extended_user_key(&self) -> PointRange<&[u8]> {
        self.current.as_ref().unwrap().next_extended_user_key()
    }

    fn current_epoch(&self) -> HummockEpoch {
        self.current.as_ref().unwrap().current_epoch()
    }

    fn next(&mut self) -> Self::NextFuture<'_> {
        self.next_inner()
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            let mut idx = 0;
            self.seek_idx(idx, None).await?;
            while idx + 1 < self.sstables.len() && !self.is_valid() {
                self.seek_idx(idx + 1, None).await?;
                idx += 1;
            }
            Ok(())
        }
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) -> Self::SeekFuture<'_> {
        async move {
            let mut idx = self
                .sstables
                .partition_point(|sst| {
                    FullKey::decode(&sst.key_range.left)
                        .user_key
                        .le(&target_user_key)
                })
                .saturating_sub(1); // considering the boundary of 0
            self.seek_idx(idx, Some(target_user_key)).await?;
            while idx + 1 < self.sstables.len() && !self.is_valid() {
                self.seek_idx(idx + 1, None).await?;
                idx += 1;
            }
            Ok(())
        }
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|iter| iter.is_valid())
            .unwrap_or(false)
    }
}
