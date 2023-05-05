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

use std::future::Future;

use risingwave_hummock_sdk::key::{FullKey, PointRange, UserKey};
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::SstableInfo;

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
                if self.idx + 1 < self.sstables.len()
                    && self.sstables[self.idx + 1].range_tombstone_count > 0
                    && iter.next_range_epoch() == HummockEpoch::MAX
                    && iter
                        .next_extended_user_key()
                        .left_user_key
                        .eq(&FullKey::decode(
                            &self.sstables[self.idx].key_range.as_ref().unwrap().right,
                        )
                        .user_key)
                {
                    // When the last range of the current sstable is equal to the first range of the
                    // next sstable, the `next` method would return two same `PointRange`. So we
                    // must skip one.
                    let exclusive_range_start = iter.next_extended_user_key().is_exclude_left_key;
                    let last_key_in_sst_start =
                        iter.next_extended_user_key()
                            .left_user_key
                            .eq(&FullKey::decode(
                                &self.sstables[self.idx + 1].key_range.as_ref().unwrap().left,
                            )
                            .user_key);
                    iter.next().await?;
                    if !iter.is_valid() && last_key_in_sst_start {
                        self.seek_idx(self.idx + 1, None).await?;
                        let next_range = self.next_extended_user_key();
                        if self.is_valid()
                            && next_range.is_exclude_left_key == exclusive_range_start
                            && next_range.left_user_key.eq(&FullKey::decode(
                                &self.sstables[self.idx].key_range.as_ref().unwrap().left,
                            )
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

    fn next_range_epoch(&self) -> HummockEpoch {
        self.current.as_ref().unwrap().next_range_epoch()
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
                    FullKey::decode(&sst.key_range.as_ref().unwrap().left)
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
#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;

    use super::*;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::test_utils::test_user_key;
    use crate::hummock::{
        create_monotonic_events, CompactionDeleteRangesBuilder, DeleteRangeTombstone,
        SstableBuilder, SstableBuilderOptions, SstableWriterOptions,
    };

    #[tokio::test]
    async fn test_concat_iterator() {
        let mut builder = CompactionDeleteRangesBuilder::default();
        let sstable_store = mock_sstable_store();
        let table_id = TableId::new(0);
        let data = vec![
            DeleteRangeTombstone::new_for_test(table_id, b"aaaa".to_vec(), b"dddd".to_vec(), 10),
            DeleteRangeTombstone::new(
                table_id,
                b"bbbb".to_vec(),
                true,
                b"eeee".to_vec(),
                false,
                12,
            ),
        ];
        for range in data {
            builder.add_delete_events(create_monotonic_events(vec![range]));
        }

        let compaction_delete_range = builder.build_for_compaction(false);
        let ranges1 = compaction_delete_range.get_tombstone_between(
            test_user_key(b"aaaa").as_ref(),
            test_user_key(b"bbbb").as_ref(),
        );
        assert_eq!(ranges1.len(), 2);
        let opts = SstableBuilderOptions::default();
        let mut builder = SstableBuilder::for_test(
            1,
            sstable_store
                .clone()
                .create_sst_writer(1, SstableWriterOptions::default()),
            opts.clone(),
        );
        builder.add_monotonic_deletes(ranges1);
        let output1 = builder.finish().await.unwrap();
        output1.writer_output.await.unwrap().unwrap();
        let mut builder = SstableBuilder::for_test(
            2,
            sstable_store
                .clone()
                .create_sst_writer(2, SstableWriterOptions::default()),
            opts.clone(),
        );
        let ranges2 = compaction_delete_range
            .get_tombstone_between(test_user_key(b"bbbb").as_ref(), test_user_key(b"").as_ref());
        assert_eq!(ranges2.len(), 3);
        builder.add_monotonic_deletes(ranges2);
        let output2 = builder.finish().await.unwrap();
        output2.writer_output.await.unwrap().unwrap();
        let mut concat_iterator = ConcatDeleteRangeIterator::new(
            vec![output1.sst_info.sst_info, output2.sst_info.sst_info],
            sstable_store,
        );
        concat_iterator.rewind().await.unwrap();
        assert_eq!(concat_iterator.current_epoch(), HummockEpoch::MAX);
        assert_eq!(
            concat_iterator.next_extended_user_key().left_user_key,
            test_user_key(b"aaaa").as_ref()
        );
        concat_iterator.next().await.unwrap();
        assert_eq!(concat_iterator.current_epoch(), 10);
        assert_eq!(
            concat_iterator.next_extended_user_key().left_user_key,
            test_user_key(b"bbbb").as_ref()
        );
        concat_iterator.next().await.unwrap();
        assert_eq!(concat_iterator.current_epoch(), 10);
        assert_eq!(
            concat_iterator.next_extended_user_key().left_user_key,
            test_user_key(b"dddd").as_ref()
        );
        concat_iterator.next().await.unwrap();
        assert_eq!(concat_iterator.current_epoch(), 12);
        assert_eq!(
            concat_iterator.next_extended_user_key().left_user_key,
            test_user_key(b"eeee").as_ref()
        );
        concat_iterator.next().await.unwrap();
        assert!(!concat_iterator.is_valid());
    }
}
