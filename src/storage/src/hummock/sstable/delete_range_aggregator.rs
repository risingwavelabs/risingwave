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

use std::cmp::Ordering;
use std::future::Future;

#[cfg(test)]
use risingwave_common::util::epoch::is_max_epoch;
use risingwave_hummock_sdk::key::{PointRange, UserKey};
use risingwave_hummock_sdk::HummockEpoch;

use super::MonotonicDeleteEvent;
use crate::hummock::iterator::{DeleteRangeIterator, ForwardMergeRangeIterator};
use crate::hummock::sstable_store::TableHolder;
use crate::hummock::{HummockResult, Sstable};

pub struct SortedBoundary {
    sequence: HummockEpoch,
    user_key: UserKey<Vec<u8>>,
}

impl PartialEq<Self> for SortedBoundary {
    fn eq(&self, other: &Self) -> bool {
        self.user_key.eq(&other.user_key) && self.sequence == other.sequence
    }
}

impl PartialOrd for SortedBoundary {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for SortedBoundary {}

impl Ord for SortedBoundary {
    fn cmp(&self, other: &Self) -> Ordering {
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

pub struct CompactionDeleteRangeIterator {
    inner: ForwardMergeRangeIterator,
}

impl CompactionDeleteRangeIterator {
    pub fn new(inner: ForwardMergeRangeIterator) -> Self {
        Self { inner }
    }

    pub(crate) async fn next(&mut self) -> HummockResult<()> {
        self.inner.next().await
    }

    #[cfg(test)]
    pub async fn get_tombstone_between(
        self,
        smallest_user_key: UserKey<&[u8]>,
        largest_user_key: UserKey<&[u8]>,
    ) -> HummockResult<Vec<MonotonicDeleteEvent>> {
        let mut iter = self;
        iter.seek(smallest_user_key).await?;
        let extended_smallest_user_key = PointRange::from_user_key(smallest_user_key, false);
        let extended_largest_user_key = PointRange::from_user_key(largest_user_key, false);
        let mut monotonic_events = vec![];
        if !is_max_epoch(iter.earliest_epoch()) {
            monotonic_events.push(MonotonicDeleteEvent {
                event_key: extended_smallest_user_key.to_vec(),
                new_epoch: iter.earliest_epoch(),
            });
        }

        while iter.is_valid() {
            if !extended_largest_user_key.is_empty() && iter.key().ge(&extended_largest_user_key) {
                if !monotonic_events.is_empty() {
                    monotonic_events.push(MonotonicDeleteEvent {
                        event_key: extended_largest_user_key.to_vec(),
                        new_epoch: HummockEpoch::MAX,
                    });
                }
                break;
            }

            let event_key = iter.key().to_vec();
            iter.next().await?;

            monotonic_events.push(MonotonicDeleteEvent {
                new_epoch: iter.earliest_epoch(),
                event_key,
            });
        }

        monotonic_events.dedup_by(|a, b| {
            a.event_key.left_user_key.table_id == b.event_key.left_user_key.table_id
                && a.new_epoch == b.new_epoch
        });
        if !monotonic_events.is_empty() {
            assert!(!is_max_epoch(monotonic_events.first().unwrap().new_epoch));
            assert!(is_max_epoch(monotonic_events.last().unwrap().new_epoch));
        }
        Ok(monotonic_events)
    }

    /// Return the earliest range-tombstone which deletes target-key.
    /// Target-key must be given in order.
    #[cfg(test)]
    pub async fn earliest_delete_which_can_see_key(
        &mut self,
        target_user_key: UserKey<&[u8]>,
        epoch: HummockEpoch,
    ) -> HummockResult<HummockEpoch> {
        let target_extended_user_key = PointRange::from_user_key(target_user_key, false);
        while self.inner.is_valid()
            && self
                .inner
                .next_extended_user_key()
                .le(&target_extended_user_key)
        {
            self.inner.next().await?;
        }
        Ok(self.earliest_delete_since(epoch))
    }

    pub fn key(&self) -> PointRange<&[u8]> {
        self.inner.next_extended_user_key()
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.inner.is_valid()
    }

    pub(crate) fn earliest_epoch(&self) -> HummockEpoch {
        self.inner.earliest_epoch()
    }

    pub(crate) fn earliest_delete_since(&self, epoch: HummockEpoch) -> HummockEpoch {
        self.inner.earliest_delete_since(epoch)
    }

    /// seek to the first key which larger than `target_user_key`.
    pub async fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) -> HummockResult<()> {
        self.inner.seek(target_user_key).await
    }

    pub async fn rewind(&mut self) -> HummockResult<()> {
        self.inner.rewind().await
    }
}

pub struct SstableDeleteRangeIterator {
    table: TableHolder,
    next_idx: usize,
}

impl SstableDeleteRangeIterator {
    pub fn new(table: TableHolder) -> Self {
        Self { table, next_idx: 0 }
    }

    /// Retrieves whether `next_extended_user_key` is the last range of this SST file.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    pub fn is_last_range(&self) -> bool {
        debug_assert!(self.next_idx < self.table.value().meta.monotonic_tombstone_events.len());
        self.next_idx + 1 == self.table.value().meta.monotonic_tombstone_events.len()
    }
}

impl DeleteRangeIterator for SstableDeleteRangeIterator {
    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next_extended_user_key(&self) -> PointRange<&[u8]> {
        self.table.value().meta.monotonic_tombstone_events[self.next_idx]
            .event_key
            .as_ref()
    }

    fn current_epoch(&self) -> HummockEpoch {
        if self.next_idx > 0 {
            self.table.value().meta.monotonic_tombstone_events[self.next_idx - 1].new_epoch
        } else {
            HummockEpoch::MAX
        }
    }

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            self.next_idx += 1;
            Ok(())
        }
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            self.next_idx = 0;
            Ok(())
        }
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) -> Self::SeekFuture<'_> {
        async move {
            let target_extended_user_key = PointRange::from_user_key(target_user_key, false);
            self.next_idx = self
                .table
                .value()
                .meta
                .monotonic_tombstone_events
                .partition_point(|MonotonicDeleteEvent { event_key, .. }| {
                    event_key.as_ref().le(&target_extended_user_key)
                });
            Ok(())
        }
    }

    fn is_valid(&self) -> bool {
        self.next_idx < self.table.value().meta.monotonic_tombstone_events.len()
    }
}

pub fn get_min_delete_range_epoch_from_sstable(
    table: &Sstable,
    query_user_key: UserKey<&[u8]>,
) -> HummockEpoch {
    let query_extended_user_key = PointRange::from_user_key(query_user_key, false);
    let idx = table.meta.monotonic_tombstone_events.partition_point(
        |MonotonicDeleteEvent { event_key, .. }| event_key.as_ref().le(&query_extended_user_key),
    );
    if idx == 0 {
        HummockEpoch::MAX
    } else {
        table.meta.monotonic_tombstone_events[idx - 1].new_epoch
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use bytes::Bytes;
    use risingwave_common::catalog::TableId;
    use risingwave_common::util::epoch::is_max_epoch;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        gen_iterator_test_sstable_with_range_tombstones, iterator_test_user_key_of,
        mock_sstable_store,
    };
    use crate::hummock::test_utils::delete_range::CompactionDeleteRangesBuilder;
    use crate::hummock::test_utils::test_user_key;
    use crate::monitor::StoreLocalStatistic;

    #[tokio::test]
    pub async fn test_compaction_delete_range_iterator() {
        let mut builder = CompactionDeleteRangesBuilder::default();
        let table_id = TableId::default();
        builder.add_delete_events(
            9,
            table_id,
            vec![
                (
                    Bound::Included(Bytes::copy_from_slice(b"aaaaaa")),
                    Bound::Excluded(Bytes::copy_from_slice(b"bbbddd")),
                ),
                (
                    Bound::Included(Bytes::copy_from_slice(b"bbbfff")),
                    Bound::Excluded(Bytes::copy_from_slice(b"ffffff")),
                ),
                (
                    Bound::Included(Bytes::copy_from_slice(b"gggggg")),
                    Bound::Excluded(Bytes::copy_from_slice(b"hhhhhh")),
                ),
            ],
        );
        builder.add_delete_events(
            12,
            table_id,
            vec![(
                Bound::Included(Bytes::copy_from_slice(b"aaaaaa")),
                Bound::Excluded(Bytes::copy_from_slice(b"bbbccc")),
            )],
        );
        builder.add_delete_events(
            8,
            table_id,
            vec![(
                Bound::Excluded(Bytes::copy_from_slice(b"bbbeee")),
                Bound::Included(Bytes::copy_from_slice(b"eeeeee")),
            )],
        );
        builder.add_delete_events(
            6,
            table_id,
            vec![(
                Bound::Included(Bytes::copy_from_slice(b"bbbaab")),
                Bound::Excluded(Bytes::copy_from_slice(b"bbbdddf")),
            )],
        );
        builder.add_delete_events(
            7,
            table_id,
            vec![(
                Bound::Excluded(Bytes::copy_from_slice(b"hhhhhh")),
                Bound::Unbounded,
            )],
        );
        let mut iter = builder.build_for_compaction();
        iter.rewind().await.unwrap();

        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbb").as_ref(), 13)
                .await
                .unwrap(),
            HummockEpoch::MAX,
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbb").as_ref(), 11)
                .await
                .unwrap(),
            12
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbb").as_ref(), 8)
                .await
                .unwrap(),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbaaa").as_ref(), 8)
                .await
                .unwrap(),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbccd").as_ref(), 8)
                .await
                .unwrap(),
            9
        );

        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbddd").as_ref(), 8)
                .await
                .unwrap(),
            HummockEpoch::MAX,
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbeee").as_ref(), 8)
                .await
                .unwrap(),
            HummockEpoch::MAX,
        );

        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"bbbeef").as_ref(), 10)
                .await
                .unwrap(),
            HummockEpoch::MAX,
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"eeeeee").as_ref(), 8)
                .await
                .unwrap(),
            8
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"gggggg").as_ref(), 8)
                .await
                .unwrap(),
            9
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"hhhhhh").as_ref(), 6)
                .await
                .unwrap(),
            HummockEpoch::MAX,
        );
        assert_eq!(
            iter.earliest_delete_which_can_see_key(test_user_key(b"iiiiii").as_ref(), 6)
                .await
                .unwrap(),
            7
        );
    }

    #[tokio::test]
    pub async fn test_delete_range_split() {
        let table_id = TableId::default();
        let mut builder = CompactionDeleteRangesBuilder::default();
        builder.add_delete_events(
            13,
            table_id,
            vec![(
                Bound::Included(Bytes::copy_from_slice(b"aaaa")),
                Bound::Excluded(Bytes::copy_from_slice(b"cccc")),
            )],
        );
        builder.add_delete_events(
            10,
            table_id,
            vec![(
                Bound::Excluded(Bytes::copy_from_slice(b"cccc")),
                Bound::Excluded(Bytes::copy_from_slice(b"dddd")),
            )],
        );
        builder.add_delete_events(
            12,
            table_id,
            vec![(
                Bound::Included(Bytes::copy_from_slice(b"cccc")),
                Bound::Included(Bytes::copy_from_slice(b"eeee")),
            )],
        );
        builder.add_delete_events(
            15,
            table_id,
            vec![(
                Bound::Excluded(Bytes::copy_from_slice(b"eeee")),
                Bound::Excluded(Bytes::copy_from_slice(b"ffff")),
            )],
        );
        let compaction_delete_range = builder.build_for_compaction();
        let split_ranges = compaction_delete_range
            .get_tombstone_between(
                test_user_key(b"bbbb").as_ref(),
                test_user_key(b"eeeeee").as_ref(),
            )
            .await
            .unwrap();
        assert_eq!(6, split_ranges.len());
        assert_eq!(
            PointRange::from_user_key(test_user_key(b"bbbb"), false),
            split_ranges[0].event_key
        );
        assert_eq!(
            PointRange::from_user_key(test_user_key(b"cccc"), false),
            split_ranges[1].event_key
        );
        assert_eq!(
            PointRange::from_user_key(test_user_key(b"cccc"), true),
            split_ranges[2].event_key
        );
        assert_eq!(
            PointRange::from_user_key(test_user_key(b"dddd"), false),
            split_ranges[3].event_key
        );
        assert_eq!(
            PointRange::from_user_key(test_user_key(b"eeee"), true),
            split_ranges[4].event_key
        );
        assert_eq!(
            PointRange::from_user_key(test_user_key(b"eeeeee"), false),
            split_ranges[5].event_key
        );
    }

    #[tokio::test]
    async fn test_delete_range_get() {
        let sstable_store = mock_sstable_store();
        // key=[idx, epoch], value
        let sst_info = gen_iterator_test_sstable_with_range_tombstones(
            0,
            vec![],
            vec![(0, 2, 300), (1, 4, 150), (3, 6, 50), (5, 8, 150)],
            sstable_store.clone(),
        )
        .await;
        let sstable = sstable_store
            .sstable(&sst_info, &mut StoreLocalStatistic::default())
            .await
            .unwrap();
        let ret = get_min_delete_range_epoch_from_sstable(
            sstable.value(),
            iterator_test_user_key_of(0).as_ref(),
        );
        assert_eq!(ret, 300);
        let ret = get_min_delete_range_epoch_from_sstable(
            sstable.value(),
            iterator_test_user_key_of(1).as_ref(),
        );
        assert_eq!(ret, 150);
        let ret = get_min_delete_range_epoch_from_sstable(
            sstable.value(),
            iterator_test_user_key_of(3).as_ref(),
        );
        assert_eq!(ret, 50);
        let ret = get_min_delete_range_epoch_from_sstable(
            sstable.value(),
            iterator_test_user_key_of(6).as_ref(),
        );
        assert_eq!(ret, 150);
        let ret = get_min_delete_range_epoch_from_sstable(
            sstable.value(),
            iterator_test_user_key_of(8).as_ref(),
        );
        assert!(is_max_epoch(ret));
    }
}
