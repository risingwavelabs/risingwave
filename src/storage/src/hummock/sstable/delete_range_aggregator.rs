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

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, BinaryHeap};
use std::sync::Arc;

use risingwave_hummock_sdk::key::{FullKey, UserKey};
use risingwave_hummock_sdk::HummockEpoch;

use super::DeleteRangeTombstone;
use crate::hummock::iterator::DeleteRangeIterator;
use crate::hummock::sstable_store::TableHolder;
use crate::hummock::Sstable;

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
        let ret = other
            .user_key
            .cmp(&self.user_key)
            .then_with(|| other.sequence.cmp(&self.sequence));
        Some(ret)
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

#[derive(Default)]
pub struct DeleteRangeAggregatorBuilder {
    delete_tombstones: Vec<DeleteRangeTombstone>,
}

pub struct RangeTombstonesCollector {
    range_tombstone_list: Vec<DeleteRangeTombstone>,
    watermark: u64,
    gc_delete_keys: bool,
}

impl DeleteRangeAggregatorBuilder {
    pub fn add_tombstone(&mut self, data: Vec<DeleteRangeTombstone>) {
        self.delete_tombstones.extend(data);
    }

    pub fn build(self, watermark: u64, gc_delete_keys: bool) -> Arc<RangeTombstonesCollector> {
        // sort tombstones by start-key.
        let mut tombstone_index = BinaryHeap::<Reverse<DeleteRangeTombstone>>::default();
        let mut sorted_tombstones: Vec<DeleteRangeTombstone> = vec![];
        for tombstone in self.delete_tombstones {
            tombstone_index.push(Reverse(tombstone));
        }
        while let Some(Reverse(tombstone)) = tombstone_index.pop() {
            for last in sorted_tombstones.iter_mut().rev() {
                if last.end_user_key.gt(&tombstone.end_user_key) {
                    let mut new_tombstone = last.clone();
                    new_tombstone.start_user_key = tombstone.end_user_key.clone();
                    last.end_user_key = tombstone.end_user_key.clone();
                    tombstone_index.push(Reverse(new_tombstone));
                } else {
                    break;
                }
            }
            sorted_tombstones.push(tombstone);
        }

        #[cfg(debug_assertions)]
        {
            check_sorted_tombstone(&sorted_tombstones);
        }

        sorted_tombstones.sort();
        Arc::new(RangeTombstonesCollector {
            range_tombstone_list: sorted_tombstones,
            gc_delete_keys,
            watermark,
        })
    }
}

#[cfg(debug_assertions)]
fn check_sorted_tombstone(sorted_tombstones: &[DeleteRangeTombstone]) {
    for idx in 1..sorted_tombstones.len() {
        assert!(sorted_tombstones[idx]
            .start_user_key
            .ge(&sorted_tombstones[idx - 1].start_user_key));
        assert!(sorted_tombstones[idx]
            .end_user_key
            .ge(&sorted_tombstones[idx - 1].end_user_key));
    }
}

impl RangeTombstonesCollector {
    pub fn for_test() -> Self {
        Self {
            range_tombstone_list: vec![],
            gc_delete_keys: false,
            watermark: 0,
        }
    }

    pub fn iter(self: &Arc<Self>) -> SingleDeleteRangeIterator {
        SingleDeleteRangeIterator {
            agg: self.clone(),
            seek_idx: 0,
        }
    }

    // split ranges to make sure they locate in [smallest_user_key, largest_user_key)
    pub fn get_tombstone_between(
        &self,
        smallest_user_key: &UserKey<&[u8]>,
        largest_user_key: &UserKey<&[u8]>,
    ) -> Vec<DeleteRangeTombstone> {
        let mut delete_ranges: Vec<DeleteRangeTombstone> = vec![];
        for tombstone in &self.range_tombstone_list {
            if !largest_user_key.is_empty()
                && tombstone.start_user_key.as_ref().ge(largest_user_key)
            {
                continue;
            }

            if !smallest_user_key.is_empty()
                && tombstone.end_user_key.as_ref().le(smallest_user_key)
            {
                continue;
            }

            if tombstone.sequence <= self.watermark {
                if self.gc_delete_keys {
                    continue;
                }
                if let Some(last) = delete_ranges.last() {
                    if last.start_user_key.eq(&tombstone.start_user_key)
                        && last.end_user_key.eq(&tombstone.end_user_key)
                        && last.sequence <= self.watermark
                    {
                        assert!(last.sequence > tombstone.sequence);
                        continue;
                    }
                }
            }

            let mut ret = tombstone.clone();
            if !smallest_user_key.is_empty() && smallest_user_key.gt(&ret.start_user_key.as_ref()) {
                ret.start_user_key = smallest_user_key.to_vec();
            }
            if !largest_user_key.is_empty() && largest_user_key.lt(&ret.end_user_key.as_ref()) {
                ret.end_user_key = largest_user_key.to_vec();
            }
            delete_ranges.push(ret);
        }
        delete_ranges
    }
}

pub struct SingleDeleteRangeIterator {
    agg: Arc<RangeTombstonesCollector>,
    seek_idx: usize,
}

impl DeleteRangeIterator for SingleDeleteRangeIterator {
    fn start_user_key(&self) -> UserKey<&[u8]> {
        self.agg.range_tombstone_list[self.seek_idx]
            .start_user_key
            .as_ref()
    }

    fn end_user_key(&self) -> UserKey<&[u8]> {
        self.agg.range_tombstone_list[self.seek_idx]
            .end_user_key
            .as_ref()
    }

    fn current_epoch(&self) -> HummockEpoch {
        self.agg.range_tombstone_list[self.seek_idx].sequence
    }

    fn next(&mut self) {
        self.seek_idx += 1;
    }

    fn rewind(&mut self) {
        self.seek_idx = 0;
    }

    fn is_valid(&self) -> bool {
        self.seek_idx < self.agg.range_tombstone_list.len()
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        self.seek_idx = self
            .agg
            .range_tombstone_list
            .partition_point(|tombstone| tombstone.end_user_key.as_ref().le(&target_user_key));
    }
}

pub struct DeleteRangeAggregator<I: DeleteRangeIterator> {
    inner: I,
    end_user_key_index: BinaryHeap<SortedBoundary>,
    epoch_index: BTreeSet<HummockEpoch>,
    watermark: u64,
}

impl<I: DeleteRangeIterator> DeleteRangeAggregator<I> {
    pub fn new(iter: I, watermark: u64) -> Self {
        DeleteRangeAggregator {
            inner: iter,
            epoch_index: BTreeSet::new(),
            end_user_key_index: BinaryHeap::new(),
            watermark,
        }
    }

    fn add_all_overlap_range(&mut self, target_key: &UserKey<&[u8]>) {
        while self.inner.is_valid() && self.inner.start_user_key().le(target_key) {
            let sequence = self.inner.current_epoch();
            if sequence > self.watermark || self.inner.end_user_key().le(target_key) {
                self.inner.next();
                continue;
            }
            self.end_user_key_index.push(SortedBoundary {
                user_key: self.inner.end_user_key().to_vec(),
                sequence,
            });
            self.epoch_index.insert(sequence);
            self.inner.next();
        }
    }

    /// Check whether the target-key is deleted by some range-tombstone. Target-key must be given
    /// in order.
    pub fn should_delete(&mut self, target_key: &UserKey<&[u8]>, epoch: HummockEpoch) -> bool {
        if epoch > self.watermark {
            return false;
        }

        // take the smallest end_user_key which would never cover the current key and remove them
        //  from covered epoch index.
        while !self.end_user_key_index.is_empty() {
            let item = self.end_user_key_index.peek().unwrap();
            if item.user_key.as_ref().gt(target_key) {
                break;
            }

            // The correctness of the algorithm needs to be guaranteed by "the epoch of the
            // intervals covering each other must be different".
            self.epoch_index.remove(&item.sequence);
            self.end_user_key_index.pop();
        }
        self.add_all_overlap_range(target_key);

        // There may be several epoch, we only care the largest one.
        self.epoch_index
            .last()
            .map(|tombstone_epoch| *tombstone_epoch >= epoch)
            .unwrap_or(false)
    }

    pub fn rewind(&mut self) {
        self.inner.rewind();
        self.epoch_index.clear();
        self.end_user_key_index.clear();
    }

    pub fn seek(&mut self, target_user_key: UserKey<&[u8]>) {
        self.inner.seek(target_user_key);
        self.epoch_index.clear();
        self.end_user_key_index.clear();
        self.add_all_overlap_range(&target_user_key);
    }
}

pub struct SstableDeleteRangeIterator {
    table: TableHolder,
    current_idx: usize,
}

impl SstableDeleteRangeIterator {
    pub fn new(table: TableHolder) -> Self {
        Self {
            table,
            current_idx: 0,
        }
    }
}

impl DeleteRangeIterator for SstableDeleteRangeIterator {
    fn start_user_key(&self) -> UserKey<&[u8]> {
        self.table.value().meta.range_tombstone_list[self.current_idx]
            .start_user_key
            .as_ref()
    }

    fn end_user_key(&self) -> UserKey<&[u8]> {
        self.table.value().meta.range_tombstone_list[self.current_idx]
            .end_user_key
            .as_ref()
    }

    fn current_epoch(&self) -> HummockEpoch {
        self.table.value().meta.range_tombstone_list[self.current_idx].sequence
    }

    fn next(&mut self) {
        self.current_idx += 1;
    }

    fn rewind(&mut self) {
        self.current_idx = 0;
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        self.current_idx = self
            .table
            .value()
            .meta
            .range_tombstone_list
            .partition_point(|tombstone| tombstone.end_user_key.as_ref().le(&target_user_key));
    }

    fn is_valid(&self) -> bool {
        self.current_idx < self.table.value().meta.range_tombstone_list.len()
    }
}

pub fn get_delete_range_epoch_from_sstable(
    table: &Sstable,
    full_key: &FullKey<&[u8]>,
) -> Option<HummockEpoch> {
    if table.meta.range_tombstone_list.is_empty() {
        return None;
    }
    let watermark = full_key.epoch;
    let mut idx = table
        .meta
        .range_tombstone_list
        .partition_point(|tombstone| tombstone.end_user_key.as_ref().le(&full_key.user_key));
    if idx >= table.meta.range_tombstone_list.len() {
        return None;
    }
    let mut epoch = None;
    while idx < table.meta.range_tombstone_list.len()
        && table.meta.range_tombstone_list[idx]
            .start_user_key
            .as_ref()
            .le(&full_key.user_key)
    {
        let sequence = table.meta.range_tombstone_list[idx].sequence;
        if sequence > watermark {
            idx += 1;
            continue;
        }
        if epoch
            .as_ref()
            .map(|epoch| *epoch < sequence)
            .unwrap_or(true)
        {
            epoch = Some(sequence);
        }
        idx += 1;
    }
    epoch
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rand::Rng;
    use risingwave_common::catalog::TableId;
    use risingwave_hummock_sdk::key::TableKey;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        gen_iterator_test_sstable_with_range_tombstones, iterator_test_key_of_epoch,
        mock_sstable_store,
    };
    use crate::hummock::test_utils::test_user_key;

    #[test]
    pub fn test_delete_range_aggregator() {
        let mut builder = DeleteRangeAggregatorBuilder::default();
        let table_id = TableId::default();
        builder.add_tombstone(vec![
            DeleteRangeTombstone::new(table_id, b"aaaaaa".to_vec(), b"bbbccc".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"aaaaaa".to_vec(), b"bbbddd".to_vec(), 9),
            DeleteRangeTombstone::new(table_id, b"bbbaab".to_vec(), b"bbbdddf".to_vec(), 6),
            DeleteRangeTombstone::new(table_id, b"bbbeee".to_vec(), b"eeeeee".to_vec(), 8),
            DeleteRangeTombstone::new(table_id, b"bbbfff".to_vec(), b"ffffff".to_vec(), 9),
            DeleteRangeTombstone::new(table_id, b"gggggg".to_vec(), b"hhhhhh".to_vec(), 9),
        ]);
        let agg = builder.build(10, false);
        let iter = agg.iter();
        let mut iter = DeleteRangeAggregator::new(iter, 10);
        // can not be removed by tombstone with smaller epoch.
        assert!(!iter.should_delete(&test_user_key(b"bbb").as_ref(), 13));
        // can not be removed by tombstone because its sequence is larger than epoch.
        assert!(!iter.should_delete(&test_user_key(b"bbb").as_ref(), 11));
        assert!(iter.should_delete(&test_user_key(b"bbb").as_ref(), 8));

        assert!(iter.should_delete(&test_user_key(b"bbbaaa").as_ref(), 8));

        assert!(iter.should_delete(&test_user_key(b"bbbccd").as_ref(), 8));
        // can not be removed by tombstone because it equals the end of delete-ranges.
        assert!(!iter.should_delete(&test_user_key(b"bbbddd").as_ref(), 8));
        assert!(iter.should_delete(&test_user_key(b"bbbeee").as_ref(), 8));
        assert!(!iter.should_delete(&test_user_key(b"bbbeef").as_ref(), 10));
        assert!(iter.should_delete(&test_user_key(b"eeeeee").as_ref(), 9));
        assert!(iter.should_delete(&test_user_key(b"gggggg").as_ref(), 8));
        assert!(!iter.should_delete(&test_user_key(b"hhhhhh").as_ref(), 8));

        let split_ranges = agg.get_tombstone_between(
            &test_user_key(b"bbb").as_ref(),
            &test_user_key(b"eeeeee").as_ref(),
        );
        assert_eq!(5, split_ranges.len());
        assert_eq!(test_user_key(b"bbb"), split_ranges[0].start_user_key);
        assert_eq!(test_user_key(b"bbb"), split_ranges[1].start_user_key,);
        assert_eq!(test_user_key(b"bbbaab"), split_ranges[2].start_user_key);
        assert_eq!(test_user_key(b"eeeeee"), split_ranges[3].end_user_key);
        assert_eq!(test_user_key(b"eeeeee"), split_ranges[4].end_user_key);
    }

    #[test]
    pub fn test_delete_range_split() {
        let table_id = TableId::default();
        let mut builder = DeleteRangeAggregatorBuilder::default();
        builder.add_tombstone(vec![
            DeleteRangeTombstone::new(table_id, b"aaaa".to_vec(), b"bbbb".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"aaaa".to_vec(), b"cccc".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"cccc".to_vec(), b"dddd".to_vec(), 10),
            DeleteRangeTombstone::new(table_id, b"cccc".to_vec(), b"eeee".to_vec(), 12),
            DeleteRangeTombstone::new(table_id, b"eeee".to_vec(), b"ffff".to_vec(), 12),
        ]);
        let agg = builder.build(10, true);
        let split_ranges = agg.get_tombstone_between(
            &test_user_key(b"bbbb").as_ref(),
            &test_user_key(b"eeeeee").as_ref(),
        );
        assert_eq!(3, split_ranges.len());
        assert_eq!(test_user_key(b"bbbb"), split_ranges[0].start_user_key);
        assert_eq!(test_user_key(b"cccc"), split_ranges[0].end_user_key);
        assert_eq!(test_user_key(b"cccc"), split_ranges[1].start_user_key);
        assert_eq!(test_user_key(b"eeee"), split_ranges[1].end_user_key);
    }

    #[tokio::test]
    async fn test_delete_range_get() {
        let sstable_store = mock_sstable_store();
        // key=[idx, epoch], value
        let sstable = gen_iterator_test_sstable_with_range_tombstones(
            0,
            vec![],
            vec![(0, 2, 300), (1, 4, 150), (3, 6, 50), (5, 8, 150)],
            sstable_store,
        )
        .await;
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(0, 200).to_ref(),
        );
        assert!(ret.is_none());
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(1, 100).to_ref(),
        );
        assert!(ret.is_none());
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(1, 200).to_ref(),
        );
        assert_eq!(ret, Some(150));
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(1, 300).to_ref(),
        );
        assert_eq!(ret, Some(300));
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(3, 100).to_ref(),
        );
        assert_eq!(ret, Some(50));
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(6, 100).to_ref(),
        );
        assert!(ret.is_none());
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(6, 200).to_ref(),
        );
        assert_eq!(ret, Some(150));
        let ret = get_delete_range_epoch_from_sstable(
            &sstable,
            &iterator_test_key_of_epoch(8, 200).to_ref(),
        );
        assert!(ret.is_none());
    }

    #[test]
    pub fn test_delete_cut_range() {
        let mut builder = DeleteRangeAggregatorBuilder::default();
        let mut rng = rand::thread_rng();
        let mut origin = vec![];
        const SEQUENCE_COUNT: HummockEpoch = 5000;
        for sequence in 1..(SEQUENCE_COUNT + 1) {
            let left: u64 = rng.gen_range(0..100);
            let right: u64 = left + rng.gen_range(0..100) + 1;
            let tombstone = DeleteRangeTombstone::new(
                TableId::default(),
                left.to_be_bytes().to_vec(),
                right.to_be_bytes().to_vec(),
                sequence,
            );
            assert!(tombstone.start_user_key.lt(&tombstone.end_user_key));
            origin.push(tombstone);
        }
        builder.add_tombstone(origin.clone());
        let agg = builder.build(0, false);
        let split_ranges = agg.get_tombstone_between(
            &UserKey::new(TableId::default(), TableKey(b"")),
            &UserKey::new(TableId::default(), TableKey(b"")),
        );
        assert!(split_ranges.len() > origin.len());
        let mut sequence_index: HashMap<u64, Vec<DeleteRangeTombstone>> = HashMap::default();
        for tombstone in split_ranges {
            let data = sequence_index.entry(tombstone.sequence).or_default();
            data.push(tombstone);
        }
        assert_eq!(SEQUENCE_COUNT, sequence_index.len() as u64);
        for (sequence, mut data) in sequence_index {
            data.sort();
            for i in 1..data.len() {
                assert_eq!(data[i - 1].end_user_key, data[i].start_user_key);
            }
            assert_eq!(
                data[0].start_user_key,
                origin[sequence as usize - 1].start_user_key
            );
            assert_eq!(
                data.last().unwrap().end_user_key,
                origin[sequence as usize - 1].end_user_key
            );
        }
    }
}
