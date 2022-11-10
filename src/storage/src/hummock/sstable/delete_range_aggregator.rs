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
use std::collections::{BTreeSet, BinaryHeap};
use std::sync::Arc;

use risingwave_hummock_sdk::key::UserKey;
use risingwave_hummock_sdk::{HummockEpoch, KeyComparator};

use super::DeleteRangeTombstone;
use crate::hummock::iterator::DeleteRangeIterator;

pub struct SortedBoundary {
    sequence: HummockEpoch,
    user_key: Vec<u8>,
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
    delete_tombstones: Vec<DeleteRangeTombstone>,
    watermark: u64,
    gc_delete_keys: bool,
}

impl DeleteRangeAggregatorBuilder {
    pub fn add_tombstone(&mut self, data: Vec<DeleteRangeTombstone>) {
        self.delete_tombstones.extend(data);
    }

    pub fn build(mut self, watermark: u64, gc_delete_keys: bool) -> Arc<RangeTombstonesCollector> {
        self.delete_tombstones.sort_by(|a, b| {
            let ret = a.start_user_key.cmp(&b.start_user_key);
            if ret == std::cmp::Ordering::Equal {
                b.sequence.cmp(&a.sequence)
            } else {
                ret
            }
        });
        Arc::new(RangeTombstonesCollector {
            delete_tombstones: self.delete_tombstones,
            gc_delete_keys,
            watermark,
        })
    }
}

impl RangeTombstonesCollector {
    pub fn for_test() -> Self {
        Self {
            delete_tombstones: vec![],
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
        smallest_user_key: &[u8],
        largest_user_key: &[u8],
    ) -> Vec<DeleteRangeTombstone> {
        let mut delete_ranges = vec![];
        for tombstone in &self.delete_tombstones {
            if !largest_user_key.is_empty()
                && tombstone.start_user_key.as_slice().ge(largest_user_key)
            {
                continue;
            }

            if !smallest_user_key.is_empty()
                && tombstone.end_user_key.as_slice().le(smallest_user_key)
            {
                continue;
            }

            if self.gc_delete_keys && tombstone.sequence <= self.watermark {
                continue;
            }

            let mut ret = tombstone.clone();
            if !smallest_user_key.is_empty() && smallest_user_key.gt(ret.start_user_key.as_slice())
            {
                ret.start_user_key = smallest_user_key.to_vec();
            }
            if !largest_user_key.is_empty() && largest_user_key.lt(ret.end_user_key.as_slice()) {
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
    fn start_user_key(&self) -> &[u8] {
        &self.agg.delete_tombstones[self.seek_idx].start_user_key
    }

    fn end_user_key(&self) -> &[u8] {
        &self.agg.delete_tombstones[self.seek_idx].end_user_key
    }

    fn current_epoch(&self) -> HummockEpoch {
        self.agg.delete_tombstones[self.seek_idx].sequence
    }

    fn next(&mut self) {
        self.seek_idx += 1;
    }

    fn rewind(&mut self) {
        self.seek_idx = 0;
    }

    fn is_valid(&self) -> bool {
        self.seek_idx < self.agg.delete_tombstones.len()
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

    /// Check whether the target-key is deleted by some range-tombstone. Target-key must be given
    /// in order.
    pub fn should_delete(
        &mut self,
        target_key: UserKey<impl AsRef<[u8]>>,
        epoch: HummockEpoch,
    ) -> bool {
        if epoch >= self.watermark {
            return false;
        }

        // take the smallest end_user_key which would never cover the current key and remove them
        //  from covered epoch index.
        while !self.end_user_key_index.is_empty() {
            let item = self.end_user_key_index.peek().unwrap();
            if Ordering::is_gt(KeyComparator::compare_user_key_cross_format(
                item.user_key.as_slice(),
                &target_key,
            )) {
                break;
            }

            // The correctness of the algorithm needs to be guaranteed by "the epoch of the
            // intervals covering each other must be different".
            self.epoch_index.remove(&item.sequence);
            self.end_user_key_index.pop();
        }
        while self.inner.is_valid()
            && Ordering::is_le(KeyComparator::compare_user_key_cross_format(
                self.inner.start_user_key(),
                &target_key,
            ))
        {
            let sequence = self.inner.current_epoch();
            if sequence > self.watermark
                || Ordering::is_le(KeyComparator::compare_user_key_cross_format(
                    self.inner.end_user_key(),
                    &target_key,
                ))
            {
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

        // There may be several epoch, we only care the largest one.
        self.epoch_index
            .last()
            .map(|tombstone_epoch| *tombstone_epoch >= epoch)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::catalog::TableId;

    use super::*;
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
        assert!(!iter.should_delete(test_user_key(b"bbb"), 13));
        // can not be removed by tombstone because its sequence is larger than epoch.
        assert!(!iter.should_delete(test_user_key(b"bbb"), 11));
        assert!(iter.should_delete(test_user_key(b"bbb"), 8));

        assert!(iter.should_delete(test_user_key(b"bbbaaa"), 8));

        assert!(iter.should_delete(test_user_key(b"bbbccd"), 8));
        // can not be removed by tombstone because it equals the end of delete-ranges.
        assert!(!iter.should_delete(test_user_key(b"bbbddd"), 8));
        assert!(iter.should_delete(test_user_key(b"bbbeee"), 8));
        assert!(!iter.should_delete(test_user_key(b"bbbeef"), 10));
        assert!(iter.should_delete(test_user_key(b"eeeeee"), 9));
        assert!(iter.should_delete(test_user_key(b"gggggg"), 8));
        assert!(!iter.should_delete(test_user_key(b"hhhhhh"), 8));

        let split_ranges = agg.get_tombstone_between(
            &test_user_key(b"bbb").encode(),
            &test_user_key(b"eeeeee").encode(),
        );
        assert_eq!(5, split_ranges.len());
        assert_eq!(
            test_user_key(b"bbb").encode(),
            split_ranges[0].start_user_key.as_slice()
        );
        assert_eq!(
            test_user_key(b"bbb").encode(),
            split_ranges[1].start_user_key.as_slice()
        );
        assert_eq!(
            test_user_key(b"bbbaab").encode(),
            split_ranges[2].start_user_key.as_slice()
        );
        assert_eq!(
            test_user_key(b"eeeeee").encode(),
            split_ranges[3].end_user_key.as_slice()
        );
        assert_eq!(
            test_user_key(b"eeeeee").encode(),
            split_ranges[4].end_user_key.as_slice()
        );
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
            &test_user_key(b"bbbb").encode(),
            &test_user_key(b"eeeeee").encode(),
        );
        assert_eq!(3, split_ranges.len());
        assert_eq!(
            test_user_key(b"bbbb").encode(),
            split_ranges[0].start_user_key.as_slice()
        );
        assert_eq!(
            test_user_key(b"cccc").encode(),
            split_ranges[0].end_user_key.as_slice()
        );
        assert_eq!(
            test_user_key(b"cccc").encode(),
            split_ranges[1].start_user_key.as_slice()
        );
        assert_eq!(
            test_user_key(b"eeee").encode(),
            split_ranges[1].end_user_key.as_slice()
        );
    }
}
