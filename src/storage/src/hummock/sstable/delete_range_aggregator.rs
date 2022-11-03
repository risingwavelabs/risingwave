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

use risingwave_hummock_sdk::key::user_key;
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::DeleteRangeTombstone;

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

pub struct DeleteRangeAggregator {
    delete_tombstones: Vec<DeleteRangeTombstone>,
    key_range: KeyRange,
    watermark: u64,
    gc_delete_keys: bool,
}

impl DeleteRangeAggregator {
    pub fn new(key_range: KeyRange, watermark: u64, gc_delete_keys: bool) -> Self {
        Self {
            key_range,
            delete_tombstones: vec![],
            watermark,
            gc_delete_keys,
        }
    }

    pub fn add_tombstone(&mut self, data: Vec<DeleteRangeTombstone>) {
        for mut tombstone in data {
            if !self.key_range.left.is_empty() {
                let split_start_user_key = user_key(&self.key_range.left);
                if split_start_user_key.ge(tombstone.end_user_key.as_slice()) {
                    continue;
                }
                if split_start_user_key.gt(tombstone.start_user_key.as_slice()) {
                    tombstone.start_user_key = split_start_user_key.to_vec();
                }
            }
            if !self.key_range.right.is_empty() {
                let split_end_user_key = user_key(&self.key_range.right);
                if split_end_user_key.le(tombstone.start_user_key.as_slice()) {
                    continue;
                }
                if split_end_user_key.lt(tombstone.end_user_key.as_slice()) {
                    tombstone.end_user_key = split_end_user_key.to_vec();
                }
            }
            self.delete_tombstones.push(tombstone);
        }
    }

    pub fn sort(&mut self) {
        self.delete_tombstones.sort_by(|a, b| {
            let ret = a.start_user_key.cmp(&b.start_user_key);
            if ret == std::cmp::Ordering::Equal {
                b.sequence.cmp(&a.sequence)
            } else {
                ret
            }
        });
    }

    pub fn iter(self: &Arc<Self>) -> DeleteRangeAggregatorIterator<SingleDeleteRangeIterator> {
        let agg = self.clone();
        let inner = SingleDeleteRangeIterator { agg, seek_idx: 0 };
        DeleteRangeAggregatorIterator {
            inner,
            epoch_index: BTreeSet::new(),
            end_user_key_index: BinaryHeap::with_capacity(self.delete_tombstones.len()),
            watermark: self.watermark,
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

pub trait DeleteRangeIterator {
    fn start_user_key(&self) -> &[u8];
    fn end_user_key(&self) -> &[u8];
    fn current_epoch(&self) -> HummockEpoch;
    fn next(&mut self);
    fn seek_to_first(&mut self);
    fn seek(&mut self, target_key: &[u8]);
    fn valid(&self) -> bool;
}

pub struct SingleDeleteRangeIterator {
    agg: Arc<DeleteRangeAggregator>,
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

    fn seek_to_first(&mut self) {
        self.seek_idx = 0;
    }

    fn seek(&mut self, target_key: &[u8]) {
        self.seek_idx = 0;
        while self.seek_idx < self.agg.delete_tombstones.len()
            && self.agg.delete_tombstones[self.seek_idx]
                .end_user_key
                .as_slice()
                .le(target_key)
        {
            self.seek_idx += 1;
        }
    }

    fn valid(&self) -> bool {
        self.seek_idx < self.agg.delete_tombstones.len()
    }
}

pub struct DeleteRangeAggregatorIterator<I: DeleteRangeIterator> {
    inner: I,
    end_user_key_index: BinaryHeap<SortedBoundary>,
    epoch_index: BTreeSet<HummockEpoch>,
    watermark: u64,
}

impl<I: DeleteRangeIterator> DeleteRangeAggregatorIterator<I> {
    pub fn should_delete(&mut self, target_key: &[u8], epoch: HummockEpoch) -> bool {
        if epoch >= self.watermark {
            return false;
        }

        // take the smallest end_user_key which would never cover the current key and remove them
        //  from covered epoch index.
        while !self.end_user_key_index.is_empty() {
            let item = self.end_user_key_index.peek().unwrap();
            if item.user_key.as_slice().gt(target_key) {
                break;
            }

            // The correctness of the algorithm needs to be guaranteed by "the epoch of the
            // intervals covering each other must be different".
            self.epoch_index.remove(&item.sequence);
            self.end_user_key_index.pop();
        }
        while self.inner.valid() && self.inner.start_user_key().le(target_key) {
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

        // There may be several epoch, we only care the largest one.
        self.epoch_index
            .last()
            .map(|tombstone_epoch| *tombstone_epoch >= epoch)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use risingwave_hummock_sdk::key::key_with_epoch;

    use super::*;

    #[test]
    pub fn test_delete_range_aggregator() {
        let mut agg = DeleteRangeAggregator::new(
            KeyRange::new(
                Bytes::from(key_with_epoch(vec![b'b'], 0)),
                Bytes::from(key_with_epoch(vec![b'j'], 0)),
            ),
            10,
            false,
        );
        agg.add_tombstone(vec![
            DeleteRangeTombstone::new(b"aaaaaa".to_vec(), b"bbbccc".to_vec(), 12),
            DeleteRangeTombstone::new(b"aaaaaa".to_vec(), b"bbbddd".to_vec(), 9),
            DeleteRangeTombstone::new(b"bbbaab".to_vec(), b"bbbdddf".to_vec(), 6),
            DeleteRangeTombstone::new(b"bbbeee".to_vec(), b"eeeeee".to_vec(), 8),
            DeleteRangeTombstone::new(b"bbbfff".to_vec(), b"ffffff".to_vec(), 9),
            DeleteRangeTombstone::new(b"gggggg".to_vec(), b"hhhhhh".to_vec(), 9),
        ]);
        agg.sort();
        let agg = Arc::new(agg);
        let mut iter = agg.iter();
        // can not be removed by tombstone with smaller epoch.
        assert!(!iter.should_delete(b"bbb", 13));
        // can not be removed by tombstone because its sequence is larger than epoch.
        assert!(!iter.should_delete(b"bbb", 11));
        assert!(iter.should_delete(b"bbb", 8));

        assert!(iter.should_delete(b"bbbaaa", 8));

        assert!(iter.should_delete(b"bbbccd", 8));
        // can not be removed by tombstone because it equals the end of delete-ranges.
        assert!(!iter.should_delete(b"bbbddd", 8));
        assert!(iter.should_delete(b"bbbeee", 8));
        assert!(!iter.should_delete(b"bbbeef", 10));
        assert!(iter.should_delete(b"eeeeee", 9));
        assert!(iter.should_delete(b"gggggg", 8));
        assert!(!iter.should_delete(b"hhhhhh", 8));

        let split_ranges = agg.get_tombstone_between(b"bbb", b"eeeeee");
        assert_eq!(5, split_ranges.len());
        assert_eq!(b"bbb", split_ranges[0].start_user_key.as_slice());
        assert_eq!(b"bbb", split_ranges[1].start_user_key.as_slice());
        assert_eq!(b"bbbaab", split_ranges[2].start_user_key.as_slice());
        assert_eq!(b"eeeeee", split_ranges[3].end_user_key.as_slice());
        assert_eq!(b"eeeeee", split_ranges[4].end_user_key.as_slice());
    }

    #[test]
    pub fn test_delete_range_split() {
        let mut agg = DeleteRangeAggregator::new(
            KeyRange::new(
                Bytes::from(key_with_epoch(b"bbbb".to_vec(), 0)),
                Bytes::from(key_with_epoch(b"eeee".to_vec(), 0)),
            ),
            10,
            true,
        );
        agg.add_tombstone(vec![
            DeleteRangeTombstone::new(b"aaaa".to_vec(), b"bbbb".to_vec(), 12),
            DeleteRangeTombstone::new(b"aaaa".to_vec(), b"cccc".to_vec(), 12),
            DeleteRangeTombstone::new(b"cccc".to_vec(), b"dddd".to_vec(), 10),
            DeleteRangeTombstone::new(b"cccc".to_vec(), b"eeee".to_vec(), 12),
            DeleteRangeTombstone::new(b"eeee".to_vec(), b"ffff".to_vec(), 12),
        ]);
        agg.sort();
        let split_ranges = agg.get_tombstone_between(b"bbb", b"eeeeee");
        assert_eq!(2, split_ranges.len());
        assert_eq!(b"bbbb", split_ranges[0].start_user_key.as_slice());
        assert_eq!(b"cccc", split_ranges[0].end_user_key.as_slice());
        assert_eq!(b"cccc", split_ranges[1].start_user_key.as_slice());
        assert_eq!(b"eeee", split_ranges[1].end_user_key.as_slice());
    }
}
