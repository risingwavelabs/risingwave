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

use risingwave_hummock_sdk::key::{get_epoch, key_with_epoch, user_key};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockEpoch;

#[derive(Clone)]
pub struct DeleteRangeTombstone {
    start_user_key: Vec<u8>,
    end_user_key: Vec<u8>,
    sequence: HummockEpoch,
}

impl PartialEq<Self> for DeleteRangeTombstone {
    fn eq(&self, other: &Self) -> bool {
        self.end_user_key.eq(&other.end_user_key) && self.sequence == other.sequence
    }
}

impl PartialOrd for DeleteRangeTombstone {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ret = self
            .end_user_key
            .cmp(&other.end_user_key)
            .then_with(|| other.sequence.cmp(&self.sequence));
        Some(ret)
    }
}

impl Eq for DeleteRangeTombstone {}

impl Ord for DeleteRangeTombstone {
    fn cmp(&self, other: &Self) -> Ordering {
        self.end_user_key
            .cmp(&other.end_user_key)
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

    pub fn add_tombstone(&mut self, data: Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, mut end_user_key) in data {
            let mut start_user_key = user_key(&key);
            let sequence = get_epoch(&key);
            if !self.key_range.left.is_empty() {
                let split_start_user_key = user_key(&self.key_range.left);
                if split_start_user_key.gt(end_user_key.as_slice()) {
                    continue;
                }
                if split_start_user_key.gt(start_user_key) {
                    start_user_key = split_start_user_key;
                }
            }
            if !self.key_range.right.is_empty() {
                let split_end_user_key = user_key(&self.key_range.right);
                if split_end_user_key.le(start_user_key) {
                    continue;
                }
                let split_end_user_key = user_key(&self.key_range.right);
                if split_end_user_key.le(start_user_key) {
                    continue;
                }
                if split_end_user_key.lt(end_user_key.as_slice()) {
                    end_user_key = split_end_user_key.to_vec();
                }
            }

            let tombstone = DeleteRangeTombstone {
                start_user_key: start_user_key.to_vec(),
                end_user_key,
                sequence,
            };
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

    pub fn iter(self: &Arc<Self>) -> DeleteRangeTombstoneIterator {
        let agg = self.clone();
        DeleteRangeTombstoneIterator {
            agg,
            epoch_index: BTreeSet::new(),
            end_user_key_index: BinaryHeap::with_capacity(self.delete_tombstones.len()),
            seek_idx: 0,
            watermark: self.watermark,
        }
    }

    // split ranges to make sure they locate in [smallest_user_key, largest_user_key)
    pub fn get_tombstone_between(
        &self,
        smallest_user_key: &[u8],
        largest_user_key: &[u8],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
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

            let start_key = if smallest_user_key.is_empty()
                || tombstone.start_user_key.as_slice().gt(smallest_user_key)
            {
                key_with_epoch(tombstone.start_user_key.clone(), tombstone.sequence)
            } else {
                key_with_epoch(smallest_user_key.to_vec(), tombstone.sequence)
            };
            let end_key = if largest_user_key.is_empty()
                || tombstone.end_user_key.as_slice().lt(largest_user_key)
            {
                tombstone.end_user_key.clone()
            } else {
                largest_user_key.to_vec()
            };
            delete_ranges.push((start_key, end_key));
        }
        delete_ranges
    }
}

pub struct DeleteRangeTombstoneIterator {
    agg: Arc<DeleteRangeAggregator>,
    seek_idx: usize,
    end_user_key_index: BinaryHeap<DeleteRangeTombstone>,
    epoch_index: BTreeSet<HummockEpoch>,
    watermark: u64,
}

impl DeleteRangeTombstoneIterator {
    pub fn should_delete(&mut self, user_key: &[u8], epoch: HummockEpoch) -> bool {
        if epoch >= self.watermark {
            return false;
        }
        while !self.end_user_key_index.is_empty() {
            let item = self.end_user_key_index.peek().unwrap();
            if item.end_user_key.as_slice().gt(user_key) {
                break;
            }
            self.epoch_index.remove(&item.sequence);
            self.end_user_key_index.pop();
        }
        while self.seek_idx < self.agg.delete_tombstones.len()
            && self.agg.delete_tombstones[self.seek_idx]
                .start_user_key
                .as_slice()
                .le(user_key)
        {
            let tombstone = &self.agg.delete_tombstones[self.seek_idx];
            // we only need to care about sequence smaller than watermark, because key with epoch
            // larger than watermark could not be delete.
            if tombstone.sequence > self.watermark {
                self.seek_idx += 1;
                continue;
            }
            self.end_user_key_index.push(tombstone.clone());
            self.epoch_index.insert(tombstone.sequence);
            self.seek_idx += 1;
        }

        // There may be several
        self.epoch_index
            .first()
            .map(|tombstone_epoch| *tombstone_epoch >= epoch)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    pub fn test_delete_range_aggregator() {
        let mut agg = DeleteRangeAggregator::new(
            KeyRange::new(
                Bytes::from(key_with_epoch(vec![b'b'], 0)),
                Bytes::from(key_with_epoch(vec![b'f'], 0)),
            ),
            10,
            false,
        );
        agg.add_tombstone(vec![
            (key_with_epoch(b"aaaaaa".to_vec(), 12), b"bbbccc".to_vec()),
            (key_with_epoch(b"bbbaaa".to_vec(), 9), b"bbbddd".to_vec()),
            (key_with_epoch(b"bbbeee".to_vec(), 9), b"ffffff".to_vec()),
        ]);
        agg.sort();
        let agg = Arc::new(agg);
        let mut iter = agg.iter();
        // can not be removed by tombstone with smaller epoch.
        assert!(!iter.should_delete(b"bbb", 13));
        // can not be removed by tombstone because its sequence is larger than epoch.
        assert!(!iter.should_delete(b"bbb", 11));
        // can not be removed by tombstone because it is the only version just after watermark.
        assert!(!iter.should_delete(b"bbb", 8));

        assert!(iter.should_delete(b"bbbaaa", 8));

        assert!(iter.should_delete(b"bbbccd", 8));
        // can not be removed by tombstone because it equals the end of delete-ranges.
        assert!(!iter.should_delete(b"bbbddd", 8));
        assert!(iter.should_delete(b"bbbeee", 8));
        assert!(!iter.should_delete(b"bbbeef", 10));

        let split_ranges = agg.get_tombstone_between(b"bbb", b"ddd");
        assert_eq!(3, split_ranges.len());
        assert_eq!(b"bbb", user_key(&split_ranges[0].0));
        assert_eq!(b"bbbccc", split_ranges[0].1.as_slice());
        assert_eq!(b"bbbaaa", user_key(&split_ranges[1].0));
        assert_eq!(b"bbbddd", split_ranges[1].1.as_slice());
        assert_eq!(b"bbbeee", user_key(&split_ranges[2].0));
        assert_eq!(b"ddd", split_ranges[2].1.as_slice());
    }
}
