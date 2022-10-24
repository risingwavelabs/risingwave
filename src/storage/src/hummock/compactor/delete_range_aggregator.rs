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

use risingwave_hummock_sdk::key::{get_epoch, key_with_epoch, user_key};
use risingwave_hummock_sdk::key_range::KeyRange;
use risingwave_hummock_sdk::HummockEpoch;

#[derive(Clone)]
pub struct DeleteRangeTombstone {
    start_user_key: Vec<u8>,
    end_user_key: Vec<u8>,
    sequence: HummockEpoch,
}

pub struct DeleteRangeAggregator {
    delete_tombstones: Vec<DeleteRangeTombstone>,
    key_range: KeyRange,
}

impl DeleteRangeAggregator {
    pub fn new(key_range: KeyRange) -> Self {
        Self {
            key_range,
            delete_tombstones: vec![],
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

    pub fn iter(&mut self) -> DeleteRangeTombstoneIterator {
        self.delete_tombstones.sort_by(|a, b| {
            let ret = a.start_user_key.cmp(&b.start_user_key);
            if ret == std::cmp::Ordering::Equal {
                b.sequence.cmp(&a.sequence)
            } else {
                ret
            }
        });
        let delete_tombstones = self.delete_tombstones.clone();
        let mut tombstone_index: Vec<DeleteRangeTombstone> = vec![];
        for mut tombstone in delete_tombstones {
            if let Some(last_tombstone) = tombstone_index.last_mut() {
                if last_tombstone.end_user_key.gt(&tombstone.start_user_key) {
                    // split to two no-overlap ranges
                    let mut new_tombstone = last_tombstone.clone();
                    new_tombstone.start_user_key = tombstone.start_user_key.clone();
                    new_tombstone.end_user_key = last_tombstone.end_user_key.clone();
                    new_tombstone.sequence =
                        std::cmp::max(new_tombstone.sequence, tombstone.sequence);
                    last_tombstone.end_user_key = new_tombstone.start_user_key.clone();
                    tombstone.start_user_key = new_tombstone.end_user_key.clone();
                    tombstone_index.push(new_tombstone);
                    tombstone_index.push(tombstone);
                }
            } else {
                tombstone_index.push(tombstone);
            }
        }

        DeleteRangeTombstoneIterator {
            tombstone_index,
            seek_idx: 0,
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
            if tombstone.start_user_key.as_slice().ge(largest_user_key) {
                continue;
            }
            if tombstone.end_user_key.as_slice().le(smallest_user_key) {
                continue;
            }
            let start_key = if tombstone.start_user_key.as_slice().gt(smallest_user_key) {
                key_with_epoch(tombstone.start_user_key.clone(), tombstone.sequence)
            } else {
                key_with_epoch(smallest_user_key.to_vec(), tombstone.sequence)
            };
            let end_key = if tombstone.end_user_key.as_slice().lt(largest_user_key) {
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
    seek_idx: usize,
    tombstone_index: Vec<DeleteRangeTombstone>,
}

impl DeleteRangeTombstoneIterator {
    pub fn should_delete(&mut self, user_key: &[u8], epoch: HummockEpoch) -> bool {
        while self.seek_idx < self.tombstone_index.len()
            && self.tombstone_index[self.seek_idx]
                .end_user_key
                .as_slice()
                .le(user_key)
        {
            self.seek_idx += 1;
        }
        if self.seek_idx >= self.tombstone_index.len() {
            return false;
        }
        self.tombstone_index[self.seek_idx]
            .start_user_key
            .as_slice()
            .le(user_key)
            && self.tombstone_index[self.seek_idx].sequence >= epoch
    }
}
