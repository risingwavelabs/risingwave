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

use std::collections::{BTreeSet, BinaryHeap};

use risingwave_hummock_sdk::key::UserKey;
use risingwave_hummock_sdk::HummockEpoch;

use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferDeleteRangeIterator;
use crate::hummock::SstableDeleteRangeIterator;

/// `DeleteRangeIterator` defines the interface of all delete-range iterators, which is used to
/// filter keys deleted by some range tombstone
///
/// After creating the iterator instance,
/// - if you want to iterate from the beginning, you need to then call its `rewind` method.
/// - if you want to iterate from some specific position, you need to then call its `seek` method.
pub trait DeleteRangeIterator {
    /// Retrieves the next user key that changes current epoch.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn next_user_key(&self) -> UserKey<&[u8]>;

    /// Retrieves the epoch of the current range-tombstone.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn current_epoch(&self) -> HummockEpoch;

    /// Moves a valid iterator to the next tombstone.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - After calling this function, you may first check whether the iterator `is_valid` again,
    ///   then get the new tombstone by calling `start_user_key`, `end_user_key` and
    ///   `current_epoch`.
    /// - If the position after calling this is invalid, this function WON'T return an `Err`. You
    ///   should check `is_valid` before continuing the iteration.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn next(&mut self);

    /// Resets the position of the iterator.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    fn rewind(&mut self);

    /// Resets iterator and seeks to the first tombstone whose left-end >= provided key, we use this
    /// method to skip tombstones which do not overlap with the provided key.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>);

    /// Indicates whether the iterator can be used.
    ///
    /// Note:
    /// - ONLY call `next_user_key`, `current_epoch` and `next` if `is_valid` returns `true`.
    /// - This function should be straightforward and return immediately.
    fn is_valid(&self) -> bool;
}

pub enum RangeIteratorTyped {
    Sst(SstableDeleteRangeIterator),
    Batch(SharedBufferDeleteRangeIterator),
}

impl DeleteRangeIterator for RangeIteratorTyped {
    fn next_user_key(&self) -> UserKey<&[u8]> {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.next_user_key(),
            RangeIteratorTyped::Batch(batch) => batch.next_user_key(),
        }
    }

    fn current_epoch(&self) -> HummockEpoch {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.current_epoch(),
            RangeIteratorTyped::Batch(batch) => batch.current_epoch(),
        }
    }

    fn next(&mut self) {
        match self {
            RangeIteratorTyped::Sst(sst) => {
                sst.next();
            }
            RangeIteratorTyped::Batch(batch) => {
                batch.next();
            }
        }
    }

    fn rewind(&mut self) {
        match self {
            RangeIteratorTyped::Sst(sst) => {
                sst.rewind();
            }
            RangeIteratorTyped::Batch(batch) => {
                batch.rewind();
            }
        }
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.seek(target_user_key),
            RangeIteratorTyped::Batch(batch) => batch.seek(target_user_key),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.is_valid(),
            RangeIteratorTyped::Batch(batch) => batch.is_valid(),
        }
    }
}

impl PartialEq<Self> for RangeIteratorTyped {
    fn eq(&self, other: &Self) -> bool {
        self.next_user_key().eq(&other.next_user_key())
    }
}

impl PartialOrd for RangeIteratorTyped {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for RangeIteratorTyped {}

impl Ord for RangeIteratorTyped {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.next_user_key().cmp(&self.next_user_key())
    }
}

pub struct ForwardMergeRangeIterator {
    heap: BinaryHeap<RangeIteratorTyped>,
    unused_iters: Vec<RangeIteratorTyped>,
    tmp_buffer: Vec<RangeIteratorTyped>,
    read_epoch: HummockEpoch,
    /// The correctness of the algorithm needs to be guaranteed by "the epoch of the
    /// intervals covering each other must be different".
    current_epochs: BTreeSet<HummockEpoch>,
}

impl ForwardMergeRangeIterator {
    pub fn new(read_epoch: HummockEpoch) -> Self {
        Self {
            heap: BinaryHeap::new(),
            unused_iters: vec![],
            tmp_buffer: vec![],
            read_epoch,
            current_epochs: BTreeSet::new(),
        }
    }

    pub fn add_batch_iter(&mut self, iter: SharedBufferDeleteRangeIterator) {
        self.unused_iters.push(RangeIteratorTyped::Batch(iter));
    }

    pub fn add_sst_iter(&mut self, iter: SstableDeleteRangeIterator) {
        self.unused_iters.push(RangeIteratorTyped::Sst(iter));
    }
}

impl ForwardMergeRangeIterator {
    pub(super) fn next_until(&mut self, target_user_key: &UserKey<&[u8]>) {
        while self.is_valid() && self.next_user_key().le(target_user_key) {
            self.next();
        }
    }
}

impl DeleteRangeIterator for ForwardMergeRangeIterator {
    fn next_user_key(&self) -> UserKey<&[u8]> {
        self.heap.peek().unwrap().next_user_key()
    }

    fn current_epoch(&self) -> HummockEpoch {
        self.current_epochs
            .range(..=self.read_epoch)
            .last()
            .map_or(HummockEpoch::MIN, |epoch| *epoch)
    }

    fn next(&mut self) {
        self.tmp_buffer
            .push(self.heap.pop().expect("no inner iter"));
        while let Some(node) = self.heap.peek() && node.is_valid() && node.next_user_key() == self.tmp_buffer[0].next_user_key() {
            self.tmp_buffer.push(self.heap.pop().unwrap());
        }
        for node in &self.tmp_buffer {
            let epoch = node.current_epoch();
            if epoch != HummockEpoch::MAX {
                self.current_epochs.remove(&epoch);
            }
        }
        // Correct because ranges in an epoch won't intersect.
        for mut node in std::mem::take(&mut self.tmp_buffer) {
            node.next();
            if node.is_valid() {
                let epoch = node.current_epoch();
                if epoch != HummockEpoch::MAX {
                    self.current_epochs.insert(epoch);
                }
                self.heap.push(node);
            } else {
                // Put back to `unused_iters`
                self.unused_iters.push(node);
            }
        }
    }

    fn rewind(&mut self) {
        self.current_epochs.clear();
        self.unused_iters.extend(self.heap.drain());
        for mut node in self.unused_iters.drain(..) {
            node.rewind();
            if node.is_valid() {
                let epoch = node.current_epoch();
                if epoch != HummockEpoch::MAX {
                    self.current_epochs.insert(epoch);
                }
                self.heap.push(node);
            }
        }
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        self.current_epochs.clear();
        self.unused_iters.extend(self.heap.drain());
        self.heap = self
            .unused_iters
            .drain_filter(|node| {
                node.seek(target_user_key);
                if node.is_valid() {
                    let epoch = node.current_epoch();
                    if epoch != HummockEpoch::MAX {
                        self.current_epochs.insert(epoch);
                    }
                    true
                } else {
                    false
                }
            })
            .collect();
    }

    fn is_valid(&self) -> bool {
        self.heap
            .peek()
            .map(|node| node.is_valid())
            .unwrap_or(false)
    }
}
