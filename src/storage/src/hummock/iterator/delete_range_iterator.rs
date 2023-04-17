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

use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

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
    /// Retrieves the left-endpoint of the current range-tombstone. Our range-tombstones are all
    /// defined by [`start_user_key`, `end_user_key`), which means that `start_user_key` is
    /// inclusive while `end_user_key` is exclusive.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn start_user_key(&self) -> UserKey<&[u8]>;

    /// Retrieves the right-endpoint of the current range-tombstone.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn end_user_key(&self) -> UserKey<&[u8]>;

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
    /// - ONLY call `start_user_key`, `end_user_key`, `current_epoch` and `next` if `is_valid`
    ///   returns `true`.
    /// - This function should be straightforward and return immediately.
    fn is_valid(&self) -> bool;
}

pub enum RangeIteratorTyped {
    Sst(SstableDeleteRangeIterator),
    Batch(SharedBufferDeleteRangeIterator),
}

impl DeleteRangeIterator for RangeIteratorTyped {
    fn start_user_key(&self) -> UserKey<&[u8]> {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.start_user_key(),
            RangeIteratorTyped::Batch(batch) => batch.start_user_key(),
        }
    }

    fn end_user_key(&self) -> UserKey<&[u8]> {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.end_user_key(),
            RangeIteratorTyped::Batch(batch) => batch.end_user_key(),
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
        self.start_user_key().eq(&other.start_user_key())
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
        other
            .start_user_key()
            .cmp(&self.start_user_key())
            .then_with(|| other.end_user_key().cmp(&self.end_user_key()))
    }
}

#[derive(Default)]
pub struct ForwardMergeRangeIterator {
    heap: BinaryHeap<RangeIteratorTyped>,
    unused_iters: Vec<RangeIteratorTyped>,
}

impl ForwardMergeRangeIterator {
    pub fn add_batch_iter(&mut self, iter: SharedBufferDeleteRangeIterator) {
        self.unused_iters.push(RangeIteratorTyped::Batch(iter));
    }

    pub fn add_sst_iter(&mut self, iter: SstableDeleteRangeIterator) {
        self.unused_iters.push(RangeIteratorTyped::Sst(iter));
    }
}

impl DeleteRangeIterator for ForwardMergeRangeIterator {
    fn start_user_key(&self) -> UserKey<&[u8]> {
        self.heap.peek().unwrap().start_user_key()
    }

    fn end_user_key(&self) -> UserKey<&[u8]> {
        self.heap.peek().unwrap().end_user_key()
    }

    fn current_epoch(&self) -> HummockEpoch {
        self.heap.peek().unwrap().current_epoch()
    }

    fn next(&mut self) {
        let mut node = self.heap.peek_mut().expect("no inner iter");
        node.next();
        if !node.is_valid() {
            // Put back to `unused_iters`
            let node = PeekMut::pop(node);
            self.unused_iters.push(node);
        } else {
            // This will update the heap top.
            drop(node);
        }
    }

    fn rewind(&mut self) {
        self.unused_iters.extend(self.heap.drain());
        for mut node in self.unused_iters.drain(..) {
            node.rewind();
            if node.is_valid() {
                self.heap.push(node);
            }
        }
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        self.unused_iters.extend(self.heap.drain());
        self.heap = self
            .unused_iters
            .drain_filter(|node| {
                node.seek(target_user_key);
                node.is_valid()
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
