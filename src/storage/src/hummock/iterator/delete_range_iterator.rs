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

use async_trait::async_trait;
use risingwave_hummock_sdk::key::{PointRange, UserKey};
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::hummock::SstableInfo;

use crate::hummock::iterator::concat_delete_range_iterator::ConcatDeleteRangeIterator;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferDeleteRangeIterator;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::{HummockResult, SstableDeleteRangeIterator};

/// `DeleteRangeIterator` defines the interface of all delete-range iterators, which is used to
/// filter keys deleted by some range tombstone
///
/// After creating the iterator instance,
/// - if you want to iterate from the beginning, you need to then call its `rewind` method.
/// - if you want to iterate from some specific position, you need to then call its `seek` method.
#[async_trait]
pub trait DeleteRangeIterator {
    /// Retrieves the next extended user key that changes current epoch.
    ///
    /// Note:
    /// - Before calling this function, makes sure the iterator `is_valid`.
    /// - This function should be straightforward and return immediately.
    ///
    /// # Panics
    /// This function will panic if the iterator is invalid.
    fn next_extended_user_key(&self) -> PointRange<&[u8]>;

    /// Retrieves the epoch of the current range delete.
    /// It returns the epoch between the previous `next_user_key` (inclusive) and the current
    /// `next_user_key` (not inclusive). When there is no range deletes, it will return
    /// `HummockEpoch::MAX`.
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
    async fn next(&mut self) -> HummockResult<()>;

    /// Resets the position of the iterator.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    async fn rewind(&mut self) -> HummockResult<()>;

    /// Resets iterator and seeks to the first tombstone whose left-end >= provided key, we use this
    /// method to skip tombstones which do not overlap with the provided key.
    ///
    /// Note:
    /// - Do not decide whether the position is valid or not by checking the returned error of this
    ///   function. This function WON'T return an `Err` if invalid. You should check `is_valid`
    ///   before starting iteration.
    async fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) -> HummockResult<()>;

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
    Concat(ConcatDeleteRangeIterator),
}

#[async_trait]
impl DeleteRangeIterator for RangeIteratorTyped {
    fn next_extended_user_key(&self) -> PointRange<&[u8]> {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.next_extended_user_key(),
            RangeIteratorTyped::Batch(batch) => batch.next_extended_user_key(),
            RangeIteratorTyped::Concat(batch) => batch.next_extended_user_key(),
        }
    }

    fn current_epoch(&self) -> HummockEpoch {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.current_epoch(),
            RangeIteratorTyped::Batch(batch) => batch.current_epoch(),
            RangeIteratorTyped::Concat(batch) => batch.current_epoch(),
        }
    }

    async fn next(&mut self) -> HummockResult<()> {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.next().await,
            RangeIteratorTyped::Batch(batch) => batch.next().await,
            RangeIteratorTyped::Concat(iter) => iter.next().await,
        }
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.rewind().await,
            RangeIteratorTyped::Batch(batch) => batch.rewind().await,
            RangeIteratorTyped::Concat(iter) => iter.rewind().await,
        }
    }

    async fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) -> HummockResult<()> {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.seek(target_user_key).await,
            RangeIteratorTyped::Batch(batch) => batch.seek(target_user_key).await,
            RangeIteratorTyped::Concat(iter) => iter.seek(target_user_key).await,
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            RangeIteratorTyped::Sst(sst) => sst.is_valid(),
            RangeIteratorTyped::Batch(batch) => batch.is_valid(),
            RangeIteratorTyped::Concat(iter) => iter.is_valid(),
        }
    }
}

impl PartialEq<Self> for RangeIteratorTyped {
    fn eq(&self, other: &Self) -> bool {
        self.next_extended_user_key()
            .eq(&other.next_extended_user_key())
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
            .next_extended_user_key()
            .cmp(&self.next_extended_user_key())
    }
}

/// For each SST or batch delete range iterator, it represents the union set of delete ranges in the
/// corresponding SST/batch. Therefore delete ranges are then ordered and do not overlap with each
/// other in every `RangeIteratorTyped`. However, in each SST, since original delete ranges are
/// replaced with a union set of delete ranges, we lose exact information about whether a key
/// is deleted by a delete range in the same SST. Therefore we need to construct a
/// corresponding delete key (aka key tombstone) to represent this.
///
/// In the `ForwardMergeRangeIterator`, assume that SST1 has delete range event
/// `<5, epoch1>`, `<8, epoch2>` and `<11, epoch3>`
/// and SST2 has delete range event
/// `<7, epoch4>`and `<9, epoch5>`.
/// Initially, `next_user_key` of `ForwardMergeRangeIterator` is 5, which is the earliest event and
/// current epochs is empty set at this time.
/// When `UserIterator` queries user key 5, `current_epochs` becomes `{epoch1}`, which means the key
/// fetched by `UserIterator` is deleted only if `key epoch <= epoch1 <= read_epoch`.
/// Simultaneously, `next_user_key` becomes 7, which means that the inequality will be kept until
/// the key fetched by `UserIterator` reaches user key 7.
/// For example, if the `UserIterator` queries user key 6 later, the delete condition is still
/// `key epoch <= epoch1 <= read_epoch`.
///
/// When `UserIterator` queries user key 8,
/// `next_user_key` of SST1 is 11, `current_epoch` of SST1 is epoch2;
/// `next_user_key` of SST2 is 9, `current_epoch` of SST2 is epoch4;
/// Therefore `current_epochs` of `ForwardMergeRangeIterator` is `{epoch2, epoch4}`,
/// `next_user_key` of `ForwardMergeRangeIterator` is min(11, 9) == 9,
/// which means that `current_epochs` won't change until user key 9.
///
/// We can then get the largest epoch which is not greater than read epoch in `{epoch2, epoch4}`.
/// The user key is deleted only if key epoch is below this epoch.
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

    pub fn add_concat_iter(&mut self, sstables: Vec<SstableInfo>, sstable_store: SstableStoreRef) {
        self.unused_iters
            .push(RangeIteratorTyped::Concat(ConcatDeleteRangeIterator::new(
                sstables,
                sstable_store,
            )))
    }
}

impl ForwardMergeRangeIterator {
    pub(super) async fn next_until(&mut self, target_user_key: UserKey<&[u8]>) -> HummockResult<()> {
        let target_extended_user_key = PointRange::from_user_key(target_user_key, false);
        while self.is_valid() && self.next_extended_user_key().le(&target_extended_user_key) {
            self.next().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl DeleteRangeIterator for ForwardMergeRangeIterator {
    fn next_extended_user_key(&self) -> PointRange<&[u8]> {
        self.heap.peek().unwrap().next_extended_user_key()
    }

    fn current_epoch(&self) -> HummockEpoch {
        self.current_epochs
            .range(..=self.read_epoch)
            .last()
            .map_or(HummockEpoch::MIN, |epoch| *epoch)
    }

    async fn next(&mut self) -> HummockResult<()> {
        self.tmp_buffer
            .push(self.heap.pop().expect("no inner iter"));
        while let Some(node) = self.heap.peek() && node.is_valid() && node.next_extended_user_key() == self.tmp_buffer[0].next_extended_user_key() {
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
            node.next().await?;
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
        Ok(())
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.current_epochs.clear();
        self.unused_iters.extend(self.heap.drain());
        for mut node in self.unused_iters.drain(..) {
            node.rewind().await?;
            if node.is_valid() {
                let epoch = node.current_epoch();
                if epoch != HummockEpoch::MAX {
                    self.current_epochs.insert(epoch);
                }
                self.heap.push(node);
            }
        }
        Ok(())
    }

    async fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) -> HummockResult<()> {
        self.current_epochs.clear();
        let mut iters = std::mem::take(&mut self.unused_iters);
        iters.extend(self.heap.drain());
        for mut node in iters {
            node.seek(target_user_key).await?;
            if node.is_valid() {
                let epoch = node.current_epoch();
                if epoch != HummockEpoch::MAX {
                    self.current_epochs.insert(epoch);
                }
                self.heap.push(node);
            } else {
                self.unused_iters.push(node);
            }
        }
        Ok(())
    }

    fn is_valid(&self) -> bool {
        self.heap
            .peek()
            .map(|node| node.is_valid())
            .unwrap_or(false)
    }
}
