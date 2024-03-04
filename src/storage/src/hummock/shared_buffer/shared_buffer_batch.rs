// Copyright 2024 RisingWave Labs
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
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::Bound::Included;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::hash::VirtualNode;
use risingwave_hummock_sdk::key::{FullKey, PointRange, TableKey, TableKeyRange, UserKey};
use risingwave_hummock_sdk::EpochWithGap;

use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::iterator::{
    Backward, DeleteRangeIterator, DirectionEnum, Forward, HummockIterator,
    HummockIteratorDirection,
};
use crate::hummock::utils::{range_overlap, MemoryTracker};
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockEpoch, HummockResult, MonotonicDeleteEvent};
use crate::mem_table::ImmId;
use crate::storage_value::StorageValue;
use crate::store::ReadOptions;

/// The key is `table_key`, which does not contain table id or epoch.
pub(crate) type SharedBufferItem = (TableKey<Bytes>, HummockValue<Bytes>);
pub type SharedBufferBatchId = u64;

pub(crate) type VersionedSharedBufferValue = (EpochWithGap, HummockValue<Bytes>);

pub(crate) struct SharedBufferVersionedEntryRef<'a> {
    pub(crate) key: &'a TableKey<Bytes>,
    pub(crate) new_values: &'a [VersionedSharedBufferValue],
}

/// Return an exclusive offset of the values of key of index `i`
fn value_end_offset<'a>(
    i: usize,
    entries: &'a [SharedBufferKeyEntry],
    values: &'a [VersionedSharedBufferValue],
) -> usize {
    entries
        .get(i + 1)
        .map(|entry| entry.value_offset)
        .unwrap_or(values.len())
}

fn values<'a>(
    i: usize,
    entries: &'a [SharedBufferKeyEntry],
    values: &'a [VersionedSharedBufferValue],
) -> &'a [VersionedSharedBufferValue] {
    &values[entries[i].value_offset..value_end_offset(i, entries, values)]
}

#[derive(PartialEq, Debug)]
pub(crate) struct SharedBufferKeyEntry {
    pub(crate) key: TableKey<Bytes>,
    /// A shared buffer may contain data from multiple epochs for a specific key.
    /// The values of all keys are stored together in the field `new_values` of `SharedBufferBatchInner`
    /// as a single vector. `value_offset` is the starting offset of values of the current `key` in the `new_values` vector.
    /// The end offset is the `value_offset` of the next entry or the vector end if the current entry is not the last one.
    pub(crate) value_offset: usize,
}

#[derive(Debug)]
pub(crate) struct SharedBufferBatchInner {
    entries: Vec<SharedBufferKeyEntry>,
    new_values: Vec<VersionedSharedBufferValue>,
    /// The epochs of the data in batch, sorted in ascending order (old to new)
    epochs: Vec<HummockEpoch>,
    /// Total size of all key-value items (excluding the `epoch` of value versions)
    size: usize,
    _tracker: Option<MemoryTracker>,
    /// For a batch created from multiple batches, this will be
    /// the largest batch id among input batches
    batch_id: SharedBufferBatchId,
}

impl SharedBufferBatchInner {
    pub(crate) fn new(
        epoch: HummockEpoch,
        spill_offset: u16,
        payload: Vec<SharedBufferItem>,
        size: usize,
        _tracker: Option<MemoryTracker>,
    ) -> Self {
        assert!(!payload.is_empty());
        debug_assert!(payload.iter().is_sorted_by_key(|(key, _)| key));

        let epoch_with_gap = EpochWithGap::new(epoch, spill_offset);
        let mut entries = Vec::with_capacity(payload.len());
        let mut new_values = Vec::with_capacity(payload.len());
        for (i, (key, value)) in payload.into_iter().enumerate() {
            entries.push(SharedBufferKeyEntry {
                key,
                value_offset: i,
            });
            new_values.push((epoch_with_gap, value));
        }

        let batch_id = SHARED_BUFFER_BATCH_ID_GENERATOR.fetch_add(1, Relaxed);
        SharedBufferBatchInner {
            entries,
            new_values,
            epochs: vec![epoch],
            size,
            _tracker,
            batch_id,
        }
    }

    pub fn values(&self, i: usize) -> &[VersionedSharedBufferValue] {
        values(i, &self.entries, &self.new_values)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_multi_epoch_batches(
        epochs: Vec<HummockEpoch>,
        entries: Vec<SharedBufferKeyEntry>,
        new_values: Vec<VersionedSharedBufferValue>,
        size: usize,
        imm_id: ImmId,
        tracker: Option<MemoryTracker>,
    ) -> Self {
        assert!(new_values.len() >= entries.len());
        assert!(!entries.is_empty());
        debug_assert!(entries.iter().is_sorted_by_key(|entry| &entry.key));
        debug_assert!(entries.iter().is_sorted_by_key(|entry| &entry.value_offset));
        debug_assert!((0..entries.len()).all(|i| values(i, &entries, &new_values)
            .iter()
            .rev()
            .is_sorted_by_key(|(epoch_with_gap, _)| epoch_with_gap)));
        debug_assert!(!epochs.is_empty());
        debug_assert!(epochs.is_sorted());

        Self {
            entries,
            new_values,
            epochs,
            size,
            _tracker: tracker,
            batch_id: imm_id,
        }
    }

    /// Return `None` if cannot find a visible version
    /// Return `HummockValue::Delete` if the key has been deleted by some epoch <= `read_epoch`
    fn get_value(
        &self,
        table_key: TableKey<&[u8]>,
        read_epoch: HummockEpoch,
    ) -> Option<(HummockValue<Bytes>, EpochWithGap)> {
        // Perform binary search on table key to find the corresponding entry
        if let Ok(i) = self
            .entries
            .binary_search_by(|m| (m.key.as_ref()).cmp(*table_key))
        {
            let entry = &self.entries[i];
            assert_eq!(entry.key.as_ref(), *table_key);
            // Scan to find the first version <= epoch
            for (e, v) in self.values(i) {
                // skip invisible versions
                if read_epoch < e.pure_epoch() {
                    continue;
                }
                return Some((v.clone(), *e));
            }
            // cannot find a visible version
        }

        None
    }
}

impl PartialEq for SharedBufferBatchInner {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries && self.new_values == other.new_values
    }
}

pub static SHARED_BUFFER_BATCH_ID_GENERATOR: LazyLock<AtomicU64> =
    LazyLock::new(|| AtomicU64::new(0));

/// A write batch stored in the shared buffer.
#[derive(Clone, Debug, PartialEq)]
pub struct SharedBufferBatch {
    pub(crate) inner: Arc<SharedBufferBatchInner>,
    pub table_id: TableId,
    pub instance_id: LocalInstanceId,
}

impl SharedBufferBatch {
    pub fn for_test(
        sorted_items: Vec<SharedBufferItem>,
        epoch: HummockEpoch,
        table_id: TableId,
    ) -> Self {
        let size = Self::measure_batch_size(&sorted_items);

        Self {
            inner: Arc::new(SharedBufferBatchInner::new(
                epoch,
                0,
                sorted_items,
                size,
                None,
            )),
            table_id,
            instance_id: SHARED_BUFFER_BATCH_ID_GENERATOR.fetch_add(1, Relaxed),
        }
    }

    pub fn measure_delete_range_size(batch_items: &[(Bound<Bytes>, Bound<Bytes>)]) -> usize {
        batch_items
            .iter()
            .map(|(left, right)| {
                // is_exclude_left_key(bool) + table_id + epoch
                let l1 = match left {
                    Bound::Excluded(x) | Bound::Included(x) => x.len() + 13,
                    Bound::Unbounded => 13,
                };
                let l2 = match right {
                    Bound::Excluded(x) | Bound::Included(x) => x.len() + 13,
                    Bound::Unbounded => 13,
                };
                l1 + l2
            })
            .sum()
    }

    pub fn measure_batch_size(batch_items: &[SharedBufferItem]) -> usize {
        // size = Sum(length of full key + length of user value)
        batch_items
            .iter()
            .map(|(k, v)| {
                k.len() + {
                    match v {
                        HummockValue::Put(val) => val.len(),
                        HummockValue::Delete => 0,
                    }
                }
            })
            .sum()
    }

    pub fn filter<R, B>(&self, table_id: TableId, table_key_range: &R) -> bool
    where
        R: RangeBounds<TableKey<B>>,
        B: AsRef<[u8]>,
    {
        let left = table_key_range
            .start_bound()
            .as_ref()
            .map(|key| TableKey(key.0.as_ref()));
        let right = table_key_range
            .end_bound()
            .as_ref()
            .map(|key| TableKey(key.0.as_ref()));
        self.table_id == table_id
            && range_overlap(
                &(left, right),
                &self.start_table_key(),
                Included(&self.end_table_key()),
            )
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn min_epoch(&self) -> HummockEpoch {
        *self.inner.epochs.first().unwrap()
    }

    pub fn max_epoch(&self) -> HummockEpoch {
        *self.inner.epochs.last().unwrap()
    }

    pub fn key_count(&self) -> usize {
        self.inner.entries.len()
    }

    pub fn value_count(&self) -> usize {
        self.inner.new_values.len()
    }

    pub fn get(
        &self,
        table_key: TableKey<&[u8]>,
        read_epoch: HummockEpoch,
        _read_options: &ReadOptions,
    ) -> Option<(HummockValue<Bytes>, EpochWithGap)> {
        self.inner.get_value(table_key, read_epoch)
    }

    pub fn range_exists(&self, table_key_range: &TableKeyRange) -> bool {
        self.inner
            .entries
            .binary_search_by(|m| {
                let key = &m.key;
                let too_left = match &table_key_range.0 {
                    std::ops::Bound::Included(range_start) => range_start.as_ref() > key.as_ref(),
                    std::ops::Bound::Excluded(range_start) => range_start.as_ref() >= key.as_ref(),
                    std::ops::Bound::Unbounded => false,
                };
                if too_left {
                    return Ordering::Less;
                }

                let too_right = match &table_key_range.1 {
                    std::ops::Bound::Included(range_end) => range_end.as_ref() < key.as_ref(),
                    std::ops::Bound::Excluded(range_end) => range_end.as_ref() <= key.as_ref(),
                    std::ops::Bound::Unbounded => false,
                };
                if too_right {
                    return Ordering::Greater;
                }

                Ordering::Equal
            })
            .is_ok()
    }

    pub fn into_directed_iter<D: HummockIteratorDirection>(self) -> SharedBufferBatchIterator<D> {
        SharedBufferBatchIterator::<D>::new(self.inner, self.table_id)
    }

    pub fn into_forward_iter(self) -> SharedBufferBatchIterator<Forward> {
        self.into_directed_iter()
    }

    pub fn into_backward_iter(self) -> SharedBufferBatchIterator<Backward> {
        self.into_directed_iter()
    }

    #[inline(always)]
    pub fn start_table_key(&self) -> TableKey<&[u8]> {
        TableKey(self.inner.entries.first().expect("non-empty").key.as_ref())
    }

    #[inline(always)]
    pub fn end_table_key(&self) -> TableKey<&[u8]> {
        TableKey(self.inner.entries.last().expect("non-empty").key.as_ref())
    }

    #[inline(always)]
    pub fn raw_largest_key(&self) -> &TableKey<Bytes> {
        &self.inner.entries.last().expect("non-empty").key
    }

    /// return inclusive left endpoint, which means that all data in this batch should be larger or
    /// equal than this key.
    pub fn start_user_key(&self) -> UserKey<&[u8]> {
        UserKey::new(self.table_id, self.start_table_key())
    }

    pub fn size(&self) -> usize {
        self.inner.size
    }

    pub fn batch_id(&self) -> SharedBufferBatchId {
        self.inner.batch_id
    }

    pub fn epochs(&self) -> &Vec<HummockEpoch> {
        &self.inner.epochs
    }

    pub fn build_shared_buffer_item_batches(
        kv_pairs: Vec<(TableKey<Bytes>, StorageValue)>,
    ) -> Vec<SharedBufferItem> {
        kv_pairs
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect()
    }

    pub fn build_shared_buffer_batch(
        epoch: HummockEpoch,
        spill_offset: u16,
        sorted_items: Vec<SharedBufferItem>,
        size: usize,
        table_id: TableId,
        instance_id: LocalInstanceId,
        tracker: Option<MemoryTracker>,
    ) -> Self {
        let inner = SharedBufferBatchInner::new(epoch, spill_offset, sorted_items, size, tracker);
        SharedBufferBatch {
            inner: Arc::new(inner),
            table_id,
            instance_id,
        }
    }

    pub fn collect_vnodes(&self) -> Vec<usize> {
        let mut vnodes = Vec::with_capacity(VirtualNode::COUNT);
        let mut next_vnode_id = 0;
        while next_vnode_id < VirtualNode::COUNT {
            let seek_key = TableKey(
                VirtualNode::from_index(next_vnode_id)
                    .to_be_bytes()
                    .to_vec(),
            );
            let idx = match self
                .inner
                .entries
                .binary_search_by(|m| (m.key.as_ref()).cmp(seek_key.as_slice()))
            {
                Ok(idx) => idx,
                Err(idx) => idx,
            };
            if idx >= self.inner.entries.len() {
                break;
            }
            let item = &self.inner.entries[idx];
            if item.key.len() <= VirtualNode::SIZE {
                break;
            }
            let current_vnode_id = item.key.vnode_part().to_index();
            vnodes.push(current_vnode_id);
            next_vnode_id = current_vnode_id + 1;
        }
        vnodes
    }

    #[cfg(any(test, feature = "test"))]
    pub fn build_shared_buffer_batch_for_test(
        epoch: HummockEpoch,
        spill_offset: u16,
        sorted_items: Vec<SharedBufferItem>,
        size: usize,
        table_id: TableId,
    ) -> Self {
        let inner = SharedBufferBatchInner::new(epoch, spill_offset, sorted_items, size, None);
        SharedBufferBatch {
            inner: Arc::new(inner),
            table_id,
            instance_id: LocalInstanceId::default(),
        }
    }
}

/// Iterate all the items in the shared buffer batch
/// If there are multiple versions of a key, the iterator will return all versions
pub struct SharedBufferBatchIterator<D: HummockIteratorDirection> {
    inner: Arc<SharedBufferBatchInner>,
    /// The index of the current entry in the payload
    current_entry_idx: usize,
    /// The index of current value
    current_value_idx: usize,
    /// The exclusive end offset of the value index of current key.
    value_end_offset: usize,
    table_id: TableId,
    _phantom: PhantomData<D>,
}

impl<D: HummockIteratorDirection> SharedBufferBatchIterator<D> {
    pub(crate) fn new(inner: Arc<SharedBufferBatchInner>, table_id: TableId) -> Self {
        Self {
            inner,
            current_entry_idx: 0,
            current_value_idx: 0,
            value_end_offset: 0,
            table_id,
            _phantom: Default::default(),
        }
    }

    fn is_valid_entry_idx(&self) -> bool {
        self.current_entry_idx < self.inner.entries.len()
    }

    fn advance_to_next_entry(&mut self) {
        debug_assert!(self.is_valid_entry_idx());
        match D::direction() {
            DirectionEnum::Forward => {
                self.current_entry_idx += 1;
            }
            DirectionEnum::Backward => {
                if self.current_entry_idx == 0 {
                    self.current_entry_idx = self.inner.entries.len();
                } else {
                    self.current_entry_idx -= 1;
                }
            }
        }
    }

    fn reset_value_idx(&mut self) {
        debug_assert!(self.is_valid_entry_idx());
        self.current_value_idx = self.inner.entries[self.current_entry_idx].value_offset;
        self.value_end_offset = self.get_value_end_offset();
    }

    fn get_value_end_offset(&self) -> usize {
        debug_assert!(self.is_valid_entry_idx());
        value_end_offset(
            self.current_entry_idx,
            &self.inner.entries,
            &self.inner.new_values,
        )
    }

    fn assert_valid_idx(&self) {
        debug_assert!(self.is_valid_entry_idx());
        debug_assert!(
            self.current_value_idx >= self.inner.entries[self.current_entry_idx].value_offset
        );
        debug_assert_eq!(self.value_end_offset, self.get_value_end_offset());
        debug_assert!(self.current_value_idx < self.value_end_offset);
    }

    fn advance_to_next_value(&mut self) {
        self.assert_valid_idx();

        if self.current_value_idx + 1 < self.value_end_offset {
            self.current_value_idx += 1;
        } else {
            self.advance_to_next_entry();
            if self.is_valid_entry_idx() {
                self.reset_value_idx();
            }
        }
    }
}

impl SharedBufferBatchIterator<Forward> {
    pub(crate) fn advance_to_next_key(&mut self) {
        self.advance_to_next_entry();
        if self.is_valid_entry_idx() {
            self.reset_value_idx();
        }
    }

    pub(crate) fn current_key_entry(&self) -> SharedBufferVersionedEntryRef<'_> {
        self.assert_valid_idx();
        debug_assert_eq!(
            self.current_value_idx,
            self.inner.entries[self.current_entry_idx].value_offset
        );
        SharedBufferVersionedEntryRef {
            key: &self.inner.entries[self.current_entry_idx].key,
            new_values: &self.inner.new_values[self.current_value_idx..self.value_end_offset],
        }
    }
}

impl<D: HummockIteratorDirection> HummockIterator for SharedBufferBatchIterator<D> {
    type Direction = D;

    async fn next(&mut self) -> HummockResult<()> {
        self.advance_to_next_value();
        Ok(())
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.assert_valid_idx();
        let key = self.inner.entries[self.current_entry_idx].key.as_ref();
        let epoch_with_gap = self.inner.new_values[self.current_value_idx].0;
        FullKey::new_with_gap_epoch(self.table_id, TableKey(key), epoch_with_gap)
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.assert_valid_idx();
        self.inner.new_values[self.current_value_idx].1.as_slice()
    }

    fn is_valid(&self) -> bool {
        self.is_valid_entry_idx()
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        match D::direction() {
            DirectionEnum::Forward => {
                self.current_entry_idx = 0;
            }
            DirectionEnum::Backward => {
                self.current_entry_idx = self.inner.entries.len() - 1;
            }
        };
        self.reset_value_idx();
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        debug_assert_eq!(key.user_key.table_id, self.table_id);
        // Perform binary search on table key because the items in SharedBufferBatch is ordered
        // by table key.
        let partition_point = self
            .inner
            .entries
            .binary_search_by(|probe| probe.key.as_ref().cmp(*key.user_key.table_key));
        let seek_key_epoch = key.epoch_with_gap;
        match partition_point {
            Ok(i) => {
                self.current_entry_idx = i;
                self.reset_value_idx();
                while self.current_value_idx < self.value_end_offset {
                    let epoch_with_gap = self.inner.new_values[self.current_value_idx].0;
                    if epoch_with_gap <= seek_key_epoch {
                        break;
                    }
                    self.current_value_idx += 1;
                }
                if self.current_value_idx == self.value_end_offset {
                    self.advance_to_next_entry();
                    if self.is_valid_entry_idx() {
                        self.reset_value_idx();
                    }
                }
            }
            Err(i) => match D::direction() {
                DirectionEnum::Forward => {
                    self.current_entry_idx = i;
                    if self.is_valid_entry_idx() {
                        self.reset_value_idx();
                    }
                }
                DirectionEnum::Backward => {
                    if i == 0 {
                        self.current_entry_idx = self.inner.entries.len();
                    } else {
                        self.current_entry_idx = i - 1;
                        self.reset_value_idx();
                    }
                }
            },
        };
        Ok(())
    }

    fn collect_local_statistic(&self, _stats: &mut crate::monitor::StoreLocalStatistic) {}
}

pub struct SharedBufferDeleteRangeIterator {
    monotonic_tombstone_events: Vec<MonotonicDeleteEvent>,
    next_idx: usize,
}

impl SharedBufferDeleteRangeIterator {
    #[cfg(any(test, feature = "test"))]
    pub(crate) fn new(
        epoch: HummockEpoch,
        table_id: TableId,
        delete_ranges: Vec<(Bound<Bytes>, Bound<Bytes>)>,
    ) -> Self {
        let point_range_pairs = delete_ranges
            .into_iter()
            .map(|(left_bound, right_bound)| {
                (
                    match left_bound {
                        Bound::Excluded(x) => PointRange::from_user_key(
                            UserKey::new(table_id, TableKey(x.to_vec())),
                            true,
                        ),
                        Bound::Included(x) => PointRange::from_user_key(
                            UserKey::new(table_id, TableKey(x.to_vec())),
                            false,
                        ),
                        Bound::Unbounded => unreachable!(),
                    },
                    match right_bound {
                        Bound::Excluded(x) => PointRange::from_user_key(
                            UserKey::new(table_id, TableKey(x.to_vec())),
                            false,
                        ),
                        Bound::Included(x) => PointRange::from_user_key(
                            UserKey::new(table_id, TableKey(x.to_vec())),
                            true,
                        ),
                        Bound::Unbounded => PointRange::from_user_key(
                            UserKey::new(
                                TableId::new(table_id.table_id() + 1),
                                TableKey::default(),
                            ),
                            false,
                        ),
                    },
                )
            })
            .collect_vec();
        let mut monotonic_tombstone_events = Vec::with_capacity(point_range_pairs.len() * 2);
        for (start_point_range, end_point_range) in point_range_pairs {
            monotonic_tombstone_events.push(MonotonicDeleteEvent {
                event_key: start_point_range,
                new_epoch: epoch,
            });
            monotonic_tombstone_events.push(MonotonicDeleteEvent {
                event_key: end_point_range,
                new_epoch: HummockEpoch::MAX,
            });
        }
        Self {
            monotonic_tombstone_events,
            next_idx: 0,
        }
    }
}

impl DeleteRangeIterator for SharedBufferDeleteRangeIterator {
    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next_extended_user_key(&self) -> PointRange<&[u8]> {
        self.monotonic_tombstone_events[self.next_idx]
            .event_key
            .as_ref()
    }

    fn current_epoch(&self) -> HummockEpoch {
        if self.next_idx > 0 {
            self.monotonic_tombstone_events[self.next_idx - 1].new_epoch
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

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) -> Self::SeekFuture<'a> {
        async move {
            let target_extended_user_key = PointRange::from_user_key(target_user_key, false);
            self.next_idx = self.monotonic_tombstone_events.partition_point(
                |MonotonicDeleteEvent { event_key, .. }| {
                    event_key.as_ref().le(&target_extended_user_key)
                },
            );
            Ok(())
        }
    }

    fn is_valid(&self) -> bool {
        self.next_idx < self.monotonic_tombstone_events.len()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::{Excluded, Included};

    use risingwave_hummock_sdk::key::map_table_key_range;

    use super::*;
    use crate::hummock::compactor::merge_imms_in_memory;
    use crate::hummock::iterator::test_utils::{
        iterator_test_key_of_epoch, iterator_test_table_key_of, transform_shared_buffer,
    };

    #[tokio::test]
    async fn test_shared_buffer_batch_basic() {
        let epoch = 1;
        let shared_buffer_items: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                iterator_test_table_key_of(0),
                HummockValue::put(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value1")),
            ),
        ];
        let shared_buffer_batch = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items.clone()),
            epoch,
            Default::default(),
        );

        // Sketch
        assert_eq!(
            *shared_buffer_batch.start_table_key(),
            shared_buffer_items[0].0
        );
        assert_eq!(
            *shared_buffer_batch.end_table_key(),
            shared_buffer_items[2].0
        );

        // Point lookup
        for (k, v) in &shared_buffer_items {
            assert_eq!(
                shared_buffer_batch
                    .get(TableKey(k.as_slice()), epoch, &ReadOptions::default())
                    .unwrap()
                    .0,
                v.clone()
            );
        }
        assert_eq!(
            shared_buffer_batch.get(
                TableKey(iterator_test_table_key_of(3).as_slice()),
                epoch,
                &ReadOptions::default()
            ),
            None
        );
        assert_eq!(
            shared_buffer_batch.get(
                TableKey(iterator_test_table_key_of(4).as_slice()),
                epoch,
                &ReadOptions::default()
            ),
            None
        );

        // Forward iterator
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.rewind().await.unwrap();
        let mut output = vec![];
        while iter.is_valid() {
            output.push((
                iter.key().user_key.table_key.to_vec(),
                iter.value().to_bytes(),
            ));
            iter.next().await.unwrap();
        }
        assert_eq!(output, shared_buffer_items);

        // Backward iterator
        let mut backward_iter = shared_buffer_batch.clone().into_backward_iter();
        backward_iter.rewind().await.unwrap();
        let mut output = vec![];
        while backward_iter.is_valid() {
            output.push((
                backward_iter.key().user_key.table_key.to_vec(),
                backward_iter.value().to_bytes(),
            ));
            backward_iter.next().await.unwrap();
        }
        output.reverse();
        assert_eq!(output, shared_buffer_items);
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_seek() {
        let epoch = 1;
        let shared_buffer_items = vec![
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value2")),
            ),
            (
                iterator_test_table_key_of(3),
                HummockValue::put(Bytes::from("value3")),
            ),
        ];
        let shared_buffer_batch = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items.clone()),
            epoch,
            Default::default(),
        );

        // FORWARD: Seek to a key < 1st key, expect all three items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(0, epoch).to_ref())
            .await
            .unwrap();
        for item in &shared_buffer_items {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to a key > the last key, expect no items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(4, epoch).to_ref())
            .await
            .unwrap();
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with current epoch, expect last two items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch).to_ref())
            .await
            .unwrap();
        for item in &shared_buffer_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with future epoch, expect last two items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch + 1).to_ref())
            .await
            .unwrap();
        for item in &shared_buffer_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with old epoch, expect last item to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch - 1).to_ref())
            .await
            .unwrap();
        let item = shared_buffer_items.last().unwrap();
        assert!(iter.is_valid());
        assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to a key < 1st key, expect no items to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(0, epoch).to_ref())
            .await
            .unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to a key > the last key, expect all items to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(4, epoch).to_ref())
            .await
            .unwrap();
        for item in shared_buffer_items.iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with current epoch, expect first two items to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch).to_ref())
            .await
            .unwrap();
        for item in shared_buffer_items[0..=1].iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with old epoch, expect first item to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch - 1).to_ref())
            .await
            .unwrap();
        assert!(iter.is_valid());
        let item = shared_buffer_items.first().unwrap();
        assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with future epoch, expect first two item to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch + 1).to_ref())
            .await
            .unwrap();
        for item in shared_buffer_items[0..=1].iter().rev() {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    #[should_panic]
    async fn test_invalid_table_id() {
        let epoch = 1;
        let shared_buffer_batch = SharedBufferBatch::for_test(vec![], epoch, Default::default());
        // Seeking to non-current epoch should panic
        let mut iter = shared_buffer_batch.into_forward_iter();
        iter.seek(FullKey::for_test(TableId::new(1), vec![], epoch).to_ref())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_range_existx() {
        let epoch = 1;
        let shared_buffer_items = vec![
            (Vec::from("a_1"), HummockValue::put(Bytes::from("value1"))),
            (Vec::from("a_3"), HummockValue::put(Bytes::from("value2"))),
            (Vec::from("a_5"), HummockValue::put(Bytes::from("value3"))),
            (Vec::from("b_2"), HummockValue::put(Bytes::from("value3"))),
        ];
        let shared_buffer_batch = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items),
            epoch,
            Default::default(),
        );

        let range = (Included(Bytes::from("a")), Excluded(Bytes::from("b")));
        assert!(shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("a_")), Excluded(Bytes::from("b_")));
        assert!(shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("a_1")), Included(Bytes::from("a_1")));
        assert!(shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("a_1")), Included(Bytes::from("a_2")));
        assert!(shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("a_0x")), Included(Bytes::from("a_2x")));
        assert!(shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("a_")), Excluded(Bytes::from("c_")));
        assert!(shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("b_0x")), Included(Bytes::from("b_2x")));
        assert!(shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("b_2")), Excluded(Bytes::from("c_1x")));
        assert!(shared_buffer_batch.range_exists(&map_table_key_range(range)));

        let range = (Included(Bytes::from("a_0")), Excluded(Bytes::from("a_1")));
        assert!(!shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("a__0")), Excluded(Bytes::from("a__5")));
        assert!(!shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("b_1")), Excluded(Bytes::from("b_2")));
        assert!(!shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("b_3")), Excluded(Bytes::from("c_1")));
        assert!(!shared_buffer_batch.range_exists(&map_table_key_range(range)));
        let range = (Included(Bytes::from("b__x")), Excluded(Bytes::from("c__x")));
        assert!(!shared_buffer_batch.range_exists(&map_table_key_range(range)));
    }

    #[tokio::test]
    async fn test_merge_imms_basic() {
        let table_id = TableId { table_id: 1004 };
        let shared_buffer_items1: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value2")),
            ),
            (
                iterator_test_table_key_of(3),
                HummockValue::put(Bytes::from("value3")),
            ),
        ];
        let epoch = 1;
        let imm1 = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items1.clone()),
            epoch,
            table_id,
        );
        let shared_buffer_items2: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value12")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value22")),
            ),
            (
                iterator_test_table_key_of(3),
                HummockValue::put(Bytes::from("value32")),
            ),
        ];
        let epoch = 2;
        let imm2 = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items2.clone()),
            epoch,
            table_id,
        );

        let shared_buffer_items3: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                iterator_test_table_key_of(1),
                HummockValue::put(Bytes::from("value13")),
            ),
            (
                iterator_test_table_key_of(2),
                HummockValue::put(Bytes::from("value23")),
            ),
            (
                iterator_test_table_key_of(3),
                HummockValue::put(Bytes::from("value33")),
            ),
        ];
        let epoch = 3;
        let imm3 = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items3.clone()),
            epoch,
            table_id,
        );

        let batch_items = [
            shared_buffer_items1,
            shared_buffer_items2,
            shared_buffer_items3,
        ];
        // newer data comes first
        let imms = vec![imm3, imm2, imm1];
        let merged_imm = merge_imms_in_memory(table_id, 0, imms.clone(), None).await;

        // Point lookup
        for (i, items) in batch_items.iter().enumerate() {
            for (key, value) in items {
                assert_eq!(
                    merged_imm
                        .get(
                            TableKey(key.as_slice()),
                            i as u64 + 1,
                            &ReadOptions::default()
                        )
                        .unwrap()
                        .0,
                    value.clone(),
                    "epoch: {}, key: {:?}",
                    i + 1,
                    String::from_utf8(key.clone())
                );
            }
        }
        assert_eq!(
            merged_imm.get(
                TableKey(iterator_test_table_key_of(4).as_slice()),
                1,
                &ReadOptions::default()
            ),
            None
        );
        assert_eq!(
            merged_imm.get(
                TableKey(iterator_test_table_key_of(5).as_slice()),
                1,
                &ReadOptions::default()
            ),
            None
        );

        // Forward iterator
        for snapshot_epoch in 1..=3 {
            let mut iter = merged_imm.clone().into_forward_iter();
            iter.rewind().await.unwrap();
            let mut output = vec![];
            while iter.is_valid() {
                let epoch = iter.key().epoch_with_gap.pure_epoch();
                if snapshot_epoch == epoch {
                    output.push((
                        iter.key().user_key.table_key.to_vec(),
                        iter.value().to_bytes(),
                    ));
                }
                iter.next().await.unwrap();
            }
            assert_eq!(output, batch_items[snapshot_epoch as usize - 1]);
        }

        // Forward and Backward iterator
        {
            let mut iter = merged_imm.clone().into_forward_iter();
            iter.rewind().await.unwrap();
            let mut output = vec![];
            while iter.is_valid() {
                output.push((
                    iter.key().user_key.table_key.to_vec(),
                    iter.value().to_bytes(),
                ));
                iter.next().await.unwrap();
            }

            let mut expected = vec![];
            for key_idx in 0..=2 {
                for epoch in (1..=3).rev() {
                    let item = batch_items[epoch - 1][key_idx].clone();
                    expected.push(item);
                }
            }
            assert_eq!(expected, output);

            let mut backward_iter = merged_imm.clone().into_backward_iter();
            backward_iter.rewind().await.unwrap();
            let mut output = vec![];
            while backward_iter.is_valid() {
                output.push((
                    backward_iter.key().user_key.table_key.to_vec(),
                    backward_iter.value().to_bytes(),
                ));
                backward_iter.next().await.unwrap();
            }
            let mut expected = vec![];
            for key_idx in (0..=2).rev() {
                for epoch in (1..=3).rev() {
                    let item = batch_items[epoch - 1][key_idx].clone();
                    expected.push(item);
                }
            }
            assert_eq!(expected, output);
        }
    }
}
