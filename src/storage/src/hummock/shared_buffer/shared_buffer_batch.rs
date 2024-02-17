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
use std::ops::{Bound, Deref, RangeBounds};
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

/// A shared buffer may contain data from multiple epochs,
/// there are multiple versions for a given key (`table_key`), we put those versions into a vector
/// and sort them in descending order, aka newest to oldest.
#[derive(PartialEq, Debug)]
pub struct SharedBufferVersionedEntry {
    pub(crate) key: TableKey<Bytes>,
    pub(crate) new_values: Vec<(EpochWithGap, HummockValue<Bytes>)>,
}

impl SharedBufferVersionedEntry {
    pub fn new(key: TableKey<Bytes>, new_values: Vec<(EpochWithGap, HummockValue<Bytes>)>) -> Self {
        Self { key, new_values }
    }
}

#[derive(Debug)]
pub(crate) struct SharedBufferBatchInner {
    payload: Vec<SharedBufferVersionedEntry>,
    /// The epochs of the data in batch, sorted in ascending order (old to new)
    epochs: Vec<HummockEpoch>,
    kv_count: usize,
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

        let kv_count = payload.len();
        let epoch_with_gap = EpochWithGap::new(epoch, spill_offset);
        let items = payload
            .into_iter()
            .map(|(k, v)| SharedBufferVersionedEntry::new(k, vec![(epoch_with_gap, v)]))
            .collect_vec();

        let batch_id = SHARED_BUFFER_BATCH_ID_GENERATOR.fetch_add(1, Relaxed);
        SharedBufferBatchInner {
            payload: items,
            epochs: vec![epoch],
            kv_count,
            size,
            _tracker,
            batch_id,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_with_multi_epoch_batches(
        epochs: Vec<HummockEpoch>,
        payload: Vec<SharedBufferVersionedEntry>,
        num_items: usize,
        size: usize,
        imm_id: ImmId,
        tracker: Option<MemoryTracker>,
    ) -> Self {
        assert!(!payload.is_empty());
        debug_assert!(payload.iter().is_sorted_by_key(|entry| &entry.key));
        debug_assert!(payload.iter().all(|entry| entry
            .new_values
            .iter()
            .rev()
            .is_sorted_by_key(|(epoch_with_gap, _)| epoch_with_gap)));
        debug_assert!(!epochs.is_empty());
        debug_assert!(epochs.is_sorted());

        Self {
            payload,
            epochs,
            kv_count: num_items,
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
            .payload
            .binary_search_by(|m| (m.key.as_ref()).cmp(*table_key))
        {
            let item = &self.payload[i];
            assert_eq!(item.key.as_ref(), *table_key);
            // Scan to find the first version <= epoch
            for (e, v) in &item.new_values {
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

impl Deref for SharedBufferBatchInner {
    type Target = [SharedBufferVersionedEntry];

    fn deref(&self) -> &Self::Target {
        self.payload.as_slice()
    }
}

impl PartialEq for SharedBufferBatchInner {
    fn eq(&self, other: &Self) -> bool {
        self.payload == other.payload
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
            instance_id: LocalInstanceId::default(),
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

    pub fn kv_count(&self) -> usize {
        self.inner.kv_count
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

    pub fn get_payload(&self) -> &[SharedBufferVersionedEntry] {
        &self.inner
    }

    #[inline(always)]
    pub fn start_table_key(&self) -> TableKey<&[u8]> {
        TableKey(self.inner.payload.first().expect("non-empty").key.as_ref())
    }

    #[inline(always)]
    pub fn end_table_key(&self) -> TableKey<&[u8]> {
        TableKey(self.inner.payload.last().expect("non-empty").key.as_ref())
    }

    #[inline(always)]
    pub fn raw_largest_key(&self) -> &TableKey<Bytes> {
        &self.inner.payload.last().expect("non-empty").key
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
                .payload
                .binary_search_by(|m| (m.key.as_ref()).cmp(seek_key.as_slice()))
            {
                Ok(idx) => idx,
                Err(idx) => idx,
            };
            if idx >= self.inner.payload.len() {
                break;
            }
            let item = &self.inner.payload[idx];
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
    current_value_idx: i32,
    // The index of the current entry in the payload
    current_idx: usize,
    table_id: TableId,
    _phantom: PhantomData<D>,
}

impl<D: HummockIteratorDirection> SharedBufferBatchIterator<D> {
    pub(crate) fn new(inner: Arc<SharedBufferBatchInner>, table_id: TableId) -> Self {
        Self {
            inner,
            current_idx: 0,
            current_value_idx: 0,
            table_id,
            _phantom: Default::default(),
        }
    }

    /// Return all values of the current key
    pub(crate) fn current_values(&self) -> &Vec<(EpochWithGap, HummockValue<Bytes>)> {
        debug_assert!(self.current_idx < self.inner.len());
        let idx = match D::direction() {
            DirectionEnum::Forward => self.current_idx,
            DirectionEnum::Backward => self.inner.len() - self.current_idx - 1,
        };
        &self.inner[idx].new_values
    }

    fn current_values_len(&self) -> i32 {
        if self.current_idx < self.inner.len() {
            self.current_values().len() as i32
        } else {
            0
        }
    }

    pub(crate) fn current_item(&self) -> (&TableKey<Bytes>, &(EpochWithGap, HummockValue<Bytes>)) {
        let (idx, value_idx) = match D::direction() {
            DirectionEnum::Forward => (self.current_idx, self.current_value_idx),
            DirectionEnum::Backward => (
                self.inner.len() - self.current_idx - 1,
                self.current_value_idx,
            ),
        };
        let cur_entry = &self.inner[idx];
        (&cur_entry.key, &cur_entry.new_values[value_idx as usize])
    }
}

impl SharedBufferBatchIterator<Forward> {
    pub(crate) fn advance_to_next_key(&mut self) {
        assert_eq!(self.current_value_idx, 0);
        self.current_idx += 1;
    }

    pub(crate) fn current_key_entry(&self) -> &SharedBufferVersionedEntry {
        assert!(self.is_valid(), "iterator is not valid");
        assert_eq!(self.current_value_idx, 0);
        &self.inner.payload[self.current_idx]
    }
}

impl<D: HummockIteratorDirection> HummockIterator for SharedBufferBatchIterator<D> {
    type Direction = D;

    async fn next(&mut self) -> HummockResult<()> {
        assert!(self.is_valid());
        match D::direction() {
            DirectionEnum::Forward => {
                // If the current key has more versions, we need to advance the value index
                if self.current_value_idx + 1 < self.current_values_len() {
                    self.current_value_idx += 1;
                } else {
                    self.current_idx += 1;
                    self.current_value_idx = 0;
                }
            }
            DirectionEnum::Backward => {
                if self.current_value_idx > 0 {
                    self.current_value_idx -= 1;
                } else {
                    self.current_idx += 1;
                    self.current_value_idx = self.current_values_len() - 1;
                }
            }
        }
        Ok(())
    }

    fn key(&self) -> FullKey<&[u8]> {
        let (key, (epoch_with_gap, _)) = self.current_item();
        FullKey::new_with_gap_epoch(self.table_id, TableKey(key), *epoch_with_gap)
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let (_, (_, value)) = self.current_item();
        value.as_slice()
    }

    fn is_valid(&self) -> bool {
        if self.current_idx >= self.inner.len() {
            return false;
        }
        self.current_value_idx >= 0 && self.current_value_idx < self.current_values().len() as i32
    }

    async fn rewind(&mut self) -> HummockResult<()> {
        self.current_idx = 0;

        match D::direction() {
            DirectionEnum::Forward => {
                self.current_value_idx = 0;
            }
            DirectionEnum::Backward => {
                self.current_value_idx = self.current_values_len() - 1;
            }
        }
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        debug_assert_eq!(key.user_key.table_id, self.table_id);
        // Perform binary search on table key because the items in SharedBufferBatch is ordered
        // by table key.
        let partition_point = self
            .inner
            .binary_search_by(|probe| probe.key.as_ref().cmp(*key.user_key.table_key));
        let seek_key_epoch = key.epoch_with_gap;
        match D::direction() {
            DirectionEnum::Forward => match partition_point {
                Ok(i) => {
                    self.current_idx = i;
                    // seek to the first version that is <= the seek key epoch
                    let mut idx: i32 = 0;
                    for (epoch_with_gap, _) in self.current_values() {
                        if epoch_with_gap <= &seek_key_epoch {
                            break;
                        }
                        idx += 1;
                    }

                    // Move onto the next key for forward iteration if seek key epoch is smaller
                    // than all versions
                    if idx >= self.current_values().len() as i32 {
                        self.current_idx += 1;
                        self.current_value_idx = 0;
                    } else {
                        self.current_value_idx = idx;
                    }
                }
                Err(i) => {
                    self.current_idx = i;
                    self.current_value_idx = 0;
                }
            },
            DirectionEnum::Backward => {
                match partition_point {
                    Ok(i) => {
                        self.current_idx = self.inner.len() - i - 1;
                        // seek from back to the first version that is >= seek_key_epoch
                        let values = self.current_values();
                        let mut idx: i32 = (values.len() - 1) as i32;
                        for (epoch_with_gap, _) in values.iter().rev() {
                            if epoch_with_gap >= &seek_key_epoch {
                                break;
                            }
                            idx -= 1;
                        }

                        if idx < 0 {
                            self.current_idx += 1;
                            self.current_value_idx = self.current_values_len() - 1;
                        } else {
                            self.current_value_idx = idx;
                        }
                    }
                    // Seek to one item before the seek partition_point:
                    // If i == 0, the iterator will be invalidated with self.current_idx ==
                    // self.inner.len().
                    Err(i) => {
                        self.current_idx = self.inner.len() - i;
                        self.current_value_idx = self.current_values_len() - 1;
                    }
                }
            }
        }
        Ok(())
    }

    fn collect_local_statistic(&self, _stats: &mut crate::monitor::StoreLocalStatistic) {}

    fn debug_print(&self) -> String {
        format!(
            "SharedBuffer: [ min_epoch: {}, max_epoch: {}, current key: {:?} ],",
            self.inner.epochs.first().cloned().unwrap_or_default(),
            self.inner.epochs.last().cloned().unwrap_or_default(),
            self.key(),
        )
    }
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

        // BACKWARD: Seek to 2nd key with future epoch, expect first item to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch + 1).to_ref())
            .await
            .unwrap();
        assert!(iter.is_valid());
        let item = shared_buffer_items.first().unwrap();
        assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());

        // BACKWARD: Seek to 2nd key with old epoch, expect first two item to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch - 1).to_ref())
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
            output.reverse();
            assert_eq!(expected, output);
        }
    }
}
