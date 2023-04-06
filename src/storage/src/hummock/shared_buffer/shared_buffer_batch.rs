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

use std::cmp::Ordering;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::ops::{Deref, RangeBounds};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_hummock_sdk::key::{FullKey, TableKey, TableKeyRange, UserKey};

use crate::hummock::event_handler::LocalInstanceId;
use crate::hummock::iterator::{
    Backward, DeleteRangeIterator, DirectionEnum, Forward, HummockIterator,
    HummockIteratorDirection,
};
use crate::hummock::store::memtable::ImmId;
use crate::hummock::utils::{range_overlap, MemoryTracker};
use crate::hummock::value::HummockValue;
use crate::hummock::{DeleteRangeTombstone, HummockEpoch, HummockResult};
use crate::storage_value::StorageValue;

/// The key is `table_key`, which does not contain table id or epoch.
pub(crate) type SharedBufferItem = (Bytes, HummockValue<Bytes>);
pub type SharedBufferBatchId = u64;

/// A shared buffer may contain data from multiple epochs,
/// there are multiple versions for a given key (`table_key`), we put those versions into a vector
/// and sort them in descending order, aka newest to oldest.
pub type SharedBufferVersionedEntry = (Bytes, Vec<(HummockEpoch, HummockValue<Bytes>)>);

#[derive(Debug)]
pub(crate) struct SharedBufferBatchInner {
    payload: Vec<SharedBufferVersionedEntry>,
    /// The list of imm ids that are merged into this batch
    /// This field is immutable
    imm_ids: Vec<ImmId>,
    /// The epochs of the data in batch, sorted in ascending order (old to new)
    epochs: Vec<HummockEpoch>,
    range_tombstone_list: Vec<DeleteRangeTombstone>,
    largest_table_key: Vec<u8>,
    smallest_table_key: Vec<u8>,
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
        payload: Vec<SharedBufferItem>,
        range_tombstone_list: Vec<DeleteRangeTombstone>,
        size: usize,
        _tracker: Option<MemoryTracker>,
    ) -> Self {
        let (smallest_empty, mut smallest_table_key, mut largest_table_key, range_tombstones) =
            Self::get_table_key_ends(range_tombstone_list);

        if let Some(item) = payload.last() {
            if item.0.gt(&largest_table_key) {
                largest_table_key.clear();
                largest_table_key.extend_from_slice(item.0.as_ref());
            }
        }
        if let Some(item) = payload.first() {
            if smallest_empty || item.0.lt(&smallest_table_key) {
                smallest_table_key.clear();
                smallest_table_key.extend_from_slice(item.0.as_ref());
            }
        }
        let kv_count = payload.len();
        let items = payload
            .into_iter()
            .map(|(k, v)| (k, vec![(epoch, v)]))
            .collect_vec();

        let batch_id = SHARED_BUFFER_BATCH_ID_GENERATOR.fetch_add(1, Relaxed);
        SharedBufferBatchInner {
            payload: items,
            imm_ids: vec![batch_id],
            epochs: vec![epoch],
            range_tombstone_list: range_tombstones,
            kv_count,
            size,
            largest_table_key,
            smallest_table_key,
            _tracker,
            batch_id,
        }
    }

    pub(crate) fn new_with_multi_epoch_batches(
        epochs: Vec<HummockEpoch>,
        payload: Vec<SharedBufferVersionedEntry>,
        num_items: usize,
        imm_ids: Vec<ImmId>,
        range_tombstone_list: Vec<DeleteRangeTombstone>,
        size: usize,
        tracker: Option<MemoryTracker>,
    ) -> Self {
        debug_assert!(!imm_ids.is_empty());
        debug_assert!(!epochs.is_empty());
        debug_assert!(epochs.is_sorted());

        let (smallest_empty, mut smallest_table_key, mut largest_table_key, range_tombstones) =
            Self::get_table_key_ends(range_tombstone_list);

        if let Some(item) = payload.last() {
            if item.0.gt(&largest_table_key) {
                largest_table_key.clear();
                largest_table_key.extend_from_slice(item.0.as_ref());
            }
        }
        if let Some(item) = payload.first() {
            if smallest_empty || item.0.lt(&smallest_table_key) {
                smallest_table_key.clear();
                smallest_table_key.extend_from_slice(item.0.as_ref());
            }
        }

        let max_imm_id = *imm_ids.iter().max().unwrap();
        Self {
            payload,
            epochs,
            imm_ids,
            range_tombstone_list: range_tombstones,
            largest_table_key,
            smallest_table_key,
            kv_count: num_items,
            size,
            _tracker: tracker,
            batch_id: max_imm_id,
        }
    }

    fn get_table_key_ends(
        mut range_tombstone_list: Vec<DeleteRangeTombstone>,
    ) -> (bool, Vec<u8>, Vec<u8>, Vec<DeleteRangeTombstone>) {
        let mut largest_table_key = vec![];
        let mut smallest_table_key = vec![];
        let mut smallest_empty = true;
        if !range_tombstone_list.is_empty() {
            range_tombstone_list.sort();
            let mut range_tombstones: Vec<DeleteRangeTombstone> = vec![];
            for tombstone in range_tombstone_list {
                // Although `end_user_key` of tombstone is exclusive, we still use it as a boundary
                // of `SharedBufferBatch` because it just expands an useless query
                // and does not affect correctness.
                if largest_table_key.lt(&tombstone.end_user_key.table_key.0) {
                    largest_table_key.clear();
                    largest_table_key.extend_from_slice(&tombstone.end_user_key.table_key.0);
                }
                if smallest_empty || smallest_table_key.gt(&tombstone.start_user_key.table_key.0) {
                    smallest_table_key.clear();
                    smallest_table_key.extend_from_slice(&tombstone.start_user_key.table_key.0);
                    smallest_empty = false;
                }
                if let Some(last) = range_tombstones.last_mut() {
                    if last.end_user_key.gt(&tombstone.start_user_key) {
                        if last.end_user_key.lt(&tombstone.end_user_key) {
                            last.end_user_key = tombstone.end_user_key;
                        }
                        continue;
                    }
                }
                range_tombstones.push(tombstone);
            }
            range_tombstone_list = range_tombstones;
        }
        (
            smallest_empty,
            smallest_table_key,
            largest_table_key,
            range_tombstone_list,
        )
    }

    // If the key is deleted by a epoch greater than the read epoch, return None
    fn get_value(
        &self,
        table_id: TableId,
        table_key: TableKey<&[u8]>,
        read_epoch: HummockEpoch,
    ) -> Option<HummockValue<Bytes>> {
        let full_key = FullKey::new(table_id, table_key, read_epoch);
        let delete_epoch = self.get_delete_range_epoch(&full_key);

        // Perform binary search on table key to find the corresponding entry
        match self.payload.binary_search_by(|m| (m.0[..]).cmp(*table_key)) {
            Ok(i) => {
                let item = &self.payload[i];
                assert_eq!(item.0.as_ref(), *table_key);
                // Scan to find the first version <= epoch
                for (e, v) in &item.1 {
                    // skip invisible versions
                    if read_epoch < *e {
                        continue;
                    }
                    return match delete_epoch {
                        Some(del_epoch) => {
                            if *e > del_epoch {
                                Some(v.clone())
                            } else {
                                // key has been deleted by del_epoch
                                Some(HummockValue::Delete)
                            }
                        }
                        None => Some(v.clone()),
                    };
                }
                // cannot find a visible version
                None
            }
            Err(_) => None,
        }
    }

    /// Get the latest epoch that deleted the given key
    fn get_delete_range_epoch(&self, full_key: &FullKey<&[u8]>) -> Option<HummockEpoch> {
        if self.range_tombstone_list.is_empty() {
            return None;
        }
        let watermark = full_key.epoch;
        let mut idx = self
            .range_tombstone_list
            .partition_point(|tombstone| tombstone.end_user_key.as_ref().le(&full_key.user_key));
        if idx >= self.range_tombstone_list.len() {
            return None;
        }
        let mut epoch = None;
        while idx < self.range_tombstone_list.len()
            && self.range_tombstone_list[idx]
                .start_user_key
                .as_ref()
                .le(&full_key.user_key)
        {
            let sequence = self.range_tombstone_list[idx].sequence;
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
                sorted_items,
                vec![],
                size,
                None,
            )),
            table_id,
            instance_id: LocalInstanceId::default(),
        }
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
                &self.end_table_key(),
            )
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    pub fn is_merged_imm(&self) -> bool {
        self.inner.epochs.len() > 1
    }

    pub fn min_epoch(&self) -> HummockEpoch {
        *self.inner.epochs.first().unwrap()
    }

    pub fn max_epoch(&self) -> HummockEpoch {
        *self.inner.epochs.last().unwrap()
    }

    pub fn get_imm_ids(&self) -> &Vec<ImmId> {
        debug_assert!(!self.inner.imm_ids.is_empty());
        &self.inner.imm_ids
    }

    pub fn kv_count(&self) -> usize {
        self.inner.kv_count
    }

    /// Return `None` if the key doesn't exist
    /// Return `HummockValue::Delete` if the key has been deleted by some epoch >= `read_epoch`
    pub fn get(
        &self,
        table_key: TableKey<&[u8]>,
        read_epoch: HummockEpoch,
    ) -> Option<HummockValue<Bytes>> {
        self.inner.get_value(self.table_id, table_key, read_epoch)
    }

    pub fn get_delete_range_epoch(&self, full_key: &FullKey<&[u8]>) -> Option<HummockEpoch> {
        self.inner.get_delete_range_epoch(full_key)
    }

    pub fn range_exists(&self, table_key_range: &TableKeyRange) -> bool {
        self.inner
            .binary_search_by(|m| {
                let key = &m.0;
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

    pub fn delete_range_iter(&self) -> SharedBufferDeleteRangeIterator {
        SharedBufferDeleteRangeIterator::new(self.inner.clone())
    }

    pub fn get_payload(&self) -> &[SharedBufferVersionedEntry] {
        &self.inner
    }

    #[inline(always)]
    pub fn start_table_key(&self) -> TableKey<&[u8]> {
        TableKey(&self.inner.smallest_table_key)
    }

    #[inline(always)]
    pub fn end_table_key(&self) -> TableKey<&[u8]> {
        TableKey(&self.inner.largest_table_key)
    }

    /// return inclusive left endpoint, which means that all data in this batch should be larger or
    /// equal than this key.
    pub fn start_user_key(&self) -> UserKey<&[u8]> {
        UserKey::new(self.table_id, self.start_table_key())
    }

    #[inline(always)]
    pub fn has_range_tombstone(&self) -> bool {
        !self.inner.range_tombstone_list.is_empty()
    }

    /// return inclusive right endpoint, which means that all data in this batch should be smaller
    /// or equal than this key.
    pub fn end_user_key(&self) -> UserKey<&[u8]> {
        UserKey::new(self.table_id, self.end_table_key())
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
        kv_pairs: Vec<(Bytes, StorageValue)>,
    ) -> Vec<SharedBufferItem> {
        kv_pairs
            .into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect()
    }

    pub fn build_shared_buffer_batch(
        epoch: HummockEpoch,
        sorted_items: Vec<SharedBufferItem>,
        size: usize,
        delete_ranges: Vec<(Bytes, Bytes)>,
        table_id: TableId,
        instance_id: Option<LocalInstanceId>,
        tracker: Option<MemoryTracker>,
    ) -> Self {
        let delete_range_tombstones = delete_ranges
            .into_iter()
            .map(|(start_table_key, end_table_key)| {
                DeleteRangeTombstone::new(
                    table_id,
                    start_table_key.to_vec(),
                    end_table_key.to_vec(),
                    epoch,
                )
            })
            .collect_vec();
        #[cfg(test)]
        {
            Self::check_tombstone_prefix(table_id, &delete_range_tombstones);
        }
        let inner = SharedBufferBatchInner::new(
            epoch,
            sorted_items,
            delete_range_tombstones,
            size,
            tracker,
        );
        SharedBufferBatch {
            inner: Arc::new(inner),
            table_id,
            instance_id: instance_id.unwrap_or(LocalInstanceId::default()),
        }
    }

    pub fn get_delete_range_tombstones(&self) -> Vec<DeleteRangeTombstone> {
        self.inner.range_tombstone_list.clone()
    }

    #[cfg(test)]
    fn check_tombstone_prefix(table_id: TableId, tombstones: &[DeleteRangeTombstone]) {
        for tombstone in tombstones {
            assert_eq!(
                tombstone.start_user_key.table_id, table_id,
                "delete range tombstone in a shared buffer batch must begin with the same table id"
            );
            assert_eq!(
                tombstone.end_user_key.table_id, table_id,
                "delete range tombstone in a shared buffer batch must begin with the same table id"
            );
        }
    }
}

/// Iterate all the items in the shared buffer batch
/// If there are multiple versions of a key, the iterator will return all versions
pub struct SharedBufferBatchIterator<D: HummockIteratorDirection> {
    inner: Arc<SharedBufferBatchInner>,
    current_version_idx: i32,
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
            current_version_idx: 0,
            table_id,
            _phantom: Default::default(),
        }
    }

    /// Return all values of the current key
    pub(crate) fn current_versions(&self) -> &Vec<(HummockEpoch, HummockValue<Bytes>)> {
        debug_assert!(self.current_idx < self.inner.len());
        let idx = match D::direction() {
            DirectionEnum::Forward => self.current_idx,
            DirectionEnum::Backward => self.inner.len() - self.current_idx - 1,
        };
        &self.inner.get(idx).unwrap().1
    }

    fn current_versions_len(&self) -> i32 {
        if self.current_idx < self.inner.len() {
            self.current_versions().len() as i32
        } else {
            0
        }
    }
}

/// trait iterface provided to the `MergedIterator` to avoid memory copy
pub trait SharedBufferBatchIterCurrent {
    fn current_item(&self) -> (&Bytes, &(HummockEpoch, HummockValue<Bytes>));
}

impl<D: HummockIteratorDirection> SharedBufferBatchIterCurrent for SharedBufferBatchIterator<D> {
    fn current_item(&self) -> (&Bytes, &(HummockEpoch, HummockValue<Bytes>)) {
        assert!(self.is_valid(), "iterator is not valid");
        let (idx, version_idx) = match D::direction() {
            DirectionEnum::Forward => (self.current_idx, self.current_version_idx),
            DirectionEnum::Backward => (
                self.inner.len() - self.current_idx - 1,
                self.current_version_idx,
            ),
        };
        let cur_entry = self.inner.get(idx).unwrap();
        let value = &cur_entry.1[version_idx as usize];
        (&cur_entry.0, value)
    }
}

impl<D: HummockIteratorDirection> HummockIterator for SharedBufferBatchIterator<D> {
    type Direction = D;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            assert!(self.is_valid());
            match D::direction() {
                DirectionEnum::Forward => {
                    // If the current key has more versions, we need to advance the value index
                    if self.current_version_idx + 1 < self.current_versions_len() {
                        self.current_version_idx += 1;
                    } else {
                        self.current_idx += 1;
                        self.current_version_idx = 0;
                    }
                }
                DirectionEnum::Backward => {
                    if self.current_version_idx > 0 {
                        self.current_version_idx -= 1;
                    } else {
                        self.current_idx += 1;
                        self.current_version_idx = self.current_versions_len() - 1;
                    }
                }
            }
            Ok(())
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        let (key, (epoch, _)) = self.current_item();
        FullKey::new(self.table_id, TableKey(key), *epoch)
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let (_, (_, value)) = self.current_item();
        value.as_slice()
    }

    fn is_valid(&self) -> bool {
        if self.current_idx >= self.inner.len() {
            return false;
        }
        self.current_version_idx >= 0
            && self.current_version_idx < self.current_versions().len() as i32
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            self.current_idx = 0;

            match D::direction() {
                DirectionEnum::Forward => {
                    self.current_version_idx = 0;
                }
                DirectionEnum::Backward => {
                    self.current_version_idx = self.current_versions_len() - 1;
                }
            }
            Ok(())
        }
    }

    fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> Self::SeekFuture<'a> {
        async move {
            debug_assert_eq!(key.user_key.table_id, self.table_id);
            // Perform binary search on table key because the items in SharedBufferBatch is ordered
            // by table key.
            let partition_point = self
                .inner
                .binary_search_by(|probe| probe.0[..].cmp(*key.user_key.table_key));
            let seek_key_epoch = key.epoch;
            match D::direction() {
                DirectionEnum::Forward => match partition_point {
                    Ok(i) => {
                        self.current_idx = i;
                        // seek to the first version that is <= the seek key epoch
                        let mut idx: i32 = 0;
                        for (epoch, _) in self.current_versions() {
                            if *epoch <= seek_key_epoch {
                                break;
                            }
                            idx += 1;
                        }

                        // Move onto the next key for forward iteration if seek key epoch is smaller
                        // than all versions
                        if idx >= self.current_versions().len() as i32 {
                            self.current_idx += 1;
                            self.current_version_idx = 0;
                        } else {
                            self.current_version_idx = idx;
                        }
                    }
                    Err(i) => {
                        self.current_idx = i;
                        self.current_version_idx = 0;
                    }
                },
                DirectionEnum::Backward => {
                    match partition_point {
                        Ok(i) => {
                            self.current_idx = self.inner.len() - i - 1;
                            // seek from back to the first version that is >= seek_key_epoch
                            let values = self.current_versions();
                            let mut idx: i32 = (values.len() - 1) as i32;
                            for (epoch, _) in values.iter().rev() {
                                if *epoch >= seek_key_epoch {
                                    break;
                                }
                                idx -= 1;
                            }

                            if idx < 0 {
                                self.current_idx += 1;
                                self.current_version_idx = self.current_versions_len() - 1;
                            } else {
                                self.current_version_idx = idx;
                            }
                        }
                        // Seek to one item before the seek partition_point:
                        // If i == 0, the iterator will be invalidated with self.current_idx ==
                        // self.inner.len().
                        Err(i) => {
                            self.current_idx = self.inner.len() - i;
                            self.current_version_idx = self.current_versions_len() - 1;
                        }
                    }
                }
            }
            Ok(())
        }
    }

    fn collect_local_statistic(&self, _stats: &mut crate::monitor::StoreLocalStatistic) {}
}

pub struct SharedBufferDeleteRangeIterator {
    inner: Arc<SharedBufferBatchInner>,
    current_idx: usize,
}

impl SharedBufferDeleteRangeIterator {
    pub(crate) fn new(inner: Arc<SharedBufferBatchInner>) -> Self {
        Self {
            inner,
            current_idx: 0,
        }
    }
}

impl DeleteRangeIterator for SharedBufferDeleteRangeIterator {
    fn start_user_key(&self) -> UserKey<&[u8]> {
        self.inner.range_tombstone_list[self.current_idx]
            .start_user_key
            .as_ref()
    }

    fn end_user_key(&self) -> UserKey<&[u8]> {
        self.inner.range_tombstone_list[self.current_idx]
            .end_user_key
            .as_ref()
    }

    fn current_epoch(&self) -> HummockEpoch {
        self.inner.range_tombstone_list[self.current_idx].sequence
    }

    fn next(&mut self) {
        self.current_idx += 1;
    }

    fn rewind(&mut self) {
        self.current_idx = 0;
    }

    fn seek<'a>(&'a mut self, target_user_key: UserKey<&'a [u8]>) {
        self.current_idx = self
            .inner
            .range_tombstone_list
            .partition_point(|tombstone| tombstone.end_user_key.as_ref().le(&target_user_key));
    }

    fn is_valid(&self) -> bool {
        self.current_idx < self.inner.range_tombstone_list.len()
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
                shared_buffer_batch.get(TableKey(k.as_slice()), epoch),
                Some(v.clone())
            );
        }
        assert_eq!(
            shared_buffer_batch.get(TableKey(iterator_test_table_key_of(3).as_slice()), epoch),
            None
        );
        assert_eq!(
            shared_buffer_batch.get(TableKey(iterator_test_table_key_of(4).as_slice()), epoch),
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

        let batch = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            vec![],
            1,
            vec![
                (Bytes::from("a"), Bytes::from("c")),
                (Bytes::from("b"), Bytes::from("d")),
            ],
            TableId::new(0),
            None,
            None,
        );
        assert_eq!(batch.start_table_key().as_ref(), "a".as_bytes());
        assert_eq!(batch.end_table_key().as_ref(), "d".as_bytes());
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
    async fn test_shared_buffer_batch_delete_range() {
        let epoch = 1;
        let delete_ranges = vec![
            (Bytes::from(b"aaa".to_vec()), Bytes::from(b"bbb".to_vec())),
            (Bytes::from(b"ccc".to_vec()), Bytes::from(b"ddd".to_vec())),
            (Bytes::from(b"ddd".to_vec()), Bytes::from(b"eee".to_vec())),
        ];
        let shared_buffer_batch = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            vec![],
            0,
            delete_ranges,
            Default::default(),
            None,
            None,
        );
        assert_eq!(
            Some(epoch),
            shared_buffer_batch.get_delete_range_epoch(&FullKey::new(
                Default::default(),
                TableKey(b"aaa"),
                epoch
            ))
        );
        assert_eq!(
            None,
            shared_buffer_batch.get_delete_range_epoch(&FullKey::new(
                Default::default(),
                TableKey(b"bbb"),
                epoch
            ))
        );
        assert_eq!(
            Some(epoch),
            shared_buffer_batch.get_delete_range_epoch(&FullKey::new(
                Default::default(),
                TableKey(b"ddd"),
                epoch
            ))
        );
        assert_eq!(
            None,
            shared_buffer_batch.get_delete_range_epoch(&FullKey::new(
                Default::default(),
                TableKey(b"eee"),
                epoch
            ))
        );
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

        let batch_items = vec![
            shared_buffer_items1,
            shared_buffer_items2,
            shared_buffer_items3,
        ];
        // newer data comes first
        let imms = vec![imm3, imm2, imm1];
        let merged_imm = merge_imms_in_memory(table_id, 0, imms.clone(), None)
            .await
            .unwrap();

        // Point lookup
        for (i, items) in batch_items.iter().enumerate() {
            for (key, value) in items {
                assert_eq!(
                    merged_imm.get(TableKey(key.as_slice()), i as u64 + 1),
                    Some(value.clone()),
                    "epoch: {}, key: {:?}",
                    i + 1,
                    String::from_utf8(key.clone())
                );
            }
        }
        assert_eq!(
            merged_imm.get(TableKey(iterator_test_table_key_of(4).as_slice()), 1),
            None
        );
        assert_eq!(
            merged_imm.get(TableKey(iterator_test_table_key_of(5).as_slice()), 1),
            None
        );

        // Forward iterator
        for snapshot_epoch in 1..=3 {
            let mut iter = merged_imm.clone().into_forward_iter();
            iter.rewind().await.unwrap();
            let mut output = vec![];
            while iter.is_valid() {
                let epoch = iter.key().epoch;
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

    fn test_table_key_of(idx: usize) -> Vec<u8> {
        format!("{:03}", idx).as_bytes().to_vec()
    }

    #[tokio::test]
    async fn test_merge_imms_delete_range() {
        let table_id = TableId { table_id: 1004 };
        let epoch = 1;
        let delete_ranges = vec![
            (Bytes::from(b"111".to_vec()), Bytes::from(b"222".to_vec())),
            (Bytes::from(b"555".to_vec()), Bytes::from(b"777".to_vec())),
        ];
        let shared_buffer_items1: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                test_table_key_of(111),
                HummockValue::put(Bytes::from("value1")),
            ),
            (
                test_table_key_of(222),
                HummockValue::put(Bytes::from("value2")),
            ),
            (
                test_table_key_of(333),
                HummockValue::put(Bytes::from("value3")),
            ),
            (
                test_table_key_of(555),
                HummockValue::put(Bytes::from("value5")),
            ),
            (
                test_table_key_of(666),
                HummockValue::put(Bytes::from("value6")),
            ),
            (
                test_table_key_of(888),
                HummockValue::put(Bytes::from("value8")),
            ),
        ];
        let sorted_items1 = transform_shared_buffer(shared_buffer_items1);
        let size = SharedBufferBatch::measure_batch_size(&sorted_items1);
        let imm1 = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            sorted_items1,
            size,
            delete_ranges,
            table_id,
            None,
            None,
        );

        let epoch = 2;
        let delete_ranges = vec![
            (Bytes::from(b"444".to_vec()), Bytes::from(b"555".to_vec())),
            (Bytes::from(b"888".to_vec()), Bytes::from(b"999".to_vec())),
        ];
        let shared_buffer_items2: Vec<(Vec<u8>, HummockValue<Bytes>)> = vec![
            (
                test_table_key_of(111),
                HummockValue::put(Bytes::from("value12")),
            ),
            (
                test_table_key_of(222),
                HummockValue::put(Bytes::from("value22")),
            ),
            (
                test_table_key_of(333),
                HummockValue::put(Bytes::from("value32")),
            ),
            (
                test_table_key_of(444),
                HummockValue::put(Bytes::from("value42")),
            ),
            (
                test_table_key_of(555),
                HummockValue::put(Bytes::from("value52")),
            ),
        ];
        let sorted_items2 = transform_shared_buffer(shared_buffer_items2);
        let size = SharedBufferBatch::measure_batch_size(&sorted_items2);
        let imm2 = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            sorted_items2,
            size,
            delete_ranges,
            table_id,
            None,
            None,
        );

        let imms = vec![imm2, imm1];
        let merged_imm = merge_imms_in_memory(table_id, 0, imms, None).await.unwrap();

        assert_eq!(
            Some(1),
            merged_imm.get_delete_range_epoch(&FullKey::new(table_id, TableKey(b"111"), 2))
        );
        assert_eq!(
            Some(1),
            merged_imm.get_delete_range_epoch(&FullKey::new(table_id, TableKey(b"555"), 2))
        );
        assert_eq!(
            Some(2),
            merged_imm.get_delete_range_epoch(&FullKey::new(table_id, TableKey(b"888"), 2))
        );

        assert_eq!(
            None,
            merged_imm.get_delete_range_epoch(&FullKey::new(table_id, TableKey(b"888"), 1))
        );

        assert_eq!(
            Some(HummockValue::put(Bytes::from("value12"))),
            merged_imm.get(TableKey(b"111"), 2)
        );

        // 555 is deleted in epoch=1
        assert_eq!(
            Some(HummockValue::Delete),
            merged_imm.get(TableKey(b"555"), 1)
        );

        // 555 is inserted again in epoch=2
        assert_eq!(
            Some(HummockValue::put(Bytes::from("value52"))),
            merged_imm.get(TableKey(b"555"), 2)
        );

        // "666" is deleted in epoch=1 and isn't inserted in later epochs
        assert_eq!(
            Some(HummockValue::Delete),
            merged_imm.get(TableKey(b"666"), 2)
        );
        // "888" is deleted in epoch=2
        assert_eq!(
            Some(HummockValue::Delete),
            merged_imm.get(TableKey(b"888"), 2)
        );

        // 888 exists in the snapshot of epoch=1
        assert_eq!(
            Some(HummockValue::put(Bytes::from("value8"))),
            merged_imm.get(TableKey(b"888"), 1)
        );
    }
}
