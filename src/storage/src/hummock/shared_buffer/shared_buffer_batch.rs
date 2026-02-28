// Copyright 2022 RisingWave Labs
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
use std::marker::PhantomData;
use std::mem::size_of_val;
use std::ops::Bound::Included;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use risingwave_common::catalog::TableId;
use risingwave_common::metrics::LabelGuardedIntGauge;
use risingwave_hummock_sdk::EpochWithGap;
use risingwave_hummock_sdk::key::{FullKey, TableKey, TableKeyRange, UserKey};

use crate::hummock::iterator::{
    Backward, DirectionEnum, Forward, HummockIterator, HummockIteratorDirection, ValueMeta,
};
use crate::hummock::shared_buffer::TableMemoryMetrics;
use crate::hummock::utils::range_overlap;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockEpoch, HummockResult};
use crate::store::ReadOptions;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SharedBufferValue<T> {
    Insert(T),
    Update(T),
    Delete,
}

impl<T> SharedBufferValue<T> {
    fn to_ref(&self) -> SharedBufferValue<&T> {
        match self {
            SharedBufferValue::Insert(val) => SharedBufferValue::Insert(val),
            SharedBufferValue::Update(val) => SharedBufferValue::Update(val),
            SharedBufferValue::Delete => SharedBufferValue::Delete,
        }
    }
}

impl<T> From<SharedBufferValue<T>> for HummockValue<T> {
    fn from(val: SharedBufferValue<T>) -> HummockValue<T> {
        match val {
            SharedBufferValue::Insert(val) | SharedBufferValue::Update(val) => {
                HummockValue::Put(val)
            }
            SharedBufferValue::Delete => HummockValue::Delete,
        }
    }
}

impl<'a, T: AsRef<[u8]>> SharedBufferValue<&'a T> {
    pub(crate) fn to_slice(self) -> SharedBufferValue<&'a [u8]> {
        match self {
            SharedBufferValue::Insert(val) => SharedBufferValue::Insert(val.as_ref()),
            SharedBufferValue::Update(val) => SharedBufferValue::Update(val.as_ref()),
            SharedBufferValue::Delete => SharedBufferValue::Delete,
        }
    }
}

/// The key is `table_key`, which does not contain table id or epoch.
pub(crate) type SharedBufferItem = (TableKey<Bytes>, SharedBufferValue<Bytes>);
pub type SharedBufferBatchId = u64;

#[derive(Debug, PartialEq)]
pub(crate) struct SharedBufferEntry {
    pub(crate) key: TableKey<Bytes>,
    pub(crate) value: SharedBufferValue<Bytes>,
}

#[derive(Debug)]
pub(crate) struct SharedBufferBatchOldValues {
    /// Store the old values. If some, the length should be the same as `entries`. It contains empty `Bytes` when the
    /// corresponding `value` is `Insert`, and contains the old values of `Update` and `Delete`.
    values: Vec<Bytes>,
    pub size: usize,
    pub global_old_value_size: LabelGuardedIntGauge,
}

impl Drop for SharedBufferBatchOldValues {
    fn drop(&mut self) {
        self.global_old_value_size.sub(self.size as _);
    }
}

impl SharedBufferBatchOldValues {
    pub(crate) fn new(
        values: Vec<Bytes>,
        size: usize,
        global_old_value_size: LabelGuardedIntGauge,
    ) -> Self {
        global_old_value_size.add(size as _);
        Self {
            values,
            size,
            global_old_value_size,
        }
    }

    pub(crate) fn for_test(values: Vec<Bytes>, size: usize) -> Self {
        Self::new(values, size, LabelGuardedIntGauge::test_int_gauge::<1>())
    }
}

#[derive(Debug)]
pub(crate) struct SharedBufferBatchInner {
    entries: Vec<SharedBufferEntry>,
    old_values: Option<SharedBufferBatchOldValues>,
    epoch_with_gap: EpochWithGap,
    /// Total size of all key-value items (excluding the `epoch` of value versions)
    size: usize,
    per_table_tracker: Arc<TableMemoryMetrics>,
    /// For a batch created from multiple batches, this will be
    /// the largest batch id among input batches
    batch_id: SharedBufferBatchId,
}

impl SharedBufferBatchInner {
    pub(crate) fn new(
        epoch: HummockEpoch,
        spill_offset: u16,
        payload: Vec<SharedBufferItem>,
        old_values: Option<SharedBufferBatchOldValues>,
        size: usize,
        table_metrics: Arc<TableMemoryMetrics>,
    ) -> Self {
        assert!(!payload.is_empty());
        debug_assert!(payload.iter().is_sorted_by_key(|(key, _)| key));
        if let Some(old_values) = &old_values {
            assert_eq!(old_values.values.len(), payload.len());
        }

        let epoch_with_gap = EpochWithGap::new(epoch, spill_offset);
        let entries = payload
            .into_iter()
            .map(|(key, value)| SharedBufferEntry { key, value })
            .collect();

        let batch_id = SHARED_BUFFER_BATCH_ID_GENERATOR.fetch_add(1, Relaxed);

        table_metrics.inc_imm(size);

        SharedBufferBatchInner {
            entries,
            old_values,
            epoch_with_gap,
            size,
            per_table_tracker: table_metrics,
            batch_id,
        }
    }

    /// Return `None` if cannot find a visible version
    /// Return `HummockValue::Delete` if the key has been deleted by some epoch <= `read_epoch`
    fn get_value<'a>(
        &'a self,
        table_key: TableKey<&[u8]>,
        read_epoch: HummockEpoch,
    ) -> Option<(HummockValue<&'a Bytes>, EpochWithGap)> {
        // Perform binary search on table key to find the corresponding entry
        if let Ok(i) = self
            .entries
            .binary_search_by(|m| (m.key.as_ref()).cmp(*table_key))
        {
            let SharedBufferEntry { key, value } = &self.entries[i];
            debug_assert_eq!(key.as_ref(), *table_key);
            if read_epoch >= self.epoch_with_gap.pure_epoch() {
                return Some((value.to_ref().into(), self.epoch_with_gap));
            }
        }

        None
    }
}

impl Drop for SharedBufferBatchInner {
    fn drop(&mut self) {
        self.per_table_tracker.dec_imm(self.size);
    }
}

pub static SHARED_BUFFER_BATCH_ID_GENERATOR: LazyLock<AtomicU64> =
    LazyLock::new(|| AtomicU64::new(0));

/// A write batch stored in the shared buffer.
#[derive(Clone, Debug)]
pub struct SharedBufferBatch {
    pub(crate) inner: Arc<SharedBufferBatchInner>,
    pub table_id: TableId,
}

impl SharedBufferBatch {
    pub fn for_test(
        sorted_items: Vec<SharedBufferItem>,
        epoch: HummockEpoch,
        table_id: TableId,
    ) -> Self {
        Self::for_test_inner(sorted_items, None, epoch, table_id)
    }

    pub fn for_test_with_old_values(
        sorted_items: Vec<SharedBufferItem>,
        old_values: Vec<Bytes>,
        epoch: HummockEpoch,
        table_id: TableId,
    ) -> Self {
        Self::for_test_inner(sorted_items, Some(old_values), epoch, table_id)
    }

    fn for_test_inner(
        sorted_items: Vec<SharedBufferItem>,
        old_values: Option<Vec<Bytes>>,
        epoch: HummockEpoch,
        table_id: TableId,
    ) -> Self {
        let (size, old_value_size) = Self::measure_batch_size(&sorted_items, old_values.as_deref());

        let old_values = old_values
            .map(|old_values| SharedBufferBatchOldValues::for_test(old_values, old_value_size));

        Self {
            inner: Arc::new(SharedBufferBatchInner::new(
                epoch,
                0,
                sorted_items,
                old_values,
                size,
                TableMemoryMetrics::for_test(),
            )),
            table_id,
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

    /// Return (total size, old value size or 0)
    pub fn measure_batch_size(
        batch_items: &[SharedBufferItem],
        old_values: Option<&[Bytes]>,
    ) -> (usize, usize) {
        let old_value_size = old_values
            .iter()
            .flat_map(|slice| slice.iter().map(|value| size_of_val(value) + value.len()))
            .sum::<usize>();
        // size = Sum(length of full key + length of user value)
        let kv_size = batch_items
            .iter()
            .map(|(k, v)| {
                k.len() + {
                    match v {
                        SharedBufferValue::Insert(val) | SharedBufferValue::Update(val) => {
                            val.len()
                        }
                        SharedBufferValue::Delete => 0,
                    }
                }
            })
            .sum::<usize>();
        (kv_size + old_value_size, old_value_size)
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

    pub fn key_count(&self) -> usize {
        self.inner.entries.len()
    }

    pub fn value_count(&self) -> usize {
        self.inner.entries.len()
    }

    pub fn has_old_value(&self) -> bool {
        self.inner.old_values.is_some()
    }

    pub fn get<'a>(
        &'a self,
        table_key: TableKey<&[u8]>,
        read_epoch: HummockEpoch,
        _read_options: &ReadOptions,
    ) -> Option<(HummockValue<&'a Bytes>, EpochWithGap)> {
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

    pub fn into_directed_iter<D: HummockIteratorDirection, const IS_NEW_VALUE: bool>(
        self,
    ) -> SharedBufferBatchIterator<D, IS_NEW_VALUE> {
        SharedBufferBatchIterator::<D, IS_NEW_VALUE>::new(self.inner, self.table_id)
    }

    pub fn into_old_value_iter(self) -> SharedBufferBatchIterator<Forward, false> {
        self.into_directed_iter()
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

    pub fn epoch(&self) -> HummockEpoch {
        self.inner.epoch_with_gap.pure_epoch()
    }

    pub(crate) fn build_shared_buffer_batch(
        epoch: HummockEpoch,
        spill_offset: u16,
        sorted_items: Vec<SharedBufferItem>,
        old_values: Option<SharedBufferBatchOldValues>,
        size: usize,
        table_id: TableId,
        table_metrics: Arc<TableMemoryMetrics>,
    ) -> Self {
        let inner = SharedBufferBatchInner::new(
            epoch,
            spill_offset,
            sorted_items,
            old_values,
            size,
            table_metrics,
        );
        SharedBufferBatch {
            inner: Arc::new(inner),
            table_id,
        }
    }

    #[cfg(any(test, feature = "test"))]
    pub fn build_shared_buffer_batch_for_test(
        epoch: HummockEpoch,
        spill_offset: u16,
        sorted_items: Vec<SharedBufferItem>,
        size: usize,
        table_id: TableId,
    ) -> Self {
        let inner = SharedBufferBatchInner::new(
            epoch,
            spill_offset,
            sorted_items,
            None,
            size,
            TableMemoryMetrics::for_test(),
        );
        SharedBufferBatch {
            inner: Arc::new(inner),
            table_id,
        }
    }
}

/// Iterate all the items in the shared buffer batch
/// If there are multiple versions of a key, the iterator will return all versions
pub struct SharedBufferBatchIterator<D: HummockIteratorDirection, const IS_NEW_VALUE: bool = true> {
    inner: Arc<SharedBufferBatchInner>,
    /// The index of the current entry in the payload
    current_entry_idx: usize,
    table_id: TableId,
    _phantom: PhantomData<D>,
}

impl<D: HummockIteratorDirection, const IS_NEW_VALUE: bool>
    SharedBufferBatchIterator<D, IS_NEW_VALUE>
{
    pub(crate) fn new(inner: Arc<SharedBufferBatchInner>, table_id: TableId) -> Self {
        if !IS_NEW_VALUE {
            assert!(
                inner.old_values.is_some(),
                "create old value iter with no old value: {:?}",
                table_id
            );
        }
        Self {
            inner,
            current_entry_idx: 0,
            table_id,
            _phantom: Default::default(),
        }
    }

    fn is_valid_entry_idx(&self) -> bool {
        self.current_entry_idx < self.inner.entries.len()
    }

    fn invalidate(&mut self) {
        self.current_entry_idx = self.inner.entries.len();
    }

    fn advance_to_next_entry(&mut self) {
        debug_assert!(self.is_valid_entry_idx());
        match D::direction() {
            DirectionEnum::Forward => {
                self.current_entry_idx += 1;
            }
            DirectionEnum::Backward => {
                if self.current_entry_idx == 0 {
                    self.invalidate();
                } else {
                    self.current_entry_idx -= 1;
                }
            }
        }
    }

    fn assert_valid_idx(&self) {
        debug_assert!(self.is_valid_entry_idx());
        if !IS_NEW_VALUE {
            debug_assert!(!matches!(
                self.inner.entries[self.current_entry_idx].value,
                SharedBufferValue::Insert(_)
            ));
        }
    }

    fn advance_until_valid_old_value(&mut self) {
        debug_assert!(!IS_NEW_VALUE);
        while self.is_valid_entry_idx()
            && matches!(
                self.inner.entries[self.current_entry_idx].value,
                SharedBufferValue::Insert(_)
            )
        {
            self.advance_to_next_entry();
        }
    }
}

impl<D: HummockIteratorDirection, const IS_NEW_VALUE: bool> HummockIterator
    for SharedBufferBatchIterator<D, IS_NEW_VALUE>
{
    type Direction = D;

    async fn next(&mut self) -> HummockResult<()> {
        self.advance_to_next_entry();
        if !IS_NEW_VALUE {
            self.advance_until_valid_old_value();
        }
        Ok(())
    }

    fn key(&self) -> FullKey<&[u8]> {
        self.assert_valid_idx();
        let entry = &self.inner.entries[self.current_entry_idx];
        FullKey::new_with_gap_epoch(
            self.table_id,
            TableKey(entry.key.as_ref()),
            self.inner.epoch_with_gap,
        )
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.assert_valid_idx();
        if IS_NEW_VALUE {
            self.inner.entries[self.current_entry_idx]
                .value
                .to_ref()
                .to_slice()
                .into()
        } else {
            HummockValue::put(
                self.inner.old_values.as_ref().unwrap().values[self.current_entry_idx].as_ref(),
            )
        }
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
        if !IS_NEW_VALUE {
            self.advance_until_valid_old_value();
        }
        Ok(())
    }

    async fn seek<'a>(&'a mut self, key: FullKey<&'a [u8]>) -> HummockResult<()> {
        match key.user_key.table_id.cmp(&self.table_id) {
            Ordering::Less => {
                match D::direction() {
                    DirectionEnum::Forward => {
                        // seek key table id < batch table id, so seek to beginning
                        self.rewind().await?;
                        return Ok(());
                    }
                    DirectionEnum::Backward => {
                        self.invalidate();
                        return Ok(());
                    }
                };
            }
            Ordering::Greater => {
                match D::direction() {
                    DirectionEnum::Forward => {
                        self.invalidate();
                        return Ok(());
                    }
                    DirectionEnum::Backward => {
                        // seek key table id > batch table id, so seek to end
                        self.rewind().await?;
                        return Ok(());
                    }
                };
            }
            Ordering::Equal => (),
        }
        // Perform binary search on table key because the items in SharedBufferBatch is ordered
        // by table key.
        let partition_point = self
            .inner
            .entries
            .binary_search_by(|probe| probe.key.as_ref().cmp(*key.user_key.table_key));
        match partition_point {
            Ok(i) => {
                self.current_entry_idx = i;
                // Epoch order --------->
                // Epochs:  epoch300 epoch200 epoch100
                // Forward: ------------------------->
                // Backward:<-------------------------
                // Assume self.inner.epoch_with_gap is epoch200
                let skip_on_epoch = match D::direction() {
                    DirectionEnum::Forward => {
                        // should advance when key.epoch_with_gap is epoch100
                        key.epoch_with_gap < self.inner.epoch_with_gap
                    }
                    DirectionEnum::Backward => {
                        // should advance when key.epoch_with_gap is epoch300
                        key.epoch_with_gap > self.inner.epoch_with_gap
                    }
                };
                if skip_on_epoch {
                    self.advance_to_next_entry()
                }
            }
            Err(i) => match D::direction() {
                DirectionEnum::Forward => {
                    self.current_entry_idx = i;
                }
                DirectionEnum::Backward => {
                    if i == 0 {
                        self.invalidate();
                    } else {
                        self.current_entry_idx = i - 1;
                    }
                }
            },
        };
        if !IS_NEW_VALUE {
            self.advance_until_valid_old_value();
        }
        Ok(())
    }

    fn collect_local_statistic(&self, _stats: &mut crate::monitor::StoreLocalStatistic) {}

    fn value_meta(&self) -> ValueMeta {
        ValueMeta::default()
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Bound::Excluded;

    use itertools::{Itertools, zip_eq};
    use risingwave_common::util::epoch::{EpochExt, test_epoch};
    use risingwave_hummock_sdk::key::map_table_key_range;

    use super::*;
    use crate::hummock::iterator::test_utils::{
        iterator_test_key_of_epoch, iterator_test_table_key_of, transform_shared_buffer,
    };

    fn to_hummock_value_batch(
        items: Vec<(Vec<u8>, SharedBufferValue<Bytes>)>,
    ) -> Vec<(Vec<u8>, HummockValue<Bytes>)> {
        items.into_iter().map(|(k, v)| (k, v.into())).collect()
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_basic() {
        let epoch = test_epoch(1);
        let shared_buffer_items: Vec<(Vec<u8>, SharedBufferValue<Bytes>)> = vec![
            (
                iterator_test_table_key_of(0),
                SharedBufferValue::Insert(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(1),
                SharedBufferValue::Insert(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(2),
                SharedBufferValue::Insert(Bytes::from("value1")),
            ),
        ];
        let shared_buffer_batch = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items.clone()),
            epoch,
            Default::default(),
        );
        let shared_buffer_items = to_hummock_value_batch(shared_buffer_items);

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
                    .0
                    .as_slice(),
                v.as_slice()
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
        let epoch = test_epoch(1);
        let shared_buffer_items = vec![
            (
                iterator_test_table_key_of(1),
                SharedBufferValue::Insert(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(2),
                SharedBufferValue::Insert(Bytes::from("value2")),
            ),
            (
                iterator_test_table_key_of(3),
                SharedBufferValue::Insert(Bytes::from("value3")),
            ),
        ];
        let shared_buffer_batch = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items.clone()),
            epoch,
            Default::default(),
        );
        let shared_buffer_items = to_hummock_value_batch(shared_buffer_items);

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
        iter.seek(iterator_test_key_of_epoch(2, test_epoch(2)).to_ref())
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
        iter.seek(iterator_test_key_of_epoch(2, test_epoch(0)).to_ref())
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

        // BACKWARD: Seek to 2nd key with old epoch, expect first two item to return
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch.prev_epoch()).to_ref())
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
        iter.seek(iterator_test_key_of_epoch(2, epoch.next_epoch()).to_ref())
            .await
            .unwrap();
        assert!(iter.is_valid());
        let item = shared_buffer_items.first().unwrap();
        assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_old_value_iter() {
        let epoch = test_epoch(1);
        let key_values = vec![
            (
                iterator_test_table_key_of(1),
                SharedBufferValue::Insert(Bytes::from("value1")),
            ),
            (
                iterator_test_table_key_of(2),
                SharedBufferValue::Update(Bytes::from("value2")),
            ),
            (
                iterator_test_table_key_of(3),
                SharedBufferValue::Insert(Bytes::from("value3")),
            ),
            (iterator_test_table_key_of(4), SharedBufferValue::Delete),
        ];
        let old_values = vec![
            Bytes::new(),
            Bytes::from("old_value2"),
            Bytes::new(),
            Bytes::from("old_value4"),
        ];
        let shared_buffer_batch = SharedBufferBatch::for_test_with_old_values(
            transform_shared_buffer(key_values.clone()),
            old_values.clone(),
            epoch,
            Default::default(),
        );
        let shared_buffer_items = to_hummock_value_batch(key_values.clone());
        let expected_old_value_iter_items = zip_eq(&key_values, &old_values)
            .filter(|((_, new_value), _)| !matches!(new_value, SharedBufferValue::Insert(_)))
            .map(|((key, _), old_value)| (key.clone(), HummockValue::Put(old_value)))
            .collect_vec();

        let mut iter = shared_buffer_batch.clone().into_old_value_iter();
        iter.rewind().await.unwrap();
        for item in &expected_old_value_iter_items {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

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

        let mut iter = shared_buffer_batch.clone().into_old_value_iter();
        iter.seek(iterator_test_key_of_epoch(0, epoch).to_ref())
            .await
            .unwrap();
        for item in &expected_old_value_iter_items {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to a key > the last key, expect no items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(5, epoch).to_ref())
            .await
            .unwrap();
        assert!(!iter.is_valid());

        let mut iter = shared_buffer_batch.clone().into_old_value_iter();
        iter.seek(iterator_test_key_of_epoch(5, epoch).to_ref())
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

        let mut iter = shared_buffer_batch.clone().into_old_value_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch).to_ref())
            .await
            .unwrap();
        for item in &expected_old_value_iter_items {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with future epoch, expect last two items to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch.next_epoch()).to_ref())
            .await
            .unwrap();
        for item in &shared_buffer_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        let mut iter = shared_buffer_batch.clone().into_old_value_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch.next_epoch()).to_ref())
            .await
            .unwrap();
        for item in &expected_old_value_iter_items {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        let mut iter = shared_buffer_batch.clone().into_old_value_iter();
        iter.seek(iterator_test_key_of_epoch(2, epoch.prev_epoch()).to_ref())
            .await
            .unwrap();
        for item in &expected_old_value_iter_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // FORWARD: Seek to 2nd key with old epoch, expect last item to return
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        iter.seek(iterator_test_key_of_epoch(3, epoch.prev_epoch()).to_ref())
            .await
            .unwrap();
        let item = shared_buffer_items.last().unwrap();
        assert!(iter.is_valid());
        assert_eq!(*iter.key().user_key.table_key, item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());

        // Seek to an insert key
        let mut iter = shared_buffer_batch.clone().into_old_value_iter();
        iter.seek(iterator_test_key_of_epoch(3, epoch).to_ref())
            .await
            .unwrap();
        for item in &expected_old_value_iter_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(*iter.key().user_key.table_key, item.0);
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());
    }

    #[tokio::test]
    #[should_panic]
    async fn test_invalid_table_id() {
        let epoch = test_epoch(1);
        let shared_buffer_batch = SharedBufferBatch::for_test(vec![], epoch, Default::default());
        // Seeking to non-current epoch should panic
        let mut iter = shared_buffer_batch.into_forward_iter();
        iter.seek(FullKey::for_test(TableId::new(1), vec![], epoch).to_ref())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_range_existx() {
        let epoch = test_epoch(1);
        let shared_buffer_items = vec![
            (
                Vec::from("a_1"),
                SharedBufferValue::Insert(Bytes::from("value1")),
            ),
            (
                Vec::from("a_3"),
                SharedBufferValue::Insert(Bytes::from("value2")),
            ),
            (
                Vec::from("a_5"),
                SharedBufferValue::Insert(Bytes::from("value3")),
            ),
            (
                Vec::from("b_2"),
                SharedBufferValue::Insert(Bytes::from("value3")),
            ),
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
    async fn test_shared_buffer_batch_seek_bug() {
        // Reproduce the bug where seek falls through to binary_search when table_id mismatch
        let epoch = test_epoch(1);
        let table_id = TableId::new(100);
        let shared_buffer_items: Vec<(Vec<u8>, SharedBufferValue<Bytes>)> = vec![(
            iterator_test_table_key_of(1), // "key_test_000...001"
            SharedBufferValue::Insert(Bytes::from("value1")),
        )];
        let shared_buffer_batch = SharedBufferBatch::for_test(
            transform_shared_buffer(shared_buffer_items.clone()),
            epoch,
            table_id,
        );

        // Case 1: Seek with smaller TableId (99), but larger TableKey ("key_test_000...002").
        // "key_test...002" > "key_test...001".
        // Forward Iterator.
        // Expected: Should land on the first item (TableId 100 > TableId 99).
        // Bug description: rewinds (index 0), then binary_search("key_2") in batch (only "key_1").
        // "key_2" > "key_1", so index becomes 1 (end). Iterator invalid.
        let mut iter = shared_buffer_batch.clone().into_forward_iter();
        let seek_key = FullKey::for_test(
            TableId::new(99),
            iterator_test_table_key_of(2), // larger key
            epoch,
        );
        iter.seek(seek_key.to_ref()).await.unwrap();

        assert!(
            iter.is_valid(),
            "Iterator should be valid when seeking with smaller table_id, even if the key part is larger"
        );
        assert_eq!(iter.key().user_key.table_id, table_id);

        // Case 2: Seek with larger TableId (101), but smaller TableKey ("key_test_000...000").
        // "key_test...000" < "key_test...001".
        // Backward Iterator.
        // Expected: Should land on the last item (TableId 100 < TableId 101).
        // Bug description: rewinds (index valid), then binary_search("key_0") which returns Err(0) (insertion point at start).
        // Backward generic logic for Err(0) is `invalidate()`.
        let mut iter = shared_buffer_batch.clone().into_backward_iter();
        let seek_key = FullKey::for_test(
            TableId::new(101),
            iterator_test_table_key_of(0), // smaller key
            epoch,
        );
        iter.seek(seek_key.to_ref()).await.unwrap();

        assert!(
            iter.is_valid(),
            "Iterator should be valid when seeking with larger table_id, even if the key part is smaller"
        );
        assert_eq!(iter.key().user_key.table_id, table_id);
    }
}
