// Copyright 2023 Singularity Data
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
use std::future::Future;
use std::io::Seek;
use std::marker::PhantomData;
use std::ops::{Deref, RangeBounds};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use bytes::Bytes;
use itertools::Itertools;
use risingwave_common::catalog::TableId;
use risingwave_common::row::Row;
use risingwave_hummock_sdk::key::{FullKey, TableKey, UserKey, EPOCH_LEN};
use risingwave_hummock_sdk::HummockEpoch;
use zstd::zstd_safe::WriteBuf;

use crate::hummock::iterator::{
    Backward, DeleteRangeIterator, DirectionEnum, Forward, HummockIterator,
    HummockIteratorDirection,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchId, SharedBufferBatchIterator,
};
use crate::hummock::shared_buffer::SHARED_BUFFER_BATCH_ID_GENERATOR;
use crate::hummock::store::memtable::{ImmId, ImmutableMemtable};
use crate::hummock::utils::{range_overlap, MemoryTracker};
use crate::hummock::value::HummockValue;
use crate::hummock::{DeleteRangeTombstone, HummockResult, MemoryLimiter};
use crate::monitor::StoreLocalStatistic;

// NOTES: After we merged multiple imms into a merged imm,
// there will be multiple versions for a single key, we put those versions into a vector and
// sort them in descending order, aka Newest to oldest.
pub(crate) type MergedImmItem = (TableKey<Vec<u8>>, Vec<(HummockEpoch, HummockValue<Bytes>)>);

#[derive(Debug)]
pub(crate) struct MergedImmutableMemtableInner {
    payload: Vec<MergedImmItem>,
    range_tombstone_list: Vec<DeleteRangeTombstone>,
    size: usize,
    /// The minimum epoch of all merged imm
    min_epoch: HummockEpoch,
    batch_id: ImmId,
    merged_imm_ids: Vec<ImmId>,
    _tracker: Option<MemoryTracker>,
}

impl MergedImmutableMemtableInner {
    fn new(
        payload: Vec<MergedImmItem>,
        range_tombstone_list: Vec<DeleteRangeTombstone>,
        size: usize,
        min_epoch: HummockEpoch,
        merged_imm_ids: Vec<ImmId>,
    ) -> Self {
        Self {
            payload,
            range_tombstone_list,
            size,
            min_epoch,
            batch_id: SHARED_BUFFER_BATCH_ID_GENERATOR.fetch_add(1, Relaxed),
            merged_imm_ids,
            _tracker: None,
        }
    }

    fn get_value(
        &self,
        table_key: TableKey<&[u8]>,
        epoch: HummockEpoch,
    ) -> Option<HummockValue<Bytes>> {
        match self.payload.binary_search_by(|m| (m.0[..]).cmp(*table_key)) {
            Ok(i) => {
                let item = &self.payload[i];
                assert_eq!(item.0.as_ref(), *table_key);
                // scan to find the first version <= epoch
                for (e, v) in &item.1 {
                    if *e <= epoch {
                        return Some(v.clone());
                    }
                }
                None
            }
            Err(_) => None,
        }
    }

    pub fn get_payload(&self) -> &Vec<MergedImmItem> {
        &self.payload
    }

    pub fn get_merged_imm_ids(&self) -> &Vec<ImmId> {
        &self.merged_imm_ids
    }
}

impl PartialEq for MergedImmutableMemtableInner {
    fn eq(&self, other: &Self) -> bool {
        self.batch_id == other.batch_id
    }
}

impl Deref for MergedImmutableMemtableInner {
    type Target = Vec<MergedImmItem>;

    fn deref(&self) -> &Self::Target {
        &self.payload
    }
}

/// Merge of multiple imms, which will contain data from multiple epochs.
/// That is the key of will be encoded with epoch
#[derive(Clone, Debug, PartialEq)]
pub struct MergedImmutableMemtable {
    inner: Arc<MergedImmutableMemtableInner>,
    table_id: TableId,
}

impl MergedImmutableMemtable {
    pub fn build_merged_imm(
        table_id: TableId,
        imms: Vec<ImmutableMemtable>,
        _memory_limiter: Option<Arc<MemoryLimiter>>,
    ) -> Self {
        // use a binary heap to merge imms
        let mut heap = BinaryHeap::new();
        let mut range_tombstone_list = Vec::new();
        let mut num_keys = 0;
        let mut min_epoch = HummockEpoch::MAX;
        let mut size = 0;
        let mut merged_imm_ids = Vec::with_capacity(imms.len());

        for imm in imms {
            assert!(imm.count() > 0, "imm should not be empty");
            assert_eq!(
                table_id,
                imm.table_id(),
                "should only merge data belonging to the same table"
            );
            merged_imm_ids.push(imm.batch_id());
            num_keys += imm.count();
            size += imm.size();
            min_epoch = std::cmp::min(min_epoch, imm.epoch());
            let mut delete_range_tombstones = imm.get_delete_range_tombstones();
            range_tombstone_list.append(&mut delete_range_tombstones);
            let mut iter = imm.into_forward_iter();
            heap.push(Node { iter });
        }

        let mut items = Vec::with_capacity(num_keys);
        while !heap.is_empty() {
            let mut node = heap.peek_mut().expect("heap is not empty");
            items.push((node.iter.current_item().clone(), node.iter.epoch()));
            node.iter.blocking_next();
            if !node.iter.is_valid() {
                // remove the invalid iter from heap
                PeekMut::pop(node);
            } else {
                // This will update the heap
                drop(node);
            }
        }

        size += items.len() * EPOCH_LEN;
        let mut merged_payload = Vec::new();
        // map items in payload to merged imm key
        let mut pivot = items
            .first()
            .map(|((k, _), _)| TableKey(k.to_vec()))
            .unwrap();

        let mut versions = Vec::new();
        for ((k, v), epoch) in items {
            let key = TableKey(k.to_vec());
            if key == pivot {
                versions.push((epoch, v));
            } else {
                merged_payload.push((pivot, versions));
                pivot = key;
                versions = vec![(epoch, v)];
            }
        }
        // process the last key
        if !versions.is_empty() {
            merged_payload.push((pivot, versions));
        }

        MergedImmutableMemtable {
            inner: Arc::new(MergedImmutableMemtableInner::new(
                merged_payload,
                range_tombstone_list,
                size,
                min_epoch,
                merged_imm_ids,
            )),
            table_id,
        }
    }

    pub fn get(
        &self,
        table_key: TableKey<&[u8]>,
        epoch: HummockEpoch,
    ) -> Option<HummockValue<Bytes>> {
        self.inner.get_value(table_key, epoch)
    }

    pub fn get_merged_imm_ids(&self) -> &Vec<ImmId> {
        self.inner.get_merged_imm_ids()
    }

    pub fn into_directed_iter<D: HummockIteratorDirection>(
        self,
        epoch: HummockEpoch,
    ) -> MergedImmIterator<D> {
        MergedImmIterator::new(self.inner, self.table_id, epoch)
    }

    pub fn into_forward_iter(self, epoch: HummockEpoch) -> MergedImmIterator<Forward> {
        self.into_directed_iter(epoch)
    }

    pub fn into_backward_iter(self, epoch: HummockEpoch) -> MergedImmIterator<Backward> {
        self.into_directed_iter(epoch)
    }

    pub fn delete_range_iter(&self) -> MergedImmDeleteRangeIterator {
        MergedImmDeleteRangeIterator::new(self.inner.clone())
    }

    pub fn start_table_key(&self) -> TableKey<&[u8]> {
        TableKey(&self.inner.first().unwrap().0)
    }

    pub fn end_table_key(&self) -> TableKey<&[u8]> {
        TableKey(&self.inner.last().unwrap().0)
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    // fn largest_table_key(&self) -> TableKey<&[u8]> {}

    pub fn epoch(&self) -> u64 {
        self.inner.min_epoch
    }

    pub fn size(&self) -> usize {
        self.inner.size
    }

    pub fn batch_id(&self) -> SharedBufferBatchId {
        self.inner.batch_id
    }

    // // methods for delete range
    pub fn get_delete_range_tombstones(&self) -> Vec<DeleteRangeTombstone> {
        self.inner.range_tombstone_list.clone()
    }

    pub fn check_delete_by_range(&self, table_key: TableKey<&[u8]>) -> bool {
        if self.inner.range_tombstone_list.is_empty() {
            return false;
        }
        let idx = self
            .inner
            .range_tombstone_list
            .partition_point(|item| item.end_user_key.table_key.as_ref().le(table_key.as_ref()));
        idx < self.inner.range_tombstone_list.len()
            && self.inner.range_tombstone_list[idx]
                .start_user_key
                .table_key
                .as_ref()
                .le(table_key.as_ref())
    }

    #[inline(always)]
    pub fn has_range_tombstone(&self) -> bool {
        !self.inner.range_tombstone_list.is_empty()
    }
}

struct Node {
    iter: SharedBufferBatchIterator<Forward>,
}

impl Ord for Node
where
    Self: PartialOrd,
{
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // compares the full key
        self.iter.key().cmp(&other.iter.key())
    }
}

impl PartialOrd<Node> for Node {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Note: to implement min-heap by using max-heap internally, the comparing
        Some(other.cmp(self))
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.iter.key() == other.iter.key()
    }
}

impl Eq for Node where Self: PartialEq {}

/// The iterator will only read data belonging to a given epoch
pub struct MergedImmIterator<D: HummockIteratorDirection> {
    inner: Arc<MergedImmutableMemtableInner>,
    current_idx: usize,
    table_id: TableId,
    epoch: HummockEpoch,
    _phantom: PhantomData<D>,
}

impl<D: HummockIteratorDirection> MergedImmIterator<D> {
    pub(crate) fn new(
        inner: Arc<MergedImmutableMemtableInner>,
        table_id: TableId,
        epoch: HummockEpoch,
    ) -> Self {
        Self {
            inner,
            current_idx: 0,
            table_id,
            epoch,
            _phantom: Default::default(),
        }
    }

    fn current_item(&self) -> &MergedImmItem {
        assert!(self.is_valid());
        let idx = match D::direction() {
            DirectionEnum::Forward => self.current_idx,
            DirectionEnum::Backward => self.inner.len() - self.current_idx - 1,
        };
        self.inner.get(idx).unwrap()
    }
}

impl<D: HummockIteratorDirection> HummockIterator for MergedImmIterator<D> {
    type Direction = D;

    type NextFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type RewindFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;
    type SeekFuture<'a> = impl Future<Output = HummockResult<()>> + 'a;

    fn next(&mut self) -> Self::NextFuture<'_> {
        async move {
            assert!(self.is_valid());
            // move to next key
            self.current_idx += 1;
            Ok(())
        }
    }

    fn key(&self) -> FullKey<&[u8]> {
        let item = self.current_item();
        FullKey::new(self.table_id, TableKey(&item.0), self.epoch)
    }

    fn value(&self) -> HummockValue<&[u8]> {
        let item = self.current_item();
        // Use linear scan here to exploit cpu cache prefetch
        for (e, v) in item.1.iter() {
            if *e <= self.epoch {
                return v.as_slice();
            }
        }
        unreachable!("should have found a value for epoch {}", self.epoch)
    }

    fn is_valid(&self) -> bool {
        self.current_idx < self.inner.len()
    }

    fn rewind(&mut self) -> Self::RewindFuture<'_> {
        async move {
            self.current_idx = 0;
            Ok(())
        }
    }

    fn seek<'a>(&'a mut self, _key: FullKey<&'a [u8]>) -> Self::SeekFuture<'a> {
        async move { unimplemented!("MergedImmIterator::seek") }
    }

    fn collect_local_statistic(&self, _stats: &mut StoreLocalStatistic) {}
}

// delete range iterator for merged imm
pub struct MergedImmDeleteRangeIterator {
    inner: Arc<MergedImmutableMemtableInner>,
    current_idx: usize,
}

impl MergedImmDeleteRangeIterator {
    pub(crate) fn new(inner: Arc<MergedImmutableMemtableInner>) -> Self {
        Self {
            inner,
            current_idx: 0,
        }
    }
}

impl DeleteRangeIterator for MergedImmDeleteRangeIterator {
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
    use super::*;
    use crate::hummock::iterator::test_utils::{
        iterator_test_table_key_of, transform_shared_buffer,
    };

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
            table_id.clone(),
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
            table_id.clone(),
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
            table_id.clone(),
        );

        let batch_items = vec![
            shared_buffer_items1,
            shared_buffer_items2,
            shared_buffer_items3,
        ];
        // newer data comes first
        let imms = vec![imm3, imm2, imm1];
        let merged_imm = MergedImmutableMemtable::build_merged_imm(table_id, imms.clone(), None);

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
        for epoch in 1..=3 {
            let mut iter = merged_imm.clone().into_forward_iter(epoch);
            iter.rewind().await.unwrap();
            let mut output = vec![];
            while iter.is_valid() {
                output.push((
                    iter.key().user_key.table_key.to_vec(),
                    iter.value().to_bytes(),
                ));
                iter.next().await.unwrap();
            }
            assert_eq!(output, batch_items[epoch as usize - 1]);

            // Backward iterator
            let mut backward_iter = merged_imm.clone().into_backward_iter(epoch);
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
            assert_eq!(output, batch_items[epoch as usize - 1]);
        }
    }

    #[tokio::test]
    async fn test_merge_imms_delete_range() {
        let table_id = TableId { table_id: 1004 };
        let epoch = 1;
        let delete_ranges = vec![
            (Bytes::from(b"aaa".to_vec()), Bytes::from(b"bbb".to_vec())),
            (Bytes::from(b"ddd".to_vec()), Bytes::from(b"eee".to_vec())),
            (Bytes::from(b"hhh".to_vec()), Bytes::from(b"lll".to_vec())),
        ];

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

        let imm1 = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            transform_shared_buffer(shared_buffer_items1),
            0,
            delete_ranges,
            Default::default(),
            None,
        );

        let epoch = 2;
        let delete_ranges = vec![
            (Bytes::from(b"aaa".to_vec()), Bytes::from(b"ccc".to_vec())),
            (Bytes::from(b"fff".to_vec()), Bytes::from(b"ggg".to_vec())),
        ];
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
        let imm2 = SharedBufferBatch::build_shared_buffer_batch(
            epoch,
            transform_shared_buffer(shared_buffer_items2),
            0,
            delete_ranges,
            Default::default(),
            None,
        );

        let imms = vec![imm2, imm1];
        let merged_imm = MergedImmutableMemtable::build_merged_imm(table_id, imms.clone(), None);

        assert!(merged_imm.check_delete_by_range(TableKey(b"aaa")));
        assert!(merged_imm.check_delete_by_range(TableKey(b"bbb")));
        assert!(!merged_imm.check_delete_by_range(TableKey(b"ccc")));
        assert!(!merged_imm.check_delete_by_range(TableKey(b"eee")));

        assert!(merged_imm.check_delete_by_range(TableKey(b"fff")));
        assert!(!merged_imm.check_delete_by_range(TableKey(b"ggg")));
        assert!(merged_imm.check_delete_by_range(TableKey(b"hhh")));
        assert!(merged_imm.check_delete_by_range(TableKey(b"hhi")));
        assert!(merged_imm.check_delete_by_range(TableKey(b"kkk")));
        assert!(!merged_imm.check_delete_by_range(TableKey(b"lll")));
    }
}
