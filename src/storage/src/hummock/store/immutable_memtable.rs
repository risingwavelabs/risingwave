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
use std::collections::{BTreeMap, BinaryHeap};
use std::future::Future;
use std::io::Seek;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, RangeBounds};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use bytes::Bytes;
use itertools::{min, Itertools};
use risingwave_common::catalog::TableId;
use risingwave_common::row::Row;
use risingwave_hummock_sdk::key::{key_with_epoch, FullKey, MergedImmKey, TableKey, EPOCH_LEN};
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_pb::connector_service::SourceType::Mysql;
use risingwave_pb::data::Op;
use zstd::zstd_safe::WriteBuf;

use crate::hummock::iterator::{
    Backward, DirectionEnum, Forward, HummockIterator, HummockIteratorDirection,
};
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchId, SharedBufferBatchInner, SharedBufferBatchIterator,
    SharedBufferItem,
};
use crate::hummock::shared_buffer::SHARED_BUFFER_BATCH_ID_GENERATOR;
use crate::hummock::store::memtable::{ImmId, ImmutableMemtable};
use crate::hummock::utils::{range_overlap, MemoryTracker};
use crate::hummock::value::HummockValue;
use crate::hummock::{DeleteRangeTombstone, HummockResult};
use crate::monitor::StoreLocalStatistic;

// NOTES: After we merged multiple imms into a merged imm,
// there will be multiple versions for a single key, we put those versions into a vector.
pub(crate) type MergedImmItem = (TableKey<Vec<u8>>, Vec<(HummockEpoch, HummockValue<Bytes>)>);

/// Abstraction of the immutable memtable that can be read from the `HummockReadVersion`
pub enum ImmutableMemtableImpl {
    Imm(SharedBufferBatch),
    MergedImm(MergedImmutableMemtable),
}

#[derive(Default)]
pub(crate) struct MergedImmutableMemtableInner {
    payload: Vec<MergedImmItem>,
    range_tombstone_list: Vec<DeleteRangeTombstone>,
    size: usize,
    /// The minimium epoch of all merged imm
    min_epoch: HummockEpoch,
    batch_id: ImmId,
    _tracker: Option<MemoryTracker>,
    // TODO: store references to merged imms to interface with `compact_shared_buffer`
    // That is we can submit these imms to the compactor instead of the payload.
    // Update: I think the merged imm would not be submitted to compactor, the flush
    // will be handled by the Uploader
    // imms: Vec<ImmutableMemtable>,
}

impl MergedImmutableMemtableInner {
    fn new(
        payload: Vec<MergedImmItem>,
        range_tombstone_list: Vec<DeleteRangeTombstone>,
        size: usize,
        min_epoch: HummockEpoch,
    ) -> Self {
        // map key of merged_imm to MergedImmKey

        Self {
            payload,
            range_tombstone_list,
            size,
            min_epoch,
            batch_id: SHARED_BUFFER_BATCH_ID_GENERATOR.fetch_add(1, Relaxed),
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
                for (e, v) in item.1.iter() {
                    if *e <= epoch {
                        return Some(v.clone());
                    }
                }
                None
            }
            Err(_) => None,
        }
    }

    /// The input imm is from the queue in `StagingVersion`
    /// So it shuold have the same lifetime as those values in `self.payload`
    // fn merge_imm(&mut self, imm: &ImmutableMemtable) {
    //     imm.get_payload().iter().for_each(|item| {
    //         let (user_key, value) = item;
    //         // let user_key_with_epoch = key_with_epoch(user_key.to_vec(), imm.epoch());
    //         let merged_imm_key = MergedImmKey::new(user_key.to_vec(), imm.epoch());
    //         self.min_epoch = std::cmp::min(self.min_epoch, imm.epoch());
    //         self.size += user_key_with_epoch.len() + mem::size_of::<&HummockValue<Bytes>>();
    //         self.payload.push((merged_imm_key, value.clone()));
    //         self.range_tombstone_list
    //             .append(&mut imm.get_delete_range_tombstones());
    //     });
    //     self.imms.push(imm.clone());
    // }

    pub fn get_payload(&self) -> &Vec<MergedImmItem> {
        &self.payload
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
pub struct MergedImmutableMemtable {
    inner: Arc<MergedImmutableMemtableInner>,
    table_id: TableId,
}

// pub type MergedImmIter<'a> =
//     impl Iterator<Item = (&'a Bytes, &'a HummockValue<Bytes>)> + ExactSizeIterator;
// pub type MergedImmIter<'a> =
//     impl Iterator<Item = (Bytes, &'a HummockValue<Bytes>)> + ExactSizeIterator;

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

//  HummockIterator trait for merged imm iterator
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

/// Splits key_with_epoch into its user key part and epoch part.
#[inline]
fn split_key_epoch(key_with_epoch: &[u8]) -> (&[u8], HummockEpoch) {
    let pos = key_with_epoch
        .len()
        .checked_sub(EPOCH_LEN)
        .unwrap_or_else(|| panic!("bad full key format: {:?}", key_with_epoch));
    let (key, epoch_slice) = key_with_epoch.split_at(pos);
    let mut epoch: HummockEpoch = 0;
    unsafe {
        std::ptr::copy_nonoverlapping(
            epoch_slice.as_ptr(),
            &mut epoch as *mut _ as *mut u8,
            EPOCH_LEN,
        );
    }
    (key, HummockEpoch::from_be(epoch))
}

// TODO: rush this struct and write some unit tests for read and merge imms
impl MergedImmutableMemtable {
    pub fn build_merged_imm(table_id: TableId, imms: Vec<ImmutableMemtable>) -> Self {
        // use a binary heap to merge imms
        let mut heap = BinaryHeap::new();
        let mut range_tombstone_list = Vec::new();
        let mut num_keys = 0;
        let mut min_epoch = HummockEpoch::MAX;
        let mut size = 0;

        for imm in imms {
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
            .map(|((k, v), epoch)| TableKey(k.to_vec()))
            .unwrap();

        let mut versions = Vec::new();
        for ((k, v), epoch) in items.into_iter() {
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

        for (k, v) in merged_payload.iter() {
            println!(
                "key: {:?}, versions: {:?}",
                String::from_utf8(k.clone().0),
                v
            );
        }

        MergedImmutableMemtable {
            inner: Arc::new(MergedImmutableMemtableInner::new(
                merged_payload,
                range_tombstone_list,
                size,
                min_epoch,
            )),
            table_id,
        }
    }

    pub fn print(&self) {
        // print the contents of the merged imm
        self.inner.iter().for_each(|(k, v)| {})
    }

    pub fn get(
        &self,
        table_key: TableKey<&[u8]>,
        epoch: HummockEpoch,
    ) -> Option<HummockValue<Bytes>> {
        self.inner.get_value(table_key, epoch)
    }

    // TODO: iterator iterfaces cannot be wrapper into the Impl enum
    // pub fn get_directed_iter<D: HummockIteratorDirection>(
    //     &self,
    //     epoch: HummockEpoch,
    // ) -> MergedImmIterator<D> {
    //     MergedImmIterator::new(self.inner.get_payload(), self.table_id, epoch)
    // }
    //
    // pub fn get_forward_iter(&self, epoch: HummockEpoch) -> MergedImmIterator<Forward> {
    //     self.get_directed_iter(epoch)
    // }
    //
    // pub fn get_backward_iter(&self, epoch: HummockEpoch) -> MergedImmIterator<Backward> {
    //     self.get_directed_iter(epoch)
    // }

    // pub fn start_table_key(&self) -> TableKey<&[u8]> {}
    //
    // pub fn end_table_key(&self) -> TableKey<&[u8]> {}
    //
    // /// return inclusive left endpoint, which means that all data in this batch should be larger
    // or /// equal than this key.
    // pub fn start_user_key(&self) -> UserKey<&[u8]> {}
    //
    // /// return inclusive right endpoint, which means that all data in this batch should be
    // smaller /// or equal than this key.
    // pub fn end_user_key(&self) -> UserKey<&[u8]> {
    //     UserKey::new(self.table_id(), self.largest_table_key())
    // }

    fn table_id(&self) -> TableId {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hummock::iterator::test_utils::{
        iterator_test_table_key_of, transform_shared_buffer,
    };
    #[test]
    fn test_merge_imms_basic() {
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

        let batch_items = vec![shared_buffer_items1, shared_buffer_items2];
        // newer data comes first
        let imms = vec![imm2, imm1];
        let merged_imm = MergedImmutableMemtable::build_merged_imm(table_id, imms.clone());

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
    }
}

// TODO: 最后再实现这个wrapper
// impl ImmutableMemtableImpl {
//     pub fn filter<R, B>(&self, table_id: TableId, table_key_range: &R) -> bool
//     where
//         R: RangeBounds<TableKey<B>>,
//         B: AsRef<[u8]>,
//     {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => {
//                 batch.table_id == table_id
//                     && range_overlap(
//                         table_key_range,
//                         *self.start_table_key(),
//                         *self.end_table_key(),
//                     )
//             }
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     pub fn get(&self, table_key: TableKey<&[u8]>) -> Option<HummockValue<Bytes>> {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => {
//                 // Perform binary search on table key because the items in SharedBufferBatch is
//                 // ordered by table key.
//                 match batch.inner.binary_search_by(|m| (m.0[..]).cmp(*table_key)) {
//                     Ok(i) => Some(self.inner[i].1.clone()),
//                     Err(_) => None,
//                 }
//             }
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     pub fn get_payload(&self) -> &[SharedBufferItem] {
//         &self.inner
//     }
//
//     pub fn start_table_key(&self) -> TableKey<&[u8]> {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.start_table_key(),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     pub fn end_table_key(&self) -> TableKey<&[u8]> {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.end_table_key(),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     /// return inclusive left endpoint, which means that all data in this batch should be larger
// or     /// equal than this key.
//     pub fn start_user_key(&self) -> UserKey<&[u8]> {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.start_user_key(),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     /// return inclusive right endpoint, which means that all data in this batch should be
// smaller     /// or equal than this key.
//     pub fn end_user_key(&self) -> UserKey<&[u8]> {
//         UserKey::new(self.table_id(), self.largest_table_key())
//     }
//
//     fn table_id(&self) -> TableId {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.table_id(),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     fn largest_table_key(&self) -> TableKey<&[u8]> {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => TableKey(&batch.inner.largest_table_key),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     pub fn epoch(&self) -> u64 {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.epoch(),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     pub fn size(&self) -> usize {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.size(),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     pub fn batch_id(&self) -> SharedBufferBatchId {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.batch_id(),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     // methods for delete range
//     pub fn get_delete_range_tombstones(&self) -> Vec<DeleteRangeTombstone> {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.get_delete_range_tombstones(),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     pub fn check_delete_by_range(&self, table_key: TableKey<&[u8]>) -> bool {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.check_delete_by_range(table_key),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
//
//     pub fn has_range_tombstone(&self) -> bool {
//         match self {
//             ImmutableMemtableImpl::Imm(batch) => batch.has_range_tombstone(),
//             ImmutableMemtableImpl::MergedImm(_) => {
//                 // todo
//             }
//         }
//     }
// }

// TODO: iterator abstraction for imm and merged_imm
// pub enum ImmutableMemtableIterator<D> {
//     Imm(SharedBufferBatchIterator<D>),
//     MergedImm(MergedImmutableMemtableIterator<D>),
// }
//
// struct MergedImmutableMemtableIterator<D> {
//     _phantom: PhantomData<D>,
// }
