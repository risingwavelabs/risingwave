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

#[allow(dead_code)]
pub mod shared_buffer_batch;
#[allow(dead_code)]
pub mod shared_buffer_uploader;

use std::collections::{BTreeMap, HashMap};
use std::mem::swap;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use itertools::Itertools;
use risingwave_hummock_sdk::is_remote_sst_id;
use risingwave_hummock_sdk::key::user_key;
use risingwave_pb::hummock::{KeyRange, SstableInfo, VNodeBitmap};

use self::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::iterator::{
    BoxedHummockIterator, OrderedMergeIteratorInner, ReadOptions, UnorderedMergeIteratorInner,
};
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTaskPayload;
use crate::hummock::state_store::HummockIteratorType;
use crate::hummock::utils::{filter_single_sst, range_overlap};
use crate::hummock::{HummockResult, SSTableIteratorType, SstableStore};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};

#[derive(Debug, Clone, PartialEq)]
pub enum UncommittedData {
    Sst(SstableInfo),
    Batch(SharedBufferBatch),
}

fn get_sst_key_range(info: &SstableInfo) -> &KeyRange {
    let key_range = info
        .key_range
        .as_ref()
        .expect("local sstable should have key range");
    assert!(
        !key_range.inf,
        "local sstable should not have infinite key range. Sstable info: {:?}",
        info,
    );
    key_range
}

impl UncommittedData {
    pub fn start_user_key(&self) -> &[u8] {
        match self {
            UncommittedData::Sst(info) => {
                let key_range = get_sst_key_range(info);
                user_key(key_range.left.as_slice())
            }
            UncommittedData::Batch(batch) => batch.start_user_key(),
        }
    }

    pub fn end_user_key(&self) -> &[u8] {
        match self {
            UncommittedData::Sst(info) => {
                let key_range = get_sst_key_range(info);
                user_key(key_range.right.as_slice())
            }
            UncommittedData::Batch(batch) => batch.end_user_key(),
        }
    }
}

pub(crate) type OrderIndex = usize;
/// `{ (end key, order_id) -> batch }`
pub(crate) type KeyIndexedUncommittedData = BTreeMap<(Vec<u8>, OrderIndex), UncommittedData>;
/// uncommitted data sorted by order index in descending order. Data in the same inner list share
/// the same order index, which means their keys don't overlap.
pub(crate) type OrderIndexedUncommittedData = Vec<Vec<UncommittedData>>;

pub(crate) fn to_order_indexed(
    key_indexed_data: &KeyIndexedUncommittedData,
) -> OrderIndexedUncommittedData {
    let mut order_indexed_data = BTreeMap::new();
    for ((_, order_id), data) in key_indexed_data {
        order_indexed_data
            .entry(*order_id)
            .or_insert_with(Vec::new)
            .push(data.clone());
    }
    // Take rev here to ensure order index sorted in descending order.
    order_indexed_data.into_values().rev().collect()
}

pub(crate) async fn build_ordered_merge_iter<T: HummockIteratorType>(
    uncommitted_data: &OrderIndexedUncommittedData,
    sstable_store: Arc<SstableStore>,
    stats: Arc<StateStoreMetrics>,
    local_stats: &mut StoreLocalStatistic,
    read_options: Arc<ReadOptions>,
) -> HummockResult<BoxedHummockIterator<T::Direction>> {
    let mut ordered_iters = Vec::new();
    // use `rev` because the larger order index comes at first.
    for data_list in uncommitted_data {
        let mut data_iters = Vec::new();
        for data in data_list {
            match data {
                UncommittedData::Batch(batch) => {
                    data_iters.push(Box::new(batch.clone().into_directed_iter::<T::Direction>())
                        as BoxedHummockIterator<T::Direction>);
                }
                UncommittedData::Sst(table_info) => {
                    let table = sstable_store.sstable(table_info.id, local_stats).await?;
                    data_iters.push(Box::new(T::SstableIteratorType::create(
                        table,
                        sstable_store.clone(),
                        read_options.clone(),
                    )));
                }
            }
        }
        if data_iters.is_empty() {
            continue;
        } else if data_iters.len() == 1 {
            ordered_iters.push(data_iters.pop().unwrap());
        } else {
            ordered_iters.push(Box::new(UnorderedMergeIteratorInner::<T::Direction>::new(
                data_iters,
                stats.clone(),
            )) as BoxedHummockIterator<T::Direction>);
        }
    }
    Ok(Box::new(OrderedMergeIteratorInner::<T::Direction>::new(
        ordered_iters,
        stats.clone(),
    )))
}

#[derive(Default, Debug)]
pub struct SharedBuffer {
    uncommitted_data: KeyIndexedUncommittedData,
    replicate_batches: BTreeMap<Vec<u8>, SharedBufferBatch>,
    uploading_tasks: HashMap<OrderIndex, KeyIndexedUncommittedData>,
    upload_batches_size: usize,
    replicate_batches_size: usize,

    next_order_index: usize,
}

pub enum UploadTaskType {
    LocalFlush,
    SyncEpoch,
}

impl SharedBuffer {
    pub fn write_batch(&mut self, batch: SharedBufferBatch) {
        self.upload_batches_size += batch.size();
        let order_index = self.get_next_order_index();

        let insert_result = self.uncommitted_data.insert(
            (batch.end_user_key().to_vec(), order_index),
            UncommittedData::Batch(batch),
        );
        assert!(
            insert_result.is_none(),
            "duplicate end key and order index when inserting a write batch. \
            Order index: {}, previous data: {:?}",
            order_index,
            insert_result
        );
    }

    pub fn replicate_batch(&mut self, batch: SharedBufferBatch) {
        self.replicate_batches_size += batch.size();
        self.replicate_batches
            .insert(batch.end_user_key().to_vec(), batch);
    }

    /// Gets batches from shared buffer that overlap with the given key range.
    /// The return tuple is (replicated batches, uncommitted data).
    pub fn get_overlap_data<R, B>(
        &self,
        key_range: &R,
        vnode_set: Option<&VNodeBitmap>,
    ) -> (Vec<SharedBufferBatch>, OrderIndexedUncommittedData)
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let replicated_batches = self
            .replicate_batches
            .range((
                key_range.start_bound().map(|b| b.as_ref().to_vec()),
                std::ops::Bound::Unbounded,
            ))
            .filter(|(_, batch)| {
                range_overlap(key_range, batch.start_user_key(), batch.end_user_key())
            })
            .map(|(_, batches)| batches.clone())
            .collect_vec();

        let range = (
            match key_range.start_bound() {
                Bound::Included(key) => Bound::Included((key.as_ref().to_vec(), OrderIndex::MIN)),
                Bound::Excluded(key) => Bound::Excluded((key.as_ref().to_vec(), OrderIndex::MAX)),
                Bound::Unbounded => Bound::Unbounded,
            },
            std::ops::Bound::Unbounded,
        );

        let local_data_iter = self
            .uncommitted_data
            .range(range.clone())
            .chain(
                self.uploading_tasks
                    .values()
                    .flat_map(|payload| payload.range(range.clone())),
            )
            .filter(|(_, data)| match data {
                UncommittedData::Batch(batch) => {
                    range_overlap(key_range, batch.start_user_key(), batch.end_user_key())
                }
                UncommittedData::Sst(info) => filter_single_sst(info, key_range, vnode_set),
            })
            .map(|((_, order_index), data)| (*order_index, data.clone()));

        let mut uncommitted_data = BTreeMap::new();
        for (order_index, data) in local_data_iter {
            uncommitted_data
                .entry(order_index)
                .or_insert_with(Vec::new)
                .push(data);
        }

        (
            replicated_batches,
            uncommitted_data.into_values().rev().collect(),
        )
    }

    pub fn clear_replicate_batch(&mut self) {
        self.replicate_batches.clear();
        self.replicate_batches_size = 0;
    }

    pub fn new_upload_task(
        &mut self,
        task_type: UploadTaskType,
    ) -> Option<(OrderIndex, UploadTaskPayload)> {
        let keyed_payload = match task_type {
            UploadTaskType::LocalFlush => {
                // For local flush, currently we only flush the write batches. We first pick the
                // write batch with the smallest order index, and then start from this order index,
                // we iterate over all order indexes in ascending order. We add the write batches to
                // the task payload and stop when we meet a sst.

                // Keep track of whether the data of an order index is non uploaded local batches
                let mut order_index_is_non_upload_batch = BTreeMap::new();
                for ((end_key, order_index), data) in &self.uncommitted_data {
                    // Bug: multiple data can shared the same order_index
                    if matches!(data, UncommittedData::Batch(_)) {
                        order_index_is_non_upload_batch
                            .insert(*order_index, Some((end_key, order_index)));
                    } else {
                        order_index_is_non_upload_batch.insert(*order_index, None);
                    }
                }
                for order_index in self.uploading_tasks.keys() {
                    order_index_is_non_upload_batch.insert(*order_index, None);
                }

                let mut payload_keys = Vec::new();
                // This will iterate over all order indexes in ascending order.
                for payload_keys_opt in order_index_is_non_upload_batch.values() {
                    match payload_keys_opt {
                        Some((end_key, order_index)) => {
                            payload_keys.push(((*end_key).clone(), **order_index));
                        }
                        None => {
                            if !payload_keys.is_empty() {
                                break;
                            }
                        }
                    }
                }

                let mut keyed_payload = KeyIndexedUncommittedData::new();
                for key in payload_keys {
                    let data = self.uncommitted_data.remove(&key).expect(
                        "the key to remote in the original non uploaded batches should exist",
                    );
                    keyed_payload.insert(key, data);
                }

                keyed_payload
            }
            UploadTaskType::SyncEpoch => {
                assert!(
                    self.uploading_tasks.is_empty(),
                    "when sync an epoch, there should not be any uploading task"
                );
                let mut keyed_payload = KeyIndexedUncommittedData::new();
                swap(&mut self.uncommitted_data, &mut keyed_payload);
                keyed_payload
            }
        };

        // The min order index in the task payload will be the order index of the payload.
        let min_order_index = keyed_payload
            .keys()
            .map(|(_, order_index)| order_index)
            .min()
            .cloned();

        if let Some(min_order_index) = min_order_index {
            let ret = Some((min_order_index, to_order_indexed(&keyed_payload)));
            self.uploading_tasks.insert(min_order_index, keyed_payload);
            ret
        } else {
            None
        }
    }

    pub fn fail_upload_task(&mut self, order_index: OrderIndex) {
        let payload = self
            .uploading_tasks
            .remove(&order_index)
            .unwrap_or_else(|| {
                panic!(
                    "the order index should exist {} when fail an upload task",
                    order_index
                )
            });
        self.uncommitted_data.extend(payload);
    }

    pub fn succeed_upload_task(
        &mut self,
        order_index: OrderIndex,
        new_sst: Vec<SstableInfo>,
    ) -> Vec<SstableInfo> {
        let payload = self
            .uploading_tasks
            .remove(&order_index)
            .unwrap_or_else(|| {
                panic!(
                    "the order index should exist {} when succeed an upload task",
                    order_index
                )
            });
        for sst in new_sst {
            let data = UncommittedData::Sst(sst);
            let insert_result = self
                .uncommitted_data
                .insert((data.end_user_key().to_vec(), order_index), data);
            assert!(
                insert_result.is_none(),
                "duplicate data end key and order index when inserting an SST. \
                Order index: {}. Previous data: {:?}",
                order_index,
                insert_result,
            );
        }
        let mut previous_sst = Vec::new();
        for data in payload.into_values() {
            match data {
                UncommittedData::Batch(batch) => {
                    self.upload_batches_size -= batch.size();
                }
                UncommittedData::Sst(sst) => {
                    previous_sst.push(sst);
                }
            }
        }
        // TODO: may want to delete the sst
        previous_sst
    }

    pub fn get_ssts_to_commit(&self) -> Vec<SstableInfo> {
        assert!(
            self.uploading_tasks.is_empty(),
            "when committing sst there should not be uploading task"
        );
        let mut ret = Vec::new();
        for data in self.uncommitted_data.values() {
            match data {
                UncommittedData::Batch(_) => {
                    panic!("there should not be any batch when committing sst");
                }
                UncommittedData::Sst(sst) => {
                    assert!(
                        is_remote_sst_id(sst.id),
                        "all sst should be remote when trying to get ssts to commit"
                    );
                    ret.push(sst.clone());
                }
            }
        }
        ret
    }

    pub fn size(&self) -> usize {
        self.upload_batches_size + self.replicate_batches_size
    }

    fn get_next_order_index(&mut self) -> OrderIndex {
        let ret = self.next_order_index;
        self.next_order_index += 1;
        ret
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_hummock_sdk::key::{key_with_epoch, user_key};

    use super::*;
    use crate::hummock::iterator::test_utils::iterator_test_value_of;
    use crate::hummock::HummockValue;

    async fn generate_and_write_batch(
        put_keys: &[Vec<u8>],
        delete_keys: &[Vec<u8>],
        epoch: u64,
        idx: &mut usize,
        shared_buffer: &mut SharedBuffer,
        is_replicate: bool,
    ) -> SharedBufferBatch {
        let mut shared_buffer_items = Vec::new();
        for key in put_keys {
            shared_buffer_items.push((
                Bytes::from(key_with_epoch(key.clone(), epoch)),
                HummockValue::put(iterator_test_value_of(*idx).into()),
            ));
            *idx += 1;
        }
        for key in delete_keys {
            shared_buffer_items.push((
                Bytes::from(key_with_epoch(key.clone(), epoch)),
                HummockValue::delete(),
            ));
        }
        shared_buffer_items.sort_by(|l, r| user_key(&l.0).cmp(&r.0));
        let batch =
            SharedBufferBatch::new(shared_buffer_items, epoch, Arc::new(AtomicUsize::new(0)));
        if is_replicate {
            shared_buffer.replicate_batch(batch.clone());
        } else {
            shared_buffer.write_batch(batch.clone());
        }
        batch
    }

    #[tokio::test]
    async fn test_get_overlap_batches() {
        let mut shared_buffer = SharedBuffer::default();
        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let large_key = format!("key_test_{:05}", 9).as_bytes().to_vec();
        let mut idx = 0;

        // Write two batches in epoch1
        let epoch1 = 1;

        // Write to upload buffer
        let shared_buffer_batch1 = generate_and_write_batch(
            &keys[0..3],
            &[],
            epoch1,
            &mut idx,
            &mut shared_buffer,
            false,
        )
        .await;

        // Write to replicate buffer
        let shared_buffer_batch2 =
            generate_and_write_batch(&keys[0..3], &[], epoch1, &mut idx, &mut shared_buffer, true)
                .await;

        // Get overlap batches and verify
        for key in &keys[0..3] {
            // Single key
            let (replicate_batches, overlap_data) =
                shared_buffer.get_overlap_data(&(key.clone()..=key.clone()), None);
            assert_eq!(overlap_data.len(), 1);
            assert_eq!(
                overlap_data[0],
                vec![UncommittedData::Batch(shared_buffer_batch1.clone())],
            );
            assert_eq!(replicate_batches.len(), 1);
            assert_eq!(replicate_batches[0], shared_buffer_batch2);

            // Forward key range
            let (replicate_batches, overlap_data) =
                shared_buffer.get_overlap_data(&(key.clone()..=keys[3].clone()), None);
            assert_eq!(overlap_data.len(), 1);
            assert_eq!(
                overlap_data[0],
                vec![UncommittedData::Batch(shared_buffer_batch1.clone())],
            );
            assert_eq!(replicate_batches.len(), 1);
            assert_eq!(replicate_batches[0], shared_buffer_batch2);
        }
        // Non-existent key
        let (replicate_batches, overlap_data) =
            shared_buffer.get_overlap_data(&(large_key.clone()..=large_key.clone()), None);
        assert!(replicate_batches.is_empty());
        assert!(overlap_data.is_empty());

        // Non-existent key range forward
        let (replicate_batches, overlap_data) =
            shared_buffer.get_overlap_data(&(keys[3].clone()..=large_key.clone()), None);
        assert!(replicate_batches.is_empty());
        assert!(overlap_data.is_empty());
    }
}
