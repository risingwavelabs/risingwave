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

mod immutable_memtable;
pub mod shared_buffer_batch;
#[expect(dead_code)]
pub mod shared_buffer_uploader;
use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

pub use immutable_memtable::ImmutableMemtable;
use itertools::Itertools;
use risingwave_hummock_sdk::key::user_key;
use risingwave_hummock_sdk::{HummockEpoch, LocalSstableInfo};
use risingwave_pb::hummock::{KeyRange, SstableInfo};
use tokio::sync::oneshot;

use self::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::iterator::{
    HummockIteratorDirection, HummockIteratorUnion, OrderedMergeIteratorInner,
    UnorderedMergeIteratorInner,
};
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatchIterator;
use crate::hummock::sstable::SstableIteratorReadOptions;
use crate::hummock::state_store::HummockIteratorType;
use crate::hummock::utils::range_overlap;
use crate::hummock::{HummockResult, SstableIteratorType, SstableStore};
use crate::monitor::{StateStoreMetrics, StoreLocalStatistic};
use crate::store::SyncResult;

#[derive(Debug, Clone, PartialEq)]
pub enum UncommittedData {
    Sst(LocalSstableInfo),
    Batch(SharedBufferBatch),
}

pub fn get_sst_key_range(info: &SstableInfo) -> &KeyRange {
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
            UncommittedData::Sst((_, info)) => {
                let key_range = get_sst_key_range(info);
                user_key(key_range.left.as_slice())
            }
            UncommittedData::Batch(batch) => batch.start_user_key(),
        }
    }

    pub fn end_user_key(&self) -> &[u8] {
        match self {
            UncommittedData::Sst((_, info)) => {
                let key_range = get_sst_key_range(info);
                user_key(key_range.right.as_slice())
            }
            UncommittedData::Batch(batch) => batch.end_user_key(),
        }
    }
}

pub(crate) type OrderIndex = usize;
/// `{ end_key -> batch }`
/// `{ (end key, order_id) -> batch }`
pub(crate) type KeyIndexedUncommittedData = BTreeMap<Vec<u8>, SharedBufferBatch>;
/// uncommitted data sorted by order index in descending order. Data in the same inner list share
/// the same order index, which means their keys don't overlap.
pub(crate) type OrderSortedUncommittedData = Vec<Vec<UncommittedData>>;

#[allow(type_alias_bounds)]
pub type UncommittedDataIteratorType<
    D: HummockIteratorDirection,
    I: SstableIteratorType<Direction = D>,
> = HummockIteratorUnion<D, SharedBufferBatchIterator<D>, I>;

#[allow(type_alias_bounds)]
pub type SharedBufferIteratorType<
    D: HummockIteratorDirection,
    I: SstableIteratorType<Direction = D>,
> = OrderedMergeIteratorInner<
    HummockIteratorUnion<
        D,
        UncommittedDataIteratorType<D, I>,
        UnorderedMergeIteratorInner<UncommittedDataIteratorType<D, I>>,
    >,
>;

pub(crate) async fn build_ordered_merge_iter<T: HummockIteratorType>(
    uncommitted_data: &OrderSortedUncommittedData,
    sstable_store: Arc<SstableStore>,
    _stats: Arc<StateStoreMetrics>,
    local_stats: &mut StoreLocalStatistic,
    read_options: Arc<SstableIteratorReadOptions>,
) -> HummockResult<SharedBufferIteratorType<T::Direction, T::SstableIteratorType>> {
    let mut ordered_iters = Vec::with_capacity(uncommitted_data.len());
    for data_list in uncommitted_data {
        let mut data_iters = Vec::new();
        for data in data_list {
            match data {
                UncommittedData::Batch(batch) => {
                    data_iters.push(UncommittedDataIteratorType::First(
                        batch.clone().into_directed_iter::<T::Direction>(),
                    ));
                }
                UncommittedData::Sst((_, table_info)) => {
                    let table = sstable_store.sstable(table_info, local_stats).await?;
                    data_iters.push(UncommittedDataIteratorType::Second(
                        T::SstableIteratorType::create(
                            table,
                            sstable_store.clone(),
                            read_options.clone(),
                        ),
                    ));
                }
            }
        }
        if data_iters.is_empty() {
            continue;
        } else if data_iters.len() == 1 {
            ordered_iters.push(HummockIteratorUnion::First(data_iters.pop().unwrap()));
        } else {
            ordered_iters.push(HummockIteratorUnion::Second(
                UnorderedMergeIteratorInner::new(data_iters),
            ));
        }
    }
    Ok(OrderedMergeIteratorInner::new(ordered_iters))
}

#[derive(Debug, Clone)]
pub struct SharedBuffer {
    uncommitted_data: KeyIndexedUncommittedData,
    upload_batches_size: usize,

    global_upload_task_size: Arc<AtomicUsize>,

    next_order_index: usize,
}

#[derive(Debug)]
pub struct WriteRequest {
    pub batch: SharedBufferBatch,
    pub epoch: HummockEpoch,
    pub grant_sender: oneshot::Sender<()>,
}

#[derive(Debug)]
pub enum SharedBufferEvent {
    /// Notify that we may flush the shared buffer.
    FlushEnd(HummockEpoch),

    /// A shared buffer batch is released. The parameter is the batch size.
    BufferRelease(usize),

    /// An epoch is going to be synced. Once the event is processed, there will be no more flush
    /// task on this epoch. Previous concurrent flush task join handle will be returned by the join
    /// handle sender.
    SyncEpoch {
        new_sync_epoch: HummockEpoch,
        sync_result_sender: oneshot::Sender<HummockResult<SyncResult>>,
    },

    /// Clear shared buffer and reset all states
    Clear(oneshot::Sender<()>),

    Shutdown,
}

impl SharedBuffer {
    pub fn new(global_upload_task_size: Arc<AtomicUsize>) -> Self {
        Self {
            uncommitted_data: Default::default(),
            upload_batches_size: 0,
            global_upload_task_size,
            next_order_index: 0,
        }
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::new(Arc::new(AtomicUsize::new(0)))
    }

    pub fn write_batch(&mut self, batch: SharedBufferBatch) {
        self.upload_batches_size += batch.size();

        let insert_result = self
            .uncommitted_data
            .insert(batch.end_user_key().to_vec(), batch);
        assert!(
            insert_result.is_none(),
            "duplicate end key and order index when inserting a write batch. \
            previous data: {:?}",
            insert_result
        );
    }

    /// Gets batches from shared buffer that overlap with the given key range.
    /// The return tuple is (replicated batches, uncommitted data).
    pub fn get_overlap_data<R, B>(&self, key_range: &R) -> Vec<SharedBufferBatch>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let range = (
            match key_range.start_bound() {
                Bound::Included(key) => Bound::Included(key.as_ref().to_vec()),
                Bound::Excluded(key) => Bound::Excluded(key.as_ref().to_vec()),
                Bound::Unbounded => Bound::Unbounded,
            },
            std::ops::Bound::Unbounded,
        );
        let mut local_data = self
            .uncommitted_data
            .range(range.clone())
            .filter(|(_, batch)| {
                range_overlap(key_range, batch.start_user_key(), batch.end_user_key())
            })
            .map(|(_, data)| data.clone())
            .collect_vec();

        local_data.reverse();
        local_data
    }

    pub fn to_uncommitted_data(&self) -> Option<(Vec<SharedBufferBatch>, usize)> {
        if self.uncommitted_data.is_empty() {
            return None;
        }
        let keyed_payload = self.uncommitted_data.values().cloned().collect_vec();
        let task_write_batch_size = keyed_payload.iter().map(|batch| batch.size()).sum();
        Some((keyed_payload, task_write_batch_size))
    }

    pub fn size(&self) -> usize {
        self.upload_batches_size
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::ops::DerefMut;

    use bytes::Bytes;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_hummock_sdk::key::{key_with_epoch, user_key};
    use tokio::sync::mpsc;

    use super::*;
    use crate::hummock::iterator::test_utils::iterator_test_value_of;
    use crate::hummock::test_utils::gen_dummy_sst_info;
    use crate::hummock::HummockValue;

    fn generate_and_write_batch(
        put_keys: &[Vec<u8>],
        delete_keys: &[Vec<u8>],
        epoch: u64,
        idx: &mut usize,
        shared_buffer: &mut SharedBuffer,
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
        let batch = SharedBufferBatch::new(
            shared_buffer_items,
            epoch,
            mpsc::unbounded_channel().0,
            StaticCompactionGroupId::StateDefault.into(),
            Default::default(),
        );
        shared_buffer.write_batch(batch.clone());

        batch
    }

    #[tokio::test]
    async fn test_get_overlap_batches() {
        let mut shared_buffer = SharedBuffer::for_test();
        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let large_key = format!("key_test_{:05}", 9).as_bytes().to_vec();
        let mut idx = 0;

        // Write two batches in epoch1
        let epoch1 = 1;

        // Write to upload buffer
        let shared_buffer_batch1 =
            generate_and_write_batch(&keys[0..3], &[], epoch1, &mut idx, &mut shared_buffer);

        // Get overlap batches and verify
        for key in &keys[0..3] {
            // Single key
            let overlap_data = shared_buffer.get_overlap_data(&(key.clone()..=key.clone()));
            assert_eq!(overlap_data.len(), 1);
            assert_eq!(
                overlap_data[0],
                vec![UncommittedData::Batch(shared_buffer_batch1.clone())],
            );

            // Forward key range
            let overlap_data = shared_buffer.get_overlap_data(&(key.clone()..=keys[3].clone()));
            assert_eq!(overlap_data.len(), 1);
            assert_eq!(
                overlap_data[0],
                vec![UncommittedData::Batch(shared_buffer_batch1.clone())],
            );
        }
        // Non-existent key
        let overlap_data = shared_buffer.get_overlap_data(&(large_key.clone()..=large_key.clone()));
        assert!(overlap_data.is_empty());

        // Non-existent key range forward
        let overlap_data = shared_buffer.get_overlap_data(&(keys[3].clone()..=large_key));
        assert!(overlap_data.is_empty());
    }

    #[tokio::test]
    async fn test_new_upload_task() {
        let shared_buffer = RefCell::new(SharedBuffer::for_test());
        let mut idx = 0;
        let mut generate_test_data = |key: &str| {
            generate_and_write_batch(
                &[key.as_bytes().to_vec()],
                &[],
                1,
                &mut idx,
                shared_buffer.borrow_mut().deref_mut(),
            )
        };

        let batch1 = generate_test_data("aa");
        let batch2 = generate_test_data("bb");

        let (order_index1, payload1, task_size) =
            shared_buffer.borrow_mut().new_upload_task().unwrap();
        assert_eq!(order_index1, 0);
        assert_eq!(2, payload1.len());
        assert_eq!(payload1[0].len(), 1);
        assert_eq!(payload1[0], vec![UncommittedData::Batch(batch2.clone())]);
        assert_eq!(payload1[1].len(), 1);
        assert_eq!(payload1[1], vec![UncommittedData::Batch(batch1.clone())]);
        assert_eq!(task_size, batch1.size() + batch2.size());

        let batch3 = generate_test_data("cc");
        let batch4 = generate_test_data("dd");

        let (order_index2, payload2, task_size) =
            shared_buffer.borrow_mut().new_upload_task().unwrap();
        assert_eq!(order_index2, 2);
        assert_eq!(2, payload2.len());
        assert_eq!(payload2[0].len(), 1);
        assert_eq!(payload2[0], vec![UncommittedData::Batch(batch4.clone())]);
        assert_eq!(payload2[1].len(), 1);
        assert_eq!(payload2[1], vec![UncommittedData::Batch(batch3.clone())]);
        assert_eq!(task_size, batch3.size() + batch4.size());

        shared_buffer.borrow_mut().fail_upload_task(order_index1);
        let (order_index1, payload1, task_size) =
            shared_buffer.borrow_mut().new_upload_task().unwrap();
        assert_eq!(order_index1, 0);
        assert_eq!(2, payload1.len());
        assert_eq!(payload1[0].len(), 1);
        assert_eq!(payload1[0], vec![UncommittedData::Batch(batch2.clone())]);
        assert_eq!(payload1[1].len(), 1);
        assert_eq!(payload1[1], vec![UncommittedData::Batch(batch1.clone())]);
        assert_eq!(task_size, batch1.size() + batch2.size());

        let sst1 = gen_dummy_sst_info(1, vec![batch1, batch2]);
        shared_buffer.borrow_mut().succeed_upload_task(
            order_index1,
            vec![(StaticCompactionGroupId::StateDefault.into(), sst1)],
        );
    }
}
