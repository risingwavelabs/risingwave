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
use std::ops::RangeBounds;

use itertools::Itertools;
use risingwave_hummock_sdk::is_remote_sst_id;
use risingwave_hummock_sdk::key::user_key;
use risingwave_pb::hummock::{KeyRange, SstableInfo, VNodeBitmap};

use self::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::shared_buffer::shared_buffer_uploader::{UploadTaskId, UploadTaskPayload};
use crate::hummock::utils::{filter_single_sst, range_overlap};

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

/// `{ (end key) -> batch }`
pub(crate) type KeyIndexedUncommittedData = BTreeMap<Vec<u8>, UncommittedData>;

#[derive(Default, Debug)]
pub struct SharedBuffer {
    uncommitted_data: KeyIndexedUncommittedData,
    replicate_batches: BTreeMap<Vec<u8>, SharedBufferBatch>,
    uploading_tasks: HashMap<UploadTaskId, KeyIndexedUncommittedData>,
    upload_batches_size: usize,
    replicate_batches_size: usize,

    next_upload_task_id: UploadTaskId,
}

impl SharedBuffer {
    pub fn write_batch(&mut self, batch: SharedBufferBatch) {
        self.upload_batches_size += batch.size();
        let insert_result = self
            .uncommitted_data
            .insert(batch.end_user_key().to_vec(), UncommittedData::Batch(batch));
        assert!(
            insert_result.is_none(),
            "duplicate end key and order index when inserting a write batch. Previous data: {:?}",
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
    ) -> (Vec<SharedBufferBatch>, Vec<UncommittedData>)
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
            key_range.start_bound().map(|b| b.as_ref().to_vec()),
            std::ops::Bound::Unbounded,
        );

        let uncommitted_data = self
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
            .map(|(_, data)| data.clone())
            .collect_vec();

        (replicated_batches, uncommitted_data)
    }

    pub fn clear_replicate_batch(&mut self) {
        self.replicate_batches.clear();
        self.replicate_batches_size = 0;
    }

    pub fn new_upload_task(&mut self) -> Option<(UploadTaskId, UploadTaskPayload)> {
        // TODO: use drain_filter when it's stable.
        let task_keys = self
            .uncommitted_data
            .iter()
            .filter_map(|(key, data)| match data {
                UncommittedData::Batch(_) => Some(key.clone()),
                UncommittedData::Sst(_) => None,
            })
            .collect_vec();
        let mut keyed_payload = BTreeMap::new();
        for key in task_keys {
            let payload = self.uncommitted_data.remove(&key).unwrap();
            keyed_payload.insert(key, payload);
        }
        if keyed_payload.is_empty() {
            return None;
        }
        let task_payload = keyed_payload
            .values()
            .map(|data| match data {
                UncommittedData::Batch(batch) => batch.clone(),
                UncommittedData::Sst(_) => unreachable!("SST should not be in task payload"),
            })
            .collect_vec();
        let task_id = self.next_upload_task_id;
        self.next_upload_task_id += 1;
        self.uploading_tasks.insert(task_id, keyed_payload);
        Some((task_id, task_payload))
    }

    pub fn fail_upload_task(&mut self, upload_task_id: UploadTaskId) {
        debug_assert!(
            self.uploading_tasks.contains_key(&upload_task_id),
            "the task id should exist {} when fail an upload task",
            upload_task_id
        );
        let payload = self.uploading_tasks.remove(&upload_task_id).unwrap();
        self.uncommitted_data.extend(payload);
    }

    pub fn succeed_upload_task(&mut self, upload_task_id: UploadTaskId, new_sst: Vec<SstableInfo>) {
        debug_assert!(
            self.uploading_tasks.contains_key(&upload_task_id),
            "the task_id should exist {} when succeed an upload task",
            upload_task_id
        );
        let payload = self.uploading_tasks.remove(&upload_task_id).unwrap();
        for sst in new_sst {
            let data = UncommittedData::Sst(sst);
            let insert_result = self
                .uncommitted_data
                .insert(data.end_user_key().to_vec(), data);
            assert!(
                insert_result.is_none(),
                "duplicate data end key when inserting an SST. Previous data: {:?}",
                insert_result,
            );
        }
        for data in payload.into_values() {
            match data {
                UncommittedData::Batch(batch) => {
                    self.upload_batches_size -= batch.size();
                }
                UncommittedData::Sst(_) => unreachable!("SST should not be in task payload"),
            }
        }
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
                UncommittedData::Batch(shared_buffer_batch1.clone())
            );
            assert_eq!(replicate_batches.len(), 1);
            assert_eq!(replicate_batches[0], shared_buffer_batch2);

            // Forward key range
            let (replicate_batches, overlap_data) =
                shared_buffer.get_overlap_data(&(key.clone()..=keys[3].clone()), None);
            assert_eq!(overlap_data.len(), 1);
            assert_eq!(
                overlap_data[0],
                UncommittedData::Batch(shared_buffer_batch1.clone()),
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
