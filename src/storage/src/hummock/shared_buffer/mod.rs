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

use std::collections::HashMap;
use std::ops::RangeBounds;

use itertools::Itertools;

use self::shared_buffer_batch::SharedBufferBatch;
use crate::hummock::shared_buffer::shared_buffer_batch::IndexedSharedBufferBatches;
use crate::hummock::shared_buffer::shared_buffer_uploader::{UploadTaskId, UploadTaskPayload};
use crate::hummock::utils::range_overlap;

#[derive(Default, Debug)]
pub struct SharedBuffer {
    non_upload_batches: IndexedSharedBufferBatches,
    replicate_batches: IndexedSharedBufferBatches,
    uploading_batches: HashMap<UploadTaskId, IndexedSharedBufferBatches>,

    upload_batches_size: usize,
    replicate_batches_size: usize,

    next_upload_task_id: UploadTaskId,
}

impl SharedBuffer {
    pub fn write_batch(&mut self, batch: SharedBufferBatch) {
        self.upload_batches_size += batch.size();
        self.non_upload_batches
            .insert(batch.end_user_key().to_vec(), batch);
    }

    pub fn replicate_batch(&mut self, batch: SharedBufferBatch) {
        self.replicate_batches_size += batch.size();
        self.replicate_batches
            .insert(batch.end_user_key().to_vec(), batch);
    }

    // Gets batches from shared buffer that overlap with the given key range.
    pub fn get_overlap_batches<R, B>(&self, key_range: &R) -> Vec<SharedBufferBatch>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        let range = (
            key_range.start_bound().map(|b| b.as_ref().to_vec()),
            std::ops::Bound::Unbounded,
        );
        self.non_upload_batches
            .range(range.clone())
            .chain(self.replicate_batches.range(range.clone()))
            .chain(
                self.uploading_batches
                    .values()
                    .flat_map(|batches| batches.range(range.clone())),
            )
            .filter(|m| range_overlap(key_range, m.1.start_user_key(), m.1.end_user_key()))
            .map(|entry| entry.1.clone())
            .collect()
    }

    pub fn delete_batch(&mut self, batches: &[SharedBufferBatch]) {
        for batch in batches {
            if let Some(batch) = &self.non_upload_batches.remove(batch.end_user_key()) {
                self.upload_batches_size -= batch.size();
            }
        }
    }

    pub fn clear_replicate_batch(&mut self) {
        self.replicate_batches.clear();
        self.replicate_batches_size = 0;
    }

    pub fn new_upload_task(
        &mut self,
        task_gen: impl Fn(&mut IndexedSharedBufferBatches) -> IndexedSharedBufferBatches,
    ) -> (UploadTaskId, UploadTaskPayload) {
        let task_id = self.next_upload_task_id;
        self.next_upload_task_id += 1;
        let indexed_batches = task_gen(&mut self.non_upload_batches);
        let batches = indexed_batches.values().cloned().collect_vec();
        self.uploading_batches.insert(task_id, indexed_batches);
        (task_id, batches)
    }

    pub fn fail_upload_task(&mut self, upload_task_id: UploadTaskId) {
        debug_assert!(self.uploading_batches.contains_key(&upload_task_id));
        let task_batches = self.uploading_batches.remove(&upload_task_id).unwrap();
        self.non_upload_batches.extend(task_batches);
    }

    pub fn succeed_upload_task(&mut self, upload_task_id: UploadTaskId) {
        debug_assert!(self.uploading_batches.contains_key(&upload_task_id));
        let task_batches = self.uploading_batches.remove(&upload_task_id).unwrap();
        for batch in task_batches.into_values() {
            self.upload_batches_size -= batch.size();
        }
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
            let overlap_batches = shared_buffer.get_overlap_batches(&(key.clone()..=key.clone()));
            assert_eq!(overlap_batches.len(), 2);
            assert_eq!(overlap_batches[0], shared_buffer_batch1);
            assert_eq!(overlap_batches[1], shared_buffer_batch2);

            // Forward key range
            let overlap_batches =
                shared_buffer.get_overlap_batches(&(key.clone()..=keys[3].clone()));
            assert_eq!(overlap_batches.len(), 2);
            assert_eq!(overlap_batches[0], shared_buffer_batch1);
            assert_eq!(overlap_batches[1], shared_buffer_batch2);
        }
        // Non-existent key
        let overlap_batches =
            shared_buffer.get_overlap_batches(&(large_key.clone()..=large_key.clone()));
        assert!(overlap_batches.is_empty());

        // Non-existent key range forward
        let overlap_batches =
            shared_buffer.get_overlap_batches(&(keys[3].clone()..=large_key.clone()));
        assert!(overlap_batches.is_empty());
    }
}
