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

use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use super::shared_buffer_batch::{SharedBufferBatch, SharedBufferItem};
use super::shared_buffer_uploader::{SharedBufferUploader, UploadItem};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::utils::range_overlap;
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

/// `{ end key -> batch }`
pub type EpochSharedBuffer = BTreeMap<Vec<u8>, SharedBufferBatch>;

#[derive(Default)]
pub struct SharedBufferManagerCore {
    pub buffer: BTreeMap<HummockEpoch, EpochSharedBuffer>,
    pub size: usize,
}

struct SharedBufferUploaderHandle {
    join_handle: Option<JoinHandle<HummockResult<()>>>,
    rx: Option<UnboundedReceiver<UploadItem>>,
}

impl SharedBufferUploaderHandle {
    fn new(rx: UnboundedReceiver<UploadItem>) -> Self {
        Self {
            join_handle: None,
            rx: Some(rx),
        }
    }
}

pub struct SharedBufferManager {
    capacity: usize,

    core: Arc<RwLock<SharedBufferManagerCore>>,

    uploader_tx: mpsc::UnboundedSender<UploadItem>,
    uploader_handle: Mutex<SharedBufferUploaderHandle>,
}

impl SharedBufferManager {
    pub fn new(capacity: usize) -> Self {
        let (uploader_tx, uploader_rx) = mpsc::unbounded_channel();
        Self {
            capacity,
            core: Arc::new(RwLock::new(SharedBufferManagerCore::default())),
            uploader_tx,
            uploader_handle: Mutex::new(SharedBufferUploaderHandle::new(uploader_rx)),
        }
    }

    pub fn start_uploader(
        &self,
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let mut handle = self.uploader_handle.lock();
        if let Some(uploader_rx) = handle.rx.take() {
            let mut uploader = SharedBufferUploader::new(
                options.clone(),
                options.shared_buffer_threshold as usize,
                sstable_store,
                local_version_manager,
                hummock_meta_client,
                self.core.clone(),
                uploader_rx,
                stats,
            );
            handle.join_handle = Some(tokio::spawn(async move { uploader.run().await }));
        }
    }

    pub fn size(&self) -> usize {
        self.core.read().size
    }

    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    /// Puts a write batch into shared buffer. The batch will be synced to S3 asynchronously.
    pub async fn write_batch(
        &self,
        items: Vec<SharedBufferItem>,
        epoch: HummockEpoch,
    ) -> HummockResult<u64> {
        let batch = SharedBufferBatch::new(items, epoch);
        let batch_size = batch.size;

        // TODO: Uncomment the following lines after flushed sstable can be accessed.
        // FYI: https://github.com/singularity-data/risingwave/pull/1928#discussion_r852698719
        // Stall writes that attempting to exceeds capacity.
        // Actively trigger flush, suspend, and wait until some flush completed.
        // while self.size() + batch_size as usize > self.capacity {
        //     self.sync(None).await?;
        // }

        let mut guard = self.core.write();
        guard
            .buffer
            .entry(epoch)
            .or_insert_with(BTreeMap::default)
            .insert(batch.end_user_key().to_vec(), batch.clone());
        guard.size += batch_size as usize;

        self.uploader_tx
            .send(UploadItem::Batch(batch))
            .map_err(HummockError::shared_buffer_error)?;

        Ok(batch_size)
    }

    /// This function was called while [`SharedBufferManager`] exited.
    pub async fn wait(self) -> HummockResult<()> {
        let join_handle = self.uploader_handle.lock().join_handle.take();
        join_handle.unwrap().await.unwrap()
    }

    pub async fn sync(&self, epoch: Option<HummockEpoch>) -> HummockResult<()> {
        if self.is_empty() {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.uploader_tx
            .send(UploadItem::Sync {
                epoch,
                notifier: tx,
            })
            .map_err(HummockError::shared_buffer_error)?;
        rx.await.map_err(HummockError::shared_buffer_error)?;
        Ok(())
    }

    // Gets batches from shared buffer that overlap with the given key range.
    // The returned batches are ordered by epoch desendingly.
    pub fn get_overlap_batches<R, B>(
        &self,
        key_range: &R,
        epoch_range: impl RangeBounds<u64>,
        reversed_range: bool,
    ) -> Vec<SharedBufferBatch>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        self.core
            .read()
            .buffer
            .range(epoch_range)
            .rev()
            .flat_map(|entry| {
                entry
                    .1
                    .range((
                        if reversed_range {
                            key_range.end_bound().map(|b| b.as_ref().to_vec())
                        } else {
                            key_range.start_bound().map(|b| b.as_ref().to_vec())
                        },
                        std::ops::Bound::Unbounded,
                    ))
                    .filter(|m| {
                        range_overlap(
                            key_range,
                            m.1.start_user_key(),
                            m.1.end_user_key(),
                            reversed_range,
                        )
                    })
            })
            .map(|e| e.1.clone())
            .collect_vec()
    }

    /// Marks an epoch as committed in shared buffer.
    /// This will delete shared buffers before a given `epoch` inclusively.
    pub fn commit_epoch(&self, epoch: HummockEpoch) {
        let mut guard = self.core.write();
        // buffer = newer part
        let mut buffer = guard.buffer.split_off(&(epoch + 1));
        // buffer = older part, core.buffer = new part
        std::mem::swap(&mut buffer, &mut guard.buffer);
        for (_epoch, batches) in buffer {
            for (_end_user_key, batch) in batches {
                guard.size -= batch.size as usize
            }
        }
    }

    /// Puts a write batch into shared buffer. The batch won't be synced to S3 asynchronously.
    pub fn replicate_remote_batch(
        &self,
        batch: Vec<SharedBufferItem>,
        epoch: u64,
    ) -> HummockResult<()> {
        let batch = SharedBufferBatch::new(batch, epoch);
        let batch_size = batch.size as usize;
        let mut guard = self.core.write();
        guard
            .buffer
            .entry(epoch)
            .or_insert(BTreeMap::new())
            .insert(batch.end_user_key().to_vec(), batch.clone());
        guard.size += batch_size;
        Ok(())
    }

    /// Deletes specific batches in shared buffer.
    /// This method should be invoked after the batches are uploaded to S3.
    pub fn delete_batches(&self, epoch: u64, batches: Vec<SharedBufferBatch>) {
        let mut removed_size = 0;
        let mut guard = self.core.write();
        let epoch_buffer = guard.buffer.get_mut(&epoch).unwrap();
        for batch in batches {
            if let Some(removed_batch) = epoch_buffer.remove(batch.end_user_key()) {
                removed_size += removed_batch.size as usize;
            }
        }
        guard.size -= removed_size;
    }

    #[cfg(test)]
    pub fn get_shared_buffer(&self) -> BTreeMap<u64, EpochSharedBuffer> {
        self.core.read().buffer.clone()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use risingwave_hummock_sdk::key::{key_with_epoch, user_key};

    use super::*;
    use crate::hummock::iterator::test_utils::iterator_test_value_of;
    use crate::hummock::test_utils::default_config_for_test;
    use crate::hummock::value::HummockValue;

    async fn generate_and_write_batch(
        put_keys: &[Vec<u8>],
        delete_keys: &[Vec<u8>],
        epoch: u64,
        idx: &mut usize,
        shared_buffer_manager: &SharedBufferManager,
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
        shared_buffer_manager
            .write_batch(shared_buffer_items.clone(), epoch)
            .await
            .unwrap();
        SharedBufferBatch::new(shared_buffer_items, epoch)
    }

    #[tokio::test]
    async fn test_get_overlap_batches() {
        let shared_buffer_manager =
            SharedBufferManager::new(default_config_for_test().shared_buffer_capacity as usize);
        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let large_key = format!("key_test_{:05}", 9).as_bytes().to_vec();
        let mut idx = 0;

        // Write a batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_batch1 = generate_and_write_batch(
            put_keys_in_epoch1,
            &[],
            epoch1,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Write a batch in epoch2, with key1 overlapping with epoch1 and key2 deleted.
        let epoch2 = epoch1 + 1;
        let put_keys_in_epoch2 = &[&keys[1..2], &keys[3..4]].concat();
        let shared_buffer_batch2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Case1: Get overlap batches in ..=epoch1
        for key in put_keys_in_epoch1 {
            // Single key
            let overlap_batches = shared_buffer_manager.get_overlap_batches(
                &(key.clone()..=key.clone()),
                ..=epoch1,
                false,
            );
            assert_eq!(overlap_batches.len(), 1);
            assert_eq!(overlap_batches[0], shared_buffer_batch1);

            // Forward key range
            let overlap_batches = shared_buffer_manager.get_overlap_batches(
                &(key.clone()..=keys[3].clone()),
                ..=epoch1,
                false,
            );
            assert_eq!(overlap_batches.len(), 1);
            assert_eq!(overlap_batches[0], shared_buffer_batch1);

            // Backward key range
            let overlap_batches = shared_buffer_manager.get_overlap_batches(
                &(keys[3].clone()..=key.clone()),
                ..=epoch1,
                true,
            );
            assert_eq!(overlap_batches.len(), 1);
            assert_eq!(overlap_batches[0], shared_buffer_batch1);
        }
        // Non-existent key
        let overlap_batches = shared_buffer_manager.get_overlap_batches(
            &(large_key.clone()..=large_key.clone()),
            ..=epoch1,
            false,
        );
        assert!(overlap_batches.is_empty());

        // Non-existent key range forward
        let overlap_batches = shared_buffer_manager.get_overlap_batches(
            &(keys[3].clone()..=large_key.clone()),
            ..=epoch1,
            false,
        );
        assert!(overlap_batches.is_empty());

        // Non-existent key range backward
        let overlap_batches = shared_buffer_manager.get_overlap_batches(
            &(large_key.clone()..=keys[3].clone()),
            ..=epoch1,
            true,
        );
        assert!(overlap_batches.is_empty());

        // Case2: Get overlap batches in ..=epoch2
        // Expected behavior:
        // - Case2.1: keys[0] -> batch1
        // - Case2.2: keys[1], keys[2] -> batch2, batch1
        // - Case2.3: keys[3] -> batch2

        // Case2.1
        let overlap_batches = shared_buffer_manager.get_overlap_batches(
            &(keys[0].clone()..=keys[0].clone()),
            ..=epoch2,
            false,
        );
        assert_eq!(overlap_batches.len(), 1);
        assert_eq!(overlap_batches[0], shared_buffer_batch1);

        // Case2.2
        for i in 1..=2 {
            // Single key
            let overlap_batches = shared_buffer_manager.get_overlap_batches(
                &(keys[i].clone()..=keys[i].clone()),
                ..=epoch2,
                false,
            );
            assert_eq!(overlap_batches.len(), 2);
            // Ordering matters. Fresher batches should be placed before older batches.
            assert_eq!(overlap_batches[0], shared_buffer_batch2);
            assert_eq!(overlap_batches[1], shared_buffer_batch1);

            // Forward key range
            let overlap_batches = shared_buffer_manager.get_overlap_batches(
                &(keys[0].clone()..=keys[i].clone()),
                ..=epoch2,
                false,
            );
            assert_eq!(overlap_batches.len(), 2);
            // Ordering matters. Fresher batches should be placed before older batches.
            assert_eq!(overlap_batches[0], shared_buffer_batch2);
            assert_eq!(overlap_batches[1], shared_buffer_batch1);

            // Backward key range
            let overlap_batches = shared_buffer_manager.get_overlap_batches(
                &(keys[i].clone()..=keys[0].clone()),
                ..=epoch2,
                true,
            );
            assert_eq!(overlap_batches.len(), 2);
            // Ordering matters. Fresher batches should be placed before older batches.
            assert_eq!(overlap_batches[0], shared_buffer_batch2);
            assert_eq!(overlap_batches[1], shared_buffer_batch1);
        }

        // Case2.3
        // Single key
        let overlap_batches = shared_buffer_manager.get_overlap_batches(
            &(keys[3].clone()..=keys[3].clone()),
            ..=epoch2,
            false,
        );
        assert_eq!(overlap_batches.len(), 1);
        assert_eq!(overlap_batches[0], shared_buffer_batch2);

        // Forward range
        let overlap_batches = shared_buffer_manager.get_overlap_batches(
            &(keys[3].clone()..=large_key.clone()),
            ..=epoch2,
            false,
        );
        assert_eq!(overlap_batches.len(), 1);
        assert_eq!(overlap_batches[0], shared_buffer_batch2);

        // Backward range
        let overlap_batches = shared_buffer_manager.get_overlap_batches(
            &(large_key.clone()..=keys[3].clone()),
            ..=epoch2,
            true,
        );
        assert_eq!(overlap_batches.len(), 1);
        assert_eq!(overlap_batches[0], shared_buffer_batch2);

        // Case3: Get overlap batches in epoch2..=epoch2
        for key in put_keys_in_epoch2 {
            // Single key
            let overlap_batches = shared_buffer_manager.get_overlap_batches(
                &(key.clone()..=key.clone()),
                epoch2..=epoch2,
                false,
            );
            assert_eq!(overlap_batches.len(), 1);
            assert_eq!(overlap_batches[0], shared_buffer_batch2);

            // Forward key range
            let overlap_batches = shared_buffer_manager.get_overlap_batches(
                &(key.clone()..=keys[3].clone()),
                epoch2..=epoch2,
                false,
            );
            assert_eq!(overlap_batches.len(), 1);
            assert_eq!(overlap_batches[0], shared_buffer_batch2);

            // Backward key range
            let overlap_batches = shared_buffer_manager.get_overlap_batches(
                &(keys[3].clone()..=key.clone()),
                epoch2..=epoch2,
                true,
            );
            assert_eq!(overlap_batches.len(), 1);
            assert_eq!(overlap_batches[0], shared_buffer_batch2);
        }
        // Non-existent key
        let overlap_batches = shared_buffer_manager.get_overlap_batches(
            &(keys[0].clone()..=keys[0].clone()),
            epoch2..=epoch2,
            false,
        );
        assert!(overlap_batches.is_empty());
    }
}
