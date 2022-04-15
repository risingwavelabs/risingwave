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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::error::StorageResult;
use crate::hummock::iterator::variants::*;
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::shared_buffer::shared_buffer_batch::{
    SharedBufferBatch, SharedBufferBatchIterator, SharedBufferItem,
};
use crate::hummock::shared_buffer::shared_buffer_uploader::{
    EpochNotifier, SharedBufferUploader, SharedBufferUploaderItem,
};
use crate::hummock::utils::range_overlap;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockEpoch, HummockError, HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

pub struct SharedBufferManagerInner {
    /// `shared_buffer` is a collection of immutable batches grouped by (epoch, end_key)
    shared_buffer: BTreeMap<HummockEpoch, BTreeMap<Vec<u8>, SharedBufferBatch>>,
    /// Total used buffer size.
    buffered_size: usize,
}

/// A manager to manage reads and writes on shared buffer.
/// Shared buffer is a node level abstraction to buffer write batches across executors.
///
/// # Resource Governance
///
/// There are 2 waterlines in shared buffer: `threshold` and `capacity`.
///
/// `threshold` : When buffer usage reaches `threshold`, `flush` will be triggered to spill data to
/// S3 and make room for shared buffer.
///
/// `capacity` : When buffer usage reaches `capacity`, writes will stall until there is enough room.
///
/// # When will the functions be called?
///
/// ## `write_batch`
///
/// `write_batch` insert data batch into shared buffer and register it in uploader.
///
/// When `Hummock::ingest_batch` is called, `write_batch` will be called.
///
/// There are 2 cases that `ingest_batch` will be called:
///
/// 1. Executor receives a barrier.
/// 2. Anytime even between two barriers (in future, AKA write anytime).
///
/// ## `flush`
///
/// `flush` is used to notify uploader that data batches in shared buffer that haven't been uploaded
/// to S3 can be uploaded now. `flush` returns immediately and will NOT wait for uploading. After
/// data batches are successfully uploaded, flushed data batches will be deleted asynchronously.
///
/// There are 2 cases that `flush` will be called:
///
/// 1. Size of shared buffer reaches `threshold`.
/// 2. When `sycn` is called.
///
/// Additionally, uploader will also be notified to upload batches by timeout. See the loop in
/// `SharedBufferUploader::run`.
///
/// ## `sync`
///
/// `sync` is also used to notify uploader that data batches in shared buffer that haven't been
/// uploaded to S3 can ne uploaded. Unlike `flush`, `sync` will wait until data batches are
/// successfully uploaded.
///
/// `sync` is called when `LocalBarrierManager` collected all barriers of the node and waits all
/// data beloned to the epoch for uploading to S3.
///
/// ## `delete_before`
///
/// `delete_before` is used to clean buffered batches in shared buffer. Make sure `sync` has been
/// called on to delete epoches.
pub struct SharedBufferManager {
    inner: RwLock<SharedBufferManagerInner>,
    /// Threshold to trigger flushing.
    threshold: usize,
    /// Maximum memory capacity of shared buffer, any writes that will exceed capacity will stall
    /// until there is space.
    capacity: usize,

    /// Shared buffer that has been scheduled to upload (uploading & uploaded).
    scheduled_size: Arc<AtomicUsize>,

    release_notifier: Notify,
    uploader_notifier: Arc<Notify>,
    uploader_tx: tokio::sync::mpsc::UnboundedSender<SharedBufferUploaderItem>,
    uploader_handle: JoinHandle<StorageResult<()>>,
}

impl SharedBufferManager {
    pub fn new(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        sstable_store: SstableStoreRef,
        // TODO: separate `HummockStats` from `StateStoreMetrics`.
        state_store_stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> Self {
        let (uploader_tx, uploader_rx) = tokio::sync::mpsc::unbounded_channel();
        let uploader_notifier = Arc::new(Notify::new());
        let scheduled_size = Arc::new(AtomicUsize::new(0));

        let uploader = SharedBufferUploader::new(
            options.clone(),
            local_version_manager,
            sstable_store,
            state_store_stats,
            hummock_meta_client,
            scheduled_size.clone(),
            uploader_notifier.clone(),
            uploader_rx,
        );
        let uploader_handle = tokio::spawn(uploader.run());

        Self {
            inner: RwLock::new(SharedBufferManagerInner {
                shared_buffer: BTreeMap::default(),
                buffered_size: 0,
            }),
            threshold: options.shared_buffer_flush_threshold as usize,
            capacity: options.shared_buffer_capacity as usize,

            scheduled_size,

            release_notifier: Notify::new(),
            uploader_notifier,
            uploader_tx,
            uploader_handle,
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().buffered_size
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn scheduled_size(&self) -> usize {
        self.scheduled_size.load(Ordering::Acquire)
    }

    /// Puts a write batch into shared buffer. The batch will be synced to S3 asynchronously.
    pub async fn write_batch(
        &self,
        batch: Vec<SharedBufferItem>,
        epoch: HummockEpoch,
    ) -> HummockResult<usize> {
        let batch = SharedBufferBatch::new(batch, epoch);
        let batch_size = batch.len();

        // Stall writes if there is no quota.
        loop {
            let buffer_size = self.len();
            if buffer_size == 0 || buffer_size + batch_size < self.capacity {
                break;
            }
            self.uploader_notifier.notify_one();
            self.release_notifier.notified().await;
        }

        let mut inner = self.inner.write();
        inner
            .shared_buffer
            .entry(epoch)
            .or_insert(BTreeMap::new())
            .insert(batch.end_user_key().to_vec(), batch.clone());
        inner.buffered_size += batch_size;
        let size = inner.buffered_size - self.scheduled_size.load(Ordering::Acquire);
        drop(inner);

        // Queue batch in uploader.
        self.uploader_tx
            .send(SharedBufferUploaderItem::Batch(batch))
            .map_err(HummockError::shared_buffer_error)?;

        // Notify uploader to flush if not-uploading buffer size exceeds threshold.
        if size > self.threshold {
            self.flush();
        }

        Ok(batch_size)
    }

    pub async fn sync(&self, epoch: Option<HummockEpoch>) -> HummockResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.uploader_tx
            .send(SharedBufferUploaderItem::Sync(EpochNotifier {
                epoch,
                notifier: Some(tx),
            }))
            .unwrap();
        rx.await.unwrap()?;
        Ok(())
    }

    fn flush(&self) {
        self.uploader_notifier.notify_one();
    }

    /// Puts a write batch into shared buffer. The batch will won't be synced to S3 asynchronously.
    pub fn replicate_remote_batch(
        &self,
        batch: Vec<SharedBufferItem>,
        epoch: u64,
    ) -> HummockResult<()> {
        let batch = SharedBufferBatch::new(batch, epoch);
        let batch_size = batch.len();
        let mut inner = self.inner.write();
        inner
            .shared_buffer
            .entry(epoch)
            .or_insert(BTreeMap::new())
            .insert(batch.end_user_key().to_vec(), batch.clone());
        inner.buffered_size += batch_size;
        self.scheduled_size.fetch_add(batch_size, Ordering::Release);
        Ok(())
    }

    /// Searches shared buffers within the `epoch_range` for the given key.
    /// Return:
    /// - None: the key doesn't exist in the shared buffer.
    /// - Some(`HummockValue`): the `HummockValue` corresponding to the key.
    pub fn get(
        &self,
        user_key: &[u8],
        epoch_range: impl RangeBounds<u64>,
    ) -> Option<HummockValue<Vec<u8>>> {
        let inner = self.inner.read();
        for (_epoch, buffers) in inner.shared_buffer.range(epoch_range).rev() {
            for (_, m) in buffers.range(user_key.to_vec()..) {
                if m.start_user_key() > user_key {
                    continue;
                }
                match m.get(user_key) {
                    Some(v) => return Some(v),
                    None => continue,
                }
            }
        }
        None
    }

    /// Gets a collection of forward `SharedBufferBatchIterator` to iterate data of shared buffer
    /// batches within the given `key_range` and `epoch_range`
    pub fn iters<R, B>(
        &self,
        key_range: &R,
        epoch_range: impl RangeBounds<u64>,
    ) -> Vec<SharedBufferBatchIterator<FORWARD>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        self.inner
            .read()
            .shared_buffer
            .range(epoch_range)
            .flat_map(|entry| {
                entry
                    .1
                    .range((
                        key_range.start_bound().map(|b| b.as_ref().to_vec()),
                        std::ops::Bound::Unbounded,
                    ))
                    .filter(|m| {
                        range_overlap(key_range, m.1.start_user_key(), m.1.end_user_key(), false)
                    })
                    .map(|m| m.1.iter())
            })
            .collect_vec()
    }

    /// Gets a collection of backward `SharedBufferBatchIterator` to iterate data of shared buffer
    /// batches within the given `key_range` and `epoch_range`
    pub fn reverse_iters<R, B>(
        &self,
        key_range: &R,
        epoch_range: impl RangeBounds<u64>,
    ) -> Vec<SharedBufferBatchIterator<BACKWARD>>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        self.inner
            .read()
            .shared_buffer
            .range(epoch_range)
            .flat_map(|entry| {
                entry
                    .1
                    .range((
                        key_range.end_bound().map(|b| b.as_ref().to_vec()),
                        std::ops::Bound::Unbounded,
                    ))
                    .filter(|m| {
                        range_overlap(key_range, m.1.start_user_key(), m.1.end_user_key(), true)
                    })
                    .map(|m| m.1.reverse_iter())
            })
            .collect_vec()
    }

    /// Deletes shared buffers before a given `epoch` exclusively.
    pub fn delete_before(&self, epoch: u64) {
        let mut inner = self.inner.write();
        // `buffer` = newer part
        let mut buffer = inner.shared_buffer.split_off(&epoch);
        // `buffer` = older part
        std::mem::swap(&mut inner.shared_buffer, &mut buffer);
        // Update buffer size and scheduled size.
        let deleted_size: usize = buffer
            .into_iter()
            .flat_map(|(_epoch, buffer)| buffer.into_iter().map(|(_end_key, batch)| batch.len()))
            .sum();
        self.scheduled_size
            .fetch_sub(deleted_size, Ordering::Release);
        inner.buffered_size -= deleted_size;
        drop(inner);
        self.release_notifier.notify_one();
    }

    /// This function was called while [`SharedBufferManager`] exited.
    pub async fn wait(self) -> StorageResult<()> {
        self.uploader_handle.await.unwrap()
    }

    #[cfg(test)]
    pub fn get_shared_buffer(&self) -> BTreeMap<u64, BTreeMap<Vec<u8>, SharedBufferBatch>> {
        self.inner.read().shared_buffer.clone()
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use itertools::Itertools;
    use risingwave_hummock_sdk::key::{key_with_epoch, user_key};
    use risingwave_meta::hummock::test_utils::setup_compute_env;
    use risingwave_meta::hummock::MockHummockMetaClient;

    use super::*;
    use crate::hummock::iterator::test_utils::iterator_test_value_of;
    use crate::hummock::iterator::{
        BoxedHummockIterator, HummockIterator, MergeIterator, ReverseMergeIterator,
    };
    use crate::hummock::test_utils::default_config_for_test;
    use crate::hummock::SstableStore;
    use crate::object::{InMemObjectStore, ObjectStoreImpl};

    async fn new_shared_buffer_manager() -> SharedBufferManager {
        let obj_client = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
        let remote_dir = "/test";
        let sstable_store = Arc::new(SstableStore::new(
            obj_client,
            remote_dir.to_string(),
            Arc::new(StateStoreMetrics::unused()),
            64 << 20,
            64 << 20,
        ));
        let vm = Arc::new(LocalVersionManager::new());
        let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref,
            worker_node.id,
        ));
        SharedBufferManager::new(
            Arc::new(default_config_for_test()),
            vm,
            sstable_store,
            Arc::new(StateStoreMetrics::unused()),
            mock_hummock_meta_client,
        )
    }

    async fn generate_and_write_batch(
        put_keys: &[Vec<u8>],
        delete_keys: &[Vec<u8>],
        epoch: u64,
        idx: &mut usize,
        shared_buffer_manager: &SharedBufferManager,
    ) -> Vec<(Vec<u8>, HummockValue<Vec<u8>>)> {
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
        shared_buffer_items
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect_vec()
    }

    #[tokio::test]
    async fn test_shared_buffer_manager_get() {
        let shared_buffer_manager = new_shared_buffer_manager().await;

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write a batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_items1 = generate_and_write_batch(
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
        let shared_buffer_items2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Get and check value with epoch 0..=epoch1
        for i in 0..3 {
            assert_eq!(
                shared_buffer_manager
                    .get(keys[i].as_slice(), ..=epoch1)
                    .unwrap(),
                shared_buffer_items1[i].1
            );
        }
        assert_eq!(
            shared_buffer_manager.get(keys[3].as_slice(), ..=epoch1),
            None
        );

        // Get and check value with epoch 0..=epoch2
        assert_eq!(
            shared_buffer_manager
                .get(keys[0].as_slice(), ..=epoch2)
                .unwrap(),
            shared_buffer_items1[0].1
        );
        assert_eq!(
            shared_buffer_manager
                .get(keys[1].as_slice(), ..=epoch2)
                .unwrap(),
            shared_buffer_items2[0].1
        );
        assert_eq!(
            shared_buffer_manager
                .get(keys[2].as_slice(), ..=epoch2)
                .unwrap(),
            HummockValue::delete()
        );
        assert_eq!(
            shared_buffer_manager
                .get(keys[3].as_slice(), ..=epoch2)
                .unwrap(),
            shared_buffer_items2[2].1
        );

        // Get and check value with epoch epoch2..=epoch2
        assert_eq!(
            shared_buffer_manager.get(keys[0].as_slice(), epoch2..=epoch2),
            None
        );
        for i in 0..3 {
            assert_eq!(
                shared_buffer_manager
                    .get(keys[i + 1].as_slice(), epoch2..=epoch2)
                    .unwrap(),
                shared_buffer_items2[i].1
            );
        }
    }

    #[tokio::test]
    async fn test_shared_buffer_manager_iter() {
        let shared_buffer_manager = new_shared_buffer_manager().await;

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write a batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_items1 = generate_and_write_batch(
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
        let shared_buffer_items2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Forward iterator with 0..=epoch1
        let range = keys[0].clone()..=keys[3].clone();
        let iters = shared_buffer_manager.iters(&range, ..=epoch1);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = MergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        for i in 0..3 {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i].clone(), epoch1)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items1[i].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(!merge_iterator.is_valid());

        // Forward iterator with 0..=epoch2
        let iters = shared_buffer_manager.iters(&range, ..=epoch2);
        assert_eq!(iters.len(), 2);
        let mut merge_iterator = MergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        assert!(merge_iterator.is_valid());
        assert_eq!(
            merge_iterator.key(),
            key_with_epoch(keys[0].clone(), epoch1)
        );
        assert_eq!(
            merge_iterator.value().to_owned_value(),
            shared_buffer_items1[0].1
        );
        merge_iterator.next().await.unwrap();
        for i in 0..2 {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch2)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items2[i].1
            );
            merge_iterator.next().await.unwrap();
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch1)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items1[i + 1].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(merge_iterator.is_valid());
        assert_eq!(
            merge_iterator.key(),
            key_with_epoch(keys[3].clone(), epoch2)
        );
        assert_eq!(
            merge_iterator.value().to_owned_value(),
            shared_buffer_items2[2].1
        );
        merge_iterator.next().await.unwrap();
        assert!(!merge_iterator.is_valid());

        // Forward iterator with epoch2..=epoch2
        let iters = shared_buffer_manager.iters(&range, epoch2..=epoch2);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = MergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        for i in 0..3 {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch2)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items2[i].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(!merge_iterator.is_valid());
    }

    #[tokio::test]
    async fn test_shared_buffer_manager_reverse_iter() {
        let shared_buffer_manager = new_shared_buffer_manager().await;

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write a batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_items1 = generate_and_write_batch(
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
        let shared_buffer_items2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        )
        .await;

        // Backward iterator with 0..=epoch1
        let range = keys[3].clone()..=keys[0].clone();
        let iters = shared_buffer_manager.reverse_iters(&range, ..=epoch1);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = ReverseMergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        for i in (0..3).rev() {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i].clone(), epoch1)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items1[i].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(!merge_iterator.is_valid());

        // Backward iterator with 0..=epoch2
        let iters = shared_buffer_manager.reverse_iters(&range, ..=epoch2);
        assert_eq!(iters.len(), 2);
        let mut merge_iterator = ReverseMergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        assert!(merge_iterator.is_valid());
        assert_eq!(
            merge_iterator.key(),
            key_with_epoch(keys[3].clone(), epoch2)
        );
        assert_eq!(
            merge_iterator.value().to_owned_value(),
            shared_buffer_items2[2].1
        );
        merge_iterator.next().await.unwrap();

        for i in (0..2).rev() {
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch1)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items1[i + 1].1
            );
            merge_iterator.next().await.unwrap();
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch2)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items2[i].1
            );
            merge_iterator.next().await.unwrap();
        }

        assert!(merge_iterator.is_valid());
        assert_eq!(
            merge_iterator.key(),
            key_with_epoch(keys[0].clone(), epoch1)
        );
        assert_eq!(
            merge_iterator.value().to_owned_value(),
            shared_buffer_items1[0].1
        );
        merge_iterator.next().await.unwrap();
        assert!(!merge_iterator.is_valid());

        // Backward iterator with epoch2..=epoch2
        let iters = shared_buffer_manager.reverse_iters(&range, epoch2..=epoch2);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = ReverseMergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
            Arc::new(StateStoreMetrics::unused()),
        );
        merge_iterator.rewind().await.unwrap();
        for i in (0..3).rev() {
            assert!(merge_iterator.is_valid());
            assert_eq!(
                merge_iterator.key(),
                key_with_epoch(keys[i + 1].clone(), epoch2)
            );
            assert_eq!(
                merge_iterator.value().to_owned_value(),
                shared_buffer_items2[i].1
            );
            merge_iterator.next().await.unwrap();
        }
        assert!(!merge_iterator.is_valid());
    }
}
