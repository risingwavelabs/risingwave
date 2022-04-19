use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;

use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::HummockEpoch;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio::task::JoinHandle;

use super::shared_buffer_batch::{SharedBufferBatch, SharedBufferBatchIterator, SharedBufferItem};
use super::shared_buffer_uploader::{SharedBufferUploader, UploadItem};
use crate::hummock::iterator::variants::{BACKWARD, FORWARD};
use crate::hummock::local_version_manager::LocalVersionManager;
use crate::hummock::utils::range_overlap;
use crate::hummock::value::HummockValue;
use crate::hummock::{HummockError, HummockResult, SstableStoreRef};
use crate::monitor::StateStoreMetrics;

/// `{ end key -> batch }`
pub type EpochSharedBuffer = BTreeMap<Vec<u8>, SharedBufferBatch>;

#[derive(Default)]
pub struct SharedBufferManagerCore {
    pub buffer: BTreeMap<HummockEpoch, EpochSharedBuffer>,
    pub size: usize,
}

pub struct SharedBufferManager {
    capacity: usize,

    core: Arc<RwLock<SharedBufferManagerCore>>,

    uploader_tx: mpsc::UnboundedSender<UploadItem>,
    uploader_handle: JoinHandle<HummockResult<()>>,

    flush_notifier: Arc<Notify>,
}

impl SharedBufferManager {
    pub fn new(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> Self {
        let (uploader_tx, uploader_rx) = mpsc::unbounded_channel();
        let core = Arc::new(RwLock::new(SharedBufferManagerCore::default()));
        let flush_notifier = Arc::new(Notify::new());
        let mut uploader = SharedBufferUploader::new(
            options.clone(),
            options.shared_buffer_threshold as usize,
            sstable_store,
            local_version_manager,
            hummock_meta_client,
            core.clone(),
            uploader_rx,
            flush_notifier.clone(),
            stats,
        );
        let uploader_handle = tokio::spawn(async move { uploader.run().await });
        Self {
            capacity: options.shared_buffer_capacity as usize,
            core,
            uploader_tx,
            uploader_handle,
            flush_notifier,
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

        // Stall writes that attempting to exceeds capacity.
        // Actively trigger flush, suspend, and wait until some flush completed.
        while self.size() + batch_size as usize > self.capacity {
            self.uploader_tx
                .send(UploadItem::Flush)
                .map_err(HummockError::shared_buffer_error)?;
            self.flush_notifier.notified().await;
        }

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
        self.uploader_handle.await.unwrap()
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

    /// Searches shared buffers within the `epoch_range` for the given key.
    /// Return:
    /// - None: the key doesn't exist in the shared buffer.
    /// - Some(`HummockValue`): the `HummockValue` corresponding to the key.
    pub fn get(
        &self,
        user_key: &[u8],
        epoch_range: impl RangeBounds<u64>,
    ) -> Option<HummockValue<Vec<u8>>> {
        let guard = self.core.read();
        for (_epoch, buffers) in guard.buffer.range(epoch_range).rev() {
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
        self.core
            .read()
            .buffer
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
        self.core
            .read()
            .buffer
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

    // TODO: REMOVE ME.
    /// Deletes shared buffers before a given `epoch` exclusively.
    pub fn delete_before(&self, epoch: HummockEpoch) {
        let mut guard = self.core.write();
        // buffer = newer part
        let mut buffer = guard.buffer.split_off(&epoch);
        // buffer = older part, core.buffer = new part
        std::mem::swap(&mut buffer, &mut guard.buffer);
        for (_epoch, batches) in buffer {
            for (_end_user_key, batch) in batches {
                guard.size -= batch.size as usize
            }
        }
    }

    /// Puts a write batch into shared buffer. The batch will won't be synced to S3 asynchronously.
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

    #[cfg(test)]
    pub fn get_shared_buffer(&self) -> BTreeMap<u64, EpochSharedBuffer> {
        self.core.read().buffer.clone()
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
