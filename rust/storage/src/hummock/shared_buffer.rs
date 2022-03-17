use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock as PLRwLock;
use risingwave_common::config::StorageConfig;
use risingwave_common::error::Result;
use risingwave_pb::hummock::SstableInfo;
use tokio::task::JoinHandle;

use super::compactor::{Compactor, SubCompactContext};
use super::hummock_meta_client::HummockMetaClient;
use super::iterator::variants::{BACKWARD, FORWARD};
use super::iterator::{BoxedHummockIterator, HummockIterator, MergeIterator};
use super::key_range::KeyRange;
use super::local_version_manager::LocalVersionManager;
use super::utils::range_overlap;
use super::value::HummockValue;
use super::{key, HummockError, HummockResult, SstableStoreRef};
use crate::hummock::conflict_detector::ConflictDetector;
use crate::monitor::StateStoreMetrics;

type SharedBufferItem = (Bytes, HummockValue<Bytes>);

/// A write batch stored in the shared buffer.
#[derive(Clone, Debug)]
pub struct SharedBufferBatch {
    inner: Arc<[SharedBufferItem]>,
    epoch: u64,
}

#[allow(dead_code)]
impl SharedBufferBatch {
    pub fn new(sorted_items: Vec<SharedBufferItem>, epoch: u64) -> Self {
        Self {
            inner: sorted_items.into(),
            epoch,
        }
    }

    pub fn get(&self, user_key: &[u8]) -> Option<HummockValue<Vec<u8>>> {
        // Perform binary search on user key because the items in SharedBufferBatch is ordered by
        // user key.
        match self
            .inner
            .binary_search_by(|m| key::user_key(&m.0).cmp(user_key))
        {
            Ok(i) => Some(self.inner[i].1.to_vec()),
            Err(_) => None,
        }
    }

    pub fn iter(&self) -> SharedBufferBatchIterator<FORWARD> {
        SharedBufferBatchIterator::<FORWARD>::new(self.inner.clone())
    }

    pub fn reverse_iter(&self) -> SharedBufferBatchIterator<BACKWARD> {
        SharedBufferBatchIterator::<BACKWARD>::new(self.inner.clone())
    }

    pub fn start_key(&self) -> &[u8] {
        &self.inner.first().unwrap().0
    }

    pub fn end_key(&self) -> &[u8] {
        &self.inner.last().unwrap().0
    }

    pub fn start_user_key(&self) -> &[u8] {
        key::user_key(&self.inner.first().unwrap().0)
    }

    pub fn end_user_key(&self) -> &[u8] {
        key::user_key(&self.inner.last().unwrap().0)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

pub struct SharedBufferBatchIterator<const DIRECTION: usize> {
    inner: Arc<[SharedBufferItem]>,
    current_idx: usize,
}

impl<const DIRECTION: usize> SharedBufferBatchIterator<DIRECTION> {
    pub fn new(inner: Arc<[SharedBufferItem]>) -> Self {
        Self {
            inner,
            current_idx: 0,
        }
    }

    fn current_item(&self) -> &SharedBufferItem {
        assert!(self.is_valid());
        let idx = match DIRECTION {
            FORWARD => self.current_idx,
            BACKWARD => self.inner.len() - self.current_idx - 1,
            _ => unreachable!(),
        };
        self.inner.get(idx).unwrap()
    }
}

#[async_trait]
impl<const DIRECTION: usize> HummockIterator for SharedBufferBatchIterator<DIRECTION> {
    async fn next(&mut self) -> super::HummockResult<()> {
        assert!(self.is_valid());
        self.current_idx += 1;
        Ok(())
    }

    fn key(&self) -> &[u8] {
        &self.current_item().0
    }

    fn value(&self) -> HummockValue<&[u8]> {
        self.current_item().1.as_slice()
    }

    fn is_valid(&self) -> bool {
        self.current_idx < self.inner.len()
    }

    async fn rewind(&mut self) -> super::HummockResult<()> {
        self.current_idx = 0;
        Ok(())
    }

    async fn seek(&mut self, key: &[u8]) -> super::HummockResult<()> {
        // Perform binary search on user key because the items in SharedBufferBatch is ordered by
        // user key.
        match self
            .inner
            .binary_search_by(|probe| key::user_key(&probe.0).cmp(key::user_key(key)))
        {
            Ok(i) => {
                self.current_idx = i;
                // Move onto the next item if key epoch > seek epoch
                if key::get_epoch(self.key()) > key::get_epoch(key) {
                    self.current_idx += 1;
                }
            }
            Err(i) => self.current_idx = i,
        }
        Ok(())
    }
}

/// A manager to manage reads and writes on shared buffer.
/// Shared buffer is a node level abstraction to buffer write batches across executors.
pub struct SharedBufferManager {
    /// `shared_buffer` is a collection of immutable batches grouped by (epoch, end_key)
    shared_buffer: PLRwLock<BTreeMap<u64, BTreeMap<Vec<u8>, SharedBufferBatch>>>,
    uploader_tx: tokio::sync::mpsc::UnboundedSender<SharedBufferUploaderItem>,
    uploader_handle: JoinHandle<Result<()>>,
}

impl SharedBufferManager {
    pub fn new(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        sstable_store: SstableStoreRef,
        // TODO: should be separated `HummockStats` instead of `StateStoreMetrics`.
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) -> Self {
        let (uploader_tx, uploader_rx) = tokio::sync::mpsc::unbounded_channel();
        let uploader = SharedBufferUploader::new(
            options,
            local_version_manager,
            sstable_store,
            stats,
            hummock_meta_client,
            uploader_rx,
        );
        let uploader_handle = tokio::spawn(uploader.run());
        Self {
            shared_buffer: PLRwLock::new(BTreeMap::new()),
            uploader_tx,
            uploader_handle,
        }
    }

    /// Put a write batch into shared buffer. The batch will be synced to S3 asynchronously.
    pub fn write_batch(&self, batch: Vec<SharedBufferItem>, epoch: u64) -> HummockResult<()> {
        let batch = SharedBufferBatch::new(batch, epoch);
        self.shared_buffer
            .write()
            .entry(epoch)
            .or_insert(BTreeMap::new())
            .insert(batch.end_user_key().to_vec(), batch.clone());
        self.uploader_tx
            .send(SharedBufferUploaderItem::Batch(batch))
            .map_err(HummockError::shared_buffer_error)
    }

    /// Put a write batch into shared buffer. The batch will won't be synced to S3 asynchronously.
    pub fn replicate_remote_batch(
        &self,
        batch: Vec<SharedBufferItem>,
        epoch: u64,
    ) -> HummockResult<()> {
        let batch = SharedBufferBatch::new(batch, epoch);
        self.shared_buffer
            .write()
            .entry(epoch)
            .or_insert(BTreeMap::new())
            .insert(batch.end_user_key().to_vec(), batch.clone());
        Ok(())
    }

    // TODO: support time-based syncing
    pub async fn sync(&self, epoch: Option<u64>) -> HummockResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.uploader_tx
            .send(SharedBufferUploaderItem::Sync(SyncItem {
                epoch,
                notifier: Some(tx),
            }))
            .unwrap();
        rx.await.unwrap()
    }

    /// Search shared buffers within the `epoch_range` for the given key.
    /// Return:
    /// - None: the key doesn't exist in the shared buffer.
    /// - Some(HummockValue): the `HummockValue` corresponding to the key.
    pub fn get(
        &self,
        user_key: &[u8],
        epoch_range: impl RangeBounds<u64>,
    ) -> Option<HummockValue<Vec<u8>>> {
        let guard = self.shared_buffer.read();
        for (_epoch, buffers) in guard.range(epoch_range).rev() {
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

    /// Get a collection of forward `SharedBufferBatchIterator` to iterate data of shared buffer
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
        self.shared_buffer
            .read()
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

    /// Get a collection of backward `SharedBufferBatchIterator` to iterate data of shared buffer
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
        self.shared_buffer
            .read()
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

    /// Delete shared buffers before a given `epoch` inclusively.
    pub fn delete_before(&self, epoch: u64) {
        let mut guard = self.shared_buffer.write();
        let new = guard.split_off(&(epoch + 1));
        *guard = new;
    }

    /// This function was called while [`SharedBufferManager`] exited.
    pub async fn wait(self) -> Result<()> {
        self.uploader_handle.await.unwrap()
    }

    pub fn reset(&mut self, epoch: u64) {
        // Reset uploader item.
        self.uploader_tx
            .send(SharedBufferUploaderItem::Reset(epoch))
            .unwrap();
        // Remove items of the given epoch from shared buffer
        self.shared_buffer.write().remove(&epoch);
    }
}

#[derive(Debug)]
pub struct SyncItem {
    /// Epoch to sync. None means syncing all epochs.
    epoch: Option<u64>,
    /// Notifier to notify on sync finishes
    notifier: Option<tokio::sync::oneshot::Sender<HummockResult<()>>>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum SharedBufferUploaderItem {
    Batch(SharedBufferBatch),
    Sync(SyncItem),
    Reset(u64),
}

pub struct SharedBufferUploader {
    /// Batches to upload grouped by epoch
    batches_to_upload: BTreeMap<u64, Vec<SharedBufferBatch>>,
    local_version_manager: Arc<LocalVersionManager>,
    options: Arc<StorageConfig>,

    /// Statistics.
    // TODO: should be separated `HummockStats` instead of `StateStoreMetrics`.
    stats: Arc<StateStoreMetrics>,
    hummock_meta_client: Arc<dyn HummockMetaClient>,
    sstable_store: SstableStoreRef,

    /// For conflict key detection. Enabled by setting `write_conflict_detection_enabled` to true
    /// in `StorageConfig`
    write_conflict_detector: Option<Arc<ConflictDetector>>,

    rx: tokio::sync::mpsc::UnboundedReceiver<SharedBufferUploaderItem>,
}

impl SharedBufferUploader {
    pub fn new(
        options: Arc<StorageConfig>,
        local_version_manager: Arc<LocalVersionManager>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        rx: tokio::sync::mpsc::UnboundedReceiver<SharedBufferUploaderItem>,
    ) -> Self {
        Self {
            batches_to_upload: BTreeMap::new(),
            options: options.clone(),
            local_version_manager,

            stats,
            hummock_meta_client,
            sstable_store,
            write_conflict_detector: if options.write_conflict_detection_enabled {
                Some(Arc::new(ConflictDetector::new()))
            } else {
                None
            },
            rx,
        }
    }

    /// Upload buffer batches to S3.
    async fn sync(&mut self, epoch: u64) -> HummockResult<()> {
        if let Some(detector) = &self.write_conflict_detector {
            detector.archive_epoch(epoch);
        }

        let buffers = match self.batches_to_upload.remove(&epoch) {
            Some(m) => m,
            None => return Ok(()),
        };

        // Compact buffers into SSTs
        let merge_iters = {
            let iters = buffers
                .into_iter()
                .map(|m| Box::new(m.iter()) as BoxedHummockIterator);
            MergeIterator::new(iters)
        };
        let sub_compact_context = SubCompactContext {
            options: self.options.clone(),
            local_version_manager: self.local_version_manager.clone(),
            hummock_meta_client: self.hummock_meta_client.clone(),
            sstable_store: self.sstable_store.clone(),
            stats: self.stats.clone(),
            is_share_buffer_compact: true,
        };
        let mut tables = Vec::new();
        Compactor::sub_compact(
            sub_compact_context,
            KeyRange::inf(),
            merge_iters,
            &mut tables,
            false,
            u64::MAX,
        )
        .await?;

        if tables.is_empty() {
            return Ok(());
        }

        // Add all tables at once.
        let version = self
            .hummock_meta_client
            .add_tables(
                epoch,
                tables
                    .iter()
                    .map(|sst| SstableInfo {
                        id: sst.id,
                        key_range: Some(risingwave_pb::hummock::KeyRange {
                            left: sst.meta.get_smallest_key().to_vec(),
                            right: sst.meta.get_largest_key().to_vec(),
                            inf: false,
                        }),
                    })
                    .collect(),
            )
            .await?;

        // Ensure the added data is available locally
        self.local_version_manager.try_set_version(version);

        Ok(())
    }

    async fn handle(&mut self, item: SharedBufferUploaderItem) -> Result<()> {
        match item {
            SharedBufferUploaderItem::Batch(m) => {
                if let Some(detector) = &self.write_conflict_detector {
                    detector.check_conflict_and_track_write_batch(&m.inner, m.epoch);
                }

                self.batches_to_upload
                    .entry(m.epoch())
                    .or_insert(Vec::new())
                    .push(m);
                Ok(())
            }
            SharedBufferUploaderItem::Sync(sync_item) => {
                let res = match sync_item.epoch {
                    Some(e) => {
                        // Sync a specific epoch
                        self.sync(e).await
                    }
                    None => {
                        // Sync all epochs
                        let epochs = self.batches_to_upload.keys().copied().collect_vec();
                        let mut res = Ok(());
                        for e in epochs {
                            res = self.sync(e).await;
                            if res.is_err() {
                                break;
                            }
                        }
                        res
                    }
                };

                if let Some(tx) = sync_item.notifier {
                    tx.send(res).map_err(|_| {
                        HummockError::shared_buffer_error(
                            "Failed to notify shared buffer sync because of send drop",
                        )
                    })?;
                }
                Ok(())
            }
            SharedBufferUploaderItem::Reset(epoch) => {
                self.batches_to_upload.remove(&epoch);
                Ok(())
            }
        }
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(m) = self.rx.recv().await {
            if let Err(e) = self.handle(m).await {
                return Err(e);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use itertools::Itertools;

    use super::SharedBufferBatch;
    use crate::hummock::iterator::test_utils::{
        iterator_test_key_of, iterator_test_key_of_epoch, iterator_test_value_of,
    };
    use crate::hummock::iterator::{
        BoxedHummockIterator, HummockIterator, MergeIterator, ReverseMergeIterator,
    };
    use crate::hummock::key::{key_with_epoch, user_key};
    use crate::hummock::local_version_manager::LocalVersionManager;
    use crate::hummock::mock::{MockHummockMetaClient, MockHummockMetaService};
    use crate::hummock::shared_buffer::SharedBufferManager;
    use crate::hummock::test_utils::default_config_for_test;
    use crate::hummock::value::HummockValue;
    use crate::hummock::SstableStore;
    use crate::monitor::StateStoreMetrics;
    use crate::object::{InMemObjectStore, ObjectStore};

    fn transform_shared_buffer(
        batches: Vec<(Vec<u8>, HummockValue<Vec<u8>>)>,
    ) -> Vec<(Bytes, HummockValue<Bytes>)> {
        batches
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect_vec()
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_basic() {
        let epoch = 1;
        let shared_buffer_items = vec![
            (
                iterator_test_key_of_epoch(0, 0, epoch),
                HummockValue::Put(b"value1".to_vec()),
            ),
            (
                iterator_test_key_of_epoch(0, 1, epoch),
                HummockValue::Put(b"value2".to_vec()),
            ),
            (
                iterator_test_key_of_epoch(0, 2, epoch),
                HummockValue::Put(b"value3".to_vec()),
            ),
        ];
        let shared_buffer_batch =
            SharedBufferBatch::new(transform_shared_buffer(shared_buffer_items.clone()), epoch);

        // Sketch
        assert_eq!(shared_buffer_batch.start_key(), shared_buffer_items[0].0);
        assert_eq!(shared_buffer_batch.end_key(), shared_buffer_items[2].0);
        assert_eq!(
            shared_buffer_batch.start_user_key(),
            user_key(&shared_buffer_items[0].0)
        );
        assert_eq!(
            shared_buffer_batch.end_user_key(),
            user_key(&shared_buffer_items[2].0)
        );

        // Point lookup
        for (k, v) in &shared_buffer_items {
            assert_eq!(
                shared_buffer_batch.get(user_key(k.as_slice())),
                Some(v.clone())
            );
        }
        assert_eq!(
            shared_buffer_batch.get(iterator_test_key_of(0, 3).as_slice()),
            None
        );
        assert_eq!(
            shared_buffer_batch.get(iterator_test_key_of(1, 0).as_slice()),
            None
        );

        // Forward iterator
        let mut iter = shared_buffer_batch.iter();
        iter.rewind().await.unwrap();
        let mut output = vec![];
        while iter.is_valid() {
            output.push((iter.key().to_owned(), iter.value().to_owned_value()));
            iter.next().await.unwrap();
        }
        assert_eq!(output, shared_buffer_items);

        // Backward iterator
        let mut reverse_iter = shared_buffer_batch.reverse_iter();
        reverse_iter.rewind().await.unwrap();
        let mut output = vec![];
        while reverse_iter.is_valid() {
            output.push((
                reverse_iter.key().to_owned(),
                reverse_iter.value().to_owned_value(),
            ));
            reverse_iter.next().await.unwrap();
        }
        output.reverse();
        assert_eq!(output, shared_buffer_items);
    }

    #[tokio::test]
    async fn test_shared_buffer_batch_seek() {
        let epoch = 1;
        let shared_buffer_items = vec![
            (
                iterator_test_key_of_epoch(0, 0, epoch),
                HummockValue::Put(b"value1".to_vec()),
            ),
            (
                iterator_test_key_of_epoch(0, 1, epoch),
                HummockValue::Put(b"value2".to_vec()),
            ),
            (
                iterator_test_key_of_epoch(0, 2, epoch),
                HummockValue::Put(b"value3".to_vec()),
            ),
        ];
        let shared_buffer_batch =
            SharedBufferBatch::new(transform_shared_buffer(shared_buffer_items.clone()), epoch);

        // Seek to 2nd key with current epoch, expect last two items to return
        let mut iter = shared_buffer_batch.iter();
        iter.seek(&iterator_test_key_of_epoch(0, 1, epoch))
            .await
            .unwrap();
        for item in &shared_buffer_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // Seek to 2nd key with future epoch, expect last two items to return
        let mut iter = shared_buffer_batch.iter();
        iter.seek(&iterator_test_key_of_epoch(0, 1, epoch + 1))
            .await
            .unwrap();
        for item in &shared_buffer_items[1..] {
            assert!(iter.is_valid());
            assert_eq!(iter.key(), item.0.as_slice());
            assert_eq!(iter.value(), item.1.as_slice());
            iter.next().await.unwrap();
        }
        assert!(!iter.is_valid());

        // Seek to 2nd key with old epoch, expect last item to return
        let mut iter = shared_buffer_batch.iter();
        iter.seek(&iterator_test_key_of_epoch(0, 1, epoch - 1))
            .await
            .unwrap();
        let item = shared_buffer_items.last().unwrap();
        assert!(iter.is_valid());
        assert_eq!(iter.key(), item.0.as_slice());
        assert_eq!(iter.value(), item.1.as_slice());
        iter.next().await.unwrap();
        assert!(!iter.is_valid());
    }

    fn new_shared_buffer_manager() -> SharedBufferManager {
        let obj_client = Arc::new(InMemObjectStore::new()) as Arc<dyn ObjectStore>;
        let remote_dir = "/test";
        let sstable_store = Arc::new(SstableStore::new(obj_client, remote_dir.to_string()));
        let vm = Arc::new(LocalVersionManager::new(sstable_store.clone()));
        let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(Arc::new(
            MockHummockMetaService::new(),
        )));
        SharedBufferManager::new(
            Arc::new(default_config_for_test()),
            vm,
            sstable_store,
            Arc::new(StateStoreMetrics::unused()),
            mock_hummock_meta_client,
        )
    }

    fn generate_and_write_batch(
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
                HummockValue::Put(iterator_test_value_of(0, *idx).into()),
            ));
            *idx += 1;
        }
        for key in delete_keys {
            shared_buffer_items.push((
                Bytes::from(key_with_epoch(key.clone(), epoch)),
                HummockValue::Delete,
            ));
        }
        shared_buffer_items.sort_by(|l, r| user_key(&l.0).cmp(&r.0));
        shared_buffer_manager
            .write_batch(shared_buffer_items.clone(), epoch)
            .unwrap();
        shared_buffer_items
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect_vec()
    }

    #[tokio::test]
    async fn test_shared_buffer_manager_get() {
        let shared_buffer_manager = new_shared_buffer_manager();

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_items1 = generate_and_write_batch(
            put_keys_in_epoch1,
            &[],
            epoch1,
            &mut idx,
            &shared_buffer_manager,
        );

        // Write batch in epoch2, with key1 overlapping with epoch1 and key2 deleted.
        let epoch2 = epoch1 + 1;
        let put_keys_in_epoch2 = &[&keys[1..2], &keys[3..4]].concat();
        let shared_buffer_items2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        );

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
            HummockValue::Delete
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
        let shared_buffer_manager = new_shared_buffer_manager();

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_items1 = generate_and_write_batch(
            put_keys_in_epoch1,
            &[],
            epoch1,
            &mut idx,
            &shared_buffer_manager,
        );

        // Write batch in epoch2, with key1 overlapping with epoch1 and key2 deleted.
        let epoch2 = epoch1 + 1;
        let put_keys_in_epoch2 = &[&keys[1..2], &keys[3..4]].concat();
        let shared_buffer_items2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        );

        // Forward iterator with 0..=epoch1
        let range = keys[0].clone()..=keys[3].clone();
        let iters = shared_buffer_manager.iters(&range, ..=epoch1);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = MergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
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
        let shared_buffer_manager = new_shared_buffer_manager();

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write batch in epoch1
        let epoch1 = 1;
        let put_keys_in_epoch1 = &keys[..3];
        let shared_buffer_items1 = generate_and_write_batch(
            put_keys_in_epoch1,
            &[],
            epoch1,
            &mut idx,
            &shared_buffer_manager,
        );

        // Write batch in epoch2, with key1 overlapping with epoch1 and key2 deleted.
        let epoch2 = epoch1 + 1;
        let put_keys_in_epoch2 = &[&keys[1..2], &keys[3..4]].concat();
        let shared_buffer_items2 = generate_and_write_batch(
            put_keys_in_epoch2,
            &keys[2..3],
            epoch2,
            &mut idx,
            &shared_buffer_manager,
        );

        // Backward iterator with 0..=epoch1
        let range = keys[3].clone()..=keys[0].clone();
        let iters = shared_buffer_manager.reverse_iters(&range, ..=epoch1);
        assert_eq!(iters.len(), 1);
        let mut merge_iterator = ReverseMergeIterator::new(
            iters
                .into_iter()
                .map(|i| Box::new(i) as BoxedHummockIterator),
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

    #[tokio::test]
    async fn test_shared_buffer_manager_reset() {
        let mut shared_buffer_manager = new_shared_buffer_manager();

        let mut keys = Vec::new();
        for i in 0..4 {
            keys.push(format!("key_test_{:05}", i).as_bytes().to_vec());
        }
        let mut idx = 0;

        // Write batches
        let epoch = 1;
        let shared_buffer_items =
            generate_and_write_batch(&keys, &[], epoch, &mut idx, &shared_buffer_manager);

        // Get and check value with epoch 0..=epoch1
        for (idx, key) in keys.iter().enumerate() {
            assert_eq!(
                shared_buffer_manager.get(key.as_slice(), ..=epoch).unwrap(),
                shared_buffer_items[idx].1
            );
        }

        // Reset shared buffer. Expect all keys are gone.
        shared_buffer_manager.reset(epoch);
        for item in &shared_buffer_items {
            assert_eq!(shared_buffer_manager.get(item.0.as_slice(), ..=epoch), None);
        }

        // Generate new items overlapping with old items and check
        keys.push(format!("key_test_{:05}", 100).as_bytes().to_vec());
        let epoch = 1;
        let new_shared_buffer_items =
            generate_and_write_batch(&keys, &[], epoch, &mut idx, &shared_buffer_manager);
        for (idx, key) in keys.iter().enumerate() {
            assert_eq!(
                shared_buffer_manager.get(key.as_slice(), ..=epoch).unwrap(),
                new_shared_buffer_items[idx].1
            );
        }
    }
}
