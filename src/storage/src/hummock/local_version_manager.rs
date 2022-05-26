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
use std::mem::swap;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_pb::hummock::{HummockVersion, SstableInfo};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_retry::strategy::jitter;

use super::local_version::{LocalVersion, PinnedVersion, ReadVersion};
use super::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use super::shared_buffer::shared_buffer_uploader::{SharedBufferUploader, UploadItem};
use super::SstableStoreRef;
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferItem;
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTask;
use crate::hummock::shared_buffer::SharedBuffer;
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockVersionId, INVALID_VERSION_ID,
};
use crate::monitor::StateStoreMetrics;
use crate::storage_value::StorageValue;

struct WorkerContext {
    version_update_notifier_tx: tokio::sync::watch::Sender<HummockVersionId>,
    shared_buffer_uploader_tx: UnboundedSender<UploadItem>,
}

struct BufferTracker {
    capacity: usize,
    upload_size: Arc<AtomicUsize>,
    replicate_size: Arc<AtomicUsize>,
}

impl BufferTracker {
    pub fn is_empty(&self) -> bool {
        self.upload_size.load(Relaxed) == 0
    }

    pub fn get_upload_size(&self) -> usize {
        self.upload_size.load(Relaxed)
    }

    pub fn get_replicate_size(&self) -> usize {
        self.replicate_size.load(Relaxed)
    }

    pub fn can_write(&self) -> bool {
        self.get_upload_size() + self.get_replicate_size() <= self.capacity
    }
}

/// The `LocalVersionManager` maintains a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `SSTables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    local_version: RwLock<LocalVersion>,
    worker_context: WorkerContext,
    buffer_tracker: BufferTracker,
    write_conflict_detector: Option<Arc<ConflictDetector>>,
}

impl LocalVersionManager {
    pub async fn new(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        write_conflict_detector: Option<Arc<ConflictDetector>>,
    ) -> Arc<LocalVersionManager> {
        let (shared_buffer_uploader_tx, shared_buffer_uploader_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (version_unpin_worker_tx, version_unpin_worker_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (version_update_notifier_tx, _) = tokio::sync::watch::channel(INVALID_VERSION_ID);

        let pinned_version = Self::pin_version_with_retry(
            hummock_meta_client.clone(),
            INVALID_VERSION_ID,
            10,
            // never break until max retry
            || false,
        )
        .await
        .expect("should be `Some` since `break_condition` is always false")
        .expect("should be able to pinned the first version");

        let global_upload_batches_size = Arc::new(AtomicUsize::new(0));
        let global_replicate_batches_size = Arc::new(AtomicUsize::new(0));

        let local_version_manager = Arc::new(LocalVersionManager {
            local_version: RwLock::new(LocalVersion::new(pinned_version, version_unpin_worker_tx)),
            worker_context: WorkerContext {
                version_update_notifier_tx,
                shared_buffer_uploader_tx,
            },
            buffer_tracker: BufferTracker {
                capacity: (options.shared_buffer_capacity_mb as usize) * (1 << 20),
                upload_size: global_upload_batches_size,
                replicate_size: global_replicate_batches_size,
            },
            write_conflict_detector: write_conflict_detector.clone(),
        });

        // Pin and get the latest version.
        tokio::spawn(LocalVersionManager::start_pin_worker(
            Arc::downgrade(&local_version_manager),
            hummock_meta_client.clone(),
        ));

        // Unpin unused version.
        tokio::spawn(LocalVersionManager::start_unpin_worker(
            version_unpin_worker_rx,
            hummock_meta_client.clone(),
        ));

        // Uploader shared buffer to S3.
        let mut uploader = SharedBufferUploader::new(
            options.clone(),
            sstable_store,
            hummock_meta_client,
            shared_buffer_uploader_rx,
            stats,
            write_conflict_detector,
        );
        tokio::spawn(async move { uploader.run().await });

        local_version_manager
    }

    /// Updates cached version if the new version is of greater id.
    /// You shouldn't unpin even the method returns false, as it is possible `hummock_version` is
    /// being referenced by some readers.
    pub fn try_update_pinned_version(&self, newly_pinned_version: HummockVersion) -> bool {
        let new_version_id = newly_pinned_version.id;
        if validate_table_key_range(&newly_pinned_version.levels).is_err() {
            return false;
        }
        let mut guard = self.local_version.write();

        if guard.pinned_version().id() >= new_version_id {
            return false;
        }

        if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
            conflict_detector.set_watermark(newly_pinned_version.max_committed_epoch);
        }
        guard.set_pinned_version(newly_pinned_version);

        self.worker_context
            .version_update_notifier_tx
            .send(new_version_id)
            .ok();
        true
    }

    pub fn update_uncommitted_ssts(
        &self,
        epoch: u64,
        sst_info: Vec<SstableInfo>,
        shared_buffer_batches: Vec<SharedBufferBatch>,
    ) {
        let mut guard = self.local_version.write();

        // Record uploaded but uncommitted SSTs and delete batches from shared buffer.
        guard.add_uncommitted_ssts(epoch, sst_info);

        if let Some(shared_buffer) = guard.get_shared_buffer(epoch) {
            let mut guard = shared_buffer.write();
            guard.delete_batch(shared_buffer_batches.as_slice());
        }
    }

    /// Waits until the local hummock version contains the given committed epoch
    pub async fn wait_epoch(&self, epoch: HummockEpoch) -> HummockResult<()> {
        if epoch == HummockEpoch::MAX {
            panic!("epoch should not be u64::MAX");
        }
        let mut receiver = self.worker_context.version_update_notifier_tx.subscribe();
        loop {
            {
                let current_version = self.local_version.read();
                if current_version.pinned_version().max_committed_epoch() >= epoch {
                    return Ok(());
                }
            }
            match tokio::time::timeout(Duration::from_secs(10), receiver.changed()).await {
                Err(_) => {
                    return Err(HummockError::wait_epoch("timeout"));
                }
                Ok(Err(_)) => {
                    return Err(HummockError::wait_epoch("tx dropped"));
                }
                Ok(Ok(_)) => {}
            }
        }
    }

    pub fn build_shared_buffer_item_batches(
        kv_pairs: Vec<(Bytes, StorageValue)>,
        epoch: HummockEpoch,
    ) -> Vec<SharedBufferItem> {
        kv_pairs
            .into_iter()
            .map(|(key, value)| {
                (
                    Bytes::from(FullKey::from_user_key(key.to_vec(), epoch).into_inner()),
                    value.into(),
                )
            })
            .collect_vec()
    }

    pub async fn write_shared_buffer(
        &self,
        epoch: HummockEpoch,
        kv_pairs: Vec<(Bytes, StorageValue)>,
        is_remote_batch: bool,
    ) -> HummockResult<usize> {
        let sorted_items = Self::build_shared_buffer_item_batches(kv_pairs, epoch);

        let batch_size = SharedBufferBatch::measure_batch_size(&sorted_items);
        while !self.buffer_tracker.can_write() {
            self.sync_shared_buffer(None).await?;
        }

        let batch = SharedBufferBatch::new_with_size(
            sorted_items,
            epoch,
            batch_size,
            if is_remote_batch {
                self.buffer_tracker.replicate_size.clone()
            } else {
                self.buffer_tracker.upload_size.clone()
            },
        );

        // Try get shared buffer with version read lock
        let shared_buffer = self.local_version.read().get_shared_buffer(epoch).cloned();

        // New a shared buffer with version write lock if shared buffer of the corresponding epoch
        // does not exist before
        let shared_buffer =
            shared_buffer.unwrap_or_else(|| self.local_version.write().new_shared_buffer(epoch));

        // Write into shared buffer
        if is_remote_batch {
            // The batch won't be synced to S3 asynchronously if it is a remote batch
            shared_buffer.write().replicate_batch(batch);
        } else {
            // The batch will be synced to S3 asynchronously if it is a local batch
            shared_buffer.write().write_batch(batch);
        }

        Ok(batch_size)
    }

    pub async fn sync_shared_buffer(&self, epoch: Option<HummockEpoch>) -> HummockResult<()> {
        if self.buffer_tracker.is_empty() {
            return Ok(());
        }

        let mut tasks = vec![];

        let mut handle_epoch = |epoch: &HummockEpoch, shared_buffer: &Arc<RwLock<SharedBuffer>>| {
            let mut guard = shared_buffer.write();
            let (task_id, task_data) = guard.new_upload_task(|batches| {
                // Take all batches of an epoch out.
                let mut ret = BTreeMap::default();
                swap(&mut ret, batches);
                ret
            });
            tasks.push(UploadTask::new(task_id, *epoch, task_data));
        };

        {
            let guard = self.local_version.read();
            match epoch {
                Some(epoch) => match guard.get_shared_buffer(epoch) {
                    None => return Ok(()),
                    Some(shared_buffer) => {
                        handle_epoch(&epoch, shared_buffer);
                    }
                },
                None => {
                    for (epoch, shared_buffer) in guard.iter_shared_buffer() {
                        handle_epoch(epoch, shared_buffer);
                    }
                }
            }
        }

        if tasks.is_empty() {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.worker_context
            .shared_buffer_uploader_tx
            .send(UploadItem::new(tasks, tx))
            .map_err(HummockError::shared_buffer_error)?;
        let upload_result = rx.await.map_err(HummockError::shared_buffer_error)?;

        let failed_epoch = upload_result
            .iter()
            .filter_map(
                |((epoch, _), result)| {
                    if result.is_err() {
                        Some(*epoch)
                    } else {
                        None
                    }
                },
            )
            .collect_vec();

        if failed_epoch.len() < upload_result.len() {
            // only acquire the lock when any of the upload task succeed.
            let mut guard = self.local_version.write();
            for ((epoch, task_id), result) in upload_result {
                match result {
                    Ok(ssts) => {
                        guard.add_uncommitted_ssts(epoch, ssts);
                        if let Some(shared_buffer) = guard.get_shared_buffer(epoch) {
                            shared_buffer.write().succeed_upload_task(task_id);
                        }
                        if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
                            conflict_detector.archive_epoch(epoch);
                        }
                    }
                    Err(_) => {
                        if let Some(shared_buffer) = guard.get_shared_buffer(epoch) {
                            shared_buffer.write().fail_upload_task(task_id);
                        }
                    }
                }
            }
        };

        if failed_epoch.is_empty() {
            Ok(())
        } else {
            Err(HummockError::shared_buffer_error(format!(
                "Failed to sync epochs: {:?}",
                failed_epoch
            )))
        }
    }

    pub fn read_version(self: &Arc<LocalVersionManager>, read_epoch: HummockEpoch) -> ReadVersion {
        self.local_version.read().read_version(read_epoch)
    }

    pub fn get_pinned_version(&self) -> Arc<PinnedVersion> {
        self.local_version.read().pinned_version().clone()
    }

    pub fn get_uncommitted_ssts(&self, epoch: HummockEpoch) -> Vec<SstableInfo> {
        self.local_version
            .read()
            .get_uncommitted_ssts()
            .get(&epoch)
            .cloned()
            .unwrap_or_default()
    }

    /// Pin a version with retry.
    ///
    /// Return:
    ///   - `Some(Ok(pinned_version))` if success
    ///   - `Some(Err(err))` if exceed the retry limit
    ///   - `None` if meet the break condition
    async fn pin_version_with_retry(
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        last_pinned: HummockVersionId,
        max_retry: usize,
        break_condition: impl Fn() -> bool,
    ) -> Option<HummockResult<HummockVersion>> {
        let max_retry_interval = Duration::from_secs(10);
        let mut retry_backoff = tokio_retry::strategy::ExponentialBackoff::from_millis(10)
            .max_delay(max_retry_interval)
            .map(jitter);

        let mut retry_count = 0;
        loop {
            if retry_count > max_retry {
                break Some(Err(HummockError::meta_error(format!(
                    "pin_version max retry reached: {}.",
                    max_retry
                ))));
            }
            if break_condition() {
                break None;
            }
            match hummock_meta_client.pin_version(last_pinned).await {
                Ok(version) => {
                    break Some(Ok(version));
                }
                Err(err) => {
                    let retry_after = retry_backoff.next().unwrap_or(max_retry_interval);
                    tracing::warn!(
                        "Failed to pin version {:?}. Will retry after about {} milliseconds",
                        err,
                        retry_after.as_millis()
                    );
                    tokio::time::sleep(retry_after).await;
                }
            }
            retry_count += 1;
        }
    }

    async fn start_pin_worker(
        local_version_manager_weak: Weak<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let min_execute_interval = Duration::from_millis(100);
        let mut min_execute_interval_tick = tokio::time::interval(min_execute_interval);
        min_execute_interval_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            min_execute_interval_tick.tick().await;
            let local_version_manager = match local_version_manager_weak.upgrade() {
                None => {
                    tracing::info!("Shutdown hummock pin worker");
                    return;
                }
                Some(local_version_manager) => local_version_manager,
            };

            let last_pinned = local_version_manager
                .local_version
                .read()
                .pinned_version()
                .id();

            match Self::pin_version_with_retry(
                hummock_meta_client.clone(),
                last_pinned,
                usize::MAX,
                || {
                    // Should stop when the `local_version_manager` in this thread is the only
                    // strong reference to the object.
                    local_version_manager_weak.strong_count() == 1
                },
            )
            .await
            {
                Some(Ok(pinned_version)) => {
                    local_version_manager.try_update_pinned_version(pinned_version);
                }
                Some(Err(_)) => {
                    unreachable!(
                        "since the max_retry is `usize::MAX`, this should never return `Err`"
                    );
                }
                None => {
                    tracing::info!("Shutdown hummock pin worker");
                    return;
                }
            };
        }
    }

    async fn start_unpin_worker(
        mut rx: UnboundedReceiver<HummockVersionId>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let min_execute_interval = Duration::from_millis(1000);
        let max_retry_interval = Duration::from_secs(10);
        let get_backoff_strategy = || {
            tokio_retry::strategy::ExponentialBackoff::from_millis(10)
                .max_delay(max_retry_interval)
                .map(jitter)
        };
        let mut retry_backoff = get_backoff_strategy();
        let mut min_execute_interval_tick = tokio::time::interval(min_execute_interval);
        min_execute_interval_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut versions_to_unpin = vec![];
        // For each run in the loop, accumulate versions to unpin and call unpin RPC once.
        loop {
            min_execute_interval_tick.tick().await;
            // 1. Collect new versions to unpin.
            'collect: loop {
                match rx.try_recv() {
                    Ok(version) => {
                        versions_to_unpin.push(version);
                    }
                    Err(err) => match err {
                        TryRecvError::Empty => {
                            break 'collect;
                        }
                        TryRecvError::Disconnected => {
                            tracing::info!("Shutdown hummock unpin worker");
                            return;
                        }
                    },
                }
            }
            if versions_to_unpin.is_empty() {
                continue;
            }
            // 2. Call unpin RPC, including versions failed to unpin in previous RPC calls.
            match hummock_meta_client.unpin_version(&versions_to_unpin).await {
                Ok(_) => {
                    versions_to_unpin.clear();
                    retry_backoff = get_backoff_strategy();
                }
                Err(err) => {
                    let retry_after = retry_backoff.next().unwrap_or(max_retry_interval);
                    tracing::warn!(
                        "Failed to unpin version {:?}. Will retry after about {} milliseconds",
                        err,
                        retry_after.as_millis()
                    );
                    tokio::time::sleep(retry_after).await;
                }
            }
        }
    }

    #[cfg(test)]
    pub async fn refresh_version(&self, hummock_meta_client: &dyn HummockMetaClient) -> bool {
        let last_pinned = self.get_pinned_version().id();
        let version = hummock_meta_client.pin_version(last_pinned).await.unwrap();
        self.try_update_pinned_version(version)
    }

    #[cfg(test)]
    pub fn get_local_version(&self) -> LocalVersion {
        self.local_version.read().clone()
    }

    #[cfg(test)]
    pub fn get_shared_buffer_size(&self) -> usize {
        self.buffer_tracker.get_replicate_size() + self.buffer_tracker.get_upload_size()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_hummock_sdk::HummockSSTableId;
    use risingwave_meta::hummock::test_utils::setup_compute_env;
    use risingwave_meta::hummock::MockHummockMetaClient;
    use risingwave_pb::hummock::{HummockVersion, KeyRange, SstableInfo};

    use super::LocalVersionManager;
    use crate::hummock::conflict_detector::ConflictDetector;
    use crate::hummock::iterator::test_utils::{iterator_test_key_of_epoch, mock_sstable_store};
    use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
    use crate::hummock::test_utils::default_config_for_test;
    use crate::monitor::StateStoreMetrics;
    use crate::storage_value::{StorageValue, ValueMeta};

    fn gen_dummy_batch(epoch: u64) -> Vec<(Bytes, StorageValue)> {
        vec![(
            iterator_test_key_of_epoch(0, epoch).into(),
            StorageValue::new_put(ValueMeta::default(), b"value1".to_vec()),
        )]
    }

    fn gen_dummy_sst_info(id: HummockSSTableId, batches: Vec<SharedBufferBatch>) -> SstableInfo {
        let mut min_key: Vec<u8> = batches[0].start_key().to_vec();
        let mut max_key: Vec<u8> = batches[0].end_key().to_vec();
        for batch in batches.iter().skip(1) {
            if min_key.as_slice() > batch.start_key() {
                min_key = batch.start_key().to_vec();
            }
            if max_key.as_slice() < batch.end_key() {
                max_key = batch.end_key().to_vec();
            }
        }
        SstableInfo {
            id,
            key_range: Some(KeyRange {
                left: min_key,
                right: max_key,
                inf: false,
            }),
            file_size: batches.len() as u64,
            vnode_bitmaps: vec![],
        }
    }

    #[tokio::test]
    async fn test_update_pinned_version() {
        let opt = Arc::new(default_config_for_test());
        let (_, hummock_manager_ref, _, worker_node) = setup_compute_env(8080).await;
        let local_version_manager = LocalVersionManager::new(
            opt.clone(),
            mock_sstable_store(),
            Arc::new(StateStoreMetrics::unused()),
            Arc::new(MockHummockMetaClient::new(
                hummock_manager_ref.clone(),
                worker_node.id,
            )),
            ConflictDetector::new_from_config(opt),
        )
        .await;

        let pinned_version = local_version_manager.get_pinned_version();
        let initial_version_id = pinned_version.id();
        let initial_max_commit_epoch = pinned_version.max_committed_epoch();

        let epochs: Vec<u64> = vec![
            initial_max_commit_epoch + 1,
            initial_max_commit_epoch + 2,
            initial_max_commit_epoch + 3,
            initial_max_commit_epoch + 4,
        ];
        let batches: Vec<Vec<(Bytes, StorageValue)>> =
            epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

        // Fill shared buffer with a dummy empty batch in epochs[0] and epochs[1]
        for i in 0..2 {
            local_version_manager
                .write_shared_buffer(epochs[i], batches[i].clone(), false)
                .await
                .unwrap();
            let local_version = local_version_manager.get_local_version();
            assert_eq!(
                local_version
                    .get_shared_buffer(epochs[i])
                    .unwrap()
                    .read()
                    .size(),
                SharedBufferBatch::measure_batch_size(
                    &LocalVersionManager::build_shared_buffer_item_batches(
                        batches[i].clone(),
                        epochs[i]
                    )
                )
            );
        }

        // Update version for epochs[0]
        let version = HummockVersion {
            id: initial_version_id + 1,
            max_committed_epoch: epochs[0],
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version);
        let local_version = local_version_manager.get_local_version();
        assert!(local_version.get_shared_buffer(epochs[0]).is_none());
        assert_eq!(
            local_version
                .get_shared_buffer(epochs[1])
                .unwrap()
                .read()
                .size(),
            SharedBufferBatch::measure_batch_size(
                &LocalVersionManager::build_shared_buffer_item_batches(
                    batches[1].clone(),
                    epochs[1]
                )
            )
        );

        // Update version for epochs[1]
        let version = HummockVersion {
            id: initial_version_id + 2,
            max_committed_epoch: epochs[1],
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version);
        let local_version = local_version_manager.get_local_version();
        assert!(local_version.get_shared_buffer(epochs[0]).is_none());
        assert!(local_version.get_shared_buffer(epochs[1]).is_none());
    }

    #[tokio::test]
    async fn test_update_uncommitted_ssts() {
        let opt = Arc::new(default_config_for_test());
        let (_, hummock_manager_ref, _, worker_node) = setup_compute_env(8080).await;
        let local_version_manager = LocalVersionManager::new(
            opt.clone(),
            mock_sstable_store(),
            Arc::new(StateStoreMetrics::unused()),
            Arc::new(MockHummockMetaClient::new(
                hummock_manager_ref.clone(),
                worker_node.id,
            )),
            ConflictDetector::new_from_config(opt),
        )
        .await;

        let pinned_version = local_version_manager.get_pinned_version();
        let max_commit_epoch = pinned_version.max_committed_epoch();
        let initial_id = pinned_version.id();
        let version = pinned_version.version();

        let epochs: Vec<u64> = vec![max_commit_epoch + 1, max_commit_epoch + 2];
        let buffer_tracker = Arc::new(AtomicUsize::new(0));
        let batches: Vec<Vec<(Bytes, StorageValue)>> =
            epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

        // Fill shared buffer with dummy batches
        for i in 0..2 {
            local_version_manager
                .write_shared_buffer(epochs[i], batches[i].clone(), false)
                .await
                .unwrap();
            let local_version = local_version_manager.get_local_version();
            assert_eq!(
                local_version
                    .get_shared_buffer(epochs[i])
                    .unwrap()
                    .read()
                    .size(),
                SharedBufferBatch::measure_batch_size(
                    &LocalVersionManager::build_shared_buffer_item_batches(
                        batches[i].clone(),
                        epochs[i]
                    )
                )
            );
        }

        // Update uncommitted sst for epochs[0]
        let uploaded_batches: Vec<SharedBufferBatch> = vec![SharedBufferBatch::new(
            LocalVersionManager::build_shared_buffer_item_batches(batches[0].clone(), epochs[0]),
            epochs[0],
            buffer_tracker.clone(),
        )];
        let sst1 = gen_dummy_sst_info(1, uploaded_batches.clone());
        local_version_manager.update_uncommitted_ssts(
            epochs[0],
            vec![sst1.clone()],
            uploaded_batches,
        );
        let local_version = local_version_manager.get_local_version();
        // Check shared buffer
        assert_eq!(
            local_version
                .get_shared_buffer(epochs[0])
                .unwrap()
                .read()
                .size(),
            0
        );
        assert_eq!(
            local_version
                .get_shared_buffer(epochs[1])
                .unwrap()
                .read()
                .size(),
            SharedBufferBatch::measure_batch_size(
                &LocalVersionManager::build_shared_buffer_item_batches(
                    batches[1].clone(),
                    epochs[1]
                )
            )
        );

        // Check pinned version
        assert_eq!(local_version.pinned_version().version(), version);
        // Check uncommitted ssts
        let uncommitted_ssts = local_version.get_uncommitted_ssts();
        assert_eq!(uncommitted_ssts.len(), 1);
        let epoch_uncommitted_ssts = uncommitted_ssts.get(&epochs[0]).unwrap();
        assert_eq!(epoch_uncommitted_ssts.len(), 1);
        assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst1);

        // Update uncommitted sst for epochs[1]
        let uploaded_batches: Vec<SharedBufferBatch> = vec![SharedBufferBatch::new(
            LocalVersionManager::build_shared_buffer_item_batches(batches[1].clone(), epochs[1]),
            epochs[1],
            buffer_tracker.clone(),
        )];
        let sst2 = gen_dummy_sst_info(2, uploaded_batches.clone());
        local_version_manager.update_uncommitted_ssts(
            epochs[1],
            vec![sst2.clone()],
            uploaded_batches,
        );
        let local_version = local_version_manager.get_local_version();
        // Check shared buffer
        for epoch in &epochs {
            assert_eq!(
                local_version
                    .get_shared_buffer(*epoch)
                    .unwrap()
                    .read()
                    .size(),
                0
            );
        }
        // Check pinned version
        assert_eq!(local_version.pinned_version().version(), version);
        // Check uncommitted ssts
        let uncommitted_ssts = local_version.get_uncommitted_ssts();
        assert_eq!(uncommitted_ssts.len(), 2);
        let epoch_uncommitted_ssts = uncommitted_ssts.get(&epochs[1]).unwrap();
        assert_eq!(epoch_uncommitted_ssts.len(), 1);
        assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst2);

        // Update version for epochs[0]
        let version = HummockVersion {
            id: initial_id + 1,
            max_committed_epoch: epochs[0],
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version.clone());
        let local_version = local_version_manager.get_local_version();
        // Check shared buffer
        assert!(local_version.get_shared_buffer(epochs[0]).is_none());
        assert_eq!(
            local_version
                .get_shared_buffer(epochs[1])
                .unwrap()
                .read()
                .size(),
            0
        );
        // Check pinned version
        assert_eq!(local_version.pinned_version().version(), version);
        // Check uncommitted ssts
        let uncommitted_ssts = local_version.get_uncommitted_ssts();
        assert!(uncommitted_ssts.get(&epochs[0]).is_none());
        let epoch_uncommitted_ssts = uncommitted_ssts.get(&epochs[1]).unwrap();
        assert_eq!(epoch_uncommitted_ssts.len(), 1);
        assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst2);

        // Update version for epochs[1]
        let version = HummockVersion {
            id: initial_id + 2,
            max_committed_epoch: epochs[1],
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version.clone());
        let local_version = local_version_manager.get_local_version();
        assert!(local_version.get_shared_buffer(epochs[0]).is_none());
        assert!(local_version.get_shared_buffer(epochs[1]).is_none());
        // Check pinned version
        assert_eq!(local_version.pinned_version().version(), version);
        // Check uncommitted ssts
        let uncommitted_ssts = local_version.get_uncommitted_ssts();
        assert!(uncommitted_ssts.get(&epochs[0]).is_none());
        assert!(uncommitted_ssts.get(&epochs[1]).is_none());
    }
}
