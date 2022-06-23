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

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use itertools::Itertools;
use parking_lot::RwLock;
use risingwave_common::config::StorageConfig;
use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
use risingwave_hummock_sdk::key::FullKey;
use risingwave_hummock_sdk::LocalSstableInfo;
use risingwave_pb::hummock::HummockVersion;
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_retry::strategy::jitter;
use tracing::error;

use super::local_version::{LocalVersion, PinnedVersion, ReadVersion};
use super::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use super::shared_buffer::shared_buffer_uploader::{SharedBufferUploader, UploadItem};
use super::SstableStoreRef;
use crate::hummock::conflict_detector::ConflictDetector;
use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferItem;
use crate::hummock::shared_buffer::shared_buffer_uploader::UploadTask;
use crate::hummock::shared_buffer::UploadTaskType::{FlushWriteBatch, SyncEpoch};
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
    #[expect(dead_code)]
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
        let uploader = SharedBufferUploader::new(
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
        for levels in newly_pinned_version.levels.values() {
            if validate_table_key_range(&levels.levels).is_err() {
                error!("invalid table key range: {:?}", levels.levels);
                return false;
            }
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
            // TODO: may apply high-low memory threshold here to avoid always await here.
            if !self.flush_shared_buffer().await? {
                // The flush has no effect. yield to avoid occupying the execution endlessly.
                tokio::task::yield_now().await;
            }
        }

        // TODO #2065: use correct compaction group id
        let batch = SharedBufferBatch::new_with_size(
            sorted_items,
            epoch,
            batch_size,
            if is_remote_batch {
                self.buffer_tracker.replicate_size.clone()
            } else {
                self.buffer_tracker.upload_size.clone()
            },
            StaticCompactionGroupId::StateDefault.into(),
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

    pub async fn flush_shared_buffer(&self) -> HummockResult<bool> {
        // The current implementation is a trivial one, which issue only one flush task and wait for
        // the task to finish.
        //
        // TODO: apply high-low threshold here and avoid always await here.
        let mut task = None;
        for (epoch, shared_buffer) in self.local_version.read().iter_shared_buffer() {
            if let Some((order_index, task_data)) =
                shared_buffer.write().new_upload_task(FlushWriteBatch)
            {
                // TODO: may apply different `is_local` according to whether local spill is enabled.
                task = Some(UploadTask::new(order_index, *epoch, task_data, true));
                break;
            }
        }
        let task = match task {
            Some(task) => task,
            None => return Ok(false),
        };

        let epoch = task.epoch;

        let (tx, rx) = oneshot::channel();
        self.worker_context
            .shared_buffer_uploader_tx
            .send(UploadItem::new(vec![task], tx))
            .map_err(HummockError::shared_buffer_error)?;
        let ((_, order_index), task_result) = rx
            .await
            .map_err(HummockError::shared_buffer_error)?
            .pop_first()
            .expect("the result should not be empty");

        let local_version_guard = self.local_version.read();
        let mut shared_buffer_guard = local_version_guard
            .get_shared_buffer(epoch)
            .expect("shared buffer should exist since some uncommitted data is not committed yet")
            .write();

        match task_result {
            Ok(ssts) => {
                shared_buffer_guard.succeed_upload_task(order_index, ssts);
                Ok(true)
            }
            Err(e) => {
                shared_buffer_guard.fail_upload_task(order_index);
                Err(e)
            }
        }
    }

    pub async fn sync_shared_buffer(&self, epoch: Option<HummockEpoch>) -> HummockResult<()> {
        let epochs = match epoch {
            Some(epoch) => vec![epoch],
            None => self
                .local_version
                .read()
                .iter_shared_buffer()
                .map(|(epoch, _)| *epoch)
                .collect(),
        };
        for epoch in epochs {
            self.sync_shared_buffer_epoch(epoch).await?;
        }
        Ok(())
    }

    pub async fn sync_shared_buffer_epoch(&self, epoch: HummockEpoch) -> HummockResult<()> {
        let task = {
            match self.local_version.read().new_upload_task(epoch, SyncEpoch) {
                Some((order_index, task_data)) => {
                    UploadTask::new(order_index, epoch, task_data, false)
                }
                None => return Ok(()),
            }
        };

        let (tx, rx) = oneshot::channel();
        self.worker_context
            .shared_buffer_uploader_tx
            .send(UploadItem::new(vec![task], tx))
            .map_err(HummockError::shared_buffer_error)?;
        let ((_, order_index), task_result) = rx
            .await
            .map_err(HummockError::shared_buffer_error)?
            .pop_first()
            .expect("the result should not be empty");

        let local_version_guard = self.local_version.read();
        let mut shared_buffer_guard = local_version_guard
            .get_shared_buffer(epoch)
            .expect("shared buffer should exist since some uncommitted data is not committed yet")
            .write();

        match task_result {
            Ok(ssts) => {
                shared_buffer_guard.succeed_upload_task(order_index, ssts);
                if let Some(conflict_detector) = self.write_conflict_detector.as_ref() {
                    conflict_detector.archive_epoch(epoch);
                }
                Ok(())
            }
            Err(e) => {
                shared_buffer_guard.fail_upload_task(order_index);
                Err(e)
            }
        }
    }

    pub fn read_version(self: &Arc<LocalVersionManager>, read_epoch: HummockEpoch) -> ReadVersion {
        LocalVersion::read_version(&self.local_version, read_epoch)
    }

    pub fn get_pinned_version(&self) -> Arc<PinnedVersion> {
        self.local_version.read().pinned_version().clone()
    }

    pub fn get_uncommitted_ssts(&self, epoch: HummockEpoch) -> Vec<LocalSstableInfo> {
        self.local_version
            .read()
            .get_shared_buffer(epoch)
            .map(|shared_buffer| shared_buffer.read().get_ssts_to_commit())
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
    use itertools::Itertools;
    use risingwave_hummock_sdk::compaction_group::StaticCompactionGroupId;
    use risingwave_meta::hummock::test_utils::setup_compute_env;
    use risingwave_meta::hummock::MockHummockMetaClient;
    use risingwave_pb::hummock::HummockVersion;

    use super::LocalVersionManager;
    use crate::hummock::conflict_detector::ConflictDetector;
    use crate::hummock::iterator::test_utils::mock_sstable_store;
    use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
    use crate::hummock::shared_buffer::UncommittedData;
    use crate::hummock::shared_buffer::UploadTaskType::SyncEpoch;
    use crate::hummock::test_utils::{
        default_config_for_test, gen_dummy_batch, gen_dummy_sst_info,
    };
    use crate::monitor::StateStoreMetrics;
    use crate::storage_value::StorageValue;

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
        let kvs: Vec<Vec<(Bytes, StorageValue)>> =
            epochs.iter().map(|e| gen_dummy_batch(*e)).collect();
        let mut batches = Vec::with_capacity(kvs.len());

        // Fill shared buffer with dummy batches
        for i in 0..2 {
            local_version_manager
                .write_shared_buffer(epochs[i], kvs[i].clone(), false)
                .await
                .unwrap();
            let local_version = local_version_manager.get_local_version();
            let batch = SharedBufferBatch::new(
                LocalVersionManager::build_shared_buffer_item_batches(kvs[i].clone(), epochs[i]),
                epochs[i],
                buffer_tracker.clone(),
                StaticCompactionGroupId::StateDefault.into(),
            );
            assert_eq!(
                local_version
                    .get_shared_buffer(epochs[i])
                    .unwrap()
                    .read()
                    .size(),
                batch.size(),
            );
            batches.push(batch);
        }

        // Update uncommitted sst for epochs[0]
        let sst1 = gen_dummy_sst_info(1, vec![batches[0].clone()]);
        {
            let local_version_guard = local_version_manager.local_version.read();
            let mut shared_buffer_guard = local_version_guard
                .get_shared_buffer(epochs[0])
                .unwrap()
                .write();
            let (task_id, payload) = shared_buffer_guard.new_upload_task(SyncEpoch).unwrap();
            {
                assert_eq!(1, payload.len());
                assert_eq!(1, payload[0].len());
                assert_eq!(payload[0][0], UncommittedData::Batch(batches[0].clone()));
            }
            shared_buffer_guard.succeed_upload_task(
                task_id,
                vec![(StaticCompactionGroupId::StateDefault.into(), sst1.clone())],
            );
        }

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
            batches[1].size(),
        );

        // Check pinned version
        assert_eq!(local_version.pinned_version().version(), version);
        // Check uncommitted ssts
        assert_eq!(local_version.iter_shared_buffer().count(), 2);
        let epoch_uncommitted_ssts = local_version
            .get_shared_buffer(epochs[0])
            .unwrap()
            .read()
            .get_ssts_to_commit()
            .into_iter()
            .map(|(_, sst)| sst)
            .collect_vec();
        assert_eq!(epoch_uncommitted_ssts.len(), 1);
        assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst1);

        // Update uncommitted sst for epochs[1]
        let sst2 = gen_dummy_sst_info(2, vec![batches[1].clone()]);
        {
            let local_version_guard = local_version_manager.local_version.read();
            let mut shared_buffer_guard = local_version_guard
                .get_shared_buffer(epochs[1])
                .unwrap()
                .write();
            let (task_id, payload) = shared_buffer_guard.new_upload_task(SyncEpoch).unwrap();
            {
                assert_eq!(1, payload.len());
                assert_eq!(1, payload[0].len());
                assert_eq!(payload[0][0], UncommittedData::Batch(batches[1].clone()));
            }
            shared_buffer_guard.succeed_upload_task(
                task_id,
                vec![(StaticCompactionGroupId::StateDefault.into(), sst2.clone())],
            );
        }
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
        let epoch_uncommitted_ssts = local_version
            .get_shared_buffer(epochs[1])
            .unwrap()
            .read()
            .get_ssts_to_commit()
            .into_iter()
            .map(|(_, sst)| sst)
            .collect_vec();
        assert_eq!(epoch_uncommitted_ssts.len(), 1);
        assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst2);

        // Update version for epochs[0]
        let version = HummockVersion {
            id: initial_id + 1,
            max_committed_epoch: epochs[0],
            ..Default::default()
        };
        assert!(local_version_manager.try_update_pinned_version(version.clone()));
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
        assert!(local_version.get_shared_buffer(epochs[0]).is_none());
        let epoch_uncommitted_ssts = local_version
            .get_shared_buffer(epochs[1])
            .unwrap()
            .read()
            .get_ssts_to_commit()
            .into_iter()
            .map(|(_, sst)| sst)
            .collect_vec();
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
        assert!(local_version.get_shared_buffer(epochs[0]).is_none());
        assert!(local_version.get_shared_buffer(epochs[1]).is_none());
    }
}
