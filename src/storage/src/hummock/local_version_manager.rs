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

use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use prometheus::core::{Atomic, AtomicU64};
use risingwave_common::config::StorageConfig;
use risingwave_pb::hummock::{HummockVersion, SstableInfo};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio_retry::strategy::jitter;

use super::local_version::{LocalVerion, PinnedVersion, ReadVersion};
use super::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use super::shared_buffer::shared_buffer_uploader::{SharedBufferUploader, UploadItem};
use super::SstableStoreRef;
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockVersionId, INVALID_VERSION_ID,
};
use crate::monitor::StateStoreMetrics;
struct WorkerContext {
    version_update_notifier_tx: tokio::sync::watch::Sender<HummockVersionId>,
    version_unpin_worker_tx: UnboundedSender<HummockVersionId>,
    shared_buffer_uploader_tx: UnboundedSender<UploadItem>,

    /// (version_unpin_worker_rx, shared_buffer_uploader_rx)
    /// Become none after the worker is started.
    worker_rx: Mutex<
        Option<(
            UnboundedReceiver<HummockVersionId>,
            UnboundedReceiver<UploadItem>,
        )>,
    >,
}

struct BufferTracker {
    capacity: u64,
    size: AtomicU64,
}

impl BufferTracker {
    pub fn is_empty(&self) -> bool {
        self.size.get() == 0
    }

    pub fn can_write(&self, batch_size: u64) -> bool {
        self.size.get() + batch_size <= self.capacity
    }

    pub fn inc(&self, delta: u64) {
        self.size.inc_by(delta)
    }

    pub fn dec(&self, delta: u64) {
        self.size.dec_by(delta)
    }
}

/// The `LocalVersionManager` maintains a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `SSTables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    local_version: RwLock<Option<LocalVerion>>,
    worker_context: WorkerContext,
    buffer_tracker: BufferTracker,
}

impl LocalVersionManager {
    pub fn new(options: Arc<StorageConfig>) -> LocalVersionManager {
        let (version_update_notifier_tx, _) = tokio::sync::watch::channel(INVALID_VERSION_ID);
        let (shared_buffer_uploader_tx, shared_buffer_uploader_rx) =
            tokio::sync::mpsc::unbounded_channel();
        let (version_unpin_worker_tx, version_unpin_worker_rx) =
            tokio::sync::mpsc::unbounded_channel();

        LocalVersionManager {
            local_version: RwLock::new(None),
            worker_context: WorkerContext {
                version_update_notifier_tx,
                version_unpin_worker_tx,
                shared_buffer_uploader_tx,
                worker_rx: Mutex::new(Some((version_unpin_worker_rx, shared_buffer_uploader_rx))),
            },
            buffer_tracker: BufferTracker {
                capacity: options.shared_buffer_capacity as u64,
                size: AtomicU64::new(0),
            },
        }
    }

    pub fn start_workers(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        local_version_manager: Arc<LocalVersionManager>,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let local_version_manager_for_uploader = local_version_manager.clone();
        if let Some((version_unpin_worker_rx, shared_buffer_uploader_rx)) =
            local_version_manager.worker_context.worker_rx.lock().take()
        {
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
                options.shared_buffer_threshold as usize,
                sstable_store,
                local_version_manager_for_uploader,
                hummock_meta_client,
                shared_buffer_uploader_rx,
                stats,
            );
            tokio::spawn(async move { uploader.run().await });
        }
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

        if let Some(cached_version) = guard.as_mut() {
            if cached_version.pinned_version().id() >= new_version_id {
                return false;
            }

            let buffer_to_release = cached_version.set_pinned_version(newly_pinned_version);
            let removed_size = buffer_to_release
                .iter()
                .fold(0, |acc, x| acc + x.1.read().size());
            self.buffer_tracker.dec(removed_size);
        } else {
            *guard = Some(LocalVerion::new(
                newly_pinned_version,
                self.worker_context.version_unpin_worker_tx.clone(),
            ));
        }

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
        if let Some(cached_version) = guard.as_mut() {
            cached_version.add_uncommitted_ssts(epoch, sst_info);

            if let Some(shared_buffer) = cached_version.get_shared_buffer(epoch) {
                let mut guard = shared_buffer.write();
                // TODO memory
                let mut removed_size = 0;
                for batch in shared_buffer_batches {
                    if let Some(removed_batch) = guard.delete_batch(batch) {
                        removed_size += removed_batch.size();
                    }
                }
                self.buffer_tracker.dec(removed_size);
            }
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
                if let Some(version) = current_version.as_ref() {
                    if version.pinned_version().max_committed_epoch() >= epoch {
                        return Ok(());
                    }
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

    pub async fn write_shared_buffer(
        &self,
        epoch: HummockEpoch,
        batch: SharedBufferBatch,
        is_remote_batch: bool,
    ) -> HummockResult<()> {
        let batch_size = batch.size();
        while !self.buffer_tracker.can_write(batch_size) {
            self.sync_shared_buffer(None).await?;
        }

        // Try get shared buffer with version read lock
        let read_guard = self.local_version.read();
        let shared_buffer = match read_guard.as_ref() {
            None => return Err(HummockError::meta_error("No version found.")),
            Some(current_version) => current_version.get_shared_buffer(epoch).cloned(),
        };
        drop(read_guard);

        // New a shared buffer with version write lock if shared buffer of the corresponding epoch
        // does not exist before
        let shared_buffer = shared_buffer.unwrap_or_else(|| {
            self.local_version
                .write()
                .as_mut()
                .unwrap()
                .new_shared_buffer(epoch)
        });

        // Write into shared buffer
        if is_remote_batch {
            // The batch won't be synced to S3 asynchronously if it is a remote batch
            shared_buffer.write().write_batch(batch);
        } else {
            // The batch will be synced to S3 asynchronously if it is a local batch
            shared_buffer.write().write_batch(batch.clone());
            self.worker_context
                .shared_buffer_uploader_tx
                .send(UploadItem::Batch(batch))
                .map_err(HummockError::shared_buffer_error)?;
        }

        self.buffer_tracker.inc(batch_size);

        Ok(())
    }

    pub async fn sync_shared_buffer(&self, epoch: Option<HummockEpoch>) -> HummockResult<()> {
        if self.buffer_tracker.is_empty() {
            return Ok(());
        }

        let (tx, rx) = oneshot::channel();
        self.worker_context
            .shared_buffer_uploader_tx
            .send(UploadItem::Sync {
                epoch,
                notifier: tx,
            })
            .map_err(HummockError::shared_buffer_error)?;
        rx.await.map_err(HummockError::shared_buffer_error)?;
        Ok(())
    }

    pub fn read_version(
        self: &Arc<LocalVersionManager>,
        read_epoch: HummockEpoch,
    ) -> HummockResult<ReadVersion> {
        match self.local_version.read().as_ref() {
            None => Err(HummockError::meta_error("No version found.")),
            Some(current_version) => Ok(current_version.read_version(read_epoch)),
        }
    }

    pub fn get_pinned_version(&self) -> HummockResult<Arc<PinnedVersion>> {
        match self.local_version.read().as_ref() {
            None => Err(HummockError::meta_error("No version found.")),
            Some(current_version) => Ok(current_version.pinned_version().clone()),
        }
    }

    async fn start_pin_worker(
        local_version_manager: Weak<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let max_retry_interval = Duration::from_secs(10);
        let min_execute_interval = Duration::from_millis(100);
        let get_backoff_strategy = || {
            tokio_retry::strategy::ExponentialBackoff::from_millis(10)
                .max_delay(max_retry_interval)
                .map(jitter)
        };
        let mut retry_backoff = get_backoff_strategy();
        let mut min_execute_interval_tick = tokio::time::interval(min_execute_interval);
        loop {
            min_execute_interval_tick.tick().await;
            let local_version_manager = match local_version_manager.upgrade() {
                None => {
                    tracing::info!("Shutdown hummock pin worker");
                    return;
                }
                Some(local_version_manager) => local_version_manager,
            };
            let last_pinned = match local_version_manager.local_version.read().as_ref() {
                None => INVALID_VERSION_ID,
                Some(v) => v.pinned_version().id(),
            };
            match hummock_meta_client.pin_version(last_pinned).await {
                Ok(version) => {
                    local_version_manager.try_update_pinned_version(version);
                    retry_backoff = get_backoff_strategy();
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
        let last_pinned = self.get_pinned_version().unwrap().id();
        let version = hummock_meta_client.pin_version(last_pinned).await.unwrap();
        self.try_update_pinned_version(version)
    }

    #[cfg(test)]
    pub fn get_local_version(&self) -> LocalVerion {
        self.local_version.read().as_ref().unwrap().clone()
    }

    #[cfg(test)]
    pub fn get_shared_buffer_size(&self) -> u64 {
        self.buffer_tracker.size.get()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_pb::hummock::{HummockVersion, KeyRange, SstableInfo};

    use super::LocalVersionManager;
    use crate::hummock::iterator::test_utils::iterator_test_key_of_epoch;
    use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
    use crate::hummock::test_utils::default_config_for_test;
    use crate::hummock::value::HummockValue;

    fn gen_dummy_batch(epoch: u64) -> SharedBufferBatch {
        SharedBufferBatch::new(
            vec![(
                iterator_test_key_of_epoch(0, epoch).into(),
                HummockValue::put(b"value1".to_vec()).into(),
            )],
            epoch,
        )
    }

    fn gen_dummy_sst_info(id: u64, batches: Vec<SharedBufferBatch>) -> SstableInfo {
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
        }
    }

    #[tokio::test]
    async fn test_update_pinned_version() {
        let local_version_manager = Arc::new(LocalVersionManager::new(Arc::new(
            default_config_for_test(),
        )));
        let version = HummockVersion {
            id: 0,
            max_committed_epoch: 0,
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version);

        let epochs: Vec<u64> = vec![1, 2, 3, 4];
        let batches: Vec<SharedBufferBatch> = epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

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
                batches[i].size()
            );
        }

        // Update version for epochs[0]
        let version = HummockVersion {
            id: 1,
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
            batches[1].size()
        );

        // Update version for epochs[1]
        let version = HummockVersion {
            id: 2,
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
        let local_version_manager = Arc::new(LocalVersionManager::new(Arc::new(
            default_config_for_test(),
        )));
        let version = HummockVersion {
            id: 0,
            max_committed_epoch: 0,
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version.clone());

        let epochs: Vec<u64> = vec![1, 2];
        let batches: Vec<SharedBufferBatch> = epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

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
                batches[i].size()
            );
        }

        // Update uncommitted sst for epochs[0]
        let uploaded_batches: Vec<SharedBufferBatch> = vec![batches[0].clone()];
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
            batches[1].size()
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
        let uploaded_batches: Vec<SharedBufferBatch> = vec![batches[1].clone()];
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
            id: 1,
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
            id: 2,
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
