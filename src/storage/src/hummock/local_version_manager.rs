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

use std::collections::btree_map::BTreeMap;
use std::ops::{DerefMut, RangeBounds};
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use risingwave_common::config::StorageConfig;
use risingwave_pb::hummock::{HummockVersion, Level, SstableInfo};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_retry::strategy::jitter;

use super::shared_buffer::shared_buffer_batch::SharedBufferBatch;
use super::SstableStoreRef;
use crate::hummock::shared_buffer::shared_buffer_manager::SharedBufferManager;
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockVersionId, INVALID_VERSION_ID,
};
use crate::monitor::StateStoreMetrics;

/// Ssts that have been uploaded to S3 by the local node but not committed.
#[derive(Debug, Clone)]
pub struct UncommittedSsts {
    pub ssts: BTreeMap<HummockEpoch, Vec<SstableInfo>>,
}

impl UncommittedSsts {
    fn new() -> Self {
        UncommittedSsts {
            ssts: BTreeMap::new(),
        }
    }

    fn add_ssts(&mut self, epoch: HummockEpoch, ssts: Vec<SstableInfo>) {
        self.ssts.entry(epoch).or_default().extend(ssts);
    }

    fn commit_epoch(&mut self, epoch: HummockEpoch) {
        let mut new_ssts = self.ssts.split_off(&(epoch + 1));
        std::mem::swap(&mut self.ssts, &mut new_ssts);
    }

    // Get an SstableInfo iterator for ssts with epoch >= epoch_lower_bound.
    // The returned ssts are ordered by epoch desendingly.
    pub fn get_sst_iter(
        &self,
        epoch_lower_bound: HummockEpoch,
    ) -> impl Iterator<Item = &SstableInfo> {
        self.ssts.range(epoch_lower_bound..).rev().flat_map(|s| s.1)
    }
}

#[derive(Debug, Clone)]
pub struct LocalVerion {
    uncommitted_ssts: UncommittedSsts,
    pinned_version: Arc<PinnedVersion>,
}

impl LocalVerion {
    fn new(
        uncommitted_ssts: UncommittedSsts,
        version: HummockVersion,
        unpin_worker_tx: UnboundedSender<HummockVersionId>,
    ) -> Self {
        Self {
            uncommitted_ssts,
            pinned_version: Arc::new(PinnedVersion::new(version, unpin_worker_tx)),
        }
    }

    pub fn pinned_version(&self) -> Arc<PinnedVersion> {
        self.pinned_version.clone()
    }

    pub fn uncommitted_ssts(&self) -> &UncommittedSsts {
        &self.uncommitted_ssts
    }

    pub fn uncommitted_ssts_mut(&mut self) -> &mut UncommittedSsts {
        &mut self.uncommitted_ssts
    }

    pub fn id(&self) -> HummockVersionId {
        self.pinned_version.version.id
    }

    pub fn levels(&self) -> Vec<Level> {
        self.pinned_version.version.levels.clone()
    }

    pub fn max_committed_epoch(&self) -> u64 {
        self.pinned_version.version.max_committed_epoch
    }

    pub fn safe_epoch(&self) -> u64 {
        self.pinned_version.version.safe_epoch
    }
}

#[derive(Debug)]
pub struct PinnedVersion {
    version: HummockVersion,
    unpin_worker_tx: UnboundedSender<HummockVersionId>,
}

impl Drop for PinnedVersion {
    fn drop(&mut self) {
        self.unpin_worker_tx.send(self.version.id).ok();
    }
}

impl PinnedVersion {
    fn new(
        version: HummockVersion,
        unpin_worker_tx: UnboundedSender<HummockVersionId>,
    ) -> PinnedVersion {
        PinnedVersion {
            version,
            unpin_worker_tx,
        }
    }
}

/// The `LocalVersionManager` maintains a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `SSTables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    current_version: RwLock<Option<LocalVerion>>,

    /// Manager for immutable shared buffers
    shared_buffer_manager: SharedBufferManager,

    update_notifier_tx: tokio::sync::watch::Sender<HummockVersionId>,
    unpin_worker_tx: UnboundedSender<HummockVersionId>,
    unpin_worker_rx: Mutex<Option<UnboundedReceiver<HummockVersionId>>>,
}

impl LocalVersionManager {
    pub fn new(options: Arc<StorageConfig>) -> LocalVersionManager {
        let (update_notifier_tx, _) = tokio::sync::watch::channel(INVALID_VERSION_ID);
        let (unpin_worker_tx, unpin_worker_rx) = tokio::sync::mpsc::unbounded_channel();

        LocalVersionManager {
            current_version: RwLock::new(None),
            shared_buffer_manager: SharedBufferManager::new(
                options.shared_buffer_capacity as usize,
            ),
            update_notifier_tx,
            unpin_worker_tx,
            unpin_worker_rx: Mutex::new(Some(unpin_worker_rx)),
        }
    }

    pub fn start_workers(
        options: Arc<StorageConfig>,
        sstable_store: SstableStoreRef,
        local_version_manager: Arc<LocalVersionManager>,
        stats: Arc<StateStoreMetrics>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let unpin_worker_rx = local_version_manager.unpin_worker_rx.lock().take();
        if let Some(unpin_worker_rx) = unpin_worker_rx {
            // Pin and get the latest version.
            tokio::spawn(LocalVersionManager::start_pin_worker(
                Arc::downgrade(&local_version_manager),
                hummock_meta_client.clone(),
            ));
            // Unpin unused version.
            tokio::spawn(LocalVersionManager::start_unpin_worker(
                unpin_worker_rx,
                hummock_meta_client.clone(),
            ));
            // Upload shared buffer to S3.
            local_version_manager.shared_buffer_manager.start_uploader(
                options,
                local_version_manager.clone(),
                sstable_store,
                stats,
                hummock_meta_client,
            )
        }
    }

    /// Updates cached version if the new version is of greater id.
    /// You shouldn't unpin even the method returns false, as it is possible `hummock_version` is
    /// being referenced by some readers.
    pub fn try_update_pinned_version(&self, newly_pinned_version: HummockVersion) -> bool {
        let new_version_id = newly_pinned_version.id;
        let new_max_committed_epoch = newly_pinned_version.max_committed_epoch;
        if validate_table_key_range(&newly_pinned_version.levels).is_err() {
            return false;
        }
        let mut guard = self.current_version.write();
        let mut uncommitted_ssts = UncommittedSsts::new();

        if let Some(cached_version) = guard.as_mut() {
            if cached_version.id() >= new_version_id {
                return false;
            } else {
                // Swap out uncommitted_ssts info into the new version
                std::mem::swap(cached_version.uncommitted_ssts_mut(), &mut uncommitted_ssts);
                if cached_version.max_committed_epoch() < new_max_committed_epoch {
                    // Remove uncommitted_ssts for committed epochs
                    uncommitted_ssts.commit_epoch(new_max_committed_epoch);

                    // Remove replicated shared buffer for committed epochs
                    self.shared_buffer_manager
                        .commit_epoch(new_max_committed_epoch);
                }
            }
        }

        // Update cached version
        *guard.deref_mut() = Some(LocalVerion::new(
            uncommitted_ssts,
            newly_pinned_version,
            self.unpin_worker_tx.clone(),
        ));

        self.update_notifier_tx.send(new_version_id).ok();
        true
    }

    pub fn update_uncommitted_ssts(
        &self,
        epoch: u64,
        sst_info: Vec<SstableInfo>,
        shared_buffer_batches: Vec<SharedBufferBatch>,
    ) {
        let mut guard = self.current_version.write();

        // Record uploaded but uncommitted SSTs.
        if let Some(cached_version) = guard.as_mut() {
            cached_version
                .uncommitted_ssts_mut()
                .add_ssts(epoch, sst_info);
        }

        // Cleanup shared buffer
        self.shared_buffer_manager
            .delete_batches(epoch, shared_buffer_batches);
    }

    /// Waits until the local hummock version contains the given committed epoch
    pub async fn wait_epoch(&self, epoch: HummockEpoch) -> HummockResult<()> {
        if epoch == HummockEpoch::MAX {
            panic!("epoch should not be u64::MAX");
        }
        let mut receiver = self.update_notifier_tx.subscribe();
        loop {
            {
                let current_version = self.current_version.read();
                if let Some(version) = current_version.as_ref() {
                    if version.max_committed_epoch() >= epoch {
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

    pub fn get_version_and_shared_buffer<R, B>(
        self: &Arc<LocalVersionManager>,
        key_range: &R,
        epoch: HummockEpoch,
        reversed_range: bool,
    ) -> HummockResult<(LocalVerion, Vec<SharedBufferBatch>)>
    where
        R: RangeBounds<B>,
        B: AsRef<[u8]>,
    {
        match self.current_version.read().as_ref() {
            None => Err(HummockError::meta_error("No version found.")),
            Some(current_version) => {
                let shared_buffer = if epoch > current_version.max_committed_epoch() {
                    self.shared_buffer_manager.get_overlap_batches(
                        key_range,
                        (current_version.max_committed_epoch() + 1)..=epoch,
                        reversed_range,
                    )
                } else {
                    vec![]
                };
                Ok((current_version.clone(), shared_buffer))
            }
        }
    }

    pub fn get_version(self: &Arc<LocalVersionManager>) -> HummockResult<LocalVerion> {
        match self.current_version.read().as_ref() {
            None => Err(HummockError::meta_error("No version found.")),
            Some(current_version) => Ok(current_version.clone()),
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
            let last_pinned = match local_version_manager.current_version.read().as_ref() {
                None => INVALID_VERSION_ID,
                Some(v) => v.id(),
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

    pub fn shared_buffer_manager(&self) -> &SharedBufferManager {
        &self.shared_buffer_manager
    }

    #[cfg(test)]
    pub async fn refresh_version(&self, hummock_meta_client: &dyn HummockMetaClient) -> bool {
        let last_pinned = match self.current_version.read().as_ref() {
            None => INVALID_VERSION_ID,
            Some(v) => v.id(),
        };
        let version = hummock_meta_client.pin_version(last_pinned).await.unwrap();
        self.try_update_pinned_version(version)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_pb::hummock::{HummockVersion, KeyRange, SstableInfo};

    use super::LocalVersionManager;
    use crate::hummock::iterator::test_utils::iterator_test_key_of_epoch;
    use crate::hummock::shared_buffer::shared_buffer_batch::SharedBufferBatch;
    use crate::hummock::test_utils::default_config_for_test;
    use crate::hummock::value::HummockValue;

    fn gen_dummy_batch(epoch: u64) -> Vec<(Bytes, HummockValue<Bytes>)> {
        vec![(
            iterator_test_key_of_epoch(0, epoch).into(),
            HummockValue::put(b"value1".to_vec()).into(),
        )]
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
        let shared_buffer_manager = local_version_manager.shared_buffer_manager();
        let version = HummockVersion {
            id: 0,
            max_committed_epoch: 0,
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version);

        let epochs: Vec<u64> = vec![1, 2, 3, 4];
        let batches: Vec<Vec<(Bytes, HummockValue<Bytes>)>> =
            epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

        // Fill shared buffer with a dummy empty batch in epochs[0] and epochs[1]
        for i in 0..2 {
            shared_buffer_manager
                .write_batch(batches[i].clone(), epochs[i])
                .await
                .unwrap();
        }
        assert_eq!(shared_buffer_manager.get_shared_buffer().len(), 2);

        // Update version for epochs[0]
        let version = HummockVersion {
            id: 1,
            max_committed_epoch: epochs[0],
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version.clone());
        assert_eq!(shared_buffer_manager.get_shared_buffer().len(), 1);
        assert_eq!(
            version,
            local_version_manager
                .get_version()
                .unwrap()
                .pinned_version
                .version
        );

        // Update version for epochs[1]
        let version = HummockVersion {
            id: 2,
            max_committed_epoch: epochs[1],
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version.clone());
        assert!(shared_buffer_manager.get_shared_buffer().is_empty());
        assert_eq!(
            version,
            local_version_manager
                .get_version()
                .unwrap()
                .pinned_version
                .version
        );
    }

    #[tokio::test]
    async fn test_update_uncommitted_ssts() {
        let local_version_manager = Arc::new(LocalVersionManager::new(Arc::new(
            default_config_for_test(),
        )));
        let shared_buffer_manager = local_version_manager.shared_buffer_manager();
        let version = HummockVersion {
            id: 0,
            max_committed_epoch: 0,
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version.clone());

        let epochs: Vec<u64> = vec![1, 2];
        let batches: Vec<Vec<(Bytes, HummockValue<Bytes>)>> =
            epochs.iter().map(|e| gen_dummy_batch(*e)).collect();

        // Fill shared buffer with dummy batches
        for i in 0..2 {
            shared_buffer_manager
                .write_batch(batches[i].clone(), epochs[i])
                .await
                .unwrap();
        }
        assert_eq!(shared_buffer_manager.get_shared_buffer().len(), 2);

        // Update uncommitted sst for epochs[0]
        let batches: Vec<SharedBufferBatch> = shared_buffer_manager
            .get_shared_buffer()
            .get(&epochs[0])
            .unwrap()
            .iter()
            .map(|e| e.1.clone())
            .collect();
        let sst = gen_dummy_sst_info(1, batches.clone());
        local_version_manager.update_uncommitted_ssts(epochs[0], vec![sst.clone()], batches);
        // Check shared buffer
        assert_eq!(shared_buffer_manager.get_shared_buffer().len(), 2);
        assert!(shared_buffer_manager
            .get_shared_buffer()
            .get(&epochs[0])
            .unwrap()
            .is_empty());
        // Check local version
        let local_version = local_version_manager.get_version().unwrap();
        assert_eq!(local_version.pinned_version.version, version);
        assert_eq!(local_version.uncommitted_ssts.ssts.len(), 1);
        let epoch_uncommitted_ssts = local_version
            .uncommitted_ssts()
            .ssts
            .get(&epochs[0])
            .unwrap();
        assert_eq!(epoch_uncommitted_ssts.len(), 1);
        assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst);

        // Update uncommitted sst for epochs[1]
        let batches: Vec<SharedBufferBatch> = shared_buffer_manager
            .get_shared_buffer()
            .get(&epochs[1])
            .unwrap()
            .iter()
            .map(|e| e.1.clone())
            .collect();
        let sst = gen_dummy_sst_info(2, batches.clone());
        local_version_manager.update_uncommitted_ssts(epochs[1], vec![sst.clone()], batches);
        // Check shared buffer
        assert_eq!(shared_buffer_manager.get_shared_buffer().len(), 2);
        assert!(shared_buffer_manager
            .get_shared_buffer()
            .get(&epochs[0])
            .unwrap()
            .is_empty());
        assert!(shared_buffer_manager
            .get_shared_buffer()
            .get(&epochs[1])
            .unwrap()
            .is_empty());
        // Check local version
        let local_version = local_version_manager.get_version().unwrap();
        assert_eq!(local_version.pinned_version.version, version);
        assert_eq!(local_version.uncommitted_ssts.ssts.len(), 2);
        let epoch_uncommitted_ssts = local_version
            .uncommitted_ssts()
            .ssts
            .get(&epochs[1])
            .unwrap();
        assert_eq!(epoch_uncommitted_ssts.len(), 1);
        assert_eq!(*epoch_uncommitted_ssts.first().unwrap(), sst);

        // Update version for epochs[0]
        let version = HummockVersion {
            id: 1,
            max_committed_epoch: epochs[0],
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version.clone());
        let local_version = local_version_manager.get_version().unwrap();
        assert_eq!(shared_buffer_manager.get_shared_buffer().len(), 1);
        assert_eq!(local_version.pinned_version.version, version);
        assert!(local_version
            .uncommitted_ssts()
            .ssts
            .get(&epochs[0])
            .is_none());
        assert_eq!(
            local_version
                .uncommitted_ssts()
                .ssts
                .get(&epochs[1])
                .unwrap()
                .len(),
            1
        );

        // Update version for epochs[1]
        let version = HummockVersion {
            id: 2,
            max_committed_epoch: epochs[1],
            ..Default::default()
        };
        local_version_manager.try_update_pinned_version(version.clone());
        assert!(shared_buffer_manager.get_shared_buffer().is_empty());
        let local_version = local_version_manager.get_version().unwrap();
        assert_eq!(local_version.pinned_version.version, version);
        assert!(local_version.uncommitted_ssts().ssts.is_empty());
    }
}
