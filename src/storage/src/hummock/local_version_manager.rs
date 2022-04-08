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

use std::borrow::Borrow;
use std::collections::btree_map::BTreeMap;
use std::ops::DerefMut;
use std::sync::{Arc, Weak};
use std::time::Duration;

use itertools::Itertools;
use parking_lot::{Mutex, RwLock};
use risingwave_pb::hummock::{HummockVersion, Level};
use risingwave_rpc_client::HummockMetaClient;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_retry::strategy::jitter;

use crate::hummock::shared_buffer::shared_buffer_manager::SharedBufferManager;
use crate::hummock::sstable_store::SstableStoreRef;
use crate::hummock::utils::validate_table_key_range;
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockVersionId, Sstable, INVALID_VERSION_ID,
};

#[derive(Debug)]
pub struct ScopedLocalVersion {
    version: Arc<HummockVersion>,
    unpin_worker_tx: UnboundedSender<Arc<HummockVersion>>,
}

impl Drop for ScopedLocalVersion {
    fn drop(&mut self) {
        self.unpin_worker_tx.send(self.version.clone()).ok();
    }
}

impl ScopedLocalVersion {
    fn new(
        version: Arc<HummockVersion>,
        unpin_worker: UnboundedSender<Arc<HummockVersion>>,
    ) -> ScopedLocalVersion {
        ScopedLocalVersion {
            version,
            unpin_worker_tx: unpin_worker,
        }
    }

    pub fn id(&self) -> HummockVersionId {
        self.version.id
    }

    pub fn levels(&self) -> Vec<Level> {
        self.version.levels.clone()
    }

    pub fn max_committed_epoch(&self) -> u64 {
        self.version.max_committed_epoch
    }

    pub fn safe_epoch(&self) -> u64 {
        self.version.safe_epoch
    }
}

/// The `LocalVersionManager` maintains a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `SSTables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    current_version: RwLock<Option<Arc<ScopedLocalVersion>>>,
    sstable_store: SstableStoreRef,

    update_notifier_tx: tokio::sync::watch::Sender<HummockVersionId>,
    unpin_worker_tx: UnboundedSender<Arc<HummockVersion>>,
    unpin_worker_rx: Mutex<Option<UnboundedReceiver<Arc<HummockVersion>>>>,

    /// Track the refcnt for committed epoch to facilitate shared buffer cleanup
    committed_epoch_refcnts: Mutex<BTreeMap<u64, u64>>,
}

impl LocalVersionManager {
    pub fn new(sstable_store: SstableStoreRef) -> LocalVersionManager {
        let (update_notifier_tx, _) = tokio::sync::watch::channel(INVALID_VERSION_ID);
        let (unpin_worker_tx, unpin_worker_rx) = tokio::sync::mpsc::unbounded_channel();

        LocalVersionManager {
            current_version: RwLock::new(None),
            sstable_store,
            update_notifier_tx,
            unpin_worker_tx,
            unpin_worker_rx: Mutex::new(Some(unpin_worker_rx)),
            committed_epoch_refcnts: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn start_workers(
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        shared_buffer_manager: Arc<SharedBufferManager>,
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
                Arc::downgrade(&local_version_manager),
                unpin_worker_rx,
                hummock_meta_client,
                shared_buffer_manager,
            ));
        }
    }

    /// Updates cached version if the new version is of greater id
    pub fn try_set_version(&self, hummock_version: HummockVersion) -> bool {
        let new_version_id = hummock_version.id;
        if validate_table_key_range(&hummock_version.levels).is_err() {
            return false;
        }
        let mut guard = self.current_version.write();
        match guard.as_ref() {
            Some(cached_version) if cached_version.id() >= new_version_id => {
                return false;
            }
            _ => {}
        }

        // Update the committed epoch ref cnt.
        self.ref_committed_epoch(hummock_version.max_committed_epoch);

        // Update cached version
        *guard.deref_mut() = Some(Arc::new(ScopedLocalVersion::new(
            Arc::new(hummock_version),
            self.unpin_worker_tx.clone(),
        )));

        self.update_notifier_tx.send(new_version_id).ok();
        true
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
                    if version.version.max_committed_epoch >= epoch {
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

    pub fn get_version(self: &Arc<LocalVersionManager>) -> HummockResult<Arc<ScopedLocalVersion>> {
        match self.current_version.read().as_ref() {
            None => Err(HummockError::meta_error("No version found.")),
            Some(current_version) => Ok(current_version.clone()),
        }
    }

    pub async fn pick_few_tables(&self, sst_ids: &[u64]) -> HummockResult<Vec<Arc<Sstable>>> {
        let mut ssts = Vec::with_capacity(sst_ids.len());
        for sst_id in sst_ids {
            ssts.push(self.sstable_store.sstable(*sst_id).await?);
        }
        Ok(ssts)
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
                Some(v) => v.version.id,
            };
            match hummock_meta_client.pin_version(last_pinned).await {
                Ok(version) => {
                    local_version_manager.try_set_version(version);
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
        local_version_manager: Weak<LocalVersionManager>,
        mut rx: UnboundedReceiver<Arc<HummockVersion>>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        shared_buffer_manager: Arc<SharedBufferManager>,
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
            let local_version_manager = match local_version_manager.upgrade() {
                None => {
                    tracing::info!("Shutdown hummock unpin worker");
                    return;
                }
                Some(local_version_manager) => local_version_manager,
            };
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
            match hummock_meta_client
                .unpin_version(&versions_to_unpin.iter().map(|v| v.id).collect_vec())
                .await
            {
                Ok(_) => {
                    for version in &versions_to_unpin {
                        local_version_manager.unref_committed_epoch(
                            version.max_committed_epoch,
                            shared_buffer_manager.borrow(),
                        )
                    }
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

    fn ref_committed_epoch(&self, committed_epoch: u64) {
        let mut epoch_ref_guard = self.committed_epoch_refcnts.lock();
        let refcnt = epoch_ref_guard.entry(committed_epoch).or_insert(0);
        *refcnt += 1;
    }

    fn unref_committed_epoch(
        &self,
        committed_epoch: u64,
        shared_buffer_manager: &SharedBufferManager,
    ) {
        let mut epoch_ref_guard = self.committed_epoch_refcnts.lock();
        let epoch_low_watermark = match epoch_ref_guard.first_key_value() {
            Some(e) => *e.0,
            None => return,
        };
        match epoch_ref_guard.entry(committed_epoch) {
            std::collections::btree_map::Entry::Vacant(_) => (),
            std::collections::btree_map::Entry::Occupied(mut e) => {
                let refcnt = e.get_mut();
                if *refcnt == 1 {
                    e.remove();
                    if epoch_low_watermark == committed_epoch {
                        // Delete data before ref epoch low watermark in shared buffer if the epoch
                        // low watermark changes after ref epoch removal.
                        shared_buffer_manager.delete_before(
                            epoch_ref_guard
                                .first_key_value()
                                .map(|e| *e.0)
                                .unwrap_or(committed_epoch + 1),
                        );
                    }
                } else {
                    *refcnt -= 1;
                }
            }
        }
    }

    #[cfg(test)]
    pub async fn refresh_version(&self, hummock_meta_client: &dyn HummockMetaClient) -> bool {
        let last_pinned = match self.current_version.read().as_ref() {
            None => INVALID_VERSION_ID,
            Some(v) => v.version.id,
        };
        let version = hummock_meta_client.pin_version(last_pinned).await.unwrap();
        self.try_set_version(version)
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Borrow;
    use std::sync::Arc;

    use bytes::Bytes;
    use risingwave_meta::hummock::test_utils::setup_compute_env;
    use risingwave_meta::hummock::MockHummockMetaClient;

    use super::LocalVersionManager;
    use crate::hummock::iterator::test_utils::{
        iterator_test_key_of_epoch, mock_sstable_store_with_object_store,
    };
    use crate::hummock::shared_buffer::shared_buffer_manager::SharedBufferManager;
    use crate::hummock::test_utils::default_config_for_test;
    use crate::hummock::value::HummockValue;
    use crate::monitor::StateStoreMetrics;
    use crate::object::{InMemObjectStore, ObjectStoreImpl};

    fn gen_dummy_batch(epoch: u64) -> Vec<(Bytes, HummockValue<Bytes>)> {
        vec![(
            iterator_test_key_of_epoch(0, epoch).into(),
            HummockValue::put(b"value1".to_vec()).into(),
        )]
    }

    #[tokio::test]
    async fn test_shared_buffer_cleanup() {
        let object_store = Arc::new(ObjectStoreImpl::Mem(InMemObjectStore::new()));
        let sstable_store = mock_sstable_store_with_object_store(object_store);
        let local_version_manager = Arc::new(LocalVersionManager::new(sstable_store.clone()));
        let (_env, hummock_manager_ref, _cluster_manager_ref, worker_node) =
            setup_compute_env(8080).await;
        let mock_hummock_meta_client = Arc::new(MockHummockMetaClient::new(
            hummock_manager_ref,
            worker_node.id,
        ));
        let shared_buffer_manager = Arc::new(SharedBufferManager::new(
            Arc::new(default_config_for_test()),
            local_version_manager.clone(),
            sstable_store,
            Arc::new(StateStoreMetrics::unused()),
            mock_hummock_meta_client,
        ));

        let epochs = vec![1, 2, 3, 4];

        // Fill shared buffer with a dummy empty batch in epochs[0]
        shared_buffer_manager
            .write_batch(gen_dummy_batch(epochs[0]), epochs[0])
            .unwrap();
        assert!(!shared_buffer_manager.get_shared_buffer().is_empty());

        // Ref epoch1 twice
        local_version_manager.ref_committed_epoch(epochs[0]);
        local_version_manager.ref_committed_epoch(epochs[0]);

        // Unref epoch1. Shared buffer should not change.
        local_version_manager.unref_committed_epoch(epochs[0], shared_buffer_manager.borrow());
        assert!(!shared_buffer_manager.get_shared_buffer().is_empty());

        // Unref epoch1 again. Shared buffer should be empty now.
        local_version_manager.unref_committed_epoch(epochs[0], shared_buffer_manager.borrow());
        assert!(shared_buffer_manager.get_shared_buffer().is_empty());

        // Fill shared buffer with a dummy empty batch in epochs[1..=3] and ref them
        for epoch in epochs.iter().skip(1) {
            shared_buffer_manager
                .write_batch(gen_dummy_batch(*epoch), *epoch)
                .unwrap();
            local_version_manager.ref_committed_epoch(*epoch);
        }

        // Unref epochs[2]. Shared buffer should not change.
        local_version_manager.unref_committed_epoch(epochs[2], shared_buffer_manager.borrow());
        let shared_buffer = shared_buffer_manager.get_shared_buffer();
        for epoch in epochs.iter().skip(1) {
            assert!(shared_buffer.contains_key(epoch));
        }

        // Unref epochs[1]. epochs[1..=2] should now be removed from shared buffer
        local_version_manager.unref_committed_epoch(epochs[1], shared_buffer_manager.borrow());
        let shared_buffer = shared_buffer_manager.get_shared_buffer();
        println!("{:?}", shared_buffer);
        assert_eq!(shared_buffer.len(), 1);
        assert!(shared_buffer.contains_key(&epochs[3]));

        // Unref epochs[3]. Shared buffer should be empty.
        local_version_manager.unref_committed_epoch(epochs[3], shared_buffer_manager.borrow());
        assert!(shared_buffer_manager.get_shared_buffer().is_empty());
    }
}
