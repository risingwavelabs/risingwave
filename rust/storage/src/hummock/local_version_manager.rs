use std::collections::btree_map::BTreeMap;
use std::ops::DerefMut;
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use risingwave_pb::hummock::{HummockVersion, Level, LevelType};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use super::BlockCacheRef;
use crate::hummock::cloud::{get_sst_data_path, get_sst_meta};
use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockSSTableId, HummockVersionId, SSTable,
    INVALID_VERSION_ID,
};
use crate::object::ObjectStore;

pub struct ScopedLocalVersion {
    version: Arc<HummockVersion>,
    unpin_worker_tx: UnboundedSender<HummockVersionId>,
}

impl Drop for ScopedLocalVersion {
    fn drop(&mut self) {
        self.unpin_worker_tx.send(self.version.id).ok();
    }
}

impl ScopedLocalVersion {
    fn new(
        version: Arc<HummockVersion>,
        unpin_worker: UnboundedSender<HummockVersionId>,
    ) -> ScopedLocalVersion {
        ScopedLocalVersion {
            version,
            unpin_worker_tx: unpin_worker,
        }
    }

    pub fn id(&self) -> HummockVersionId {
        self.version.id
    }

    // TODO: may return only committed levels in some cases.
    pub fn levels(&self) -> Vec<Level> {
        let uncommitted_level = self
            .version
            .uncommitted_epochs
            .iter()
            .map(|uncommitted| Level {
                level_type: LevelType::Overlapping as i32,
                table_ids: uncommitted.table_ids.clone(),
            });
        self.version
            .levels
            .clone()
            .into_iter()
            .chain(uncommitted_level)
            .collect()
    }
}

/// The `LocalVersionManager` maintain a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `SSTables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    current_version: RwLock<Option<Arc<ScopedLocalVersion>>>,
    sstables: RwLock<BTreeMap<HummockSSTableId, Arc<SSTable>>>,

    obj_client: Arc<dyn ObjectStore>,
    remote_dir: Arc<String>,
    pub block_cache: BlockCacheRef,
    update_notifier_tx: tokio::sync::watch::Sender<HummockVersionId>,
    unpin_worker_tx: UnboundedSender<HummockVersionId>,
    unpin_worker_rx: Mutex<Option<UnboundedReceiver<HummockVersionId>>>,
}

impl LocalVersionManager {
    pub fn new(
        obj_client: Arc<dyn ObjectStore>,
        remote_dir: &str,
        block_cache: BlockCacheRef,
    ) -> LocalVersionManager {
        let (update_notifier_tx, _) = tokio::sync::watch::channel(INVALID_VERSION_ID);
        let (unpin_worker_tx, unpin_worker_rx) = tokio::sync::mpsc::unbounded_channel();

        LocalVersionManager {
            current_version: RwLock::new(None),
            sstables: RwLock::new(BTreeMap::new()),
            obj_client,
            remote_dir: Arc::new(remote_dir.to_string()),
            block_cache,
            update_notifier_tx,
            unpin_worker_tx,
            unpin_worker_rx: Mutex::new(Some(unpin_worker_rx)),
        }
    }

    pub async fn start_workers(
        local_version_manager: Arc<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let unpin_worker_rx = local_version_manager.unpin_worker_rx.lock().take();
        if let Some(unpin_worker_rx) = unpin_worker_rx {
            // Pin and get latest version.
            tokio::spawn(LocalVersionManager::start_pin_worker(
                Arc::downgrade(&local_version_manager),
                hummock_meta_client.clone(),
            ));
            // Unpin unused version.
            tokio::spawn(LocalVersionManager::start_unpin_worker(
                unpin_worker_rx,
                hummock_meta_client,
            ));
        }
    }

    /// Update cached version if the new version is of greater id
    pub fn try_set_version(&self, hummock_version: HummockVersion) -> bool {
        let new_version_id = hummock_version.id;
        let mut guard = self.current_version.write();
        match guard.as_ref() {
            Some(cached_version) if cached_version.id() >= new_version_id => {
                return false;
            }
            _ => {}
        }
        // Update cached version
        *guard.deref_mut() = Some(Arc::new(ScopedLocalVersion::new(
            Arc::new(hummock_version),
            self.unpin_worker_tx.clone(),
        )));
        self.update_notifier_tx.send(new_version_id).ok();
        true
    }

    /// Wait until the local hummock version contains the given committed epoch
    pub async fn wait_epoch(&self, epoch: HummockEpoch) {
        // TODO: review usage of all HummockEpoch::MAX
        if epoch == HummockEpoch::MAX {
            return;
        }
        let mut receiver = self.update_notifier_tx.subscribe();
        loop {
            {
                let current_version = self.current_version.read();
                if let Some(version) = current_version.as_ref() {
                    if version.version.max_committed_epoch >= epoch {
                        return;
                    }
                }
            }
            if receiver.changed().await.is_err() {
                // The tx is dropped.
                return;
            }
        }
    }

    pub fn get_version(self: &Arc<LocalVersionManager>) -> HummockResult<Arc<ScopedLocalVersion>> {
        match self.current_version.read().as_ref() {
            None => Err(HummockError::meta_error("No version found.")),
            Some(current_version) => Ok(current_version.clone()),
        }
    }

    fn try_get_sstable_from_cache(&self, table_id: HummockSSTableId) -> Option<Arc<SSTable>> {
        self.sstables.read().get(&table_id).cloned()
    }

    fn add_sstable_to_cache(&self, sstable: Arc<SSTable>) {
        self.sstables.write().insert(sstable.id, sstable);
    }

    pub async fn pick_few_tables(&self, table_ids: &[u64]) -> HummockResult<Vec<Arc<SSTable>>> {
        let mut tables = vec![];
        for table_id in table_ids {
            let sstable = match self.try_get_sstable_from_cache(*table_id) {
                None => {
                    let fetched_sstable = Arc::new(SSTable {
                        id: *table_id,
                        meta: get_sst_meta(self.obj_client.clone(), &self.remote_dir, *table_id)
                            .await?,
                        obj_client: self.obj_client.clone(),
                        data_path: get_sst_data_path(&self.remote_dir, *table_id),
                        block_cache: self.block_cache.clone(),
                    });
                    self.add_sstable_to_cache(fetched_sstable.clone());
                    fetched_sstable
                }
                Some(cached_sstable) => cached_sstable,
            };
            tables.push(sstable);
        }
        Ok(tables)
    }

    /// Get the iterators on the underlying tables.
    pub async fn tables(
        &self,
        levels: &[risingwave_pb::hummock::Level],
    ) -> HummockResult<Vec<Arc<SSTable>>> {
        // Should the LevelType be returned and made use of?
        let mut out: Vec<Arc<SSTable>> = Vec::new();
        for level in levels {
            match level.level_type() {
                LevelType::Overlapping => {
                    let mut tables = self.pick_few_tables(&level.table_ids).await?;
                    out.append(&mut tables);
                }
                LevelType::Nonoverlapping => {
                    let mut tables = self.pick_few_tables(&level.table_ids).await?;
                    out.append(&mut tables);
                }
            }
        }

        Ok(out)
    }

    async fn start_pin_worker(
        local_version_manager: Weak<LocalVersionManager>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        let mut min_interval = tokio::time::interval(Duration::from_millis(100));
        loop {
            min_interval.tick().await;
            if let Some(local_version_manager) = local_version_manager.upgrade() {
                if let Ok(version) = hummock_meta_client.pin_version().await {
                    local_version_manager.try_set_version(version);
                }
            } else {
                return;
            }
        }
    }

    async fn start_unpin_worker(
        mut rx: UnboundedReceiver<HummockVersionId>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
    ) {
        loop {
            match rx.recv().await {
                None => {
                    return;
                }
                Some(version_id) => {
                    hummock_meta_client.unpin_version(version_id).await.ok();
                }
            }
        }
    }
}
