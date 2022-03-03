use std::borrow::Borrow;
use std::collections::btree_map::BTreeMap;
use std::ops::DerefMut;
use std::sync::{Arc, Weak};
use std::time::Duration;

use moka::future::Cache;
use parking_lot::{Mutex, RwLock};
use risingwave_pb::hummock::{HummockVersion, Level, LevelType};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use super::shared_buffer::SharedBufferManager;
use super::Block;
use crate::hummock::hummock_meta_client::HummockMetaClient;
use crate::hummock::sstable_manager::SstableManagerRef;
use crate::hummock::{
    HummockEpoch, HummockError, HummockResult, HummockSSTableId, HummockVersionId, Sstable,
    INVALID_VERSION_ID,
};

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
}

/// The `LocalVersionManager` maintain a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `SSTables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    current_version: RwLock<Option<Arc<ScopedLocalVersion>>>,
    sstables: RwLock<BTreeMap<HummockSSTableId, Arc<Sstable>>>,
    sstable_manager: SstableManagerRef,

    pub block_cache: Arc<Cache<Vec<u8>, Arc<Block>>>,
    update_notifier_tx: tokio::sync::watch::Sender<HummockVersionId>,
    unpin_worker_tx: UnboundedSender<Arc<HummockVersion>>,
    unpin_worker_rx: Mutex<Option<UnboundedReceiver<Arc<HummockVersion>>>>,

    /// Track the refcnt for committed epoch to facilitate shared buffer cleanup
    committed_epoch_refcnts: Mutex<BTreeMap<u64, u64>>,
}

impl LocalVersionManager {
    pub fn new(
        sstable_manager: SstableManagerRef,
        block_cache: Option<Arc<Cache<Vec<u8>, Arc<Block>>>>,
    ) -> LocalVersionManager {
        let (update_notifier_tx, _) = tokio::sync::watch::channel(INVALID_VERSION_ID);
        let (unpin_worker_tx, unpin_worker_rx) = tokio::sync::mpsc::unbounded_channel();

        LocalVersionManager {
            current_version: RwLock::new(None),
            sstable_manager,
            sstables: RwLock::new(BTreeMap::new()),

            block_cache: if let Some(block_cache) = block_cache {
                block_cache
            } else {
                #[cfg(test)]
                {
                    Arc::new(Cache::new(2333))
                }
                #[cfg(not(test))]
                {
                    panic!("must enable block cache in production mode")
                }
            },
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
            // Pin and get latest version.
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

    /// Wait until the local hummock version contains the given committed epoch
    pub async fn wait_epoch(&self, epoch: HummockEpoch) {
        // TODO: review usage of all HummockEpoch::MAX
        if epoch == HummockEpoch::MAX {
            panic!("epoch should not be u64::MAXX");
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

    fn try_get_sstable_from_cache(&self, table_id: HummockSSTableId) -> Option<Arc<Sstable>> {
        self.sstables.read().get(&table_id).cloned()
    }

    fn add_sstable_to_cache(&self, sstable: Arc<Sstable>) {
        self.sstables.write().insert(sstable.id, sstable);
    }

    pub async fn pick_few_tables(&self, table_ids: &[u64]) -> HummockResult<Vec<Arc<Sstable>>> {
        let mut tables = vec![];
        for table_id in table_ids {
            let sstable = match self.try_get_sstable_from_cache(*table_id) {
                None => {
                    let fetched_sstable = Arc::new(Sstable {
                        id: *table_id,
                        meta: self.sstable_manager.meta(*table_id).await?,
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
    ) -> HummockResult<Vec<Arc<Sstable>>> {
        // Should the LevelType be returned and made use of?
        let mut out: Vec<Arc<Sstable>> = Vec::new();
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
        local_version_manager: Weak<LocalVersionManager>,
        mut rx: UnboundedReceiver<Arc<HummockVersion>>,
        hummock_meta_client: Arc<dyn HummockMetaClient>,
        shared_buffer_manager: Arc<SharedBufferManager>,
    ) {
        loop {
            match rx.recv().await {
                None => {
                    return;
                }
                Some(version) => {
                    hummock_meta_client.unpin_version(version.id).await.ok();
                    if let Some(local_version_manager) = local_version_manager.upgrade() {
                        local_version_manager.unref_committed_epoch(
                            version.max_committed_epoch,
                            shared_buffer_manager.borrow(),
                        )
                    }
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
        let min_epoch = match epoch_ref_guard.first_key_value() {
            Some(e) => *e.0,
            None => return,
        };
        match epoch_ref_guard.entry(committed_epoch) {
            std::collections::btree_map::Entry::Vacant(_) => (),
            std::collections::btree_map::Entry::Occupied(e) => {
                if *e.get() == 1 {
                    if e.key() == &min_epoch {
                        // Do shared buffer cleanup if the min_epoch is pop
                        shared_buffer_manager.delete_before(min_epoch);
                    }
                    e.remove();
                }
            }
        }
    }
}
