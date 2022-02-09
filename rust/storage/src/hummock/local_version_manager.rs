use std::collections::BTreeMap;
use std::sync::Arc;

use itertools::Itertools;
use moka::future::Cache;
use parking_lot::{Mutex, RwLock};
use risingwave_pb::hummock::{HummockVersion, Level, LevelType};

use super::Block;
use crate::hummock::cloud::{get_sst_data_path, get_sst_meta};
use crate::hummock::{
    HummockMetaClient, HummockRefCount, HummockResult, HummockSSTableId, HummockVersionId, SSTable,
    INVALID_EPOCH, INVALID_VERSION,
};
use crate::object::ObjectStore;

pub struct ScopedLocalVersion {
    version_id: HummockVersionId,
    version: Arc<HummockVersion>,
    local_version_manager: Arc<LocalVersionManager>,
}

impl Drop for ScopedLocalVersion {
    fn drop(&mut self) {
        self.local_version_manager
            .unpin_local_version(self.version_id);
    }
}

impl ScopedLocalVersion {
    pub fn new(local_version_manager: Arc<LocalVersionManager>) -> ScopedLocalVersion {
        let (version_id, version) = local_version_manager.pin_greatest_local_version();
        ScopedLocalVersion {
            version_id,
            version,
            local_version_manager,
        }
    }

    pub fn version_id(&self) -> HummockVersionId {
        self.version_id
    }

    pub fn merged_version(&self) -> Vec<Level> {
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

struct LocalVersionManagerInner {
    version_ref_counts: BTreeMap<HummockVersionId, HummockRefCount>,
    /// greatest version comes last
    pinned_versions: BTreeMap<HummockVersionId, Arc<HummockVersion>>,
}

impl LocalVersionManagerInner {
    fn new() -> LocalVersionManagerInner {
        LocalVersionManagerInner {
            version_ref_counts: BTreeMap::new(),
            pinned_versions: BTreeMap::new(),
        }
    }
}

/// The `LocalVersionManager` maintain a local copy of storage service's hummock version data.
/// By acquiring a `ScopedLocalVersion`, the `SSTables` of this version is guaranteed to be valid
/// during the lifetime of `ScopedLocalVersion`. Internally `LocalVersionManager` will pin/unpin the
/// versions in storage service.
pub struct LocalVersionManager {
    inner: Mutex<LocalVersionManagerInner>,
    sstables: RwLock<BTreeMap<HummockSSTableId, Arc<SSTable>>>,

    obj_client: Arc<dyn ObjectStore>,
    remote_dir: Arc<String>,
    pub block_cache: Arc<Cache<Vec<u8>, Arc<Block>>>,
}

impl LocalVersionManager {
    pub fn new(
        obj_client: Arc<dyn ObjectStore>,
        remote_dir: &str,
        block_cache: Option<Arc<Cache<Vec<u8>, Arc<Block>>>>,
    ) -> LocalVersionManager {
        let instance = Self {
            inner: Mutex::new(LocalVersionManagerInner::new()),
            sstables: RwLock::new(BTreeMap::new()),
            obj_client,
            remote_dir: Arc::new(remote_dir.to_string()),
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
        };
        // Insert an artificial empty version.
        instance.inner.lock().pinned_versions.insert(
            INVALID_VERSION,
            Arc::new(HummockVersion {
                levels: vec![],
                uncommitted_epochs: vec![],
                max_committed_epoch: INVALID_EPOCH,
            }),
        );
        instance
    }

    /// Get the greatest version from storage service and add it to local state.
    /// Currently it's invoked in two places:
    /// - At the end of `HummockStorage` creation.
    /// - At the end of `write_batch`.
    pub async fn update_local_version(
        &self,
        hummock_meta_client: &dyn HummockMetaClient,
    ) -> HummockResult<()> {
        let (new_pinned_version_id, new_pinned_version) = hummock_meta_client.pin_version().await?;
        let versions_to_unpin = {
            // Add to local state
            let mut guard = self.inner.lock();
            guard
                .pinned_versions
                .insert(new_pinned_version_id, Arc::new(new_pinned_version));

            // Unpin versions with ref_count = 0 except for the greatest version in local state.
            // TODO Should be called periodically.
            let versions_to_unpin = guard
                .version_ref_counts
                .iter()
                .rev()
                .skip(1)
                .filter(|(_version_id, ref_count)| **ref_count == 0)
                .map(|(version_id, _ref_count)| version_id)
                .cloned()
                .collect_vec();
            for version_id in &versions_to_unpin {
                guard.version_ref_counts.remove(version_id).unwrap();
                guard.pinned_versions.remove(version_id).unwrap();
            }
            versions_to_unpin
        };

        // Unpin versions with ref_count = 0 except for the greatest version in storage service.
        for version_id in versions_to_unpin {
            if version_id == INVALID_VERSION {
                // Edge case. This is an artificial version.
                continue;
            }
            hummock_meta_client.unpin_version(version_id).await?;
        }

        Ok(())
    }

    /// Get and pin the greatest local version
    fn pin_greatest_local_version(&self) -> (HummockVersionId, Arc<HummockVersion>) {
        let mut guard = self.inner.lock();
        let (version_id, version) = guard
            .pinned_versions
            .last_key_value()
            .map(|(version_id, version)| (*version_id, version.clone()))
            .unwrap();
        let entry = guard.version_ref_counts.entry(version_id);
        *entry.or_insert(0) += 1;
        (version_id, version)
    }

    fn unpin_local_version(&self, version_id: HummockVersionId) {
        let mut guard = self.inner.lock();
        let entry = guard.version_ref_counts.entry(version_id);
        *entry.or_insert(1) -= 1;
    }

    pub fn get_scoped_local_version(self: &Arc<LocalVersionManager>) -> ScopedLocalVersion {
        ScopedLocalVersion::new(self.clone())
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
}
