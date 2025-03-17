// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::config::ObjectStoreConfig;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{
    InMemObjectStore, MonitoredObjectStore, ObjectError, ObjectStoreImpl, ObjectStoreRef,
};

use crate::meta_snapshot::{MetaSnapshot, Metadata};
use crate::{
    BackupError, BackupResult, MetaSnapshotId, MetaSnapshotManifest, MetaSnapshotMetadata,
};

pub type MetaSnapshotStorageRef = Arc<ObjectStoreMetaSnapshotStorage>;

#[async_trait::async_trait]
pub trait MetaSnapshotStorage: 'static + Sync + Send {
    /// Creates a snapshot.
    async fn create<S: Metadata>(
        &self,
        snapshot: &MetaSnapshot<S>,
        remarks: Option<String>,
    ) -> BackupResult<()>;

    /// Gets a snapshot by id.
    async fn get<S: Metadata>(&self, id: MetaSnapshotId) -> BackupResult<MetaSnapshot<S>>;

    /// Gets local snapshot manifest.
    fn manifest(&self) -> Arc<MetaSnapshotManifest>;

    /// Refreshes local snapshot manifest.
    async fn refresh_manifest(&self) -> BackupResult<()>;

    /// Deletes snapshots by ids.
    async fn delete(&self, ids: &[MetaSnapshotId]) -> BackupResult<()>;
}

#[derive(Clone)]
pub struct ObjectStoreMetaSnapshotStorage {
    path: String,
    store: ObjectStoreRef,
    manifest: Arc<parking_lot::RwLock<Arc<MetaSnapshotManifest>>>,
}

// TODO #6482: purge stale snapshots that is not in manifest.
impl ObjectStoreMetaSnapshotStorage {
    pub async fn new(path: &str, store: ObjectStoreRef) -> BackupResult<Self> {
        let instance = Self {
            path: path.to_owned(),
            store,
            manifest: Default::default(),
        };
        instance.refresh_manifest().await?;
        Ok(instance)
    }

    async fn update_manifest(&self, new_manifest: MetaSnapshotManifest) -> BackupResult<()> {
        let bytes =
            serde_json::to_vec(&new_manifest).map_err(|e| BackupError::Encoding(e.into()))?;
        self.store
            .upload(&self.get_manifest_path(), bytes.into())
            .await?;
        *self.manifest.write() = Arc::new(new_manifest);
        Ok(())
    }

    async fn get_manifest(&self) -> BackupResult<Option<MetaSnapshotManifest>> {
        let manifest_path = self.get_manifest_path();
        let bytes = match self.store.read(&manifest_path, ..).await {
            Ok(bytes) => bytes,
            Err(e) => {
                if e.is_object_not_found_error() {
                    return Ok(None);
                }
                return Err(e.into());
            }
        };
        let manifest: MetaSnapshotManifest =
            serde_json::from_slice(&bytes).map_err(|e| BackupError::Encoding(e.into()))?;
        Ok(Some(manifest))
    }

    fn get_manifest_path(&self) -> String {
        format!("{}/manifest.json", self.path)
    }

    fn get_snapshot_path(&self, id: MetaSnapshotId) -> String {
        format!("{}/{}.snapshot", self.path, id)
    }

    #[allow(dead_code)]
    fn get_snapshot_id_from_path(path: &str) -> MetaSnapshotId {
        let split = path.split(&['/', '.']).collect_vec();
        debug_assert!(split.len() > 2);
        debug_assert!(split[split.len() - 1] == "snapshot");
        split[split.len() - 2]
            .parse::<MetaSnapshotId>()
            .expect("valid meta snapshot id")
    }
}

#[async_trait::async_trait]
impl MetaSnapshotStorage for ObjectStoreMetaSnapshotStorage {
    async fn create<S: Metadata>(
        &self,
        snapshot: &MetaSnapshot<S>,
        remarks: Option<String>,
    ) -> BackupResult<()> {
        let path = self.get_snapshot_path(snapshot.id);
        self.store.upload(&path, snapshot.encode()?.into()).await?;

        // update manifest last
        let mut new_manifest = (**self.manifest.read()).clone();
        new_manifest.manifest_id += 1;
        new_manifest
            .snapshot_metadata
            .push(MetaSnapshotMetadata::new(
                snapshot.id,
                snapshot.metadata.hummock_version_ref(),
                snapshot.format_version,
                remarks,
            ));
        self.update_manifest(new_manifest).await?;
        Ok(())
    }

    async fn get<S: Metadata>(&self, id: MetaSnapshotId) -> BackupResult<MetaSnapshot<S>> {
        let path = self.get_snapshot_path(id);
        let data = self.store.read(&path, ..).await?;
        MetaSnapshot::decode(&data)
    }

    fn manifest(&self) -> Arc<MetaSnapshotManifest> {
        self.manifest.read().clone()
    }

    async fn refresh_manifest(&self) -> BackupResult<()> {
        if let Some(manifest) = self.get_manifest().await? {
            let mut guard = self.manifest.write();
            if manifest.manifest_id > guard.manifest_id {
                *guard = Arc::new(manifest);
            }
        }
        Ok(())
    }

    async fn delete(&self, ids: &[MetaSnapshotId]) -> BackupResult<()> {
        // update manifest first
        let to_delete: HashSet<MetaSnapshotId> = HashSet::from_iter(ids.iter().cloned());
        let mut new_manifest = (**self.manifest.read()).clone();
        new_manifest.manifest_id += 1;
        new_manifest
            .snapshot_metadata
            .retain(|m| !to_delete.contains(&m.id));
        self.update_manifest(new_manifest).await?;

        let paths = ids
            .iter()
            .map(|id| self.get_snapshot_path(*id))
            .collect_vec();
        self.store.delete_objects(&paths).await?;
        Ok(())
    }
}

impl From<ObjectError> for BackupError {
    fn from(e: ObjectError) -> Self {
        BackupError::BackupStorage(e.into())
    }
}

// #[cfg(test)]
pub async fn unused() -> ObjectStoreMetaSnapshotStorage {
    ObjectStoreMetaSnapshotStorage::new(
        "",
        Arc::new(ObjectStoreImpl::InMem(MonitoredObjectStore::new(
            InMemObjectStore::new(),
            Arc::new(ObjectStoreMetrics::unused()),
            Arc::new(ObjectStoreConfig::default()),
        ))),
    )
    .await
    .unwrap()
}
