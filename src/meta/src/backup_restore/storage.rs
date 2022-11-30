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

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_object_store::object::{ObjectError, ObjectStoreRef};
use serde::{Deserialize, Serialize};

use crate::backup_restore::db_snapshot::DbSnapshot;
use crate::backup_restore::error::{BackupError, BackupResult};
use crate::backup_restore::{DbSnapshotId, DbSnapshotMetadata};

pub type BackupStorageRef = Box<dyn BackupStorage>;

#[async_trait::async_trait]
pub trait BackupStorage: 'static + Sync + Send {
    /// Creates a db snapshot.
    async fn create(&self, snapshot: &DbSnapshot) -> BackupResult<()>;

    /// Gets a db snapshot by id.
    async fn get(&self, id: DbSnapshotId) -> BackupResult<DbSnapshot>;

    /// List all db snapshots.
    async fn list(&self) -> BackupResult<Vec<DbSnapshotMetadata>>;

    /// Deletes db snapshots by ids.
    async fn delete(&self, ids: &[DbSnapshotId]) -> BackupResult<()>;
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct BackupManifest {
    pub id: u64,
    pub db_snapshots: Vec<DbSnapshotMetadata>,
}

#[derive(Clone)]
pub struct ObjectStoreBackupStorage {
    path: String,
    store: ObjectStoreRef,
    manifest: Arc<parking_lot::RwLock<BackupManifest>>,
}

// TODO #6482: purge stale db snapshot that is not in manifest.
impl ObjectStoreBackupStorage {
    pub async fn new(path: &str, store: ObjectStoreRef) -> BackupResult<Self> {
        let mut instance = Self {
            path: path.to_string(),
            store,
            manifest: Default::default(),
        };
        let manifest = match instance.get_manifest().await? {
            None => BackupManifest::default(),
            Some(manifest) => manifest,
        };
        instance.manifest = Arc::new(parking_lot::RwLock::new(manifest));
        Ok(instance)
    }

    async fn update_manifest(&self, new_manifest: BackupManifest) -> BackupResult<()> {
        let bytes =
            serde_json::to_vec(&new_manifest).map_err(|e| BackupError::Encoding(e.into()))?;
        self.store
            .upload(&self.get_manifest_path(), bytes.into())
            .await?;
        *self.manifest.write() = new_manifest;
        Ok(())
    }

    async fn get_manifest(&self) -> BackupResult<Option<BackupManifest>> {
        let manifest_path = self.get_manifest_path();
        let manifest = match self
            .store
            .list(&manifest_path)
            .await?
            .into_iter()
            .find(|m| m.key == manifest_path)
        {
            None => {
                return Ok(None);
            }
            Some(manifest) => manifest,
        };
        let bytes = self.store.read(&manifest.key, None).await?;
        let manifest: BackupManifest =
            serde_json::from_slice(&bytes).map_err(|e| BackupError::Encoding(e.into()))?;
        Ok(Some(manifest))
    }

    fn get_manifest_path(&self) -> String {
        format!("{}/manifest.json", self.path)
    }

    fn get_db_snapshot_path(&self, id: DbSnapshotId) -> String {
        format!("{}/{}.snapshot", self.path, id)
    }

    fn get_db_snapshot_id_from_path(path: &str) -> DbSnapshotId {
        let split = path.split(&['/', '.']).collect_vec();
        debug_assert!(split.len() > 2);
        debug_assert!(split[split.len() - 1] == "snapshot");
        split[split.len() - 2]
            .parse::<DbSnapshotId>()
            .expect("valid db snapshot id")
    }
}

#[async_trait::async_trait]
impl BackupStorage for ObjectStoreBackupStorage {
    async fn create(&self, snapshot: &DbSnapshot) -> BackupResult<()> {
        let path = self.get_db_snapshot_path(snapshot.id);
        self.store.upload(&path, snapshot.encode().into()).await?;

        // update manifest last
        let mut new_manifest = self.manifest.read().clone();
        new_manifest.id += 1;
        new_manifest.db_snapshots.push(DbSnapshotMetadata::new(
            snapshot.id,
            &snapshot.metadata.hummock_version,
        ));
        self.update_manifest(new_manifest).await?;
        Ok(())
    }

    async fn get(&self, id: DbSnapshotId) -> BackupResult<DbSnapshot> {
        let path = self.get_db_snapshot_path(id);
        let data = self.store.read(&path, None).await?;
        DbSnapshot::decode(&data)
    }

    async fn list(&self) -> BackupResult<Vec<DbSnapshotMetadata>> {
        Ok(self.manifest.read().db_snapshots.clone())
    }

    async fn delete(&self, ids: &[DbSnapshotId]) -> BackupResult<()> {
        // update manifest first
        let to_delete: HashSet<DbSnapshotId> = HashSet::from_iter(ids.iter().cloned());
        let mut new_manifest = self.manifest.read().clone();
        new_manifest.id += 1;
        new_manifest
            .db_snapshots
            .retain(|m| !to_delete.contains(&m.id));
        self.update_manifest(new_manifest).await?;

        let paths = ids
            .iter()
            .map(|id| self.get_db_snapshot_path(*id))
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

pub struct DummyBackupStorage {}

#[async_trait::async_trait]
impl BackupStorage for DummyBackupStorage {
    async fn create(&self, _snapshot: &DbSnapshot) -> BackupResult<()> {
        panic!("should not create from DummyBackupStorage")
    }

    async fn get(&self, _id: DbSnapshotId) -> BackupResult<DbSnapshot> {
        panic!("should not get from DummyBackupStorage")
    }

    async fn list(&self) -> BackupResult<Vec<DbSnapshotMetadata>> {
        // Satisfy `BackupManager`
        Ok(vec![])
    }

    async fn delete(&self, _ids: &[DbSnapshotId]) -> BackupResult<()> {
        panic!("should not delete from DummyBackupStorage")
    }
}
