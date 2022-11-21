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

use itertools::Itertools;
use risingwave_object_store::object::{ObjectError, ObjectStoreRef};

use crate::backup_restore::db_snapshot::DbSnapshot;
use crate::backup_restore::error::{BackupError, BackupResult};
use crate::backup_restore::DbSnapshotId;

pub type BackupStorageRef = Box<dyn BackupStorage>;

#[async_trait::async_trait]
pub trait BackupStorage: 'static + Sync + Send {
    /// Creates a db snapshot.
    async fn create(&self, snapshot: DbSnapshot) -> BackupResult<()>;

    /// Gets a db snapshot by id.
    async fn get(&self, id: DbSnapshotId) -> BackupResult<DbSnapshot>;

    /// List all db snapshots.
    async fn list(&self) -> BackupResult<Vec<DbSnapshot>>;

    /// Deletes db snapshots by ids.
    async fn delete(&self, ids: &[DbSnapshotId]) -> BackupResult<()>;
}

#[derive(Clone)]
pub struct ObjectStoreBackupStorage {
    path: String,
    store: ObjectStoreRef,
}

impl ObjectStoreBackupStorage {
    pub fn new(path: String, store: ObjectStoreRef) -> Self {
        Self { path, store }
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
    async fn create(&self, snapshot: DbSnapshot) -> BackupResult<()> {
        let path = self.get_db_snapshot_path(snapshot.id);
        self.store.upload(&path, snapshot.encode().into()).await?;
        Ok(())
    }

    async fn get(&self, id: DbSnapshotId) -> BackupResult<DbSnapshot> {
        let path = self.get_db_snapshot_path(id);
        let data = self.store.read(&path, None).await?;
        DbSnapshot::decode(&data)
    }

    async fn list(&self) -> BackupResult<Vec<DbSnapshot>> {
        let object_metadata_list = self.store.list(&self.path).await?;
        let mut db_snapshots_join_handles = vec![];
        for object_metadata in object_metadata_list {
            let id = ObjectStoreBackupStorage::get_db_snapshot_id_from_path(&object_metadata.key);
            let this = self.clone();
            let db_snapshot_join_handle = tokio::spawn(async move { this.get(id).await });
            db_snapshots_join_handles.push(db_snapshot_join_handle);
        }
        let db_snapshots = futures::future::try_join_all(db_snapshots_join_handles)
            .await
            .map_err(|e| BackupError::Other(e.into()))?
            .into_iter()
            .collect::<BackupResult<Vec<DbSnapshot>>>()?;
        Ok(db_snapshots)
    }

    async fn delete(&self, ids: &[DbSnapshotId]) -> BackupResult<()> {
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
        BackupError::Storage(e.into())
    }
}

pub struct DummyBackupStorage {}

#[async_trait::async_trait]
impl BackupStorage for DummyBackupStorage {
    async fn create(&self, _snapshot: DbSnapshot) -> BackupResult<()> {
        panic!("should not create from DummyBackupStorage")
    }

    async fn get(&self, _id: DbSnapshotId) -> BackupResult<DbSnapshot> {
        panic!("should not get from DummyBackupStorage")
    }

    async fn list(&self) -> BackupResult<Vec<DbSnapshot>> {
        // Satisfy `BackupManager`
        Ok(vec![])
    }

    async fn delete(&self, _ids: &[DbSnapshotId]) -> BackupResult<()> {
        panic!("should not delete from DummyBackupStorage")
    }
}
