// Copyright 2023 RisingWave Labs
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

use std::iter;

use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot::MetaSnapshot;
use risingwave_backup::meta_snapshot_v2::{MetaSnapshotV2, MetadataV2};
use risingwave_backup::storage::{MetaSnapshotStorage, MetaSnapshotStorageRef};
use risingwave_backup::MetaSnapshotId;

use crate::backup_restore::restore_impl::{Loader, Writer};
use crate::controller::SqlMetaStore;

pub struct LoaderV2 {
    backup_store: MetaSnapshotStorageRef,
}

impl LoaderV2 {
    pub fn new(backup_store: MetaSnapshotStorageRef) -> Self {
        Self { backup_store }
    }
}

#[async_trait::async_trait]
impl Loader<MetadataV2> for LoaderV2 {
    async fn load(&self, target_id: MetaSnapshotId) -> BackupResult<MetaSnapshot<MetadataV2>> {
        let target_snapshot: MetaSnapshotV2 = self.backup_store.get(target_id).await?;
        tracing::info!(
            "snapshot {} before rewrite:\n{}",
            target_id,
            target_snapshot
        );
        todo!("validate and rewrite seq")
    }
}

pub struct WriterModelV2ToMetaStoreV2 {
    meta_store: SqlMetaStore,
}

impl WriterModelV2ToMetaStoreV2 {
    pub fn new(meta_store: SqlMetaStore) -> Self {
        Self { meta_store }
    }
}

#[async_trait::async_trait]
impl Writer<MetadataV2> for WriterModelV2ToMetaStoreV2 {
    async fn write(&self, target_snapshot: MetaSnapshot<MetadataV2>) -> BackupResult<()> {
        let metadata = target_snapshot.metadata;
        let db = &self.meta_store.conn;
        insert_models(iter::once(metadata.version_stats), db).await?;
        insert_models(metadata.compaction_configs, db).await?;
        todo!("write other metadata")
    }
}

async fn insert_models<S, A>(
    models: impl IntoIterator<Item = S>,
    db: &impl sea_orm::ConnectionTrait,
) -> BackupResult<()>
where
    S: sea_orm::ModelTrait + Sync + Send + Sized + sea_orm::IntoActiveModel<A>,
    A: sea_orm::ActiveModelTrait + sea_orm::ActiveModelBehavior + Send + Sync + From<S>,
    <<A as sea_orm::ActiveModelTrait>::Entity as sea_orm::EntityTrait>::Model:
        sea_orm::IntoActiveModel<A>,
{
    use sea_orm::EntityTrait;
    if <S as sea_orm::ModelTrait>::Entity::find()
        .one(db)
        .await
        .map_err(|e| BackupError::MetaStorage(e.into()))?
        .is_some()
    {
        return Err(BackupError::NonemptyMetaStorage);
    }
    for m in models {
        m.into_active_model()
            .insert(db)
            .await
            .map_err(|e| BackupError::MetaStorage(e.into()))?;
    }
    Ok(())
}
