// Copyright 2024 RisingWave Labs
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

use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot::MetaSnapshot;
use risingwave_backup::meta_snapshot_v2::{MetaSnapshotV2, MetadataV2};
use risingwave_backup::storage::{MetaSnapshotStorage, MetaSnapshotStorageRef};
use risingwave_backup::MetaSnapshotId;
use sea_orm::{DatabaseBackend, DbBackend, DbErr, Statement};

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
        let snapshot_list = &self.backup_store.manifest().snapshot_metadata;
        let mut target_snapshot: MetaSnapshotV2 = self.backup_store.get(target_id).await?;
        tracing::debug!(
            "snapshot {} before rewrite:\n{}",
            target_id,
            target_snapshot
        );
        let newest_id = snapshot_list
            .iter()
            .map(|m| m.id)
            .max()
            .expect("should exist");
        assert!(
            newest_id >= target_id,
            "newest_id={}, target_id={}",
            newest_id,
            target_id
        );

        // validate and rewrite seq
        if newest_id > target_id {
            let newest_snapshot: MetaSnapshotV2 = self.backup_store.get(newest_id).await?;
            for seq in &target_snapshot.metadata.hummock_sequences {
                let newest = newest_snapshot
                    .metadata
                    .hummock_sequences
                    .iter()
                    .find(|s| s.name == seq.name)
                    .unwrap_or_else(|| {
                        panic!(
                            "violate superset requirement. Hummock sequence name {}",
                            seq.name
                        )
                    });
                assert!(newest.seq >= seq.seq, "violate monotonicity requirement");
            }
            target_snapshot.metadata.hummock_sequences = newest_snapshot.metadata.hummock_sequences;
            tracing::info!(
                "snapshot {} is rewritten by snapshot {}:\n",
                target_id,
                newest_id,
            );
            tracing::debug!("{target_snapshot}");
        }
        Ok(target_snapshot)
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
        insert_models(metadata.seaql_migrations.clone(), db).await?;
        insert_models(metadata.clusters.clone(), db).await?;
        insert_models(metadata.version_stats.clone(), db).await?;
        insert_models(metadata.compaction_configs.clone(), db).await?;
        insert_models(metadata.hummock_sequences.clone(), db).await?;
        insert_models(metadata.workers.clone(), db).await?;
        insert_models(metadata.worker_properties.clone(), db).await?;
        insert_models(metadata.users.clone(), db).await?;
        insert_models(metadata.objects.clone(), db).await?;
        insert_models(metadata.user_privileges.clone(), db).await?;
        insert_models(metadata.object_dependencies.clone(), db).await?;
        insert_models(metadata.databases.clone(), db).await?;
        insert_models(metadata.schemas.clone(), db).await?;
        insert_models(metadata.streaming_jobs.clone(), db).await?;
        insert_models(metadata.fragments.clone(), db).await?;
        insert_models(metadata.actors.clone(), db).await?;
        insert_models(metadata.actor_dispatchers.clone(), db).await?;
        insert_models(metadata.connections.clone(), db).await?;
        insert_models(metadata.sources.clone(), db).await?;
        insert_models(metadata.tables.clone(), db).await?;
        insert_models(metadata.sinks.clone(), db).await?;
        insert_models(metadata.views.clone(), db).await?;
        insert_models(metadata.indexes.clone(), db).await?;
        insert_models(metadata.functions.clone(), db).await?;
        insert_models(metadata.system_parameters.clone(), db).await?;
        insert_models(metadata.catalog_versions.clone(), db).await?;
        insert_models(metadata.subscriptions.clone(), db).await?;
        insert_models(metadata.session_parameters.clone(), db).await?;
        insert_models(metadata.secrets.clone(), db).await?;
        // update_auto_inc must be called last.
        update_auto_inc(&metadata, db).await?;
        Ok(())
    }
}

fn map_db_err(e: DbErr) -> BackupError {
    BackupError::MetaStorage(e.into())
}

// TODO: the code snippet is similar to the one found in migration.rs
async fn update_auto_inc(
    metadata: &MetadataV2,
    db: &impl sea_orm::ConnectionTrait,
) -> BackupResult<()> {
    match db.get_database_backend() {
        DbBackend::MySql => {
            if let Some(next_worker_id) = metadata.workers.iter().map(|w| w.worker_id + 1).max() {
                db.execute(Statement::from_string(
                    DatabaseBackend::MySql,
                    format!("ALTER TABLE worker AUTO_INCREMENT = {next_worker_id};"),
                ))
                .await
                .map_err(map_db_err)?;
            }
            if let Some(next_object_id) = metadata.objects.iter().map(|o| o.oid + 1).max() {
                db.execute(Statement::from_string(
                    DatabaseBackend::MySql,
                    format!("ALTER TABLE object AUTO_INCREMENT = {next_object_id};"),
                ))
                .await
                .map_err(map_db_err)?;
            }
            if let Some(next_user_id) = metadata.users.iter().map(|u| u.user_id + 1).max() {
                db.execute(Statement::from_string(
                    DatabaseBackend::MySql,
                    format!("ALTER TABLE user AUTO_INCREMENT = {next_user_id};"),
                ))
                .await
                .map_err(map_db_err)?;
            }
        }
        DbBackend::Postgres => {
            db.execute(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT setval('worker_worker_id_seq', (SELECT MAX(worker_id) FROM worker));",
            ))
            .await
            .map_err(map_db_err)?;
            db.execute(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT setval('object_oid_seq', (SELECT MAX(oid) FROM object) + 1);",
            ))
            .await
            .map_err(map_db_err)?;
            db.execute(Statement::from_string(
                DatabaseBackend::Postgres,
                "SELECT setval('user_user_id_seq', (SELECT MAX(user_id) FROM \"user\") + 1);",
            ))
            .await
            .map_err(map_db_err)?;
        }
        DbBackend::Sqlite => {}
    }
    Ok(())
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
        .map_err(map_db_err)?
        .is_some()
    {
        return Err(BackupError::NonemptyMetaStorage);
    }
    for m in models {
        m.into_active_model().insert(db).await.map_err(map_db_err)?;
    }
    Ok(())
}
