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

use std::cmp;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem::size_of;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_backup::MetaSnapshotId;
use risingwave_backup::error::{BackupError, BackupResult};
use risingwave_backup::meta_snapshot_v2::HUMMOCK_VERSION_ENCODING_INDEX;
use risingwave_backup::storage::{
    MetaSnapshotStorage, MetaSnapshotStorageRef, MetaSnapshotStreamingReader,
};
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_pb::hummock::HummockVersion as PbHummockVersion;
use sea_orm::sea_query::{Expr, Func};
use sea_orm::{
    ActiveModelTrait, DbErr, EntityTrait, IntoActiveModel, QuerySelect, TransactionTrait,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tempfile::NamedTempFile;

use crate::controller::SqlMetaStore;

pub struct LoaderV2 {
    backup_store: MetaSnapshotStorageRef,
}

impl LoaderV2 {
    pub fn new(backup_store: MetaSnapshotStorageRef) -> Self {
        Self { backup_store }
    }

    pub async fn load_spooled(
        &self,
        target_id: MetaSnapshotId,
    ) -> BackupResult<SpooledMetaSnapshotV2> {
        let snapshot_list = &self.backup_store.manifest().await.snapshot_metadata;
        let mut target_snapshot =
            SpooledMetaSnapshotV2::read_from(&self.backup_store, target_id).await?;
        tracing::debug!(
            snapshot_id = target_snapshot.id,
            format_version = target_snapshot.format_version,
            "loaded metadata snapshot into spooled rows"
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
            let mut newest_snapshot =
                SpooledMetaSnapshotV2::read_from(&self.backup_store, newest_id).await?;
            let target_sequences = target_snapshot
                .metadata
                .hummock_sequences
                .read_all::<risingwave_meta_model::hummock_sequence::Model>()?;
            let newest_sequences = newest_snapshot
                .metadata
                .hummock_sequences
                .read_all::<risingwave_meta_model::hummock_sequence::Model>()?;
            for seq in &target_sequences {
                let newest = newest_sequences
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
            target_snapshot.metadata.hummock_sequences = TableRows::from_models(newest_sequences)?;
            tracing::info!(
                "snapshot {} is rewritten by snapshot {}",
                target_id,
                newest_id,
            );
        }
        Ok(target_snapshot)
    }
}

pub struct SpooledMetaSnapshotV2 {
    pub format_version: u32,
    pub id: MetaSnapshotId,
    pub metadata: SpooledMetadataV2,
}

impl SpooledMetaSnapshotV2 {
    async fn read_from(
        backup_store: &MetaSnapshotStorageRef,
        target_id: MetaSnapshotId,
    ) -> BackupResult<Self> {
        let mut reader = backup_store.begin_snapshot_read(target_id).await?;
        let format_version = reader.read_u32_le().await?;
        let id = reader.read_u64_le().await?;
        if id != target_id {
            return Err(BackupError::Decoding(
                anyhow!("snapshot id mismatch: expected {}, got {}", target_id, id).into(),
            ));
        }
        let metadata = SpooledMetadataV2::read_from(&mut reader).await?;
        reader.finish().await?;
        Ok(Self {
            format_version,
            id,
            metadata,
        })
    }
}

pub struct TableRows {
    file: NamedTempFile,
    count: u32,
}

impl TableRows {
    async fn read_from(reader: &mut MetaSnapshotStreamingReader) -> BackupResult<Self> {
        let count = reader.read_u32_le().await?;
        let mut rows = Self::empty(count)?;
        for _ in 0..count {
            let bytes = read_len_prefixed_json_bytes(reader).await?;
            rows.write_row(&bytes)?;
        }
        rows.rewind()?;
        Ok(rows)
    }

    fn from_models<T: Serialize>(models: Vec<T>) -> BackupResult<Self> {
        let count = u32::try_from(models.len())
            .map_err(|_| BackupError::Other(anyhow!("too many metadata rows")))?;
        let mut rows = Self::empty(count)?;
        for model in models {
            let bytes = serde_json::to_vec(&model)?;
            rows.write_row(&bytes)?;
        }
        rows.rewind()?;
        Ok(rows)
    }

    fn empty(count: u32) -> BackupResult<Self> {
        Ok(Self {
            file: NamedTempFile::new().map_err(map_io_err)?,
            count,
        })
    }

    fn write_row(&mut self, bytes: &[u8]) -> BackupResult<()> {
        let len = u64::try_from(bytes.len())
            .map_err(|_| BackupError::Other(anyhow!("cannot convert {} into u64", bytes.len())))?;
        self.file
            .as_file_mut()
            .write_all(&len.to_le_bytes())
            .map_err(map_io_err)?;
        self.file
            .as_file_mut()
            .write_all(bytes)
            .map_err(map_io_err)?;
        Ok(())
    }

    fn rewind(&mut self) -> BackupResult<()> {
        self.file
            .as_file_mut()
            .seek(SeekFrom::Start(0))
            .map_err(map_io_err)?;
        Ok(())
    }

    fn read_all<T: DeserializeOwned>(&mut self) -> BackupResult<Vec<T>> {
        self.rewind()?;
        let mut rows = Vec::with_capacity(self.count as usize);
        for _ in 0..self.count {
            rows.push(self.read_next()?);
        }
        Ok(rows)
    }

    fn read_next<T: DeserializeOwned>(&mut self) -> BackupResult<T> {
        let mut len_bytes = [0; size_of::<u64>()];
        self.file
            .as_file_mut()
            .read_exact(&mut len_bytes)
            .map_err(map_io_err)?;
        let len = usize::try_from(u64::from_le_bytes(len_bytes))
            .map_err(|_| BackupError::Other(anyhow!("cannot convert row length into usize")))?;
        let mut bytes = vec![0; len];
        self.file
            .as_file_mut()
            .read_exact(&mut bytes)
            .map_err(map_io_err)?;
        Ok(serde_json::from_slice(&bytes)?)
    }
}

macro_rules! define_spooled_metadata_v2 {
    ($( {$name:ident, $mod_path:ident::$mod_name:ident} ),*) => {
        pub struct SpooledMetadataV2 {
            pub hummock_version: HummockVersion,
            $(
                pub $name: TableRows,
            )*
        }

        impl SpooledMetadataV2 {
            async fn read_from(reader: &mut MetaSnapshotStreamingReader) -> BackupResult<Self> {
                let mut _idx = 0;
                let mut hummock_version = None;
                $(
                    if _idx == HUMMOCK_VERSION_ENCODING_INDEX {
                        hummock_version = Some(read_hummock_version(reader).await?);
                    }
                    let $name = TableRows::read_from(reader).await?;
                    _idx += 1;
                )*
                Ok(Self {
                    hummock_version: hummock_version
                        .ok_or_else(|| BackupError::Decoding(anyhow!("missing hummock version").into()))?,
                    $(
                        $name,
                    )*
                })
            }

            pub fn storage_url(&mut self) -> BackupResult<String> {
                storage_url_from_system_parameters(
                    &self.system_parameters.read_all::<risingwave_meta_model::system_parameter::Model>()?,
                )
            }

            pub fn storage_directory(&mut self) -> BackupResult<String> {
                storage_directory_from_system_parameters(
                    &self.system_parameters.read_all::<risingwave_meta_model::system_parameter::Model>()?,
                )
            }
        }
    };
}

risingwave_backup::for_all_metadata_models_v2!(define_spooled_metadata_v2);

async fn read_hummock_version(
    reader: &mut MetaSnapshotStreamingReader,
) -> BackupResult<HummockVersion> {
    let pb: PbHummockVersion = read_1_json(reader).await?;
    Ok(HummockVersion::from_persisted_protobuf_owned(pb))
}

async fn read_1_json<T: DeserializeOwned>(
    reader: &mut MetaSnapshotStreamingReader,
) -> BackupResult<T> {
    let count = reader.read_u32_le().await?;
    if count != 1 {
        return Err(BackupError::Decoding(
            anyhow!("expected exactly one row, got {}", count).into(),
        ));
    }
    let bytes = read_len_prefixed_json_bytes(reader).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

async fn read_len_prefixed_json_bytes(
    reader: &mut MetaSnapshotStreamingReader,
) -> BackupResult<Vec<u8>> {
    let len = match reader.read_u32_le().await? {
        0 => usize::try_from(reader.read_u64_le().await?)
            .map_err(|_| BackupError::Other(anyhow!("cannot convert row length into usize")))?,
        len => len as usize,
    };
    Ok(reader.read_bytes(len).await?.to_vec())
}

fn map_io_err(e: std::io::Error) -> BackupError {
    BackupError::Other(e.into())
}

pub struct WriterModelV2ToMetaStoreV2 {
    meta_store: SqlMetaStore,
}

impl WriterModelV2ToMetaStoreV2 {
    pub fn new(meta_store: SqlMetaStore) -> Self {
        Self { meta_store }
    }

    pub async fn write_spooled(
        &self,
        metadata: &mut SpooledMetadataV2,
        overwrite: Option<RestoreOverwrite<'_>>,
    ) -> BackupResult<()> {
        let txn = self.meta_store.conn.begin().await.map_err(map_db_err)?;
        ensure_all_meta_store_tables_are_empty(&txn).await?;
        insert_spooled_metadata(metadata, &txn).await?;
        if let Some(overwrite) = overwrite {
            overwrite_system_parameters(
                &txn,
                overwrite.new_storage_url,
                overwrite.new_storage_dir,
                overwrite.new_backup_url,
                overwrite.new_backup_dir,
            )
            .await?;
        }
        txn.commit().await.map_err(map_db_err)?;
        // MySQL `ALTER TABLE ... AUTO_INCREMENT` implicitly commits, so keep sequence repair
        // outside the restore transaction. Failures here leave a complete restored metadata set,
        // not a partially committed restore.
        update_auto_inc_from_db(&self.meta_store.conn).await?;
        Ok(())
    }
}

pub struct RestoreOverwrite<'a> {
    pub new_storage_url: &'a str,
    pub new_storage_dir: &'a str,
    pub new_backup_url: &'a str,
    pub new_backup_dir: &'a str,
}

fn compare_object_restore_order(
    a: &risingwave_meta_model::object::Model,
    b: &risingwave_meta_model::object::Model,
) -> cmp::Ordering {
    use risingwave_meta_model::object::ObjectType;
    match (a.obj_type, b.obj_type) {
        (ObjectType::Database, ObjectType::Database) => a.oid.cmp(&b.oid),
        (ObjectType::Database, _) => cmp::Ordering::Less,
        (_, ObjectType::Database) => cmp::Ordering::Greater,
        (ObjectType::Schema, ObjectType::Schema) => a.oid.cmp(&b.oid),
        (ObjectType::Schema, _) => cmp::Ordering::Less,
        (_, ObjectType::Schema) => cmp::Ordering::Greater,
        (_, _) => a.oid.cmp(&b.oid),
    }
}

async fn insert_spooled_metadata(
    metadata: &mut SpooledMetadataV2,
    db: &impl sea_orm::ConnectionTrait,
) -> BackupResult<()> {
    insert_spooled_models::<risingwave_meta_model::serde_seaql_migration::Entity>(
        &mut metadata.seaql_migrations,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::cluster::Entity>(&mut metadata.clusters, db)
        .await?;
    insert_spooled_models::<risingwave_meta_model::hummock_version_stats::Entity>(
        &mut metadata.version_stats,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::compaction_config::Entity>(
        &mut metadata.compaction_configs,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::hummock_sequence::Entity>(
        &mut metadata.hummock_sequences,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::worker::Entity>(&mut metadata.workers, db)
        .await?;
    insert_spooled_models::<risingwave_meta_model::worker_property::Entity>(
        &mut metadata.worker_properties,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::user::Entity>(&mut metadata.users, db).await?;

    let mut objects = metadata
        .objects
        .read_all::<risingwave_meta_model::object::Model>()?;
    objects.sort_by(compare_object_restore_order);
    insert_models(objects, db).await?;

    let mut user_privileges = metadata
        .user_privileges
        .read_all::<risingwave_meta_model::user_privilege::Model>()?;
    user_privileges.sort_by_key(|u| u.id);
    insert_models(user_privileges, db).await?;

    insert_spooled_models::<risingwave_meta_model::object_dependency::Entity>(
        &mut metadata.object_dependencies,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::database::Entity>(&mut metadata.databases, db)
        .await?;
    insert_spooled_models::<risingwave_meta_model::schema::Entity>(&mut metadata.schemas, db)
        .await?;
    insert_spooled_models::<risingwave_meta_model::streaming_job::Entity>(
        &mut metadata.streaming_jobs,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::fragment::Entity>(&mut metadata.fragments, db)
        .await?;
    insert_spooled_models::<risingwave_meta_model::fragment_relation::Entity>(
        &mut metadata.fragment_relation,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::connection::Entity>(
        &mut metadata.connections,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::source::Entity>(&mut metadata.sources, db)
        .await?;
    insert_spooled_models::<risingwave_meta_model::table::Entity>(&mut metadata.tables, db).await?;
    insert_spooled_models::<risingwave_meta_model::sink::Entity>(&mut metadata.sinks, db).await?;
    insert_spooled_models::<risingwave_meta_model::view::Entity>(&mut metadata.views, db).await?;
    insert_spooled_models::<risingwave_meta_model::index::Entity>(&mut metadata.indexes, db)
        .await?;
    insert_spooled_models::<risingwave_meta_model::function::Entity>(&mut metadata.functions, db)
        .await?;
    insert_spooled_models::<risingwave_meta_model::system_parameter::Entity>(
        &mut metadata.system_parameters,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::catalog_version::Entity>(
        &mut metadata.catalog_versions,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::subscription::Entity>(
        &mut metadata.subscriptions,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::session_parameter::Entity>(
        &mut metadata.session_parameters,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::secret::Entity>(&mut metadata.secrets, db)
        .await?;
    insert_spooled_models::<risingwave_meta_model::exactly_once_iceberg_sink::Entity>(
        &mut metadata.exactly_once_iceberg_sinks,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::iceberg_tables::Entity>(
        &mut metadata.iceberg_tables,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::iceberg_namespace_properties::Entity>(
        &mut metadata.iceberg_namespace_properties,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::user_default_privilege::Entity>(
        &mut metadata.user_default_privilege,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::fragment_splits::Entity>(
        &mut metadata.fragment_splits,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::pending_sink_state::Entity>(
        &mut metadata.pending_sink_state,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::refresh_job::Entity>(
        &mut metadata.refresh_jobs,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::cdc_table_snapshot_split::Entity>(
        &mut metadata.cdc_table_snapshot_splits,
        db,
    )
    .await?;
    insert_spooled_models::<risingwave_meta_model::hummock_table_change_log::Entity>(
        &mut metadata.hummock_table_change_logs,
        db,
    )
    .await?;
    Ok(())
}

async fn overwrite_system_parameters(
    db: &impl sea_orm::ConnectionTrait,
    new_storage_url: &str,
    new_storage_dir: &str,
    new_backup_url: &str,
    new_backup_dir: &str,
) -> BackupResult<()> {
    overwrite_system_parameter_values(
        db,
        [
            ("state_store", new_storage_url),
            ("data_directory", new_storage_dir),
            ("backup_storage_url", new_backup_url),
            ("backup_storage_directory", new_backup_dir),
        ],
    )
    .await
}

async fn overwrite_system_parameter_values<'a>(
    db: &impl sea_orm::ConnectionTrait,
    kvs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> BackupResult<()> {
    for (k, v) in kvs {
        let Some(model) = risingwave_meta_model::system_parameter::Entity::find_by_id(k)
            .one(db)
            .await
            .map_err(map_db_err)?
        else {
            return Err(BackupError::MetaStorage(
                anyhow!("{k} not found in system_parameter table").into(),
            ));
        };
        let mut kv: risingwave_meta_model::system_parameter::ActiveModel = model.into();
        kv.value = sea_orm::ActiveValue::Set(v.to_owned());
        risingwave_meta_model::system_parameter::Entity::update(kv)
            .exec(db)
            .await
            .map_err(map_db_err)?;
    }
    Ok(())
}

fn storage_url_from_system_parameters(
    system_parameters: &[risingwave_meta_model::system_parameter::Model],
) -> BackupResult<String> {
    let storage_url_from_snapshot =
        Itertools::exactly_one(system_parameters.iter().filter_map(|m| {
            if m.name == "state_store" {
                return Some(m.value.clone());
            }
            None
        }))
        .map_err(|_| BackupError::Other(anyhow!("expect state_store")))?;
    storage_url_from_snapshot
        .strip_prefix("hummock+")
        .map(|s| s.to_owned())
        .ok_or_else(|| {
            BackupError::Other(anyhow!(
                "invalid state_store from metadata snapshot: {}",
                storage_url_from_snapshot
            ))
        })
}

fn storage_directory_from_system_parameters(
    system_parameters: &[risingwave_meta_model::system_parameter::Model],
) -> BackupResult<String> {
    Itertools::exactly_one(system_parameters.iter().filter_map(|m| {
        if m.name == "data_directory" {
            return Some(m.value.clone());
        }
        None
    }))
    .map_err(|_| BackupError::Other(anyhow!("expect data_directory")))
}

async fn ensure_all_meta_store_tables_are_empty(
    db: &impl sea_orm::ConnectionTrait,
) -> BackupResult<()> {
    macro_rules! ensure_entity_empty {
        ($($entity_mod:ident),* $(,)?) => {
            $(
                if risingwave_meta_model::$entity_mod::Entity::find()
                    .one(db)
                    .await
                    .map_err(map_db_err)?
                    .is_some()
                {
                    return Err(BackupError::NonemptyMetaStorage);
                }
            )*
        };
    }

    risingwave_meta_model::for_all_meta_model_entities!(ensure_entity_empty);
    Ok(())
}

fn map_db_err(e: DbErr) -> BackupError {
    BackupError::MetaStorage(e.into())
}

macro_rules! reset_sql_sequence_from_db {
    ($db:ident, $( {$table:expr, $entity_mod:ident, $id_field:ident, $column:ident} ),*) => {
        $(
        match $db.get_database_backend() {
            sea_orm::DbBackend::MySql => {
                let next_value: i32 = risingwave_meta_model::$entity_mod::Entity::find()
                    .select_only()
                    .expr(Func::if_null(
                        Expr::col(risingwave_meta_model::$entity_mod::Column::$column)
                            .max()
                            .add(1),
                        1,
                    ))
                    .into_tuple()
                    .one($db)
                    .await
                    .map_err(map_db_err)?
                    .unwrap_or_default();
                $db.execute(sea_orm::Statement::from_string(
                    sea_orm::DatabaseBackend::MySql,
                    format!("ALTER TABLE {} AUTO_INCREMENT = {};", $table, next_value),
                ))
                .await
                .map_err(map_db_err)?;
            }
            sea_orm::DbBackend::Postgres => {
                $db.execute(sea_orm::Statement::from_string(
                    sea_orm::DatabaseBackend::Postgres,
                    format!("SELECT setval('{}_{}_seq', (SELECT MAX({}) FROM \"{}\"));", $table, stringify!($id_field), stringify!($id_field), $table),
                ))
                .await
                .map_err(map_db_err)?;
            }
            sea_orm::DbBackend::Sqlite => {}
            }
        )*
    };
}

async fn update_auto_inc_from_db(db: &impl sea_orm::ConnectionTrait) -> BackupResult<()> {
    reset_sql_sequence_from_db!(
        db,
        {"worker", worker, worker_id, WorkerId},
        {"object", object, oid, Oid},
        {"user", user, user_id, UserId},
        {"user_privilege", user_privilege, id, Id},
        {"fragment", fragment, fragment_id, FragmentId},
        {"object_dependency", object_dependency, id, Id}
    );
    Ok(())
}

async fn insert_spooled_models<E>(
    rows: &mut TableRows,
    db: &impl sea_orm::ConnectionTrait,
) -> BackupResult<()>
where
    E: sea_orm::EntityTrait,
    E::Model: DeserializeOwned + Sync + Send + Sized + sea_orm::IntoActiveModel<E::ActiveModel>,
    E::ActiveModel:
        sea_orm::ActiveModelTrait<Entity = E> + sea_orm::ActiveModelBehavior + Send + Sync,
{
    if E::find().one(db).await.map_err(map_db_err)?.is_some() {
        return Err(BackupError::NonemptyMetaStorage);
    }
    rows.rewind()?;
    for _ in 0..rows.count {
        let model: E::Model = rows.read_next()?;
        model
            .into_active_model()
            .insert(db)
            .await
            .map_err(map_db_err)?;
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_backup::meta_snapshot_v2::{MetaSnapshotV2, MetadataV2};
    use risingwave_backup::storage::{MetaSnapshotStorage, unused};

    use super::*;

    #[tokio::test]
    async fn test_spooled_snapshot_reader_matches_legacy_snapshot() {
        let store = Arc::new(unused().await);
        let snapshot = MetaSnapshotV2 {
            format_version: 2,
            id: 44,
            metadata: MetadataV2::default(),
        };
        store.create(&snapshot, None).await.unwrap();

        let mut spooled = SpooledMetaSnapshotV2::read_from(&store, snapshot.id)
            .await
            .unwrap();
        assert_eq!(spooled.format_version, snapshot.format_version);
        assert_eq!(spooled.id, snapshot.id);
        assert_eq!(
            spooled.metadata.hummock_version.id,
            snapshot.metadata.hummock_version.id
        );
        assert!(
            spooled
                .metadata
                .seaql_migrations
                .read_all::<risingwave_meta_model::serde_seaql_migration::Model>()
                .unwrap()
                .is_empty()
        );
    }
}
