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

mod database;
mod fragment;
mod user;

use std::collections::{HashMap, HashSet, VecDeque};
use std::option::Option::Some;
use std::sync::Arc;

use anyhow::anyhow;
use database::*;
pub use fragment::*;
use itertools::Itertools;
use risingwave_common::catalog::{
    DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_FOR_PG,
    DEFAULT_SUPER_USER_FOR_PG_ID, DEFAULT_SUPER_USER_ID, PG_CATALOG_SCHEMA_NAME,
};
use risingwave_common::{bail, ensure};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{Database, Index, Schema, Sink, Source, Table};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::user::grant_privilege::{ActionWithGrantOption, Object};
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::{GrantPrivilege, UserInfo};
use tokio::sync::{Mutex, MutexGuard};
use user::*;

use crate::manager::{IdCategory, MetaSrvEnv, NotificationVersion, StreamingJob};
use crate::model::{MetadataModel, Transactional};
use crate::storage::{MetaStore, Transaction};
use crate::{MetaError, MetaResult};

pub type DatabaseId = u32;
pub type SchemaId = u32;
pub type TableId = u32;
pub type SourceId = u32;
pub type SinkId = u32;
pub type RelationId = u32;
pub type IndexId = u32;

pub type UserId = u32;

pub type CatalogManagerRef<S> = Arc<CatalogManager<S>>;

/// `CatalogManager` managers the user info, including authentication and privileges. It only
/// responds to manager the user info and some basic validation. Other authorization relate to the
/// current session user should be done in Frontend before passing to Meta.
pub struct CatalogManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    core: Mutex<CatalogManagerCore<S>>,
}

pub struct CatalogManagerCore<S: MetaStore> {
    pub database: DatabaseManager<S>,
    pub user: UserManager,
}

impl<S> CatalogManagerCore<S>
where
    S: MetaStore,
{
    async fn new(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let database = DatabaseManager::new(env.clone()).await?;
        let user = UserManager::new(env.clone()).await?;
        Ok(Self { database, user })
    }
}

impl<S> CatalogManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let core = Mutex::new(CatalogManagerCore::new(env.clone()).await?);
        let catalog_manager = Self { env, core };
        catalog_manager.init().await?;
        Ok(catalog_manager)
    }

    async fn init(&self) -> MetaResult<()> {
        self.init_database().await?;
        self.init_user().await?;
        Ok(())
    }

    pub async fn get_catalog_core_guard(&self) -> MutexGuard<CatalogManagerCore<S>> {
        self.core.lock().await
    }
}

// Database
impl<S> CatalogManager<S>
where
    S: MetaStore,
{
    async fn init_database(&self) -> MetaResult<()> {
        let mut database = Database {
            name: DEFAULT_DATABASE_NAME.to_string(),
            owner: DEFAULT_SUPER_USER_ID,
            ..Default::default()
        };
        if !self.core.lock().await.database.has_database(&database) {
            database.id = self
                .env
                .id_gen_manager()
                .generate::<{ IdCategory::Database }>()
                .await? as u32;
            self.create_database(&database).await?;
        }
        Ok(())
    }

    pub async fn create_database(&self, database: &Database) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        if !core.has_database(database) {
            let mut transaction = Transaction::default();
            database.upsert_in_transaction(&mut transaction)?;
            let mut schemas = vec![];
            for schema_name in [DEFAULT_SCHEMA_NAME, PG_CATALOG_SCHEMA_NAME] {
                let schema = Schema {
                    id: self
                        .env
                        .id_gen_manager()
                        .generate::<{ IdCategory::Schema }>()
                        .await? as u32,
                    database_id: database.id,
                    name: schema_name.to_string(),
                    owner: database.owner,
                };
                schema.upsert_in_transaction(&mut transaction)?;
                schemas.push(schema);
            }
            self.env.meta_store().txn(transaction).await?;

            core.add_database(database);
            let mut version = self
                .notify_frontend(Operation::Add, Info::Database(database.to_owned()))
                .await;
            for schema in schemas {
                core.add_schema(&schema);
                version = self
                    .env
                    .notification_manager()
                    .notify_frontend(Operation::Add, Info::Schema(schema))
                    .await;
            }

            Ok(version)
        } else {
            bail!("database already exists");
        }
    }

    pub async fn drop_database(&self, database_id: DatabaseId) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let database = Database::select(self.env.meta_store(), &database_id).await?;
        if let Some(database) = database {
            let schemas = Schema::list(self.env.meta_store())
                .await?
                .into_iter()
                .filter(|s| s.database_id == database_id)
                .collect_vec();
            if schemas.len() != 1 {
                bail!("database is not empty!");
            }
            assert_eq!(PG_CATALOG_SCHEMA_NAME, schemas[0].name);

            let mut transaction = Transaction::default();
            // TODO(zehua): remove duplicate users
            let mut users_need_update = Self::release_privileges(
                user_core.list_users(),
                &Object::SchemaId(schemas[0].id),
                &mut transaction,
            )?;
            users_need_update.append(&mut Self::release_privileges(
                user_core.list_users(),
                &Object::DatabaseId(database_id),
                &mut transaction,
            )?);
            database.delete_in_transaction(&mut transaction)?;
            schemas[0].delete_in_transaction(&mut transaction)?;
            self.env.meta_store().txn(transaction).await?;

            database_core.drop_schema(&schemas[0]);
            database_core.drop_database(&database);

            for user in users_need_update {
                user_core.insert_user_info(user.id, user.clone());
                self.notify_frontend(Operation::Update, Info::User(user))
                    .await;
            }
            let version = self
                .notify_frontend(Operation::Delete, Info::Database(database))
                .await;

            Ok(version)
        } else {
            bail!("database doesn't exist");
        }
    }

    pub async fn create_schema(&self, schema: &Schema) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        if !core.has_schema(schema) {
            schema.insert(self.env.meta_store()).await?;
            core.add_schema(schema);

            let version = self
                .notify_frontend(Operation::Add, Info::Schema(schema.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("schema already exists");
        }
    }

    pub async fn drop_schema(&self, schema_id: SchemaId) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let schema = Schema::select(self.env.meta_store(), &schema_id).await?;
        if let Some(schema) = schema {
            let tables = Table::list(self.env.meta_store())
                .await?
                .into_iter()
                .filter(|t| t.database_id == schema.database_id && t.schema_id == schema_id)
                .collect_vec();
            if !tables.is_empty() {
                bail!("schema is not empty!");
            }

            let mut transaction = Transaction::default();
            let users_need_update = Self::release_privileges(
                user_core.list_users(),
                &Object::SchemaId(schema_id),
                &mut transaction,
            )?;
            schema.delete_in_transaction(&mut transaction)?;
            self.env.meta_store().txn(transaction).await?;

            database_core.drop_schema(&schema);

            for user in users_need_update {
                user_core.insert_user_info(user.id, user.clone());
                self.notify_frontend(Operation::Update, Info::User(user))
                    .await;
            }
            let version = self
                .notify_frontend(Operation::Delete, Info::Schema(schema))
                .await;

            Ok(version)
        } else {
            bail!("schema doesn't exist");
        }
    }

    pub async fn start_create_stream_job_procedure(
        &self,
        stream_job: &StreamingJob,
    ) -> MetaResult<()> {
        match stream_job {
            StreamingJob::MaterializedView(table) => self.start_create_table_procedure(table).await,
            StreamingJob::Sink(sink) => self.start_create_sink_procedure(sink).await,
            StreamingJob::Index(index, index_table) => {
                self.start_create_index_procedure(index, index_table).await
            }
            StreamingJob::MaterializedSource(source, table) => {
                self.start_create_materialized_source_procedure(source, table)
                    .await
            }
        }
    }

    pub async fn mark_creating_tables(&self, creating_tables: &[Table]) {
        let core = &mut self.core.lock().await.database;
        core.mark_creating_tables(creating_tables);
        for table in creating_tables {
            self.notify_compute_and_compactor(Operation::Add, Info::Table(table.to_owned()))
                .await;
        }
    }

    pub async fn unmark_creating_tables(&self, creating_table_ids: &[TableId], need_notify: bool) {
        let core = &mut self.core.lock().await.database;
        core.unmark_creating_tables(creating_table_ids);
        if need_notify {
            for table_id in creating_table_ids {
                self.notify_compute_and_compactor(
                    Operation::Delete,
                    Info::Table(Table {
                        id: *table_id,
                        ..Default::default()
                    }),
                )
                .await;
            }
        }
    }

    async fn notify_compute_and_compactor(&self, operation: Operation, info: Info) {
        self.env
            .notification_manager()
            .notify_compute(operation, info.clone())
            .await;

        self.env
            .notification_manager()
            .notify_compactor(operation, info)
            .await;
    }

    pub async fn start_create_table_procedure(&self, table: &Table) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            for &dependent_relation_id in &table.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("table already exists or in creating procedure");
        }
    }

    pub async fn finish_create_table_procedure(
        &self,
        internal_tables: Vec<Table>,
        table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            let mut transaction = Transaction::default();
            for table in &internal_tables {
                table.upsert_in_transaction(&mut transaction)?;
            }
            table.upsert_in_transaction(&mut transaction)?;
            self.env.meta_store().txn(transaction).await?;

            for internal_table in internal_tables {
                core.add_table(&internal_table);

                self.notify_frontend(Operation::Add, Info::Table(internal_table.to_owned()))
                    .await;
            }
            core.add_table(table);
            let version = self
                .notify_frontend(Operation::Add, Info::Table(table.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("table already exist or not in creating procedure");
        }
    }

    pub async fn cancel_create_table_procedure(&self, table: &Table) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            for &dependent_relation_id in &table.dependent_relations {
                core.decrease_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("table already exist or not in creating procedure");
        }
    }

    pub async fn drop_table(
        &self,
        table_id: TableId,
        internal_table_ids: Vec<TableId>,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let table = Table::select(self.env.meta_store(), &table_id).await?;
        if let Some(table) = table {
            match database_core.get_ref_count(table_id) {
                Some(ref_count) => Err(MetaError::permission_denied(format!(
                    "Fail to delete table `{}` because {} other relation(s) depend on it",
                    table.name, ref_count
                ))),
                None => {
                    let mut transaction = Transaction::default();
                    let mut users_need_update = Self::release_privileges(
                        user_core.list_users(),
                        &Object::TableId(table_id),
                        &mut transaction,
                    )?;
                    table.delete_in_transaction(&mut transaction)?;
                    let mut tables_to_drop = vec![];
                    for internal_table_id in internal_table_ids {
                        let internal_table =
                            Table::select(self.env.meta_store(), &internal_table_id).await?;
                        if let Some(internal_table) = internal_table {
                            users_need_update.append(&mut Self::release_privileges(
                                user_core.list_users(),
                                &Object::TableId(internal_table.id),
                                &mut transaction,
                            )?);
                            internal_table.delete_in_transaction(&mut transaction)?;
                            tables_to_drop.push(internal_table);
                        }
                    }
                    self.env.meta_store().txn(transaction).await?;

                    for user in users_need_update {
                        user_core.insert_user_info(user.id, user.clone());
                        self.notify_frontend(Operation::Update, Info::User(user))
                            .await;
                    }
                    for table in tables_to_drop {
                        database_core.drop_table(&table);
                        self.notify_frontend(Operation::Delete, Info::Table(table))
                            .await;
                    }
                    database_core.drop_table(&table);
                    for &dependent_relation_id in &table.dependent_relations {
                        database_core.decrease_ref_count(dependent_relation_id);
                    }

                    let version = self
                        .notify_frontend(Operation::Delete, Info::Table(table.to_owned()))
                        .await;

                    Ok(version)
                }
            }
        } else {
            bail!("table doesn't exist");
        }
    }

    pub async fn get_index_table(&self, index_id: IndexId) -> MetaResult<TableId> {
        let index = Index::select(self.env.meta_store(), &index_id).await?;
        if let Some(index) = index {
            Ok(index.index_table_id)
        } else {
            bail!("index doesn't exist");
        }
    }

    pub async fn drop_index(
        &self,
        index_id: IndexId,
        index_table_id: TableId,
        internal_table_ids: Vec<TableId>,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let index = Index::select(self.env.meta_store(), &index_id).await?;
        if let Some(index) = index {
            let mut transaction = Transaction::default();
            index.delete_in_transaction(&mut transaction)?;
            assert_eq!(index_table_id, index.index_table_id);

            // drop index table
            let mut tables_to_drop = vec![];
            let table = Table::select(self.env.meta_store(), &index_table_id).await?;
            if let Some(table) = table {
                match database_core.get_ref_count(index_table_id) {
                    Some(ref_count) => Err(MetaError::permission_denied(format!(
                        "Fail to delete table `{}` because {} other relation(s) depend on it",
                        table.name, ref_count
                    ))),
                    None => {
                        let mut users_need_update = Self::release_privileges(
                            user_core.list_users(),
                            &Object::TableId(index_table_id),
                            &mut transaction,
                        )?;
                        table.delete_in_transaction(&mut transaction)?;
                        for internal_table_id in internal_table_ids {
                            let internal_table =
                                Table::select(self.env.meta_store(), &internal_table_id).await?;
                            if let Some(internal_table) = internal_table {
                                users_need_update.append(&mut Self::release_privileges(
                                    user_core.list_users(),
                                    &Object::TableId(internal_table.id),
                                    &mut transaction,
                                )?);
                                internal_table.delete_in_transaction(&mut transaction)?;
                                tables_to_drop.push(internal_table);
                            }
                        }
                        self.env.meta_store().txn(transaction).await?;

                        database_core.drop_index(&index);
                        database_core.drop_table(&table);
                        for user in users_need_update {
                            user_core.insert_user_info(user.id, user.clone());
                            self.notify_frontend(Operation::Update, Info::User(user))
                                .await;
                        }
                        for table in tables_to_drop {
                            database_core.drop_table(&table);
                            self.notify_frontend(Operation::Delete, Info::Table(table))
                                .await;
                        }
                        for &dependent_relation_id in &table.dependent_relations {
                            database_core.decrease_ref_count(dependent_relation_id);
                        }
                        self.notify_frontend(Operation::Delete, Info::Index(index.to_owned()))
                            .await;

                        let version = self
                            .notify_frontend(Operation::Delete, Info::Table(table.to_owned()))
                            .await;

                        Ok(version)
                    }
                }
            } else {
                bail!("index table doesn't exist",)
            }
        } else {
            bail!("index doesn't exist",)
        }
    }

    pub async fn start_create_source_procedure(&self, source: &Source) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            Ok(())
        } else {
            bail!("source already exists or in creating procedure");
        }
    }

    pub async fn finish_create_source_procedure(
        &self,
        source: &Source,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            source.insert(self.env.meta_store()).await?;
            core.add_source(source);

            let version = self
                .notify_frontend(Operation::Add, Info::Source(source.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("source already exist or not in creating procedure");
        }
    }

    pub async fn cancel_create_source_procedure(&self, source: &Source) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            Ok(())
        } else {
            bail!("source already exist or not in creating procedure");
        }
    }

    pub async fn drop_source(&self, source_id: SourceId) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let source = Source::select(self.env.meta_store(), &source_id).await?;
        if let Some(source) = source {
            match database_core.get_ref_count(source_id) {
                Some(ref_count) => Err(MetaError::permission_denied(format!(
                    "Fail to delete source `{}` because {} other relation(s) depend on it",
                    source.name, ref_count
                ))),
                None => {
                    let mut transaction = Transaction::default();
                    let users_need_update = Self::release_privileges(
                        user_core.list_users(),
                        &Object::SourceId(source_id),
                        &mut transaction,
                    )?;
                    source.delete_in_transaction(&mut transaction)?;
                    self.env.meta_store().txn(transaction).await?;

                    database_core.drop_source(&source);

                    for user in users_need_update {
                        user_core.insert_user_info(user.id, user.clone());
                        self.notify_frontend(Operation::Update, Info::User(user))
                            .await;
                    }
                    let version = self
                        .notify_frontend(Operation::Delete, Info::Source(source))
                        .await;

                    Ok(version)
                }
            }
        } else {
            bail!("source doesn't exist");
        }
    }

    pub async fn start_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
    ) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let source_key = (source.database_id, source.schema_id, source.name.clone());
        let mview_key = (mview.database_id, mview.schema_id, mview.name.clone());
        if !core.has_source(source)
            && !core.has_table(mview)
            && !core.has_in_progress_creation(&source_key)
            && !core.has_in_progress_creation(&mview_key)
        {
            core.mark_creating(&source_key);
            core.mark_creating(&mview_key);
            ensure!(mview.dependent_relations.is_empty());
            Ok(())
        } else {
            bail!("source or table already exist");
        }
    }

    pub async fn finish_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
        tables: Vec<Table>,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let source_key = (source.database_id, source.schema_id, source.name.clone());
        let mview_key = (mview.database_id, mview.schema_id, mview.name.clone());
        if !core.has_source(source)
            && !core.has_table(mview)
            && core.has_in_progress_creation(&source_key)
            && core.has_in_progress_creation(&mview_key)
        {
            core.unmark_creating(&source_key);
            core.unmark_creating(&mview_key);

            let mut transaction = Transaction::default();
            source.upsert_in_transaction(&mut transaction)?;
            mview.upsert_in_transaction(&mut transaction)?;
            for table in &tables {
                table.upsert_in_transaction(&mut transaction)?;
            }
            self.env.meta_store().txn(transaction).await?;

            core.add_source(source);
            core.add_table(mview);

            for table in tables {
                core.add_table(&table);
                self.notify_frontend(Operation::Add, Info::Table(table.to_owned()))
                    .await;
            }
            self.notify_frontend(Operation::Add, Info::Table(mview.to_owned()))
                .await;

            // Currently frontend uses source's version
            let version = self
                .notify_frontend(Operation::Add, Info::Source(source.to_owned()))
                .await;
            Ok(version)
        } else {
            bail!("source already exist or not in creating procedure");
        }
    }

    pub async fn cancel_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
    ) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let source_key = (source.database_id, source.schema_id, source.name.clone());
        let mview_key = (mview.database_id, mview.schema_id, mview.name.clone());
        if !core.has_source(source)
            && !core.has_table(mview)
            && core.has_in_progress_creation(&source_key)
            && core.has_in_progress_creation(&mview_key)
        {
            core.unmark_creating(&source_key);
            core.unmark_creating(&mview_key);
            Ok(())
        } else {
            bail!("source already exist or not in creating procedure");
        }
    }

    pub async fn drop_materialized_source(
        &self,
        source_id: SourceId,
        mview_id: TableId,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mview = Table::select(self.env.meta_store(), &mview_id).await?;
        let source = Source::select(self.env.meta_store(), &source_id).await?;
        match (mview, source) {
            (Some(mview), Some(source)) => {
                if let Some(OptionalAssociatedSourceId::AssociatedSourceId(associated_source_id)) =
                    mview.optional_associated_source_id
                {
                    if associated_source_id != source_id {
                        bail!("mview's associated source id doesn't match source id");
                    }
                } else {
                    bail!("mview do not have associated source id");
                }
                // check ref count
                if let Some(ref_count) = database_core.get_ref_count(mview_id) {
                    return Err(MetaError::permission_denied(format!(
                        "Fail to delete table `{}` because {} other relation(s) depend on it",
                        mview.name, ref_count
                    )));
                }
                if let Some(ref_count) = database_core.get_ref_count(source_id) {
                    return Err(MetaError::permission_denied(format!(
                        "Fail to delete source `{}` because {} other relation(s) depend on it",
                        source.name, ref_count
                    )));
                }

                // now is safe to delete both mview and source
                let mut transaction = Transaction::default();
                let mut users_need_update = Self::release_privileges(
                    user_core.list_users(),
                    &Object::TableId(mview_id),
                    &mut transaction,
                )?;
                users_need_update.append(&mut Self::release_privileges(
                    user_core.list_users(),
                    &Object::SourceId(source_id),
                    &mut transaction,
                )?);
                mview.delete_in_transaction(&mut transaction)?;
                source.delete_in_transaction(&mut transaction)?;
                self.env.meta_store().txn(transaction).await?;
                database_core.drop_table(&mview);
                database_core.drop_source(&source);
                for &dependent_relation_id in &mview.dependent_relations {
                    database_core.decrease_ref_count(dependent_relation_id);
                }

                for user in users_need_update {
                    user_core.insert_user_info(user.id, user.clone());
                    self.notify_frontend(Operation::Update, Info::User(user))
                        .await;
                }
                self.notify_frontend(Operation::Delete, Info::Table(mview.to_owned()))
                    .await;

                let version = self
                    .notify_frontend(Operation::Delete, Info::Source(source))
                    .await;

                Ok(version)
            }

            _ => bail!("table or source doesn't exist"),
        }
    }

    pub async fn start_create_index_procedure(
        &self,
        index: &Index,
        index_table: &Table,
    ) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (index.database_id, index.schema_id, index.name.clone());
        if !core.has_index(index) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            for &dependent_relation_id in &index_table.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("index already exists or in creating procedure".to_string(),)
        }
    }

    pub async fn cancel_create_index_procedure(
        &self,
        index: &Index,
        index_table: &Table,
    ) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (index.database_id, index.schema_id, index.name.clone());
        if !core.has_index(index) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            for &dependent_relation_id in &index_table.dependent_relations {
                core.decrease_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("index already exist or not in creating procedure",)
        }
    }

    pub async fn finish_create_index_procedure(
        &self,
        index: &Index,
        internal_tables: Vec<Table>,
        table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let key = (table.database_id, table.schema_id, index.name.clone());
        if !core.has_index(index) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            let mut transaction = Transaction::default();

            index.upsert_in_transaction(&mut transaction)?;

            for table in &internal_tables {
                table.upsert_in_transaction(&mut transaction)?;
            }
            table.upsert_in_transaction(&mut transaction)?;
            self.env.meta_store().txn(transaction).await?;

            for internal_table in internal_tables {
                core.add_table(&internal_table);

                self.notify_frontend(Operation::Add, Info::Table(internal_table.to_owned()))
                    .await;
            }
            core.add_table(table);
            core.add_index(index);

            self.notify_frontend(Operation::Add, Info::Table(table.to_owned()))
                .await;

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Add, Info::Index(index.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("table already exist or not in creating procedure",)
        }
    }

    pub async fn start_create_sink_procedure(&self, sink: &Sink) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        if !core.has_sink(sink) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            for &dependent_relation_id in &sink.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            bail!("sink already exists or in creating procedure");
        }
    }

    pub async fn finish_create_sink_procedure(
        &self,
        sink: &Sink,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        if !core.has_sink(sink) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            sink.insert(self.env.meta_store()).await?;
            core.add_sink(sink);

            let version = self
                .notify_frontend(Operation::Add, Info::Sink(sink.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("sink already exist or not in creating procedure");
        }
    }

    pub async fn cancel_create_sink_procedure(&self, sink: &Sink) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        if !core.has_sink(sink) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            Ok(())
        } else {
            bail!("sink already exist or not in creating procedure");
        }
    }

    pub async fn create_sink(&self, sink: &Sink) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        if !core.has_sink(sink) {
            sink.insert(self.env.meta_store()).await?;
            core.add_sink(sink);

            let version = self
                .notify_frontend(Operation::Add, Info::Sink(sink.to_owned()))
                .await;

            Ok(version)
        } else {
            bail!("sink already exists");
        }
    }

    pub async fn drop_sink(&self, sink_id: SinkId) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let sink = Sink::select(self.env.meta_store(), &sink_id).await?;
        if let Some(sink) = sink {
            Sink::delete(self.env.meta_store(), &sink_id).await?;
            core.drop_sink(&sink);
            for &dependent_relation_id in &sink.dependent_relations {
                core.decrease_ref_count(dependent_relation_id);
            }

            let version = self
                .notify_frontend(Operation::Delete, Info::Sink(sink))
                .await;

            Ok(version)
        } else {
            bail!("sink doesn't exist");
        }
    }

    pub async fn list_tables(&self, schema_id: SchemaId) -> MetaResult<Vec<TableId>> {
        let _core = &self.core.lock().await.user;
        let tables = Table::list(self.env.meta_store()).await?;
        Ok(tables
            .iter()
            .filter(|t| t.schema_id == schema_id)
            .map(|t| t.id)
            .collect())
    }

    pub async fn list_sources(&self) -> MetaResult<Vec<Source>> {
        self.core.lock().await.database.list_sources().await
    }

    pub async fn list_source_ids(&self, schema_id: SchemaId) -> MetaResult<Vec<SourceId>> {
        self.core
            .lock()
            .await
            .database
            .list_source_ids(schema_id)
            .await
    }

    pub async fn list_stream_job_ids(&self) -> MetaResult<HashSet<RelationId>> {
        self.core
            .lock()
            .await
            .database
            .list_stream_job_ids()
            .await
            .map(|iter| iter.collect())
    }

    async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }
}

impl<S> CatalogManager<S>
where
    S: MetaStore,
{
    async fn init_user(&self) -> MetaResult<()> {
        let core = &mut self.core.lock().await.user;
        for (user, id) in [
            (DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID),
            (DEFAULT_SUPER_USER_FOR_PG, DEFAULT_SUPER_USER_FOR_PG_ID),
        ] {
            if !core.has_user_name(user) {
                let default_user = UserInfo {
                    id,
                    name: user.to_string(),
                    is_super: true,
                    can_create_db: true,
                    can_create_user: true,
                    can_login: true,
                    ..Default::default()
                };

                default_user.insert(self.env.meta_store()).await?;
                core.create_user(default_user);
            }
        }

        Ok(())
    }

    pub async fn list_users(&self) -> Vec<UserInfo> {
        self.core.lock().await.user.list_users()
    }

    pub async fn create_user(&self, user: &UserInfo) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.user;
        if core.has_user_name(&user.name) {
            return Err(MetaError::permission_denied(format!(
                "User {} already exists",
                user.name
            )));
        }
        user.insert(self.env.meta_store()).await?;
        core.create_user(user.clone());

        let version = self
            .env
            .notification_manager()
            .notify_frontend(Operation::Add, Info::User(user.to_owned()))
            .await;
        Ok(version)
    }

    pub async fn update_user(
        &self,
        user: &UserInfo,
        update_fields: &[UpdateField],
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.user;
        let rename_flag = update_fields
            .iter()
            .any(|&field| field == UpdateField::Rename);
        if rename_flag && core.has_user_name(&user.name) {
            return Err(MetaError::permission_denied(format!(
                "User {} already exists",
                user.name
            )));
        }
        user.insert(self.env.meta_store()).await?;
        let new_user = core.update_user(user, update_fields);

        let version = self
            .env
            .notification_manager()
            .notify_frontend(Operation::Update, Info::User(new_user))
            .await;
        Ok(version)
    }

    pub async fn get_user(&self, id: UserId) -> MetaResult<UserInfo> {
        let core = &self.core.lock().await.user;

        core.get_user_info(&id)
            .ok_or_else(|| anyhow!("User {} not found", id).into())
    }

    pub async fn drop_user(&self, id: UserId) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.user;
        if !core.has_user_id(&id) {
            bail!("User {} not found", id);
        }
        let user = core.get_user_info(&id).unwrap();

        if user.name == DEFAULT_SUPER_USER || user.name == DEFAULT_SUPER_USER_FOR_PG {
            return Err(MetaError::permission_denied(format!(
                "Cannot drop default super user {}",
                id
            )));
        }
        if !core.get_user_info(&id).unwrap().grant_privileges.is_empty() {
            return Err(MetaError::permission_denied(format!(
                "Cannot drop user {} with privileges",
                id
            )));
        }
        if core
            .get_user_grant_relation(&id)
            .is_some_and(|set| !set.is_empty())
        {
            return Err(MetaError::permission_denied(format!(
                "Cannot drop user {} with privileges granted to others",
                id
            )));
        }
        UserInfo::delete(self.env.meta_store(), &id).await?;
        core.drop_user(id);

        let version = self
            .env
            .notification_manager()
            .notify_frontend(Operation::Delete, Info::User(user))
            .await;
        Ok(version)
    }

    // Defines privilege grant for a user.

    // Merge new granted privilege.
    #[inline(always)]
    fn merge_privilege(origin_privilege: &mut GrantPrivilege, new_privilege: &GrantPrivilege) {
        assert_eq!(origin_privilege.object, new_privilege.object);

        let mut action_map = HashMap::<i32, (bool, u32)>::from_iter(
            origin_privilege
                .action_with_opts
                .iter()
                .map(|ao| (ao.action, (ao.with_grant_option, ao.granted_by))),
        );
        for nao in &new_privilege.action_with_opts {
            if let Some(o) = action_map.get_mut(&nao.action) {
                o.0 |= nao.with_grant_option;
            } else {
                action_map.insert(nao.action, (nao.with_grant_option, nao.granted_by));
            }
        }
        origin_privilege.action_with_opts = action_map
            .into_iter()
            .map(
                |(action, (with_grant_option, granted_by))| ActionWithGrantOption {
                    action,
                    with_grant_option,
                    granted_by,
                },
            )
            .collect();
    }

    // Check whether new_privilege is a subset of origin_privilege, and check grand_option if
    // `need_grand_option` is set.
    #[inline(always)]
    fn check_privilege(
        origin_privilege: &GrantPrivilege,
        new_privilege: &GrantPrivilege,
        need_grand_option: bool,
    ) -> bool {
        assert_eq!(origin_privilege.object, new_privilege.object);

        let action_map = HashMap::<i32, bool>::from_iter(
            origin_privilege
                .action_with_opts
                .iter()
                .map(|ao| (ao.action, ao.with_grant_option)),
        );
        for nao in &new_privilege.action_with_opts {
            if let Some(with_grant_option) = action_map.get(&nao.action) {
                if !with_grant_option && need_grand_option {
                    return false;
                }
            } else {
                return false;
            }
        }
        true
    }

    pub async fn grant_privilege(
        &self,
        users: &[UserId],
        new_grant_privileges: &[GrantPrivilege],
        grantor: UserId,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.user;
        let mut transaction = Transaction::default();
        let mut user_updated = Vec::with_capacity(users.len());
        let grantor_info = core
            .get_user_info(&grantor)
            .ok_or_else(|| anyhow!("User {} does not exist", &grantor))?;
        for user_id in users {
            let mut user = core
                .get_user_info(user_id)
                .ok_or_else(|| anyhow!("User {} does not exist", user_id))?;

            let grant_user = core
                .get_user_grant_relation_entry(grantor)
                .or_insert_with(HashSet::new);

            if user.is_super {
                return Err(MetaError::permission_denied(format!(
                    "Cannot grant privilege to super user {}",
                    user_id
                )));
            }
            if !grantor_info.is_super {
                for new_grant_privilege in new_grant_privileges {
                    if let Some(privilege) = grantor_info
                        .grant_privileges
                        .iter()
                        .find(|p| p.object == new_grant_privilege.object)
                    {
                        if !Self::check_privilege(privilege, new_grant_privilege, true) {
                            return Err(MetaError::permission_denied(format!(
                                "Cannot grant privilege without grant permission for user {}",
                                grantor
                            )));
                        }
                    } else {
                        return Err(MetaError::permission_denied(format!(
                            "Grantor {} does not have one of the privileges",
                            grantor
                        )));
                    }
                }
            }
            grant_user.insert(*user_id);
            new_grant_privileges.iter().for_each(|new_grant_privilege| {
                if let Some(privilege) = user
                    .grant_privileges
                    .iter_mut()
                    .find(|p| p.object == new_grant_privilege.object)
                {
                    Self::merge_privilege(privilege, new_grant_privilege);
                } else {
                    user.grant_privileges.push(new_grant_privilege.clone());
                }
            });
            user.upsert_in_transaction(&mut transaction)?;
            user_updated.push(user);
        }

        self.env.meta_store().txn(transaction).await?;
        let mut version = 0;
        for user in user_updated {
            core.insert_user_info(user.id, user.clone());
            version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Update, Info::User(user))
                .await;
        }

        Ok(version)
    }

    // Revoke privilege from object.
    #[inline(always)]
    fn revoke_privilege_inner(
        origin_privilege: &mut GrantPrivilege,
        revoke_grant_privilege: &GrantPrivilege,
        revoke_grant_option: bool,
    ) -> bool {
        assert_eq!(origin_privilege.object, revoke_grant_privilege.object);
        let mut has_change = false;
        if revoke_grant_option {
            // Only revoke with grant option.
            origin_privilege.action_with_opts.iter_mut().for_each(|ao| {
                if revoke_grant_privilege
                    .action_with_opts
                    .iter()
                    .any(|ro| ro.action == ao.action)
                {
                    ao.with_grant_option = false;
                    has_change = true;
                }
            })
        } else {
            let sz = origin_privilege.action_with_opts.len();
            // Revoke all privileges matched with revoke_grant_privilege.
            origin_privilege.action_with_opts.retain(|ao| {
                !revoke_grant_privilege
                    .action_with_opts
                    .iter()
                    .any(|rao| rao.action == ao.action)
            });
            has_change = sz != origin_privilege.action_with_opts.len();
        }
        has_change
    }

    pub async fn revoke_privilege(
        &self,
        users: &[UserId],
        revoke_grant_privileges: &[GrantPrivilege],
        granted_by: UserId,
        revoke_by: UserId,
        revoke_grant_option: bool,
        cascade: bool,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.user;
        let mut transaction = Transaction::default();
        let mut user_updated = HashMap::new();
        let mut users_info: VecDeque<UserInfo> = VecDeque::new();
        let mut visited = HashSet::new();
        // check revoke permission
        let revoke_by = core
            .get_user_info(&revoke_by)
            .ok_or_else(|| anyhow!("User {} does not exist", &revoke_by))?;
        let same_user = granted_by == revoke_by.id;
        if !revoke_by.is_super {
            for privilege in revoke_grant_privileges {
                if let Some(user_privilege) = revoke_by
                    .grant_privileges
                    .iter()
                    .find(|p| p.object == privilege.object)
                {
                    if !Self::check_privilege(user_privilege, privilege, same_user) {
                        return Err(MetaError::permission_denied(format!(
                            "Cannot revoke privilege without permission for user {}",
                            &revoke_by.name
                        )));
                    }
                } else {
                    return Err(MetaError::permission_denied(format!(
                        "User {} does not have one of the privileges",
                        &revoke_by.name
                    )));
                }
            }
        }
        // revoke privileges
        for user_id in users {
            let user = core
                .get_user_info(user_id)
                .ok_or_else(|| anyhow!("User {} does not exist", user_id))?;
            if user.is_super {
                return Err(MetaError::permission_denied(format!(
                    "Cannot revoke privilege from supper user {}",
                    user_id
                )));
            }
            users_info.push_back(user);
        }
        while !users_info.is_empty() {
            let mut now_user = users_info.pop_front().unwrap();
            let now_relations = core
                .get_user_grant_relation(&now_user.id)
                .cloned()
                .unwrap_or_default();
            let mut recursive_flag = false;
            let mut empty_privilege = false;
            let grant_option_now = revoke_grant_option && users.contains(&now_user.id);
            visited.insert(now_user.id);
            revoke_grant_privileges
                .iter()
                .for_each(|revoke_grant_privilege| {
                    for privilege in &mut now_user.grant_privileges {
                        if privilege.object == revoke_grant_privilege.object {
                            recursive_flag |= Self::revoke_privilege_inner(
                                privilege,
                                revoke_grant_privilege,
                                grant_option_now,
                            );
                            empty_privilege |= privilege.action_with_opts.is_empty();
                            break;
                        }
                    }
                });
            if recursive_flag {
                // check with cascade/restrict strategy
                if !cascade && !users.contains(&now_user.id) {
                    return Err(MetaError::permission_denied(format!(
                        "Cannot revoke privilege from user {} for restrict",
                        &now_user.name
                    )));
                }
                for next_user_id in now_relations {
                    if core.has_user_id(&next_user_id) && !visited.contains(&next_user_id) {
                        users_info.push_back(core.get_user_info(&next_user_id).unwrap());
                    }
                }
                if empty_privilege {
                    now_user
                        .grant_privileges
                        .retain(|privilege| !privilege.action_with_opts.is_empty());
                }
                if let std::collections::hash_map::Entry::Vacant(e) =
                    user_updated.entry(now_user.id)
                {
                    now_user.upsert_in_transaction(&mut transaction)?;
                    e.insert(now_user);
                }
            }
        }

        self.env.meta_store().txn(transaction).await?;
        let mut version = 0;
        for (user_id, user_info) in user_updated {
            core.insert_user_info(user_id, user_info.clone());
            version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Update, Info::User(user_info))
                .await;
        }

        Ok(version)
    }

    /// `release_privileges` removes the privileges with given object from given users, it will be
    /// called when a database/schema/table/source is dropped.
    #[inline(always)]
    fn release_privileges(
        users: Vec<UserInfo>,
        object: &Object,
        txn: &mut Transaction,
    ) -> MetaResult<Vec<UserInfo>> {
        let mut users_need_update = vec![];
        for mut user in users {
            let cnt = user.grant_privileges.len();
            user.grant_privileges
                .retain(|p| p.object.as_ref().unwrap() != object);
            if cnt != user.grant_privileges.len() {
                user.upsert_in_transaction(txn)?;
                users_need_update.push(user);
            }
        }
        Ok(users_need_update)
    }
}
