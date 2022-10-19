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
    valid_table_name, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, DEFAULT_SUPER_USER,
    DEFAULT_SUPER_USER_FOR_PG, DEFAULT_SUPER_USER_FOR_PG_ID, DEFAULT_SUPER_USER_ID,
    PG_CATALOG_SCHEMA_NAME,
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

use crate::manager::{IdCategory, MetaSrvEnv, NotificationVersion, StreamingJob, StreamingJobId};
use crate::model::{BTreeMapTransaction, MetadataModel, ValTransaction};
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
        let user = UserManager::new(env).await?;
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

    pub async fn get_catalog_core_guard(&self) -> MutexGuard<'_, CatalogManagerCore<S>> {
        self.core.lock().await
    }
}

macro_rules! commit_meta {
    ($meta_store:expr, $($val_txn:expr),*) => {
        {
            async {
                let mut trx = Transaction::default();
                // Apply the change in `ValTransaction` to trx
                $(
                    $val_txn.apply_to_txn(&mut trx)?;
                )*
                // Commit to state store
                $meta_store.txn(trx).await?;
                // Upon successful commit, commit the change to local in-mem state
                $(
                    $val_txn.commit();
                )*
                MetaResult::Ok(())
            }.await
        }
    };
}
pub(crate) use commit_meta;

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
        if !self
            .core
            .lock()
            .await
            .database
            .has_database_key(&database.name)
        {
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
        let mut databases = BTreeMapTransaction::new(&mut core.databases);
        let mut schemas = BTreeMapTransaction::new(&mut core.schemas);
        if !databases.contains_key(&database.id) {
            databases.insert(database.id, database.clone());
            let mut schemas_added = vec![];
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
                schemas.insert(schema.id, schema.clone());
                schemas_added.push(schema);
            }

            commit_meta!(self.env.meta_store(), databases, schemas)?;

            let mut version = self
                .notify_frontend(Operation::Add, Info::Database(database.to_owned()))
                .await;
            for schema in schemas_added {
                version = self
                    .env
                    .notification_manager()
                    .notify_frontend(Operation::Add, Info::Schema(schema))
                    .await;
            }

            Ok(version)
        } else {
            Err(MetaError::catalog_duplicated("database", &database.name))
        }
    }

    /// return id of streaming jobs in the database which need to be dropped in
    /// `StreamingJobBackgroundDeleter`.
    pub async fn drop_database(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<(NotificationVersion, Vec<StreamingJobId>)> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mut databases = BTreeMapTransaction::new(&mut database_core.databases);
        let mut schemas = BTreeMapTransaction::new(&mut database_core.schemas);
        let mut sources = BTreeMapTransaction::new(&mut database_core.sources);
        let mut sinks = BTreeMapTransaction::new(&mut database_core.sinks);
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut indexes = BTreeMapTransaction::new(&mut database_core.indexes);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let database = databases.remove(database_id);
        if let Some(database) = database {
            let schema_ids = schemas.tree_ref().keys().copied().collect_vec();
            let mut schemas_to_drop = vec![];
            for schema_id in &schema_ids {
                if database_id == schemas.get(schema_id).unwrap().database_id {
                    schemas_to_drop.push(schemas.remove(*schema_id).unwrap());
                }
            }

            let source_ids = sources.tree_ref().keys().copied().collect_vec();
            let mut sources_to_drop = vec![];
            for source_id in &source_ids {
                if database_id == sources.get(source_id).unwrap().database_id {
                    sources_to_drop.push(sources.remove(*source_id).unwrap());
                }
            }
            let sink_ids = sinks.tree_ref().keys().copied().collect_vec();
            let mut sinks_to_drop = vec![];
            for sink_in in &sink_ids {
                if database_id == sinks.get(sink_in).unwrap().database_id {
                    sinks_to_drop.push(sinks.remove(*sink_in).unwrap());
                }
            }

            let table_ids = tables.tree_ref().keys().copied().collect_vec();
            let mut tables_to_drop = vec![];
            for table_id in &table_ids {
                if database_id == tables.get(table_id).unwrap().database_id {
                    tables_to_drop.push(tables.remove(*table_id).unwrap());
                }
            }

            let index_ids = indexes.tree_ref().keys().copied().collect_vec();
            let mut indexes_to_drop = vec![];
            for index_id in &index_ids {
                if database_id == indexes.get(index_id).unwrap().database_id {
                    indexes_to_drop.push(indexes.remove(*index_id).unwrap());
                }
            }

            let mut objects = Vec::with_capacity(
                1 + schemas_to_drop.len() + tables_to_drop.len() + sources_to_drop.len(),
            );
            objects.push(Object::DatabaseId(database.id));
            objects.extend(
                schemas_to_drop
                    .iter()
                    .map(|schema| Object::SchemaId(schema.id)),
            );
            objects.extend(tables_to_drop.iter().map(|table| Object::TableId(table.id)));
            objects.extend(
                sources_to_drop
                    .iter()
                    .map(|source| Object::SourceId(source.id)),
            );
            // FIXME: Is sink miss here?

            let users_need_update = Self::update_user_privileges(&mut users, &objects);

            commit_meta!(
                self.env.meta_store(),
                databases,
                schemas,
                sources,
                sinks,
                tables,
                indexes,
                users
            )?;

            database_core
                .relation_ref_count
                .retain(|k, _| (!table_ids.contains(k)) && (!source_ids.contains(k)));

            for user in users_need_update {
                self.notify_frontend(Operation::Update, Info::User(user))
                    .await;
            }

            // Frontend will drop cache of schema and table in the database.
            let version = self
                .notify_frontend(Operation::Delete, Info::Database(database))
                .await;

            // prepare catalog sent to catalog background deleter.
            let valid_tables = tables_to_drop
                .into_iter()
                .filter(|table| valid_table_name(&table.name))
                .collect_vec();

            let mut catalog_deleted_ids =
                Vec::with_capacity(valid_tables.len() + source_ids.len() + sinks_to_drop.len());
            catalog_deleted_ids.extend(
                valid_tables
                    .into_iter()
                    .map(|table| StreamingJobId::Table(table.id.into())),
            );
            catalog_deleted_ids.extend(source_ids.into_iter().map(StreamingJobId::Source));
            catalog_deleted_ids.extend(
                sinks_to_drop
                    .into_iter()
                    .map(|sink| StreamingJobId::Sink(sink.id.into())),
            );

            Ok((version, catalog_deleted_ids))
        } else {
            bail!("database doesn't exist");
        }
    }

    pub async fn create_schema(&self, schema: &Schema) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let mut schemas = BTreeMapTransaction::new(&mut core.schemas);
        if !schemas.contains_key(&schema.id) {
            schemas.insert(schema.id, schema.clone());
            commit_meta!(self.env.meta_store(), schemas)?;

            let version = self
                .notify_frontend(Operation::Add, Info::Schema(schema.to_owned()))
                .await;

            Ok(version)
        } else {
            Err(MetaError::catalog_duplicated("schema", &schema.name))
        }
    }

    pub async fn drop_schema(&self, schema_id: SchemaId) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mut schemas = BTreeMapTransaction::new(&mut database_core.schemas);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);
        let schema = schemas.remove(schema_id);
        if let Some(schema) = schema {
            if database_core
                .tables
                .values()
                .any(|t| t.database_id == schema.database_id && t.schema_id == schema_id)
            {
                bail!("schema is not empty!");
            }

            let users_need_update =
                Self::update_user_privileges(&mut users, &[Object::SchemaId(schema_id)]);

            commit_meta!(self.env.meta_store(), schemas, users)?;

            for user in users_need_update {
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
            self.notify_hummock_and_compactor(Operation::Add, Info::Table(table.to_owned()))
                .await;
        }
    }

    pub async fn unmark_creating_tables(&self, creating_table_ids: &[TableId], need_notify: bool) {
        let core = &mut self.core.lock().await.database;
        core.unmark_creating_tables(creating_table_ids);
        if need_notify {
            for table_id in creating_table_ids {
                self.notify_hummock_and_compactor(
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

    async fn notify_hummock_and_compactor(&self, operation: Operation, info: Info) {
        self.env
            .notification_manager()
            .notify_hummock(operation, info.clone())
            .await;

        self.env
            .notification_manager()
            .notify_compactor(operation, info)
            .await;
    }

    pub async fn start_create_table_procedure(&self, table: &Table) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (table.database_id, table.schema_id, table.name.clone());

        core.check_relation_name_duplicated(&(
            table.database_id,
            table.schema_id,
            table.name.clone(),
        ))?;
        if core.has_in_progress_creation(&key) {
            bail!("table is in creating procedure");
        } else {
            core.mark_creating(&key);
            core.mark_creating_streaming_job(table.id);
            for &dependent_relation_id in &table.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        }
    }

    pub async fn finish_create_table_procedure(
        &self,
        internal_tables: Vec<Table>,
        table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let mut tables = BTreeMapTransaction::new(&mut core.tables);
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !tables.contains_key(&table.id) && core.in_progress_creation_tracker.contains(&key) {
            core.in_progress_creation_tracker.remove(&key);
            core.in_progress_creation_streaming_job.remove(&table.id);

            tables.insert(table.id, table.clone());
            for table in &internal_tables {
                tables.insert(table.id, table.clone());
            }
            commit_meta!(self.env.meta_store(), tables)?;

            for internal_table in internal_tables {
                self.notify_frontend(Operation::Add, Info::Table(internal_table))
                    .await;
            }

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
        if !core.tables.contains_key(&table.id) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            core.unmark_creating_streaming_job(table.id);
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
        index_and_table_ids: Vec<(IndexId, TableId)>,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mut indexes = BTreeMapTransaction::new(&mut database_core.indexes);
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let table = tables.remove(table_id);
        if let Some(table) = table {
            if let Some(ref_count) = database_core.relation_ref_count.get(&table_id).cloned() {
                if ref_count > index_and_table_ids.len() {
                    return Err(MetaError::permission_denied(format!(
                        "Fail to delete table `{}` because {} other relation(s) depend on it",
                        table.name, ref_count
                    )));
                }
            }

            let dependent_relations = table.dependent_relations.clone();

            let mut indexes_post_work_vec = vec![];
            // Delete indexes
            for (index_id, index_table_id) in index_and_table_ids {
                let index = indexes.remove(index_id);
                if let Some(index) = index {
                    assert_eq!(index_table_id, index.index_table_id);

                    // drop index table
                    let table = tables.remove(index_table_id);
                    if let Some(table) = table {
                        match database_core.relation_ref_count.get(&index_table_id) {
                            Some(ref_count) => return Err(MetaError::permission_denied(format!(
                                "Fail to delete table `{}` because {} other relation(s) depend on it",
                                table.name, ref_count
                            ))),
                            None => {
                                let dependent_relations = table.dependent_relations.clone();
                                indexes_post_work_vec.push((index, table, dependent_relations));
                            }
                        }
                    } else {
                        bail!("index table doesn't exist",)
                    }
                } else {
                    bail!("index doesn't exist",)
                }
            }

            let mut tables_to_drop = internal_table_ids
                .into_iter()
                .map(|id| {
                    tables
                        .remove(id)
                        .ok_or_else(|| MetaError::catalog_not_found("table", id.to_string()))
                })
                .collect::<MetaResult<Vec<Table>>>()?;
            tables_to_drop.push(table);

            let objects = tables_to_drop
                .iter()
                .map(|table| Object::TableId(table.id))
                .chain(
                    indexes_post_work_vec
                        .iter()
                        .map(|(_, table, _)| Object::TableId(table.id)),
                )
                .collect_vec();
            let users_need_update = Self::update_user_privileges(&mut users, &objects);

            commit_meta!(self.env.meta_store(), tables, indexes, users)?;

            for (index, table, dependent_relations) in indexes_post_work_vec {
                self.notify_frontend(Operation::Delete, Info::Table(table))
                    .await;

                for dependent_relation_id in dependent_relations {
                    database_core.decrease_ref_count(dependent_relation_id);
                }

                self.notify_frontend(Operation::Delete, Info::Index(index))
                    .await;
            }

            for user in users_need_update {
                self.notify_frontend(Operation::Update, Info::User(user))
                    .await;
            }

            let mut version = NotificationVersion::default();
            for table in tables_to_drop {
                version = self
                    .notify_frontend(Operation::Delete, Info::Table(table))
                    .await;
            }
            for dependent_relation_id in dependent_relations {
                database_core.decrease_ref_count(dependent_relation_id);
            }

            Ok(version)
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
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mut indexes = BTreeMapTransaction::new(&mut database_core.indexes);
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let index = indexes.remove(index_id);
        if let Some(index) = index {
            assert_eq!(index_table_id, index.index_table_id);

            // drop index table
            let table = tables.remove(index_table_id);
            if let Some(table) = table {
                match database_core
                    .relation_ref_count
                    .get(&index_table_id)
                    .cloned()
                {
                    Some(ref_count) => Err(MetaError::permission_denied(format!(
                        "Fail to delete table `{}` because {} other relation(s) depend on it",
                        table.name, ref_count
                    ))),
                    None => {
                        let dependent_relations = table.dependent_relations.clone();

                        let objects = &[Object::TableId(table.id)];

                        let users_need_update = Self::update_user_privileges(&mut users, objects);

                        commit_meta!(self.env.meta_store(), tables, indexes, users)?;

                        for user in users_need_update {
                            self.notify_frontend(Operation::Update, Info::User(user))
                                .await;
                        }

                        self.notify_frontend(Operation::Delete, Info::Table(table))
                            .await;

                        for dependent_relation_id in dependent_relations {
                            database_core.decrease_ref_count(dependent_relation_id);
                        }

                        let version = self
                            .notify_frontend(Operation::Delete, Info::Index(index))
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

        core.check_relation_name_duplicated(&(
            source.database_id,
            source.schema_id,
            source.name.clone(),
        ))?;
        if core.has_in_progress_creation(&key) {
            bail!("table is in creating procedure");
        } else {
            core.mark_creating(&key);
            Ok(())
        }
    }

    pub async fn finish_create_source_procedure(
        &self,
        source: &Source,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let mut sources = BTreeMapTransaction::new(&mut core.sources);
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !sources.contains_key(&source.id) && core.in_progress_creation_tracker.contains(&key) {
            core.in_progress_creation_tracker.remove(&key);
            sources.insert(source.id, source.clone());

            commit_meta!(self.env.meta_store(), sources)?;

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
        if !core.sources.contains_key(&source.id) && core.has_in_progress_creation(&key) {
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
        let mut sources = BTreeMapTransaction::new(&mut database_core.sources);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let source = sources.remove(source_id);
        if let Some(source) = source {
            match database_core.relation_ref_count.get(&source_id) {
                Some(ref_count) => Err(MetaError::permission_denied(format!(
                    "Fail to delete source `{}` because {} other relation(s) depend on it",
                    source.name, ref_count
                ))),
                None => {
                    let users_need_update =
                        Self::update_user_privileges(&mut users, &[Object::SourceId(source_id)]);
                    commit_meta!(self.env.meta_store(), sources, users)?;

                    for user in users_need_update {
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
            Err(MetaError::catalog_not_found(
                "source",
                source_id.to_string(),
            ))
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

        core.check_relation_name_duplicated(&(
            source.database_id,
            source.schema_id,
            source.name.clone(),
        ))?;
        if core.has_in_progress_creation(&source_key) || core.has_in_progress_creation(&mview_key) {
            bail!("table or source is in creating procedure");
        } else {
            core.mark_creating(&source_key);
            core.mark_creating(&mview_key);
            core.mark_creating_streaming_job(mview.id);
            ensure!(mview.dependent_relations.is_empty());
            Ok(())
        }
    }

    pub async fn finish_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
        internal_tables: Vec<Table>,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let mut tables = BTreeMapTransaction::new(&mut core.tables);
        let mut sources = BTreeMapTransaction::new(&mut core.sources);

        let source_key = (source.database_id, source.schema_id, source.name.clone());
        let mview_key = (mview.database_id, mview.schema_id, mview.name.clone());
        if !sources.contains_key(&source.id)
            && !tables.contains_key(&mview.id)
            && core.in_progress_creation_tracker.contains(&source_key)
            && core.in_progress_creation_tracker.contains(&mview_key)
        {
            core.in_progress_creation_tracker.remove(&source_key);
            core.in_progress_creation_tracker.remove(&mview_key);
            core.in_progress_creation_streaming_job.remove(&mview.id);

            sources.insert(source.id, source.clone());
            tables.insert(mview.id, mview.clone());
            for table in &internal_tables {
                tables.insert(table.id, table.clone());
            }
            commit_meta!(self.env.meta_store(), sources, tables)?;

            for table in internal_tables {
                self.notify_frontend(Operation::Add, Info::Table(table))
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
        if !core.sources.contains_key(&source.id)
            && !core.tables.contains_key(&mview.id)
            && core.has_in_progress_creation(&source_key)
            && core.has_in_progress_creation(&mview_key)
        {
            core.unmark_creating(&source_key);
            core.unmark_creating(&mview_key);
            core.unmark_creating_streaming_job(mview.id);
            Ok(())
        } else {
            bail!("source already exist or not in creating procedure");
        }
    }

    pub async fn drop_materialized_source(
        &self,
        source_id: SourceId,
        mview_id: TableId,
        internal_table_id: TableId,
        index_and_table_ids: Vec<(IndexId, TableId)>,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;

        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut sources = BTreeMapTransaction::new(&mut database_core.sources);
        let mut indexes = BTreeMapTransaction::new(&mut database_core.indexes);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let mview = tables.remove(mview_id);
        let source = sources.remove(source_id);
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
                if let Some(ref_count) = database_core.relation_ref_count.get(&mview_id).cloned() {
                    // Indexes are dependent on mv. We can drop mv only if its ref_count is strictly
                    // equal to number of indexes.
                    if ref_count > index_and_table_ids.len() {
                        return Err(MetaError::permission_denied(format!(
                            "Fail to delete table `{}` because {} other relation(s) depend on it",
                            mview.name, ref_count
                        )));
                    }
                }
                if let Some(ref_count) = database_core.relation_ref_count.get(&source_id).cloned() {
                    return Err(MetaError::permission_denied(format!(
                        "Fail to delete source `{}` because {} other relation(s) depend on it",
                        source.name, ref_count
                    )));
                }
                let internal_table = tables.get(&internal_table_id).cloned().ok_or_else(|| {
                    MetaError::catalog_not_found("table", internal_table_id.to_string())
                })?;

                // now is safe to delete both mview and source

                let mut indexes_post_work_vec = vec![];
                // Delete indexes
                for (index_id, index_table_id) in index_and_table_ids {
                    let index = indexes.remove(index_id);
                    if let Some(index) = index {
                        // index.delete_in_transaction(&mut transaction)?;
                        assert_eq!(index_table_id, index.index_table_id);

                        // drop index table
                        let table = tables.remove(index_table_id);
                        if let Some(table) = table {
                            match database_core.relation_ref_count.get(&index_table_id).cloned() {
                                Some(ref_count) => return Err(MetaError::permission_denied(format!(
                                    "Fail to delete table `{}` because {} other relation(s) depend on it",
                                    table.name, ref_count
                                ))),
                                None => {
                                    let dependent_relations = table.dependent_relations.clone();
                                    // table.delete_in_transaction(&mut transaction)?;
                                    indexes_post_work_vec.push((index, table, dependent_relations));
                                }
                            }
                        } else {
                            bail!("index table doesn't exist",)
                        }
                    } else {
                        bail!("index doesn't exist",)
                    }
                }

                let objects = [
                    Object::SourceId(source_id),
                    Object::TableId(mview_id),
                    Object::TableId(internal_table_id),
                ]
                .into_iter()
                .chain(
                    indexes_post_work_vec
                        .iter()
                        .map(|(_, table, _)| Object::TableId(table.id)),
                )
                .collect_vec();

                let users_need_update = Self::update_user_privileges(&mut users, &objects);

                tables.remove(mview_id);
                tables.remove(internal_table_id);
                sources.remove(source_id);

                // Commit point
                commit_meta!(self.env.meta_store(), tables, sources, indexes, users)?;

                for (index, table, dependent_relations) in indexes_post_work_vec {
                    self.notify_frontend(Operation::Delete, Info::Table(table))
                        .await;
                    for dependent_relation_id in dependent_relations {
                        database_core.decrease_ref_count(dependent_relation_id);
                    }

                    self.notify_frontend(Operation::Delete, Info::Index(index.to_owned()))
                        .await;
                }

                for &dependent_relation_id in &mview.dependent_relations {
                    database_core.decrease_ref_count(dependent_relation_id);
                }
                for user in users_need_update {
                    self.notify_frontend(Operation::Update, Info::User(user))
                        .await;
                }
                self.notify_frontend(Operation::Delete, Info::Table(mview))
                    .await;
                self.notify_frontend(Operation::Delete, Info::Table(internal_table))
                    .await;

                let version = self
                    .notify_frontend(Operation::Delete, Info::Source(source))
                    .await;

                Ok(version)
            }

            _ => Err(MetaError::catalog_not_found(
                "source",
                source_id.to_string(),
            )),
        }
    }

    pub async fn start_create_index_procedure(
        &self,
        index: &Index,
        index_table: &Table,
    ) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (index.database_id, index.schema_id, index.name.clone());

        core.check_relation_name_duplicated(&(
            index.database_id,
            index.schema_id,
            index.name.clone(),
        ))?;
        if core.has_in_progress_creation(&key) {
            bail!("index already in creating procedure");
        } else {
            core.mark_creating(&key);
            core.mark_creating_streaming_job(index_table.id);
            for &dependent_relation_id in &index_table.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        }
    }

    pub async fn cancel_create_index_procedure(
        &self,
        index: &Index,
        index_table: &Table,
    ) -> MetaResult<()> {
        let core = &mut self.core.lock().await.database;
        let key = (index.database_id, index.schema_id, index.name.clone());
        if !core.indexes.contains_key(&index.id) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            core.unmark_creating_streaming_job(index_table.id);
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
        table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let key = (table.database_id, table.schema_id, index.name.clone());

        let mut indexes = BTreeMapTransaction::new(&mut core.indexes);
        let mut tables = BTreeMapTransaction::new(&mut core.tables);
        if !indexes.contains_key(&index.id) && core.in_progress_creation_tracker.contains(&key) {
            core.in_progress_creation_tracker.remove(&key);
            core.in_progress_creation_streaming_job.remove(&table.id);

            indexes.insert(index.id, index.clone());
            tables.insert(table.id, table.clone());

            commit_meta!(self.env.meta_store(), indexes, tables)?;

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

        core.check_relation_name_duplicated(&(
            sink.database_id,
            sink.schema_id,
            sink.name.clone(),
        ))?;
        if core.has_in_progress_creation(&key) {
            bail!("sink already in creating procedure");
        } else {
            core.mark_creating(&key);
            core.mark_creating_streaming_job(sink.id);
            for &dependent_relation_id in &sink.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        }
    }

    pub async fn finish_create_sink_procedure(
        &self,
        sink: &Sink,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let mut sinks = BTreeMapTransaction::new(&mut core.sinks);
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        if !sinks.contains_key(&sink.id) && core.in_progress_creation_tracker.contains(&key) {
            core.in_progress_creation_tracker.remove(&key);
            core.in_progress_creation_streaming_job.remove(&sink.id);

            sinks.insert(sink.id, sink.clone());
            commit_meta!(self.env.meta_store(), sinks)?;

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
        if !core.sinks.contains_key(&sink.id) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            core.unmark_creating_streaming_job(sink.id);
            Ok(())
        } else {
            bail!("sink already exist or not in creating procedure");
        }
    }

    pub async fn drop_sink(&self, sink_id: SinkId) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.database;
        let mut sinks = BTreeMapTransaction::new(&mut core.sinks);
        let sink = sinks.remove(sink_id);
        if let Some(sink) = sink {
            commit_meta!(self.env.meta_store(), sinks)?;

            for &dependent_relation_id in &sink.dependent_relations {
                core.decrease_ref_count(dependent_relation_id);
            }

            let version = self
                .notify_frontend(Operation::Delete, Info::Sink(sink))
                .await;

            Ok(version)
        } else {
            Err(MetaError::catalog_not_found("sink", sink_id.to_string()))
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

    pub async fn list_sources(&self) -> Vec<Source> {
        self.core.lock().await.database.list_sources()
    }

    pub async fn list_source_ids(&self, schema_id: SchemaId) -> Vec<SourceId> {
        self.core.lock().await.database.list_source_ids(schema_id)
    }

    /// `list_stream_job_ids` returns all running and creating stream job ids, this is for recovery
    /// clean up progress.
    pub async fn list_stream_job_ids(&self) -> MetaResult<HashSet<TableId>> {
        let guard = self.core.lock().await;
        let mut all_streaming_jobs: HashSet<TableId> =
            guard.database.list_stream_job_ids().collect();

        all_streaming_jobs.extend(guard.database.all_creating_streaming_jobs());
        Ok(all_streaming_jobs)
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
                core.user_info.insert(default_user.id, default_user);
            }
        }

        Ok(())
    }

    #[cfg(test)]
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
        let mut users = BTreeMapTransaction::new(&mut core.user_info);
        users.insert(user.id, user.clone());
        commit_meta!(self.env.meta_store(), users)?;

        let version = self
            .env
            .notification_manager()
            .notify_frontend(Operation::Add, Info::User(user.to_owned()))
            .await;
        Ok(version)
    }

    pub async fn update_user(
        &self,
        update_user: &UserInfo,
        update_fields: &[UpdateField],
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.user;
        let rename_flag = update_fields
            .iter()
            .any(|&field| field == UpdateField::Rename);
        if rename_flag && core.has_user_name(&update_user.name) {
            return Err(MetaError::permission_denied(format!(
                "User {} already exists",
                update_user.name
            )));
        }

        let mut users = BTreeMapTransaction::new(&mut core.user_info);
        let mut user = users.get_mut(update_user.id).unwrap();

        update_fields.iter().for_each(|&field| match field {
            UpdateField::Unspecified => unreachable!(),
            UpdateField::Super => user.is_super = update_user.is_super,
            UpdateField::Login => user.can_login = update_user.can_login,
            UpdateField::CreateDb => user.can_create_db = update_user.can_create_db,
            UpdateField::CreateUser => user.can_create_user = update_user.can_create_user,
            UpdateField::AuthInfo => user.auth_info = update_user.auth_info.clone(),
            UpdateField::Rename => {
                user.name = update_user.name.clone();
            }
        });

        let new_user: UserInfo = user.clone();

        commit_meta!(self.env.meta_store(), users)?;

        let version = self
            .env
            .notification_manager()
            .notify_frontend(Operation::Update, Info::User(new_user))
            .await;
        Ok(version)
    }

    #[cfg(test)]
    pub async fn get_user(&self, id: UserId) -> MetaResult<UserInfo> {
        let core = &self.core.lock().await.user;
        core.user_info
            .get(&id)
            .cloned()
            .ok_or_else(|| anyhow!("User {} not found", id).into())
    }

    pub async fn drop_user(&self, id: UserId) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.user;
        let mut users = BTreeMapTransaction::new(&mut core.user_info);
        if !users.contains_key(&id) {
            bail!("User {} not found", id);
        }

        let user = users.remove(id).unwrap();

        if user.name == DEFAULT_SUPER_USER || user.name == DEFAULT_SUPER_USER_FOR_PG {
            return Err(MetaError::permission_denied(format!(
                "Cannot drop default super user {}",
                id
            )));
        }
        if !user.grant_privileges.is_empty() {
            return Err(MetaError::permission_denied(format!(
                "Cannot drop user {} with privileges",
                id
            )));
        }
        if core
            .user_grant_relation
            .get(&id)
            .is_some_and(|set| !set.is_empty())
        {
            return Err(MetaError::permission_denied(format!(
                "Cannot drop user {} with privileges granted to others",
                id
            )));
        }

        commit_meta!(self.env.meta_store(), users)?;

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
        user_ids: &[UserId],
        new_grant_privileges: &[GrantPrivilege],
        grantor: UserId,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.user;
        let mut users = BTreeMapTransaction::new(&mut core.user_info);
        let mut user_updated = Vec::with_capacity(user_ids.len());
        let grantor_info = users
            .get(&grantor)
            .cloned()
            .ok_or_else(|| anyhow!("User {} does not exist", &grantor))?;
        for user_id in user_ids {
            let mut user = users
                .get_mut(*user_id)
                .ok_or_else(|| anyhow!("User {} does not exist", user_id))?;

            let grant_user = core
                .user_grant_relation
                .entry(grantor)
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
            user_updated.push(user.clone());
        }

        commit_meta!(self.env.meta_store(), users)?;

        let mut version = 0;
        for user in user_updated {
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
        user_ids: &[UserId],
        revoke_grant_privileges: &[GrantPrivilege],
        granted_by: UserId,
        revoke_by: UserId,
        revoke_grant_option: bool,
        cascade: bool,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut self.core.lock().await.user;
        let mut users = BTreeMapTransaction::new(&mut core.user_info);
        let mut user_updated = HashMap::new();
        let mut users_info: VecDeque<UserInfo> = VecDeque::new();
        let mut visited = HashSet::new();
        // check revoke permission
        let revoke_by = users
            .get(&revoke_by)
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
        for user_id in user_ids {
            let user = users
                .get(user_id)
                .cloned()
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
                .user_grant_relation
                .get(&now_user.id)
                .cloned()
                .unwrap_or_default();
            let mut recursive_flag = false;
            let mut empty_privilege = false;
            let grant_option_now = revoke_grant_option && user_ids.contains(&now_user.id);
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
                if !cascade && !user_ids.contains(&now_user.id) {
                    return Err(MetaError::permission_denied(format!(
                        "Cannot revoke privilege from user {} for restrict",
                        &now_user.name
                    )));
                }
                for next_user_id in now_relations {
                    if users.contains_key(&next_user_id) && !visited.contains(&next_user_id) {
                        users_info.push_back(users.get(&next_user_id).cloned().unwrap());
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
                    users.insert(now_user.id, now_user.clone());
                    e.insert(now_user);
                }
            }
        }

        commit_meta!(self.env.meta_store(), users)?;
        let mut version = 0;
        for (_, user_info) in user_updated {
            version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Update, Info::User(user_info))
                .await;
        }

        Ok(version)
    }

    /// `update_user_privileges` removes the privileges with given object from given users, it will
    /// be called when a database/schema/table/source is dropped.
    #[inline(always)]
    fn update_user_privileges(
        users: &mut BTreeMapTransaction<'_, UserId, UserInfo>,
        objects: &[Object],
    ) -> Vec<UserInfo> {
        let mut users_need_update = vec![];
        let user_keys = users.tree_ref().keys().copied().collect_vec();
        for user_id in user_keys {
            let mut user = users.get_mut(user_id).unwrap();
            let mut new_grant_privileges = user.grant_privileges.clone();
            new_grant_privileges.retain(|p| !objects.contains(p.object.as_ref().unwrap()));
            if new_grant_privileges.len() != user.grant_privileges.len() {
                user.grant_privileges = new_grant_privileges;
                users_need_update.push(user.clone());
            }
        }
        users_need_update
    }
}
