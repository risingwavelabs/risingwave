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

mod connection;
mod database;
mod fragment;
mod user;

use std::collections::{HashMap, HashSet, VecDeque};
use std::iter;
use std::option::Option::Some;
use std::sync::Arc;

use anyhow::{anyhow, Context};
pub use connection::*;
pub use database::*;
pub use fragment::*;
use itertools::Itertools;
use risingwave_common::catalog::{
    valid_table_name, TableId as StreamingJobId, TableOption, DEFAULT_DATABASE_NAME,
    DEFAULT_SCHEMA_NAME, DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_FOR_PG,
    DEFAULT_SUPER_USER_FOR_PG_ID, DEFAULT_SUPER_USER_ID, SYSTEM_SCHEMAS,
};
use risingwave_common::{bail, ensure};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{
    Connection, Database, Function, Index, Schema, Sink, Source, Table, View,
};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::user::grant_privilege::{ActionWithGrantOption, Object};
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::{GrantPrivilege, UserInfo};
use tokio::sync::{Mutex, MutexGuard};
use user::*;

use crate::manager::{IdCategory, MetaSrvEnv, NotificationVersion, StreamingJob};
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
pub type ViewId = u32;
pub type FunctionId = u32;

pub type UserId = u32;
pub type ConnectionId = u32;

/// `commit_meta` provides a wrapper for committing metadata changes to both in-memory and
/// meta store.
/// * $`manager`: metadata manager, which should contains an env field to access meta store.
/// * $`val_txn`: transactions to commit.
macro_rules! commit_meta {
    ($manager:expr, $($val_txn:expr),*) => {
        {
            async {
                let mut trx = Transaction::default();
                // Apply the change in `ValTransaction` to trx
                $(
                    $val_txn.apply_to_txn(&mut trx)?;
                )*
                // Commit to meta store
                $manager.env.meta_store().txn(trx).await?;
                // Upon successful commit, commit the change to in-mem meta
                $(
                    $val_txn.commit();
                )*
                MetaResult::Ok(())
            }.await
        }
    };
}
pub(crate) use commit_meta;
use risingwave_pb::meta::CreatingJobInfo;

pub type CatalogManagerRef<S> = Arc<CatalogManager<S>>;

/// `CatalogManager` manages database catalog information and user information, including
/// authentication and privileges.
///
/// It only has some basic validation for the user information.
/// Other authorization relate to the current session user should be done in Frontend before passing
/// to Meta.
pub struct CatalogManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    core: Mutex<CatalogManagerCore>,
}

pub struct CatalogManagerCore {
    pub database: DatabaseManager,
    pub user: UserManager,
    pub connection: ConnectionManager,
}

impl CatalogManagerCore {
    async fn new<S: MetaStore>(env: MetaSrvEnv<S>) -> MetaResult<Self> {
        let database = DatabaseManager::new(env.clone()).await?;
        let user = UserManager::new(env.clone(), &database).await?;
        let connection = ConnectionManager::new(env).await?;
        Ok(Self {
            database,
            user,
            connection,
        })
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
        self.init_user().await?;
        self.init_database().await?;
        Ok(())
    }

    pub async fn get_catalog_core_guard(&self) -> MutexGuard<'_, CatalogManagerCore> {
        self.core.lock().await
    }
}

// Database catalog related methods
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
        if self
            .core
            .lock()
            .await
            .database
            .check_database_duplicated(&database.name)
            .is_ok()
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
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        database_core.check_database_duplicated(&database.name)?;
        #[cfg(not(test))]
        user_core.ensure_user_id(database.owner)?;

        let mut databases = BTreeMapTransaction::new(&mut database_core.databases);
        let mut schemas = BTreeMapTransaction::new(&mut database_core.schemas);
        databases.insert(database.id, database.clone());
        let mut schemas_added = vec![];
        for schema_name in iter::once(DEFAULT_SCHEMA_NAME).chain(SYSTEM_SCHEMAS) {
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

        commit_meta!(self, databases, schemas)?;

        // database and schemas.
        user_core.increase_ref_count(database.owner, 1 + schemas_added.len());

        let mut version = self
            .notify_frontend(Operation::Add, Info::Database(database.to_owned()))
            .await;
        for schema in schemas_added {
            version = self
                .notify_frontend(Operation::Add, Info::Schema(schema))
                .await;
        }

        Ok(version)
    }

    /// return id of streaming jobs in the database which need to be dropped by stream manager.
    pub async fn drop_database(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<(NotificationVersion, Vec<StreamingJobId>, Vec<SourceId>)> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mut databases = BTreeMapTransaction::new(&mut database_core.databases);
        let mut schemas = BTreeMapTransaction::new(&mut database_core.schemas);
        let mut sources = BTreeMapTransaction::new(&mut database_core.sources);
        let mut sinks = BTreeMapTransaction::new(&mut database_core.sinks);
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut indexes = BTreeMapTransaction::new(&mut database_core.indexes);
        let mut views = BTreeMapTransaction::new(&mut database_core.views);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        /// `drop_by_database_id` provides a wrapper for dropping relations by database id, it will
        /// return the relation ids that dropped.
        /// * $`val_txn`: transactions to the relations.
        /// * $`database_id`: database id.
        macro_rules! drop_by_database_id {
            ($val_txn:expr, $database_id:ident) => {{
                let ids_to_drop = $val_txn
                    .tree_ref()
                    .values()
                    .filter(|relation| relation.database_id == $database_id)
                    .map(|relation| relation.id)
                    .collect_vec();
                ids_to_drop
                    .into_iter()
                    .map(|id| $val_txn.remove(id).unwrap())
                    .collect_vec()
            }};
        }

        let database = databases.remove(database_id);
        if let Some(database) = database {
            let schemas_to_drop = drop_by_database_id!(schemas, database_id);
            let sources_to_drop = drop_by_database_id!(sources, database_id);
            let sinks_to_drop = drop_by_database_id!(sinks, database_id);
            let tables_to_drop = drop_by_database_id!(tables, database_id);
            let indexes_to_drop = drop_by_database_id!(indexes, database_id);
            let views_to_drop = drop_by_database_id!(views, database_id);

            let objects = std::iter::once(Object::DatabaseId(database_id))
                .chain(
                    schemas_to_drop
                        .iter()
                        .map(|schema| Object::SchemaId(schema.id)),
                )
                .chain(views_to_drop.iter().map(|view| Object::ViewId(view.id)))
                .chain(tables_to_drop.iter().map(|table| Object::TableId(table.id)))
                .chain(
                    sources_to_drop
                        .iter()
                        .map(|source| Object::SourceId(source.id)),
                )
                .collect_vec();
            let users_need_update = Self::update_user_privileges(&mut users, &objects);

            commit_meta!(self, databases, schemas, sources, sinks, tables, indexes, views, users)?;

            std::iter::once(database.owner)
                .chain(schemas_to_drop.iter().map(|schema| schema.owner))
                .chain(sources_to_drop.iter().map(|source| source.owner))
                .chain(sinks_to_drop.iter().map(|sink| sink.owner))
                .chain(
                    tables_to_drop
                        .iter()
                        .filter(|table| valid_table_name(&table.name))
                        .map(|table| table.owner),
                )
                .chain(indexes_to_drop.iter().map(|index| index.owner))
                .chain(views_to_drop.iter().map(|view| view.owner))
                .for_each(|owner_id| user_core.decrease_ref(owner_id));

            // Update relation ref count.
            for table in &tables_to_drop {
                database_core.relation_ref_count.remove(&table.id);
            }
            for source in &sources_to_drop {
                database_core.relation_ref_count.remove(&source.id);
            }
            for view in &views_to_drop {
                database_core.relation_ref_count.remove(&view.id);
            }

            for user in users_need_update {
                self.notify_frontend(Operation::Update, Info::User(user))
                    .await;
            }

            // Frontend will drop cache of schema and table in the database.
            let version = self
                .notify_frontend(Operation::Delete, Info::Database(database))
                .await;

            let catalog_deleted_ids = tables_to_drop
                .into_iter()
                .filter(|table| valid_table_name(&table.name))
                .map(|table| StreamingJobId::new(table.id))
                .chain(
                    sinks_to_drop
                        .into_iter()
                        .map(|sink| StreamingJobId::new(sink.id)),
                )
                .collect_vec();
            let source_deleted_ids = sources_to_drop
                .into_iter()
                .map(|source| source.id)
                .collect_vec();

            Ok((version, catalog_deleted_ids, source_deleted_ids))
        } else {
            Err(MetaError::catalog_id_not_found("database", database_id))
        }
    }

    pub async fn create_connection(
        &self,
        service_name: &str,
        connection: Connection,
    ) -> MetaResult<()> {
        let core = &mut self.core.lock().await.connection;

        let conn_id = connection.id;
        let mut connections = BTreeMapTransaction::new(&mut core.connections);
        // if there is a connection with the same service name, just overwrite it
        connections.insert(conn_id, connection);
        commit_meta!(self, connections)?;

        core.connection_by_name
            .insert(service_name.to_string(), conn_id);
        Ok(())
    }

    pub async fn create_schema(&self, schema: &Schema) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        database_core.ensure_database_id(schema.database_id)?;
        database_core.check_schema_duplicated(&(schema.database_id, schema.name.clone()))?;
        #[cfg(not(test))]
        user_core.ensure_user_id(schema.owner)?;

        let mut schemas = BTreeMapTransaction::new(&mut database_core.schemas);
        schemas.insert(schema.id, schema.clone());
        commit_meta!(self, schemas)?;

        user_core.increase_ref(schema.owner);

        let version = self
            .notify_frontend(Operation::Add, Info::Schema(schema.to_owned()))
            .await;

        Ok(version)
    }

    pub async fn drop_schema(&self, schema_id: SchemaId) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        if !database_core.schemas.contains_key(&schema_id) {
            return Err(MetaError::catalog_id_not_found("schema", schema_id));
        }
        if !database_core.schema_is_empty(schema_id) {
            bail!("schema is not empty!");
        }
        let mut schemas = BTreeMapTransaction::new(&mut database_core.schemas);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);
        let schema = schemas.remove(schema_id).unwrap();

        let users_need_update =
            Self::update_user_privileges(&mut users, &[Object::SchemaId(schema_id)]);

        commit_meta!(self, schemas, users)?;

        user_core.decrease_ref(schema.owner);

        for user in users_need_update {
            self.notify_frontend(Operation::Update, Info::User(user))
                .await;
        }
        let version = self
            .notify_frontend(Operation::Delete, Info::Schema(schema))
            .await;

        Ok(version)
    }

    pub async fn create_view(&self, view: &View) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        database_core.ensure_database_id(view.database_id)?;
        database_core.ensure_schema_id(view.schema_id)?;
        for dependent_id in &view.dependent_relations {
            // TODO(zehua): refactor when using SourceId.
            database_core.ensure_table_or_source_id(dependent_id)?;
        }
        let key = (view.database_id, view.schema_id, view.name.clone());
        database_core.check_relation_name_duplicated(&key)?;
        #[cfg(not(test))]
        user_core.ensure_user_id(view.owner)?;

        let mut views = BTreeMapTransaction::new(&mut database_core.views);
        views.insert(view.id, view.clone());
        commit_meta!(self, views)?;

        user_core.increase_ref(view.owner);

        for &dependent_relation_id in &view.dependent_relations {
            database_core.increase_ref_count(dependent_relation_id);
        }

        let version = self
            .notify_frontend(Operation::Add, Info::View(view.to_owned()))
            .await;

        Ok(version)
    }

    pub async fn drop_view(&self, view_id: ViewId) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mut views = BTreeMapTransaction::new(&mut database_core.views);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let view = views.remove(view_id);
        if let Some(view) = view {
            match database_core.relation_ref_count.get(&view_id) {
                Some(ref_count) => Err(MetaError::permission_denied(format!(
                    "Fail to delete view `{}` because {} other relation(s) depend on it",
                    view.name, ref_count
                ))),
                None => {
                    let users_need_update =
                        Self::update_user_privileges(&mut users, &[Object::ViewId(view_id)]);
                    commit_meta!(self, views, users)?;

                    user_core.decrease_ref(view.owner);

                    for &dependent_relation_id in &view.dependent_relations {
                        database_core.decrease_ref_count(dependent_relation_id);
                    }

                    for user in users_need_update {
                        self.notify_frontend(Operation::Update, Info::User(user))
                            .await;
                    }
                    let version = self
                        .notify_frontend(Operation::Delete, Info::View(view))
                        .await;

                    Ok(version)
                }
            }
        } else {
            Err(MetaError::catalog_id_not_found("view", view_id))
        }
    }

    pub async fn create_function(&self, function: &Function) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        database_core.ensure_database_id(function.database_id)?;
        database_core.ensure_schema_id(function.schema_id)?;

        #[cfg(not(test))]
        user_core.ensure_user_id(function.owner)?;

        let mut functions = BTreeMapTransaction::new(&mut database_core.functions);
        functions.insert(function.id, function.clone());
        commit_meta!(self, functions)?;

        user_core.increase_ref(function.owner);

        let version = self
            .notify_frontend(Operation::Add, Info::Function(function.to_owned()))
            .await;

        Ok(version)
    }

    pub async fn drop_function(&self, function_id: FunctionId) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mut functions = BTreeMapTransaction::new(&mut database_core.functions);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let function = functions
            .remove(function_id)
            .ok_or_else(|| anyhow!("function not found"))?;

        let objects = &[Object::FunctionId(function_id)];
        let users_need_update = Self::update_user_privileges(&mut users, objects);

        commit_meta!(self, functions, users)?;

        user_core.decrease_ref(function.owner);

        for user in users_need_update {
            self.notify_frontend(Operation::Update, Info::User(user))
                .await;
        }

        let version = self
            .notify_frontend(Operation::Delete, Info::Function(function))
            .await;

        Ok(version)
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
            StreamingJob::Table(source, table) => {
                if let Some(source) = source {
                    self.start_create_table_procedure_with_source(source, table)
                        .await
                } else {
                    self.start_create_table_procedure(table).await
                }
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

    /// This is used for both `CREATE TABLE` and `CREATE MATERIALIZED VIEW`.
    pub async fn start_create_table_procedure(&self, table: &Table) -> MetaResult<()> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        database_core.ensure_database_id(table.database_id)?;
        database_core.ensure_schema_id(table.schema_id)?;
        for dependent_id in &table.dependent_relations {
            // TODO(zehua): refactor when using SourceId.
            database_core.ensure_table_or_source_id(dependent_id)?;
        }
        #[cfg(not(test))]
        user_core.ensure_user_id(table.owner)?;
        let key = (table.database_id, table.schema_id, table.name.clone());
        database_core.check_relation_name_duplicated(&key)?;

        if database_core.has_in_progress_creation(&key) {
            bail!("table is in creating procedure");
        } else {
            database_core.mark_creating(&key);
            database_core.mark_creating_streaming_job(table.id, key);
            for &dependent_relation_id in &table.dependent_relations {
                database_core.increase_ref_count(dependent_relation_id);
            }
            user_core.increase_ref(table.owner);
            Ok(())
        }
    }

    /// This is used for both `CREATE TABLE` and `CREATE MATERIALIZED VIEW`.
    pub async fn finish_create_table_procedure(
        &self,
        internal_tables: Vec<Table>,
        table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let key = (table.database_id, table.schema_id, table.name.clone());
        assert!(
            !tables.contains_key(&table.id)
                && database_core.in_progress_creation_tracker.contains(&key),
            "table must be in creating procedure"
        );
        database_core.in_progress_creation_tracker.remove(&key);
        database_core
            .in_progress_creation_streaming_job
            .remove(&table.id);

        tables.insert(table.id, table.clone());
        for table in &internal_tables {
            tables.insert(table.id, table.clone());
        }
        commit_meta!(self, tables)?;

        for internal_table in internal_tables {
            self.notify_frontend(Operation::Add, Info::Table(internal_table))
                .await;
        }

        let version = self
            .notify_frontend(Operation::Add, Info::Table(table.to_owned()))
            .await;

        Ok(version)
    }

    pub async fn cancel_create_table_procedure(&self, table: &Table) {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let key = (table.database_id, table.schema_id, table.name.clone());
        assert!(
            !database_core.tables.contains_key(&table.id)
                && database_core.has_in_progress_creation(&key),
            "table must be in creating procedure"
        );

        database_core.unmark_creating(&key);
        database_core.unmark_creating_streaming_job(table.id);
        for &dependent_relation_id in &table.dependent_relations {
            database_core.decrease_ref_count(dependent_relation_id);
        }
        user_core.decrease_ref(table.owner);
    }

    /// return id of streaming jobs in the database which need to be dropped by stream manager.
    pub async fn drop_table(
        &self,
        table_id: TableId,
        internal_table_ids: Vec<TableId>,
    ) -> MetaResult<(NotificationVersion, Vec<StreamingJobId>)> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mut indexes = BTreeMapTransaction::new(&mut database_core.indexes);
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let table = tables.remove(table_id);
        if let Some(table) = table {
            let (index_ids, index_table_ids): (Vec<_>, Vec<_>) = indexes
                .tree_ref()
                .iter()
                .filter(|(_, index)| index.primary_table_id == table_id)
                .map(|(index_id, index)| (*index_id, index.index_table_id))
                .unzip();

            if let Some(ref_count) = database_core.relation_ref_count.get(&table_id).cloned() {
                if ref_count > index_ids.len() {
                    return Err(MetaError::permission_denied(format!(
                        "Fail to delete table `{}` because {} other relation(s) depend on it",
                        table.name, ref_count
                    )));
                }
            }

            let indexes_removed = index_ids
                .iter()
                .map(|index_id| indexes.remove(*index_id).unwrap())
                .collect_vec();
            let index_tables = index_table_ids
                .iter()
                .map(|index_table_id| tables.remove(*index_table_id).unwrap())
                .collect_vec();
            for index_table in &index_tables {
                if let Some(ref_count) = database_core.relation_ref_count.get(&index_table.id) {
                    return Err(MetaError::permission_denied(format!(
                        "Fail to delete table `{}` because {} other relation(s) depend on it",
                        index_table.name, ref_count
                    )));
                }
            }

            let internal_tables = internal_table_ids
                .iter()
                .map(|internal_table_id| {
                    tables
                        .remove(*internal_table_id)
                        .expect("internal table should exist")
                })
                .collect_vec();

            let users_need_update = {
                let table_to_drop_ids = index_table_ids
                    .iter()
                    .chain(&internal_table_ids)
                    .chain([&table_id])
                    .collect_vec();

                Self::update_user_privileges(
                    &mut users,
                    &table_to_drop_ids
                        .into_iter()
                        .map(|table_id| Object::TableId(*table_id))
                        .collect_vec(),
                )
            };

            commit_meta!(self, tables, indexes, users)?;

            indexes_removed.iter().for_each(|index| {
                // index table and index.
                user_core.decrease_ref_count(index.owner, 2);
            });
            user_core.decrease_ref(table.owner);

            for index in indexes_removed {
                self.notify_frontend(Operation::Delete, Info::Index(index))
                    .await;
            }

            for index_table in index_tables {
                for dependent_relation_id in &index_table.dependent_relations {
                    database_core.decrease_ref_count(*dependent_relation_id);
                }
                self.notify_frontend(Operation::Delete, Info::Table(index_table))
                    .await;
            }

            for internal_table in internal_tables {
                self.notify_frontend(Operation::Delete, Info::Table(internal_table))
                    .await;
            }

            for user in users_need_update {
                self.notify_frontend(Operation::Update, Info::User(user))
                    .await;
            }

            for dependent_relation_id in &table.dependent_relations {
                database_core.decrease_ref_count(*dependent_relation_id);
            }

            let version = self
                .notify_frontend(Operation::Delete, Info::Table(table))
                .await;

            let catalog_deleted_ids = index_table_ids
                .into_iter()
                .chain(std::iter::once(table_id))
                .map(|id| id.into())
                .collect_vec();

            Ok((version, catalog_deleted_ids))
        } else {
            bail!("table doesn't exist");
        }
    }

    pub async fn get_index_table(&self, index_id: IndexId) -> MetaResult<TableId> {
        let guard = self.core.lock().await;
        let index = guard.database.indexes.get(&index_id);
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

                        commit_meta!(self, tables, indexes, users)?;

                        // index table and index.
                        user_core.decrease_ref_count(index.owner, 2);

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
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        database_core.ensure_database_id(source.database_id)?;
        database_core.ensure_schema_id(source.schema_id)?;
        let key = (source.database_id, source.schema_id, source.name.clone());
        database_core.check_relation_name_duplicated(&key)?;
        #[cfg(not(test))]
        user_core.ensure_user_id(source.owner)?;

        if database_core.has_in_progress_creation(&key) {
            bail!("source is in creating procedure");
        } else {
            database_core.mark_creating(&key);
            user_core.increase_ref(source.owner);
            Ok(())
        }
    }

    pub async fn get_connection_by_name(&self, name: &str) -> MetaResult<Connection> {
        let core = &mut self.core.lock().await.connection;
        core.get_connection_by_name(name)
            .cloned()
            .ok_or_else(|| anyhow!(format!("unknown service connection")).into())
    }

    pub async fn finish_create_source_procedure(
        &self,
        source: &Source,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let mut sources = BTreeMapTransaction::new(&mut database_core.sources);
        let key = (source.database_id, source.schema_id, source.name.clone());
        assert!(
            !sources.contains_key(&source.id)
                && database_core.in_progress_creation_tracker.contains(&key),
            "source must be in creating procedure"
        );
        database_core.in_progress_creation_tracker.remove(&key);
        sources.insert(source.id, source.clone());

        commit_meta!(self, sources)?;

        let version = self
            .notify_frontend(Operation::Add, Info::Source(source.to_owned()))
            .await;

        Ok(version)
    }

    pub async fn cancel_create_source_procedure(&self, source: &Source) -> MetaResult<()> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let key = (source.database_id, source.schema_id, source.name.clone());
        assert!(
            !database_core.sources.contains_key(&source.id)
                && database_core.has_in_progress_creation(&key),
            "source must be in creating procedure"
        );

        database_core.unmark_creating(&key);
        user_core.decrease_ref(source.owner);
        Ok(())
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
                    commit_meta!(self, sources, users)?;

                    user_core.decrease_ref(source.owner);

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
            Err(MetaError::catalog_id_not_found("source", source_id))
        }
    }

    pub async fn start_create_table_procedure_with_source(
        &self,
        source: &Source,
        table: &Table,
    ) -> MetaResult<()> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        database_core.ensure_database_id(source.database_id)?;
        database_core.ensure_schema_id(source.schema_id)?;
        let source_key = (source.database_id, source.schema_id, source.name.clone());
        database_core.check_relation_name_duplicated(&source_key)?;
        #[cfg(not(test))]
        user_core.ensure_user_id(source.owner)?;
        assert_eq!(source.owner, table.owner);

        let mview_key = (table.database_id, table.schema_id, table.name.clone());
        if database_core.has_in_progress_creation(&source_key)
            || database_core.has_in_progress_creation(&mview_key)
        {
            bail!("table or source is in creating procedure");
        } else {
            database_core.mark_creating(&source_key);
            database_core.mark_creating(&mview_key);
            database_core.mark_creating_streaming_job(table.id, mview_key);
            ensure!(table.dependent_relations.is_empty());
            // source and table
            user_core.increase_ref_count(source.owner, 2);
            Ok(())
        }
    }

    pub async fn finish_create_table_procedure_with_source(
        &self,
        source: &Source,
        mview: &Table,
        internal_table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut sources = BTreeMapTransaction::new(&mut database_core.sources);

        let source_key = (source.database_id, source.schema_id, source.name.clone());
        let mview_key = (mview.database_id, mview.schema_id, mview.name.clone());
        assert!(
            !sources.contains_key(&source.id)
                && !tables.contains_key(&mview.id)
                && database_core
                    .in_progress_creation_tracker
                    .contains(&source_key)
                && database_core
                    .in_progress_creation_tracker
                    .contains(&mview_key),
            "table and source must be in creating procedure"
        );
        database_core
            .in_progress_creation_tracker
            .remove(&source_key);
        database_core
            .in_progress_creation_tracker
            .remove(&mview_key);
        database_core
            .in_progress_creation_streaming_job
            .remove(&mview.id);

        sources.insert(source.id, source.clone());
        tables.insert(mview.id, mview.clone());
        tables.insert(internal_table.id, internal_table.clone());

        commit_meta!(self, sources, tables)?;
        self.notify_frontend(Operation::Add, Info::Table(internal_table.to_owned()))
            .await;
        self.notify_frontend(Operation::Add, Info::Table(mview.to_owned()))
            .await;

        // Currently frontend uses source's version
        let version = self
            .notify_frontend(Operation::Add, Info::Source(source.to_owned()))
            .await;
        Ok(version)
    }

    pub async fn cancel_create_table_procedure_with_source(&self, source: &Source, table: &Table) {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let source_key = (source.database_id, source.schema_id, source.name.clone());
        let table_key = (table.database_id, table.schema_id, table.name.clone());
        assert!(
            !database_core.sources.contains_key(&source.id)
                && !database_core.tables.contains_key(&table.id)
                && database_core.has_in_progress_creation(&source_key)
                && database_core.has_in_progress_creation(&table_key),
            "table and source must be in creating procedure"
        );

        database_core.unmark_creating(&source_key);
        database_core.unmark_creating(&table_key);
        database_core.unmark_creating_streaming_job(table.id);
        user_core.decrease_ref_count(source.owner, 2); // source and table
    }

    /// return id of streaming jobs in the database which need to be dropped by stream manager.
    /// NOTE: This method repeats a lot with `drop_table`. Might need refactor.
    pub async fn drop_table_with_source(
        &self,
        source_id: SourceId,
        table_id: TableId,
        internal_table_id: TableId,
    ) -> MetaResult<(NotificationVersion, Vec<StreamingJobId>)> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;

        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut sources = BTreeMapTransaction::new(&mut database_core.sources);
        let mut indexes = BTreeMapTransaction::new(&mut database_core.indexes);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let mview = tables.remove(table_id);
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

                // Check `ref_count`
                let (index_ids, index_table_ids): (Vec<_>, Vec<_>) = indexes
                    .tree_ref()
                    .iter()
                    .filter(|(_, index)| index.primary_table_id == table_id)
                    .map(|(index_id, index)| (*index_id, index.index_table_id))
                    .unzip();
                if let Some(ref_count) = database_core.relation_ref_count.get(&table_id).cloned() {
                    // Indexes are dependent on table. We can drop table only if its `ref_count` is
                    // strictly equal to number of indexes.
                    if ref_count > index_ids.len() {
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

                let indexes_removed = index_ids
                    .iter()
                    .map(|index_id| indexes.remove(*index_id).unwrap())
                    .collect_vec();
                let index_tables = index_table_ids
                    .iter()
                    .map(|index_table_id| tables.remove(*index_table_id).unwrap())
                    .collect_vec();
                for index_table in &index_tables {
                    if let Some(ref_count) = database_core.relation_ref_count.get(&index_table.id) {
                        return Err(MetaError::permission_denied(format!(
                            "Fail to delete table `{}` because {} other relation(s) depend on it",
                            index_table.name, ref_count
                        )));
                    }
                }

                let internal_table = tables
                    .remove(internal_table_id)
                    .expect("internal table should exist");

                let objects = [
                    Object::SourceId(source_id),
                    Object::TableId(table_id),
                    Object::TableId(internal_table_id),
                ]
                .into_iter()
                .chain(index_table_ids.iter().map(|id| Object::TableId(*id)))
                .collect_vec();

                let users_need_update = Self::update_user_privileges(&mut users, &objects);

                // Commit point
                commit_meta!(self, tables, sources, indexes, users)?;

                indexes_removed.iter().for_each(|index| {
                    user_core.decrease_ref_count(index.owner, 2); // index table and index
                });

                user_core.decrease_ref_count(mview.owner, 2); // source and mview.

                for index in indexes_removed {
                    self.notify_frontend(Operation::Delete, Info::Index(index))
                        .await;
                }

                for index_table in index_tables {
                    for dependent_relation_id in &index_table.dependent_relations {
                        database_core.decrease_ref_count(*dependent_relation_id);
                    }
                    self.notify_frontend(Operation::Delete, Info::Table(index_table))
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

                let catalog_deleted_ids = index_table_ids
                    .into_iter()
                    .chain(std::iter::once(table_id))
                    .map(|id| id.into())
                    .collect_vec();
                Ok((version, catalog_deleted_ids))
            }

            _ => Err(MetaError::catalog_id_not_found("source", source_id)),
        }
    }

    pub async fn start_create_index_procedure(
        &self,
        index: &Index,
        index_table: &Table,
    ) -> MetaResult<()> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        database_core.ensure_database_id(index.database_id)?;
        database_core.ensure_schema_id(index.schema_id)?;
        database_core.ensure_table_id(index.primary_table_id)?;
        let key = (index.database_id, index.schema_id, index.name.clone());
        database_core.check_relation_name_duplicated(&key)?;
        #[cfg(not(test))]
        user_core.ensure_user_id(index.owner)?;
        assert_eq!(index.owner, index_table.owner);

        // `dependent_relations` should contains 1 and only 1 item that is the `primary_table_id`
        assert_eq!(index_table.dependent_relations.len(), 1);
        assert_eq!(index.primary_table_id, index_table.dependent_relations[0]);

        if database_core.has_in_progress_creation(&key) {
            bail!("index already in creating procedure");
        } else {
            database_core.mark_creating(&key);
            database_core.mark_creating_streaming_job(index_table.id, key);
            for &dependent_relation_id in &index_table.dependent_relations {
                database_core.increase_ref_count(dependent_relation_id);
            }
            // index table and index.
            user_core.increase_ref_count(index.owner, 2);
            Ok(())
        }
    }

    pub async fn cancel_create_index_procedure(&self, index: &Index, index_table: &Table) {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let key = (index.database_id, index.schema_id, index.name.clone());
        assert!(
            !database_core.indexes.contains_key(&index.id)
                && database_core.has_in_progress_creation(&key),
            "index must be in creating procedure"
        );

        database_core.unmark_creating(&key);
        database_core.unmark_creating_streaming_job(index_table.id);
        for &dependent_relation_id in &index_table.dependent_relations {
            database_core.decrease_ref_count(dependent_relation_id);
        }
        // index table and index.
        user_core.decrease_ref_count(index.owner, 2);
    }

    pub async fn finish_create_index_procedure(
        &self,
        index: &Index,
        table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let key = (table.database_id, table.schema_id, index.name.clone());

        let mut indexes = BTreeMapTransaction::new(&mut database_core.indexes);
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        assert!(
            !indexes.contains_key(&index.id)
                && database_core.in_progress_creation_tracker.contains(&key),
            "index must be in creating procedure"
        );

        database_core.in_progress_creation_tracker.remove(&key);
        database_core
            .in_progress_creation_streaming_job
            .remove(&table.id);

        indexes.insert(index.id, index.clone());
        tables.insert(table.id, table.clone());

        commit_meta!(self, indexes, tables)?;

        self.notify_frontend(Operation::Add, Info::Table(table.to_owned()))
            .await;

        let version = self
            .notify_frontend(Operation::Add, Info::Index(index.to_owned()))
            .await;

        Ok(version)
    }

    pub async fn start_create_sink_procedure(&self, sink: &Sink) -> MetaResult<()> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        database_core.ensure_database_id(sink.database_id)?;
        database_core.ensure_schema_id(sink.schema_id)?;
        for dependent_id in &sink.dependent_relations {
            // TODO(zehua): refactor when using SourceId.
            database_core.ensure_table_or_source_id(dependent_id)?;
        }
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        database_core.check_relation_name_duplicated(&key)?;
        #[cfg(not(test))]
        user_core.ensure_user_id(sink.owner)?;

        if database_core.has_in_progress_creation(&key) {
            bail!("sink already in creating procedure");
        } else {
            database_core.mark_creating(&key);
            database_core.mark_creating_streaming_job(sink.id, key);
            for &dependent_relation_id in &sink.dependent_relations {
                database_core.increase_ref_count(dependent_relation_id);
            }
            user_core.increase_ref(sink.owner);
            Ok(())
        }
    }

    pub async fn finish_create_sink_procedure(
        &self,
        internal_tables: Vec<Table>,
        sink: &Sink,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut sinks = BTreeMapTransaction::new(&mut database_core.sinks);
        assert!(
            !sinks.contains_key(&sink.id)
                && database_core.in_progress_creation_tracker.contains(&key),
            "sink must be in creating procedure"
        );

        database_core.in_progress_creation_tracker.remove(&key);
        database_core
            .in_progress_creation_streaming_job
            .remove(&sink.id);

        sinks.insert(sink.id, sink.clone());
        for table in &internal_tables {
            tables.insert(table.id, table.clone());
        }
        commit_meta!(self, sinks, tables)?;

        for internal_table in internal_tables {
            self.notify_frontend(Operation::Add, Info::Table(internal_table))
                .await;
        }

        let version = self
            .notify_frontend(Operation::Add, Info::Sink(sink.to_owned()))
            .await;

        Ok(version)
    }

    pub async fn cancel_create_sink_procedure(&self, sink: &Sink) {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let key = (sink.database_id, sink.schema_id, sink.name.clone());
        assert!(
            !database_core.sinks.contains_key(&sink.id)
                && database_core.has_in_progress_creation(&key),
            "sink must be in creating procedure"
        );

        database_core.unmark_creating(&key);
        database_core.unmark_creating_streaming_job(sink.id);
        for &dependent_relation_id in &sink.dependent_relations {
            database_core.decrease_ref_count(dependent_relation_id);
        }
        user_core.decrease_ref(sink.owner);
    }

    pub async fn drop_sink(
        &self,
        sink_id: SinkId,
        internal_table_ids: Vec<TableId>,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let user_core = &mut core.user;
        let mut sinks = BTreeMapTransaction::new(&mut database_core.sinks);
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);

        let sink = sinks.remove(sink_id);
        if let Some(sink) = sink {
            match database_core.relation_ref_count.get(&sink_id).cloned() {
                Some(_) => bail!("No relation should depend on Sink"),
                None => {
                    let dependent_relations = sink.dependent_relations.clone();

                    let objects = &[Object::SinkId(sink.id)]
                        .into_iter()
                        .chain(
                            internal_table_ids
                                .iter()
                                .map(|table_id| Object::TableId(*table_id))
                                .collect_vec(),
                        )
                        .collect_vec();

                    let internal_tables = internal_table_ids
                        .iter()
                        .map(|internal_table_id| {
                            tables
                                .remove(*internal_table_id)
                                .expect("internal table should exist")
                        })
                        .collect_vec();

                    let users_need_update = Self::update_user_privileges(&mut users, objects);

                    commit_meta!(self, sinks, tables, users)?;

                    user_core.decrease_ref(sink.owner);

                    for user in users_need_update {
                        self.notify_frontend(Operation::Update, Info::User(user))
                            .await;
                    }

                    for dependent_relation_id in dependent_relations {
                        database_core.decrease_ref_count(dependent_relation_id);
                    }

                    for internal_table in internal_tables {
                        self.notify_frontend(Operation::Delete, Info::Table(internal_table))
                            .await;
                    }

                    let version = self
                        .notify_frontend(Operation::Delete, Info::Sink(sink))
                        .await;

                    Ok(version)
                }
            }
        } else {
            Err(MetaError::catalog_id_not_found("sink", sink_id))
        }
    }

    /// This is used for `ALTER TABLE ADD/DROP COLUMN`.
    pub async fn start_replace_table_procedure(&self, table: &Table) -> MetaResult<()> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        database_core.ensure_database_id(table.database_id)?;
        database_core.ensure_schema_id(table.schema_id)?;

        assert!(table.dependent_relations.is_empty());

        let key = (table.database_id, table.schema_id, table.name.clone());
        let original_table = database_core
            .get_table(table.id)
            .context("table to alter must exist")?;

        // Check whether the frontend is operating on the latest version of the table.
        if table.get_version()?.version != original_table.get_version()?.version + 1 {
            bail!("table version is stale");
        }

        // TODO: Here we reuse the `creation` tracker for `alter` procedure, as an `alter` must
        // occur after it's created. We may need to add a new tracker for `alter` procedure.
        if database_core.has_in_progress_creation(&key) {
            bail!("table is in altering procedure");
        } else {
            database_core.mark_creating(&key);
            Ok(())
        }
    }

    /// This is used for `ALTER TABLE ADD/DROP COLUMN`.
    pub async fn finish_replace_table_procedure(
        &self,
        table: &Table,
    ) -> MetaResult<NotificationVersion> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let mut tables = BTreeMapTransaction::new(&mut database_core.tables);
        let key = (table.database_id, table.schema_id, table.name.clone());
        assert!(
            tables.contains_key(&table.id)
                && database_core.in_progress_creation_tracker.contains(&key),
            "table must exist and be in altering procedure"
        );

        // TODO: Here we reuse the `creation` tracker for `alter` procedure, as an `alter` must
        database_core.in_progress_creation_tracker.remove(&key);

        tables.insert(table.id, table.clone());
        commit_meta!(self, tables)?;

        let version = self
            .notify_frontend(Operation::Update, Info::Table(table.to_owned()))
            .await;

        Ok(version)
    }

    /// This is used for `ALTER TABLE ADD/DROP COLUMN`.
    pub async fn cancel_replace_table_procedure(&self, table: &Table) -> MetaResult<()> {
        let core = &mut *self.core.lock().await;
        let database_core = &mut core.database;
        let key = (table.database_id, table.schema_id, table.name.clone());

        assert!(table.dependent_relations.is_empty());

        assert!(
            database_core.tables.contains_key(&table.id)
                && database_core.has_in_progress_creation(&key),
            "table must exist and must be in altering procedure"
        );

        // TODO: Here we reuse the `creation` tracker for `alter` procedure, as an `alter` must
        // occur after it's created. We may need to add a new tracker for `alter` procedure.s
        database_core.unmark_creating(&key);
        Ok(())
    }

    pub async fn list_connections(&self) -> Vec<Connection> {
        self.core.lock().await.connection.list_connections()
    }

    pub async fn list_databases(&self) -> Vec<Database> {
        self.core.lock().await.database.list_databases()
    }

    pub async fn list_tables(&self) -> Vec<Table> {
        self.core.lock().await.database.list_tables()
    }

    pub async fn get_all_table_options(&self) -> HashMap<TableId, TableOption> {
        self.core.lock().await.database.get_all_table_options()
    }

    pub async fn list_table_ids(&self, schema_id: SchemaId) -> Vec<TableId> {
        self.core.lock().await.database.list_table_ids(schema_id)
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

    pub async fn find_creating_streaming_job_ids(
        &self,
        infos: Vec<CreatingJobInfo>,
    ) -> Vec<TableId> {
        let guard = self.core.lock().await;
        infos
            .into_iter()
            .flat_map(|info| {
                guard.database.find_creating_streaming_job_id(&(
                    info.database_id,
                    info.schema_id,
                    info.name,
                ))
            })
            .collect_vec()
    }

    async fn notify_frontend(&self, operation: Operation, info: Info) -> NotificationVersion {
        self.env
            .notification_manager()
            .notify_frontend(operation, info)
            .await
    }
}

// User related methods
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
        commit_meta!(self, users)?;

        let version = self
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

        commit_meta!(self, users)?;

        let version = self
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
        let core = &mut *self.core.lock().await;
        let user_core = &mut core.user;
        let mut users = BTreeMapTransaction::new(&mut user_core.user_info);
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
        if user_core.catalog_create_ref_count.get(&id).is_some() {
            return Err(MetaError::permission_denied(format!(
                "User {} cannot be dropped because some objects depend on it",
                user.name
            )));
        }
        if user_core
            .user_grant_relation
            .get(&id)
            .is_some_and(|set| !set.is_empty())
        {
            return Err(MetaError::permission_denied(format!(
                "Cannot drop user {} with privileges granted to others",
                id
            )));
        }

        commit_meta!(self, users)?;

        let version = self
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

        commit_meta!(self, users)?;

        let grant_user = core
            .user_grant_relation
            .entry(grantor)
            .or_insert_with(HashSet::new);
        grant_user.extend(user_ids);

        let mut version = 0;
        for user in user_updated {
            version = self
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
            let mut cur_user = users_info.pop_front().unwrap();
            let cur_relations = core
                .user_grant_relation
                .get(&cur_user.id)
                .cloned()
                .unwrap_or_default();
            let mut recursive_flag = false;
            let mut empty_privilege = false;
            let cur_revoke_grant_option = revoke_grant_option && user_ids.contains(&cur_user.id);
            visited.insert(cur_user.id);
            revoke_grant_privileges
                .iter()
                .for_each(|revoke_grant_privilege| {
                    for privilege in &mut cur_user.grant_privileges {
                        if privilege.object == revoke_grant_privilege.object {
                            recursive_flag |= Self::revoke_privilege_inner(
                                privilege,
                                revoke_grant_privilege,
                                cur_revoke_grant_option,
                            );
                            empty_privilege |= privilege.action_with_opts.is_empty();
                            break;
                        }
                    }
                });
            if recursive_flag {
                // check with cascade/restrict strategy
                if !cascade && !user_ids.contains(&cur_user.id) {
                    return Err(MetaError::permission_denied(format!(
                        "Cannot revoke privilege from user {} for restrict",
                        &cur_user.name
                    )));
                }
                for next_user_id in cur_relations {
                    if users.contains_key(&next_user_id) && !visited.contains(&next_user_id) {
                        users_info.push_back(users.get(&next_user_id).cloned().unwrap());
                    }
                }
                if empty_privilege {
                    cur_user
                        .grant_privileges
                        .retain(|privilege| !privilege.action_with_opts.is_empty());
                }
                if let std::collections::hash_map::Entry::Vacant(e) =
                    user_updated.entry(cur_user.id)
                {
                    users.insert(cur_user.id, cur_user.clone());
                    e.insert(cur_user);
                }
            }
        }

        commit_meta!(self, users)?;

        // Since we might revoke privileges recursively, just simply re-build the grant relation
        // map here.
        core.build_grant_relation_map();

        let mut version = 0;
        for (_, user_info) in user_updated {
            version = self
                .notify_frontend(Operation::Update, Info::User(user_info))
                .await;
        }

        Ok(version)
    }

    /// `update_user_privileges` removes the privileges with given object from given users, it will
    /// be called when a database/schema/table/source/sink is dropped.
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
