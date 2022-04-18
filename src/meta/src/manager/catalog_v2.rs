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

#![allow(dead_code)]
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::option::Option::Some;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::catalog::{CatalogVersion, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use risingwave_common::ensure;
use risingwave_common::error::ErrorCode::{CatalogError, InternalError};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{Database, Schema, Source, Table};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::sync::{Mutex, MutexGuard};

use super::IdCategory;
use crate::manager::MetaSrvEnv;
use crate::model::{MetadataModel, Transactional};
use crate::storage::{MetaStore, Transaction};

pub type DatabaseId = u32;
pub type SchemaId = u32;
pub type TableId = u32;
pub type SourceId = u32;
pub type RelationId = u32;

pub type Catalog = (Vec<Database>, Vec<Schema>, Vec<Table>, Vec<Source>);

pub struct CatalogManager<S: MetaStore> {
    env: MetaSrvEnv<S>,
    core: Mutex<CatalogManagerCore<S>>,
}

pub type CatalogManagerRef<S> = Arc<CatalogManager<S>>;

impl<S> CatalogManager<S>
where
    S: MetaStore,
{
    pub async fn new(env: MetaSrvEnv<S>) -> Result<Self> {
        let catalog_manager = Self {
            core: Mutex::new(CatalogManagerCore::new(env.clone()).await?),
            env,
        };
        catalog_manager.init().await?;
        Ok(catalog_manager)
    }

    // Create default database and schema.
    async fn init(&self) -> Result<()> {
        let mut database = Database {
            name: DEFAULT_DATABASE_NAME.to_string(),
            ..Default::default()
        };
        if !self.core.lock().await.has_database(&database) {
            database.id = self
                .env
                .id_gen_manager()
                .generate::<{ IdCategory::Database }>()
                .await? as u32;
            self.create_database(&database).await?;
        }
        let databases = Database::list(self.env.meta_store())
            .await?
            .into_iter()
            .filter(|db| db.name == DEFAULT_DATABASE_NAME)
            .collect::<Vec<Database>>();
        assert_eq!(1, databases.len());

        let mut schema = Schema {
            name: DEFAULT_SCHEMA_NAME.to_string(),
            database_id: databases[0].id,
            ..Default::default()
        };
        if !self.core.lock().await.has_schema(&schema) {
            schema.id = self
                .env
                .id_gen_manager()
                .generate::<{ IdCategory::Schema }>()
                .await? as u32;
            self.create_schema(&schema).await?;
        }
        Ok(())
    }

    /// Used in `NotificationService::subscribe`.
    /// Need to pay attention to the order of acquiring locks to prevent deadlock problems.
    pub async fn get_catalog_core_guard(&self) -> MutexGuard<'_, CatalogManagerCore<S>> {
        self.core.lock().await
    }

    pub async fn get_catalog(&self) -> Result<Catalog> {
        let core = self.core.lock().await;
        core.get_catalog().await
    }

    pub async fn create_database(&self, database: &Database) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        if !core.has_database(database) {
            database.insert(self.env.meta_store()).await?;
            core.add_database(database);

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Add, &Info::DatabaseV2(database.to_owned()))
                .await
                .into_inner();

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "database already exists".to_string(),
            )))
        }
    }

    pub async fn drop_database(&self, database_id: DatabaseId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let database = Database::select(self.env.meta_store(), &database_id).await?;
        if let Some(database) = database {
            Database::delete(self.env.meta_store(), &database_id).await?;
            core.drop_database(&database);

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Delete, &Info::DatabaseV2(database))
                .await
                .into_inner();

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "database doesn't exist".to_string(),
            )))
        }
    }

    pub async fn create_schema(&self, schema: &Schema) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        if !core.has_schema(schema) {
            schema.insert(self.env.meta_store()).await?;
            core.add_schema(schema);

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Add, &Info::SchemaV2(schema.to_owned()))
                .await
                .into_inner();

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "schema already exists".to_string(),
            )))
        }
    }

    pub async fn drop_schema(&self, schema_id: SchemaId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let schema = Schema::select(self.env.meta_store(), &schema_id).await?;
        if let Some(schema) = schema {
            Schema::delete(self.env.meta_store(), &schema_id).await?;
            core.drop_schema(&schema);

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Delete, &Info::SchemaV2(schema))
                .await
                .into_inner();

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "schema doesn't exist".to_string(),
            )))
        }
    }

    pub async fn start_create_table_procedure(&self, table: &Table) -> Result<()> {
        let mut core = self.core.lock().await;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            for &dependent_relation_id in &table.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            Err(RwError::from(InternalError(
                "table already exists or in creating procedure".to_string(),
            )))
        }
    }

    pub async fn finish_create_table_procedure(&self, table: &Table) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            table.insert(self.env.meta_store()).await?;
            core.add_table(table);

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Add, &Info::TableV2(table.to_owned()))
                .await
                .into_inner();

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "table already exist or not in creating procedure".to_string(),
            )))
        }
    }

    pub async fn cancel_create_table_procedure(&self, table: &Table) -> Result<()> {
        let mut core = self.core.lock().await;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            for &dependent_relation_id in &table.dependent_relations {
                core.decrease_ref_count(dependent_relation_id);
            }
            Ok(())
        } else {
            Err(RwError::from(InternalError(
                "table already exist or not in creating procedure".to_string(),
            )))
        }
    }

    pub async fn create_table(&self, table: &Table) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        if !core.has_table(table) {
            table.insert(self.env.meta_store()).await?;
            core.add_table(table);
            for &dependent_relation_id in &table.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Add, &Info::TableV2(table.to_owned()))
                .await
                .into_inner();

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "table already exists".to_string(),
            )))
        }
    }

    pub async fn drop_table(&self, table_id: TableId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let table = Table::select(self.env.meta_store(), &table_id).await?;
        if let Some(table) = table {
            match core.get_ref_count(table_id) {
                Some(ref_count) => Err(CatalogError(
                    anyhow!(
                        "Fail to delete table `{}` because {} other relation(s) depend on it.",
                        table.name,
                        ref_count
                    )
                    .into(),
                )
                .into()),
                None => {
                    Table::delete(self.env.meta_store(), &table_id).await?;
                    core.drop_table(&table);
                    for &dependent_relation_id in &table.dependent_relations {
                        core.decrease_ref_count(dependent_relation_id);
                    }

                    let version = self
                        .env
                        .notification_manager()
                        .notify_frontend(Operation::Delete, &Info::TableV2(table))
                        .await
                        .into_inner();

                    Ok(version)
                }
            }
        } else {
            Err(RwError::from(InternalError(
                "table doesn't exist".to_string(),
            )))
        }
    }

    pub async fn start_create_source_procedure(&self, source: &Source) -> Result<()> {
        let mut core = self.core.lock().await;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            Ok(())
        } else {
            Err(RwError::from(InternalError(
                "source already exists or in creating procedure".to_string(),
            )))
        }
    }

    pub async fn finish_create_source_procedure(&self, source: &Source) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            source.insert(self.env.meta_store()).await?;
            core.add_source(source);

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Add, &Info::Source(source.to_owned()))
                .await
                .into_inner();

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "source already exist or not in creating procedure".to_string(),
            )))
        }
    }

    pub async fn cancel_create_source_procedure(&self, source: &Source) -> Result<()> {
        let mut core = self.core.lock().await;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            Ok(())
        } else {
            Err(RwError::from(InternalError(
                "source already exist or not in creating procedure".to_string(),
            )))
        }
    }

    pub async fn create_source(&self, source: &Source) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        if !core.has_source(source) {
            source.insert(self.env.meta_store()).await?;
            core.add_source(source);

            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Add, &Info::Source(source.to_owned()))
                .await
                .into_inner();

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "source already exists".to_string(),
            )))
        }
    }

    pub async fn drop_source(&self, source_id: SourceId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let source = Source::select(self.env.meta_store(), &source_id).await?;
        if let Some(source) = source {
            match core.get_ref_count(source_id) {
                Some(ref_count) => Err(CatalogError(
                    anyhow!(
                        "Fail to delete source `{}` because {} other relation(s) depend on it.",
                        source.name,
                        ref_count
                    )
                    .into(),
                )
                .into()),
                None => {
                    Source::delete(self.env.meta_store(), &source_id).await?;
                    core.drop_source(&source);

                    let version = self
                        .env
                        .notification_manager()
                        .notify_frontend(Operation::Delete, &Info::Source(source))
                        .await
                        .into_inner();

                    Ok(version)
                }
            }
        } else {
            Err(RwError::from(InternalError(
                "source doesn't exist".to_string(),
            )))
        }
    }

    pub async fn start_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
    ) -> Result<()> {
        let mut core = self.core.lock().await;
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
            Err(RwError::from(InternalError(
                "source or table already exist".to_string(),
            )))
        }
    }

    pub async fn finish_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
    ) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
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
            core.env.meta_store().txn(transaction).await?;
            core.add_source(source);
            core.add_table(mview);

            self.env
                .notification_manager()
                .notify_frontend(Operation::Add, &Info::TableV2(mview.to_owned()))
                .await;
            // Currently frontend uses source's version
            let version = self
                .env
                .notification_manager()
                .notify_frontend(Operation::Add, &Info::Source(source.to_owned()))
                .await
                .into_inner();
            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "source already exist or not in creating procedure".to_string(),
            )))
        }
    }

    pub async fn cancel_create_materialized_source_procedure(
        &self,
        source: &Source,
        mview: &Table,
    ) -> Result<()> {
        let mut core = self.core.lock().await;
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
            Err(RwError::from(InternalError(
                "source already exist or not in creating procedure".to_string(),
            )))
        }
    }

    pub async fn drop_materialized_source(
        &self,
        source_id: SourceId,
        mview_id: TableId,
    ) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let mview = Table::select(self.env.meta_store(), &mview_id).await?;
        let source = Source::select(self.env.meta_store(), &source_id).await?;
        match (mview, source) {
            (Some(mview), Some(source)) => {
                // decrease associated source's ref count first to avoid deadlock
                if let Some(OptionalAssociatedSourceId::AssociatedSourceId(associated_source_id)) =
                    mview.optional_associated_source_id
                {
                    if associated_source_id != source_id {
                        return Err(RwError::from(InternalError(
                            "mview's associated source id doesn't match source id".to_string(),
                        )));
                    }
                } else {
                    return Err(RwError::from(InternalError(
                        "mview do not have associated source id".to_string(),
                    )));
                }
                // check ref count
                if let Some(ref_count) = core.get_ref_count(mview_id) {
                    return Err(CatalogError(
                        anyhow!(
                            "Fail to delete table `{}` because {} other relation(s) depend on it.",
                            mview.name,
                            ref_count
                        )
                        .into(),
                    )
                    .into());
                }
                if let Some(ref_count) = core.get_ref_count(source_id) {
                    return Err(CatalogError(
                        anyhow!(
                            "Fail to delete source `{}` because {} other relation(s) depend on it.",
                            source.name,
                            ref_count
                        )
                        .into(),
                    )
                    .into());
                }

                // now is safe to delete both mview and source
                let mut transaction = Transaction::default();
                mview.delete_in_transaction(&mut transaction)?;
                source.delete_in_transaction(&mut transaction)?;
                core.env.meta_store().txn(transaction).await?;
                core.drop_table(&mview);
                core.drop_source(&source);
                for &dependent_relation_id in &mview.dependent_relations {
                    core.decrease_ref_count(dependent_relation_id);
                }

                self.env
                    .notification_manager()
                    .notify_frontend(Operation::Delete, &Info::TableV2(mview))
                    .await;
                let version = self
                    .env
                    .notification_manager()
                    .notify_frontend(Operation::Delete, &Info::Source(source))
                    .await
                    .into_inner();
                Ok(version)
            }

            _ => Err(RwError::from(InternalError(
                "table or source doesn't exist".to_string(),
            ))),
        }
    }
}

type DatabaseKey = String;
type SchemaKey = (DatabaseId, String);
type TableKey = (DatabaseId, SchemaId, String);
type SourceKey = (DatabaseId, SchemaId, String);
type RelationKey = (DatabaseId, SchemaId, String);

/// [`CatalogManagerCore`] caches meta catalog information and maintains dependent relationship
/// between tables.
pub struct CatalogManagerCore<S: MetaStore> {
    env: MetaSrvEnv<S>,
    /// Cached database key information.
    databases: HashSet<DatabaseKey>,
    /// Cached schema key information.
    schemas: HashSet<SchemaKey>,
    /// Cached source key information.
    sources: HashSet<SourceKey>,
    /// Cached table key information.
    tables: HashSet<TableKey>,
    /// Relation refer count mapping.
    relation_ref_count: HashMap<RelationId, usize>,

    // In-progress creation tracker
    in_progress_creation_tracker: HashSet<RelationKey>,
}

impl<S> CatalogManagerCore<S>
where
    S: MetaStore,
{
    async fn new(env: MetaSrvEnv<S>) -> Result<Self> {
        let databases = Database::list(env.meta_store()).await?;
        let schemas = Schema::list(env.meta_store()).await?;
        let sources = Source::list(env.meta_store()).await?;
        let tables = Table::list(env.meta_store()).await?;

        let mut relation_ref_count = HashMap::new();

        let databases = HashSet::from_iter(databases.into_iter().map(|database| (database.name)));
        let schemas = HashSet::from_iter(
            schemas
                .into_iter()
                .map(|schema| (schema.database_id, schema.name)),
        );
        let sources = HashSet::from_iter(
            sources
                .into_iter()
                .map(|source| (source.database_id, source.schema_id, source.name)),
        );
        let tables = HashSet::from_iter(tables.into_iter().map(|table| {
            for depend_relation_id in &table.dependent_relations {
                relation_ref_count.entry(*depend_relation_id).or_insert(0);
            }
            (table.database_id, table.schema_id, table.name)
        }));

        let in_progress_creation_tracker = HashSet::new();

        Ok(Self {
            env,
            databases,
            schemas,
            sources,
            tables,
            relation_ref_count,
            in_progress_creation_tracker,
        })
    }

    pub async fn get_catalog(&self) -> Result<Catalog> {
        Ok((
            Database::list(self.env.meta_store()).await?,
            Schema::list(self.env.meta_store()).await?,
            Table::list(self.env.meta_store()).await?,
            Source::list(self.env.meta_store()).await?,
        ))
    }

    pub async fn list_sources(&self) -> Result<Vec<Source>> {
        Source::list(self.env.meta_store()).await
    }

    fn has_database(&self, database: &Database) -> bool {
        self.databases.contains(database.get_name())
    }

    fn add_database(&mut self, database: &Database) {
        self.databases.insert(database.name.clone());
    }

    fn drop_database(&mut self, database: &Database) -> bool {
        self.databases.remove(database.get_name())
    }

    fn has_schema(&self, schema: &Schema) -> bool {
        self.schemas
            .contains(&(schema.database_id, schema.name.clone()))
    }

    fn add_schema(&mut self, schema: &Schema) {
        self.schemas
            .insert((schema.database_id, schema.name.clone()));
    }

    fn drop_schema(&mut self, schema: &Schema) -> bool {
        self.schemas
            .remove(&(schema.database_id, schema.name.clone()))
    }

    fn has_table(&self, table: &Table) -> bool {
        self.tables
            .contains(&(table.database_id, table.schema_id, table.name.clone()))
    }

    fn add_table(&mut self, table: &Table) {
        self.tables
            .insert((table.database_id, table.schema_id, table.name.clone()));
    }

    fn drop_table(&mut self, table: &Table) -> bool {
        self.tables
            .remove(&(table.database_id, table.schema_id, table.name.clone()))
    }

    fn has_source(&self, source: &Source) -> bool {
        self.sources
            .contains(&(source.database_id, source.schema_id, source.name.clone()))
    }

    fn add_source(&mut self, source: &Source) {
        self.sources
            .insert((source.database_id, source.schema_id, source.name.clone()));
    }

    fn drop_source(&mut self, source: &Source) -> bool {
        self.sources
            .remove(&(source.database_id, source.schema_id, source.name.clone()))
    }

    pub async fn get_source(&self, id: SourceId) -> Result<Option<Source>> {
        Source::select(self.env.meta_store(), &id).await
    }

    fn get_ref_count(&self, relation_id: RelationId) -> Option<usize> {
        self.relation_ref_count.get(&relation_id).cloned()
    }

    fn increase_ref_count(&mut self, relation_id: RelationId) {
        *self.relation_ref_count.entry(relation_id).or_insert(0) += 1;
    }

    fn decrease_ref_count(&mut self, relation_id: RelationId) {
        match self.relation_ref_count.entry(relation_id) {
            Entry::Occupied(mut o) => {
                *o.get_mut() -= 1;
                if *o.get() == 0 {
                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }

    fn has_in_progress_creation(&self, relation: &RelationKey) -> bool {
        self.in_progress_creation_tracker
            .contains(&relation.clone())
    }

    fn mark_creating(&mut self, relation: &RelationKey) {
        self.in_progress_creation_tracker.insert(relation.clone());
    }

    fn unmark_creating(&mut self, relation: &RelationKey) {
        self.in_progress_creation_tracker.remove(&relation.clone());
    }
}
