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
//
#![allow(dead_code)]
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::option::Option::Some;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::ErrorCode::{CatalogError, InternalError};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::{Database, Schema, Source, Table};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use tokio::sync::Mutex;

use crate::manager::NotificationManagerRef;
use crate::model::{CatalogVersionGenerator, MetadataModel};
use crate::storage::MetaStore;

pub type DatabaseId = u32;
pub type SchemaId = u32;
pub type TableId = u32;
pub type SourceId = u32;
pub type RelationId = u32;

pub type Catalog = (Vec<Database>, Vec<Schema>, Vec<Table>, Vec<Source>);

pub struct CatalogManager<S> {
    core: Mutex<CatalogManagerCore<S>>,
    meta_store_ref: Arc<S>,
    nm: NotificationManagerRef,
}

pub type CatalogManagerRef<S> = Arc<CatalogManager<S>>;

impl<S> CatalogManager<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store_ref: Arc<S>, nm: NotificationManagerRef) -> Result<Self> {
        Ok(Self {
            core: Mutex::new(CatalogManagerCore::new(meta_store_ref.clone()).await?),
            meta_store_ref,
            nm,
        })
    }

    pub async fn get_catalog(&self) -> Result<Catalog> {
        let core = self.core.lock().await;
        core.get_catalog().await
    }

    pub async fn create_database(&self, database: &Database) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        if !core.has_database(database) {
            let version = core.new_version_id().await?;
            database.insert(&*self.meta_store_ref).await?;
            core.add_database(database);

            self.nm
                .notify_fe(Operation::Add, &Info::DatabaseV2(database.to_owned()))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "database already exists".to_string(),
            )))
        }
    }

    pub async fn drop_database(&self, database_id: DatabaseId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let database = Database::select(&*self.meta_store_ref, &database_id).await?;
        if let Some(database) = database {
            let version = core.new_version_id().await?;
            Database::delete(&*self.meta_store_ref, &database_id).await?;
            core.drop_database(&database);

            self.nm
                .notify_fe(Operation::Delete, &Info::DatabaseV2(database))
                .await;

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
            let version = core.new_version_id().await?;
            schema.insert(&*self.meta_store_ref).await?;
            core.add_schema(schema);

            self.nm
                .notify_fe(Operation::Add, &Info::SchemaV2(schema.to_owned()))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "schema already exists".to_string(),
            )))
        }
    }

    pub async fn drop_schema(&self, schema_id: SchemaId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let schema = Schema::select(&*self.meta_store_ref, &schema_id).await?;
        if let Some(schema) = schema {
            let version = core.new_version_id().await?;
            Schema::delete(&*self.meta_store_ref, &schema_id).await?;
            core.drop_schema(&schema);

            self.nm
                .notify_fe(Operation::Delete, &Info::SchemaV2(schema))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "schema doesn't exist".to_string(),
            )))
        }
    }

    pub async fn start_create_table_process(&self, table: &Table) -> Result<()> {
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
                "table already exists or in progress creation".to_string(),
            )))
        }
    }

    pub async fn finish_create_table_process(&self, table: &Table) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let key = (table.database_id, table.schema_id, table.name.clone());
        if !core.has_table(table) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            let version = core.new_version_id().await?;
            table.insert(&*self.meta_store_ref).await?;
            core.add_table(table);

            self.nm
                .notify_fe(Operation::Add, &Info::TableV2(table.to_owned()))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "table already exist or not in progress creation".to_string(),
            )))
        }
    }

    pub async fn cancel_create_table_process(&self, table: &Table) -> Result<()> {
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
                "table already exist or not in progress creation".to_string(),
            )))
        }
    }

    pub async fn create_table(&self, table: &Table) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        if !core.has_table(table) {
            let version = core.new_version_id().await?;
            table.insert(&*self.meta_store_ref).await?;
            core.add_table(table);
            for &dependent_relation_id in &table.dependent_relations {
                core.increase_ref_count(dependent_relation_id);
            }

            self.nm
                .notify_fe(Operation::Add, &Info::TableV2(table.to_owned()))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "table already exists".to_string(),
            )))
        }
    }

    pub async fn drop_table(&self, table_id: TableId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let table = Table::select(&*self.meta_store_ref, &table_id).await?;
        if let Some(table) = table {
            match core.get_ref_count(table_id) {
                Some(ref_count) => Err(CatalogError(
                    anyhow!(
                        "Fail to delete table {} because {} other table(s) depends on it.",
                        table_id,
                        ref_count
                    )
                    .into(),
                )
                .into()),
                None => {
                    let version = core.new_version_id().await?;
                    Table::delete(&*self.meta_store_ref, &table_id).await?;
                    core.drop_table(&table);
                    for &dependent_relation_id in &table.dependent_relations {
                        core.decrease_ref_count(dependent_relation_id);
                    }

                    self.nm
                        .notify_fe(Operation::Delete, &Info::TableV2(table))
                        .await;

                    Ok(version)
                }
            }
        } else {
            Err(RwError::from(InternalError(
                "table doesn't exist".to_string(),
            )))
        }
    }

    pub async fn start_create_source_process(&self, source: &Source) -> Result<()> {
        let mut core = self.core.lock().await;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && !core.has_in_progress_creation(&key) {
            core.mark_creating(&key);
            Ok(())
        } else {
            Err(RwError::from(InternalError(
                "source already exists or in progress creation".to_string(),
            )))
        }
    }

    pub async fn finish_create_source_process(&self, source: &Source) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            let version = core.new_version_id().await?;
            source.insert(&*self.meta_store_ref).await?;
            core.add_source(source);

            self.nm
                .notify_fe(Operation::Add, &Info::Source(source.to_owned()))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "source already exist or not in progress creation".to_string(),
            )))
        }
    }

    pub async fn cancel_create_source_process(&self, source: &Source) -> Result<()> {
        let mut core = self.core.lock().await;
        let key = (source.database_id, source.schema_id, source.name.clone());
        if !core.has_source(source) && core.has_in_progress_creation(&key) {
            core.unmark_creating(&key);
            Ok(())
        } else {
            Err(RwError::from(InternalError(
                "source already exist or not in progress creation".to_string(),
            )))
        }
    }

    pub async fn create_source(&self, source: &Source) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        if !core.has_source(source) {
            let version = core.new_version_id().await?;
            source.insert(&*self.meta_store_ref).await?;
            core.add_source(source);

            self.nm
                .notify_fe(Operation::Add, &Info::Source(source.to_owned()))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "source already exists".to_string(),
            )))
        }
    }

    pub async fn drop_source(&self, source_id: SourceId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let source = Source::select(&*self.meta_store_ref, &source_id).await?;
        if let Some(source) = source {
            match core.get_ref_count(source_id) {
                Some(ref_count) => Err(CatalogError(
                    anyhow!(
                        "Fail to delete source {} because {} other table(s) depends on it.",
                        source_id,
                        ref_count
                    )
                    .into(),
                )
                .into()),
                None => {
                    let version = core.new_version_id().await?;
                    Source::delete(&*self.meta_store_ref, &source_id).await?;
                    core.drop_source(&source);

                    self.nm
                        .notify_fe(Operation::Delete, &Info::Source(source))
                        .await;

                    Ok(version)
                }
            }
        } else {
            Err(RwError::from(InternalError(
                "source doesn't exist".to_string(),
            )))
        }
    }
}

type DatabaseKey = DatabaseId;
type SchemaKey = (DatabaseId, String);
type TableKey = (DatabaseId, SchemaId, String);
type SourceKey = (DatabaseId, SchemaId, String);
type RelationKey = (DatabaseId, SchemaId, String);

/// [`CatalogManagerCore`] caches meta catalog information and maintains dependent relationship
/// between tables.
struct CatalogManagerCore<S> {
    meta_store_ref: Arc<S>,
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

    /// Catalog version generator.
    version_generator: CatalogVersionGenerator,
    // In-progress creation tracker
    in_progress_creation_tracker: HashSet<RelationKey>,
}

impl<S> CatalogManagerCore<S>
where
    S: MetaStore,
{
    async fn new(meta_store_ref: Arc<S>) -> Result<Self> {
        let databases = Database::list(&*meta_store_ref).await?;
        let schemas = Schema::list(&*meta_store_ref).await?;
        let sources = Source::list(&*meta_store_ref).await?;
        let tables = Table::list(&*meta_store_ref).await?;

        let mut relation_ref_count = HashMap::new();

        let databases = HashSet::from_iter(databases.into_iter().map(|database| (database.id)));
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

        let version_generator = CatalogVersionGenerator::new(&*meta_store_ref).await?;
        let in_progress_creation_tracker = HashSet::new();

        Ok(Self {
            meta_store_ref,
            databases,
            schemas,
            sources,
            tables,
            relation_ref_count,
            version_generator,
            in_progress_creation_tracker,
        })
    }

    async fn new_version_id(&mut self) -> Result<CatalogVersion> {
        let version = self.version_generator.next();
        self.version_generator.insert(&*self.meta_store_ref).await?;
        Ok(version)
    }

    async fn get_catalog(&self) -> Result<Catalog> {
        Ok((
            Database::list(&*self.meta_store_ref).await?,
            Schema::list(&*self.meta_store_ref).await?,
            Table::list(&*self.meta_store_ref).await?,
            Source::list(&*self.meta_store_ref).await?,
        ))
    }

    fn has_database(&self, database: &Database) -> bool {
        self.databases.contains(&database.id)
    }

    fn add_database(&mut self, database: &Database) {
        self.databases.insert(database.id);
    }

    fn drop_database(&mut self, database: &Database) -> bool {
        self.databases.remove(&database.id)
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
