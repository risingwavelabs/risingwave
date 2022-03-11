#![allow(dead_code)]
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::option::Option::Some;
use std::sync::Arc;

use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_pb::catalog::{Database, Schema, Source, Table};
use tokio::sync::Mutex;

use crate::manager::NotificationManagerRef;
use crate::model::{CatalogVersionGenerator, MetadataModel};
use crate::storage::MetaStore;

pub type DatabaseId = i32;
pub type SchemaId = i32;
pub type TableId = i32;
pub type SourceId = i32;

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

            // TODO: Notify frontends to create database.
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

            // TODO: Notify frontends to drop database.
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

            // TODO: Notify frontends to create schema.
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

            // TODO: Notify frontends to drop schema.
            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "schema doesn't exist".to_string(),
            )))
        }
    }

    pub async fn create_table(&self, table: &Table) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        if !core.has_table(table) {
            let version = core.new_version_id().await?;
            table.insert(&*self.meta_store_ref).await?;
            core.add_table(table);

            // TODO: Notify frontends to create schema.
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
            let version = core.new_version_id().await?;
            Table::delete(&*self.meta_store_ref, &table_id).await?;
            core.drop_table(&table);

            // TODO: Notify frontends to drop table.
            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "table doesn't exist".to_string(),
            )))
        }
    }

    pub async fn create_source(&self, source: &Source) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        if !core.has_source(source) {
            let version = core.new_version_id().await?;
            source.insert(&*self.meta_store_ref).await?;
            core.add_source(source);

            // TODO: Notify frontends to create source.
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
            let version = core.new_version_id().await?;
            Source::delete(&*self.meta_store_ref, &source_id).await?;
            core.drop_source(&source);

            // TODO: Notify frontends to drop source.
            Ok(version)
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
    /// Table refer count mapping.
    table_ref_count: HashMap<TableId, usize>,

    /// Catalog version generator.
    version_generator: CatalogVersionGenerator,
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

        let mut table_ref_count = HashMap::new();

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
            for depend_table_id in &table.dependent_tables {
                table_ref_count.entry(*depend_table_id).or_insert(0);
            }
            (table.database_id, table.schema_id, table.name)
        }));

        let version_generator = CatalogVersionGenerator::new(&*meta_store_ref).await?;

        Ok(Self {
            meta_store_ref,
            databases,
            schemas,
            sources,
            tables,
            table_ref_count,
            version_generator,
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

    fn get_ref_count(&self, table_id: TableId) -> Option<usize> {
        self.table_ref_count.get(&table_id).cloned()
    }

    fn increase_ref_count(&mut self, table_id: TableId) {
        *self.table_ref_count.entry(table_id).or_insert(0) += 1;
    }

    fn decrease_ref_count(&mut self, table_id: TableId) {
        match self.table_ref_count.entry(table_id) {
            Entry::Occupied(mut o) => {
                *o.get_mut() -= 1;
                if *o.get() == 0 {
                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }
}
