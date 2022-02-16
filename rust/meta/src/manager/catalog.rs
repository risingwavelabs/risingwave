use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use risingwave_common::catalog::{CatalogId, DatabaseId, SchemaId, TableId};
use risingwave_common::error::ErrorCode::MetaError;
use risingwave_common::error::Result;
use risingwave_pb::meta::{Database, Schema, Table};
use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};
use tokio::sync::Mutex;

use crate::model::{Catalog, MetadataModel};
use crate::storage::MetaStoreRef;

/// [`CatalogManagerCore`] maintains dependent relationship between tables
pub struct CatalogManagerCore {
    databases: HashMap<DatabaseId, Database>,
    schemas: HashMap<SchemaId, Schema>,
    tables: HashMap<TableId, Table>,
    table_ref_count: HashMap<TableId, usize>,
}

impl CatalogManagerCore {
    pub async fn new(meta_store: &MetaStoreRef) -> Result<Self> {
        let catalog = Catalog::get(meta_store).await?;
        let databases = HashMap::from_iter(
            catalog
                .0
                .databases
                .into_iter()
                .map(|database| (DatabaseId::from(&database.database_ref_id), database)),
        );
        let schemas = HashMap::from_iter(
            catalog
                .0
                .schemas
                .into_iter()
                .map(|schema| (SchemaId::from(&schema.schema_ref_id), schema)),
        );
        let tables = HashMap::from_iter(
            catalog
                .0
                .tables
                .into_iter()
                .map(|table| (TableId::from(&table.table_ref_id), table)),
        );
        Ok(Self {
            databases,
            schemas,
            tables,
            table_ref_count: HashMap::new(),
        })
    }

    pub fn add_database(&mut self, database: Database) -> Option<DatabaseId> {
        let database_id = DatabaseId::from(&database.database_ref_id);
        if self.databases.contains_key(&database_id) {
            None
        } else {
            // ??
            self.databases.insert(database_id.clone(), database);
            Some(database_id)
        }
    }

    pub fn delete_database(&mut self, database_id: DatabaseId) -> Option<DatabaseId> {
        self.databases.remove(&database_id).map(|_| database_id)
    }

    pub fn add_schema(&mut self, schema: Schema) -> Option<SchemaId> {
        let schema_id = SchemaId::from(&schema.schema_ref_id);
        if self.schemas.contains_key(&schema_id) {
            None
        } else {
            self.schemas.insert(schema_id.clone(), schema);
            Some(schema_id)
        }
    }

    pub fn delete_schema(&mut self, schema_id: SchemaId) -> Option<SchemaId> {
        self.schemas.remove(&schema_id).map(|_| schema_id)
    }

    pub fn add_table(&mut self, table: Table) -> Option<TableId> {
        let table_id = TableId::from(&table.table_ref_id);
        if self.tables.contains_key(&table_id) {
            None
        } else {
            self.tables.insert(table_id.clone(), table);
            Some(table_id)
        }
    }

    pub fn delete_table(&mut self, table_id: TableId) -> Option<Vec<TableId>> {
        match self.tables.remove(&table_id) {
            Some(table) => Some(
                table
                    .dependent_tables
                    .into_iter()
                    .map(|table_ref_id| TableId::from(&Some(table_ref_id)))
                    .collect(),
            ),
            None => None,
        }
    }

    pub fn get_ref_count(&self, table_id: &TableId) -> Option<usize> {
        self.table_ref_count.get(table_id).cloned()
    }

    pub fn increase_dependency(&mut self, table_id: TableId) {
        *self.table_ref_count.entry(table_id).or_insert(0) += 1;
    }

    pub fn decrease_dependency(&mut self, table_id: TableId) {
        match self.table_ref_count.entry(table_id) {
            Entry::Occupied(mut o) => {
                *o.get_mut() -= 1;
                if o.get() == &0 {
                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => {}
        }
    }
}

pub struct StoredCatalogManager {
    core: Mutex<CatalogManagerCore>,
    meta_store_ref: MetaStoreRef,
}

pub type StoredCatalogManagerRef = Arc<StoredCatalogManager>;

impl StoredCatalogManager {
    pub async fn new(meta_store_ref: MetaStoreRef) -> Result<Self> {
        let core = Mutex::new(CatalogManagerCore::new(&meta_store_ref).await?);
        Ok(Self {
            core,
            meta_store_ref,
        })
    }

    pub async fn get_catalog(&self) -> Result<Catalog> {
        Catalog::get(&self.meta_store_ref).await
    }

    pub async fn create_database(&self, database: Database) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        if core.add_database(database.clone()).is_some() {
            database.insert(&self.meta_store_ref).await?;
            Ok(Some(CatalogId::DatabaseId(DatabaseId::from(
                &database.database_ref_id,
            ))))
        } else {
            Ok(None)
        }
    }

    pub async fn delete_database(
        &self,
        database_ref_id: &DatabaseRefId,
    ) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        let database_id = DatabaseId::from(&Some(database_ref_id.clone()));
        if let Some(database_id) = core.delete_database(database_id) {
            Database::delete(&self.meta_store_ref, database_ref_id).await?;
            Ok(Some(CatalogId::DatabaseId(database_id)))
        } else {
            Ok(None)
        }
    }

    pub async fn create_schema(&self, schema: Schema) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        if core.add_schema(schema.clone()).is_some() {
            schema.insert(&self.meta_store_ref).await?;
            Ok(Some(CatalogId::SchemaId(SchemaId::from(
                &schema.schema_ref_id,
            ))))
        } else {
            Ok(None)
        }
    }

    pub async fn delete_schema(&self, schema_ref_id: &SchemaRefId) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        let schema_id = SchemaId::from(&Some(schema_ref_id.clone()));
        if let Some(schema_id) = core.delete_schema(schema_id) {
            Schema::delete(&self.meta_store_ref, schema_ref_id).await?;
            Ok(Some(CatalogId::SchemaId(schema_id)))
        } else {
            Ok(None)
        }
    }

    pub async fn create_table(&self, table: Table) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        if core.add_table(table.clone()).is_some() {
            table.insert(&self.meta_store_ref).await?;
            for table_ref_id in table.dependent_tables {
                let table_id = TableId::from(&Some(table_ref_id.clone()));
                core.increase_dependency(table_id);
            }
            Ok(Some(CatalogId::TableId(TableId::from(&table.table_ref_id))))
        } else {
            Ok(None)
        }
    }

    pub async fn delete_table(&self, table_ref_id: &TableRefId) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        let table_id = TableId::from(&Some(table_ref_id.clone()));
        match core.get_ref_count(&table_id) {
            Some(ref_count) => Err(MetaError(format!(
                "Fail to delete table {} because {} other table(s) depends on it.",
                table_id.table_id(),
                ref_count
            ))
            .into()),
            None => {
                if let Some(dependent_tables) = core.delete_table(table_id.clone()) {
                    Table::delete(&self.meta_store_ref, table_ref_id).await?;
                    for table_id in dependent_tables {
                        core.decrease_dependency(table_id);
                    }
                    Ok(Some(CatalogId::TableId(table_id)))
                } else {
                    Ok(None)
                }
            }
        }
    }
}
