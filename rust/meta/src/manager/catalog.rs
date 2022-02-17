use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::catalog::{CatalogId, DatabaseId, SchemaId, TableId};
use risingwave_common::error::ErrorCode::CatalogError;
use risingwave_common::error::Result;
use risingwave_pb::meta::{Database, Schema, Table};
use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};
use tokio::sync::Mutex;

use crate::model::{Catalog, MetadataModel};
use crate::storage::MetaStoreRef;

/// [`StoredCatalogManager`] manages meta operations including retrieving catalog info, creating
/// a table and dropping a table. Besides, it contains a cache for meta info in the `core`.
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
        let database_id = DatabaseId::from(&database.database_ref_id);
        if !core.has_database(&database_id) {
            database.insert(&self.meta_store_ref).await?;
            core.add_database(database);
            Ok(Some(CatalogId::DatabaseId(database_id)))
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
        if core.has_database(&database_id) {
            Database::delete(&self.meta_store_ref, database_ref_id).await?;
            core.delete_database(&database_id);
            Ok(Some(CatalogId::DatabaseId(database_id)))
        } else {
            Ok(None)
        }
    }

    pub async fn create_schema(&self, schema: Schema) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        let schema_id = SchemaId::from(&schema.schema_ref_id);
        if !core.has_schema(&schema_id) {
            schema.insert(&self.meta_store_ref).await?;
            core.add_schema(schema);
            Ok(Some(CatalogId::SchemaId(schema_id)))
        } else {
            Ok(None)
        }
    }

    pub async fn delete_schema(&self, schema_ref_id: &SchemaRefId) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        let schema_id = SchemaId::from(&Some(schema_ref_id.clone()));
        if core.has_schema(&schema_id) {
            Schema::delete(&self.meta_store_ref, schema_ref_id).await?;
            core.delete_schema(&schema_id);
            Ok(Some(CatalogId::SchemaId(schema_id)))
        } else {
            Ok(None)
        }
    }

    pub async fn create_table(&self, table: Table) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        let table_id = TableId::from(&table.table_ref_id);
        if !core.has_table(&table_id) {
            table.insert(&self.meta_store_ref).await?;
            core.add_table(table.clone());
            for table_ref_id in table.dependent_tables {
                let dependent_table_id = TableId::from(&Some(table_ref_id.clone()));
                core.increase_ref_count(dependent_table_id);
            }
            Ok(Some(CatalogId::TableId(table_id)))
        } else {
            Ok(None)
        }
    }

    pub async fn delete_table(&self, table_ref_id: &TableRefId) -> Result<Option<CatalogId>> {
        let mut core = self.core.lock().await;
        let table_id = TableId::from(&Some(table_ref_id.clone()));
        if core.has_table(&table_id) {
            match core.get_ref_count(&table_id) {
                Some(ref_count) => Err(CatalogError(
                    anyhow!(
                        "Fail to delete table {} because {} other table(s) depends on it.",
                        table_id.table_id(),
                        ref_count
                    )
                    .into(),
                )
                .into()),
                None => {
                    Table::delete(&self.meta_store_ref, table_ref_id).await?;
                    let dependent_tables = core.delete_table(&table_id);
                    for dependent_table_id in dependent_tables {
                        core.decrease_ref_count(dependent_table_id);
                    }
                    Ok(Some(CatalogId::TableId(table_id)))
                }
            }
        } else {
            Ok(None)
        }
    }
}

/// [`CatalogManagerCore`] caches meta catalog information and maintains dependent relationship
/// between tables.
struct CatalogManagerCore {
    databases: HashMap<DatabaseId, Database>,
    schemas: HashMap<SchemaId, Schema>,
    tables: HashMap<TableId, Table>,
    table_ref_count: HashMap<TableId, usize>,
}

impl CatalogManagerCore {
    async fn new(meta_store: &MetaStoreRef) -> Result<Self> {
        let catalog = Catalog::get(meta_store).await?;
        let mut table_ref_count = HashMap::new();
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
        let tables = HashMap::from_iter(catalog.0.tables.into_iter().map(|table| {
            let dependencies = table.get_dependent_tables();
            for table_ref_id in dependencies {
                let table_id = TableId::from(&Some(table_ref_id.clone()));
                *table_ref_count.entry(table_id).or_insert(0) += 1;
            }
            (TableId::from(&table.table_ref_id), table)
        }));
        Ok(Self {
            databases,
            schemas,
            tables,
            table_ref_count,
        })
    }

    fn has_database(&self, database_id: &DatabaseId) -> bool {
        self.databases.contains_key(database_id)
    }

    fn add_database(&mut self, database: Database) {
        let database_id = DatabaseId::from(&database.database_ref_id);
        self.databases.insert(database_id, database);
    }

    fn delete_database(&mut self, database_id: &DatabaseId) {
        self.databases.remove(database_id);
    }

    fn has_schema(&self, schema_id: &SchemaId) -> bool {
        self.schemas.contains_key(schema_id)
    }

    fn add_schema(&mut self, schema: Schema) {
        let schema_id = SchemaId::from(&schema.schema_ref_id);
        self.schemas.insert(schema_id, schema);
    }

    fn delete_schema(&mut self, schema_id: &SchemaId) {
        self.schemas.remove(schema_id);
    }

    fn has_table(&self, table_id: &TableId) -> bool {
        self.tables.contains_key(table_id)
    }

    fn add_table(&mut self, table: Table) {
        let table_id = TableId::from(&table.table_ref_id);
        self.tables.insert(table_id, table);
    }

    fn delete_table(&mut self, table_id: &TableId) -> Vec<TableId> {
        match self.tables.remove(table_id) {
            Some(table) => table
                .dependent_tables
                .into_iter()
                .map(|table_ref_id| TableId::from(&Some(table_ref_id)))
                .collect(),
            None => unreachable!(),
        }
    }

    fn get_ref_count(&self, table_id: &TableId) -> Option<usize> {
        self.table_ref_count.get(table_id).cloned()
    }

    fn increase_ref_count(&mut self, table_id: TableId) {
        *self.table_ref_count.entry(table_id).or_insert(0) += 1;
    }

    fn decrease_ref_count(&mut self, table_id: TableId) {
        match self.table_ref_count.entry(table_id) {
            Entry::Occupied(mut o) => {
                *o.get_mut() -= 1;
                if o.get() == &0 {
                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }
}
