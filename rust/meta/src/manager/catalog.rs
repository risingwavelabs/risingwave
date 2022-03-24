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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use risingwave_common::catalog::{CatalogVersion, DatabaseId, SchemaId, TableId};
use risingwave_common::error::ErrorCode::{CatalogError, InternalError};
use risingwave_common::error::{Result, RwError};
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::table::Info as TableInfo;
use risingwave_pb::meta::{Catalog, Database, Schema, Table};
use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};
use tokio::sync::Mutex;

use super::NotificationManagerRef;
use crate::model::{CatalogVersionGenerator, MetadataModel};
use crate::storage::MetaStore;

/// [`StoredCatalogManager`] manages meta operations including retrieving catalog info, creating
/// a table and dropping a table. Besides, it contains a cache for meta info in the `core`.
pub struct StoredCatalogManager<S> {
    core: Mutex<CatalogManagerCore>,
    meta_store_ref: Arc<S>,
    nm: NotificationManagerRef,
}

pub type StoredCatalogManagerRef<S> = Arc<StoredCatalogManager<S>>;

impl<S> StoredCatalogManager<S>
where
    S: MetaStore,
{
    pub async fn new(meta_store_ref: Arc<S>, nm: NotificationManagerRef) -> Result<Self> {
        let databases = Database::list(&*meta_store_ref).await?;
        let schemas = Schema::list(&*meta_store_ref).await?;
        let tables = Table::list(&*meta_store_ref).await?;
        let version = CatalogVersionGenerator::new(&*meta_store_ref).await?;

        let core = Mutex::new(CatalogManagerCore::new(databases, schemas, tables, version));
        Ok(Self {
            core,
            meta_store_ref,
            nm,
        })
    }

    pub async fn get_catalog(&self) -> Catalog {
        let core = self.core.lock().await;
        core.get_catalog()
    }

    pub async fn create_database(&self, mut database: Database) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let database_id = DatabaseId::from(&database.database_ref_id);
        if !core.has_database(&database_id) {
            let version = core.new_version_id(&*self.meta_store_ref).await?;
            database.version = version;

            database.insert(&*self.meta_store_ref).await?;
            core.add_database(database.clone());

            // Notify frontends to create database.
            self.nm
                .notify_frontend(Operation::Add, &Info::Database(database))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "database already exists".to_string(),
            )))
        }
    }

    pub async fn delete_database(&self, database_ref_id: &DatabaseRefId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let database_id = DatabaseId::from(&Some(database_ref_id.clone()));
        if core.has_database(&database_id) {
            Database::delete(&*self.meta_store_ref, database_ref_id).await?;
            let version = core.new_version_id(&*self.meta_store_ref).await?;
            let mut database = core.delete_database(&database_id).unwrap();
            database.version = version;

            // Notify frontends to delete database.
            self.nm
                .notify_frontend(Operation::Delete, &Info::Database(database))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "database doesn't exist".to_string(),
            )))
        }
    }

    pub async fn create_schema(&self, mut schema: Schema) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let exist = core
            .schemas
            .values()
            .any(|s| s.schema_name == schema.schema_name);
        if !exist {
            let version = core.new_version_id(&*self.meta_store_ref).await?;
            schema.version = version;

            schema.insert(&*self.meta_store_ref).await?;
            core.add_schema(schema.clone());

            // Notify frontends to create schema.
            self.nm
                .notify_frontend(Operation::Add, &Info::Schema(schema))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "schema already exists".to_string(),
            )))
        }
    }

    pub async fn delete_schema(&self, schema_ref_id: &SchemaRefId) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let schema_id = SchemaId::from(&Some(schema_ref_id.clone()));
        if core.has_schema(&schema_id) {
            Schema::delete(&*self.meta_store_ref, schema_ref_id).await?;
            let version = core.new_version_id(&*self.meta_store_ref).await?;

            let mut schema = core.delete_schema(&schema_id).unwrap();
            schema.version = version;

            // Notify frontends to delete schema.
            self.nm
                .notify_frontend(Operation::Delete, &Info::Schema(schema))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "schema doesn't exist".to_string(),
            )))
        }
    }

    pub async fn create_table(&self, mut table: Table) -> Result<CatalogVersion> {
        let mut core = self.core.lock().await;
        let exist = core
            .tables
            .values()
            .any(|t| t.table_name == table.table_name);
        if !exist {
            let version = core.new_version_id(&*self.meta_store_ref).await?;
            table.version = version;

            table.insert(&*self.meta_store_ref).await?;
            core.add_table(table.clone());

            if let TableInfo::MaterializedView(mview_info) = table.get_info().unwrap() {
                for table_ref_id in &mview_info.dependent_tables {
                    let dependent_table_id = TableId::from(&Some(table_ref_id.clone()));
                    core.increase_ref_count(dependent_table_id);
                }
            }

            // Notify frontends to create table.
            self.nm
                .notify_frontend(Operation::Add, &Info::Table(table))
                .await;

            Ok(version)
        } else {
            Err(RwError::from(InternalError(
                "table already exists".to_string(),
            )))
        }
    }

    pub async fn delete_table(&self, table_ref_id: &TableRefId) -> Result<CatalogVersion> {
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
                    Table::delete(&*self.meta_store_ref, table_ref_id).await?;
                    let version = core.new_version_id(&*self.meta_store_ref).await?;

                    let mut table = core.delete_table(&table_id).unwrap();
                    let dependent_tables: Vec<TableId> = if let TableInfo::MaterializedView(
                        mview_info,
                    ) = table.get_info().unwrap()
                    {
                        mview_info
                            .dependent_tables
                            .clone()
                            .into_iter()
                            .map(|table_ref_id| TableId::from(&Some(table_ref_id)))
                            .collect()
                    } else {
                        Default::default()
                    };
                    for dependent_table_id in dependent_tables {
                        core.decrease_ref_count(dependent_table_id);
                    }
                    table.version = version;

                    // Notify frontends to delete table.
                    self.nm
                        .notify_frontend(Operation::Delete, &Info::Table(table))
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
}

/// [`CatalogManagerCore`] caches meta catalog information and maintains dependent relationship
/// between tables.
struct CatalogManagerCore {
    databases: HashMap<DatabaseId, Database>,
    schemas: HashMap<SchemaId, Schema>,
    tables: HashMap<TableId, Table>,
    table_ref_count: HashMap<TableId, usize>,
    catalog_version: CatalogVersionGenerator,
}

impl CatalogManagerCore {
    fn new(
        databases: Vec<Database>,
        schemas: Vec<Schema>,
        tables: Vec<Table>,
        catalog_version: CatalogVersionGenerator,
    ) -> Self {
        let mut table_ref_count = HashMap::new();
        let databases = HashMap::from_iter(
            databases
                .into_iter()
                .map(|database| (DatabaseId::from(&database.database_ref_id), database)),
        );
        let schemas = HashMap::from_iter(
            schemas
                .into_iter()
                .map(|schema| (SchemaId::from(&schema.schema_ref_id), schema)),
        );
        let tables = HashMap::from_iter(tables.into_iter().map(|table| {
            if let TableInfo::MaterializedView(mview_info) = table.get_info().unwrap() {
                let dependencies = mview_info.get_dependent_tables();
                for table_ref_id in dependencies {
                    let table_id = TableId::from(&Some(table_ref_id.clone()));
                    *table_ref_count.entry(table_id).or_insert(0) += 1;
                }
            }

            (TableId::from(&table.table_ref_id), table)
        }));
        Self {
            databases,
            schemas,
            tables,
            table_ref_count,
            catalog_version,
        }
    }

    async fn new_version_id<S>(&mut self, store: &S) -> Result<CatalogVersion>
    where
        S: MetaStore,
    {
        let version = self.catalog_version.next();
        self.catalog_version.insert(store).await?;
        Ok(version)
    }

    fn get_catalog(&self) -> Catalog {
        Catalog {
            version: self.catalog_version.version(),
            databases: self.databases.values().cloned().collect(),
            schemas: self.schemas.values().cloned().collect(),
            tables: self.tables.values().cloned().collect(),
        }
    }

    fn has_database(&self, database_id: &DatabaseId) -> bool {
        self.databases.contains_key(database_id)
    }

    fn add_database(&mut self, database: Database) {
        let database_id = DatabaseId::from(&database.database_ref_id);
        self.databases.insert(database_id, database);
    }

    fn delete_database(&mut self, database_id: &DatabaseId) -> Option<Database> {
        self.databases.remove(database_id)
    }

    fn has_schema(&self, schema_id: &SchemaId) -> bool {
        self.schemas.contains_key(schema_id)
    }

    fn add_schema(&mut self, schema: Schema) {
        let schema_id = SchemaId::from(&schema.schema_ref_id);
        self.schemas.insert(schema_id, schema);
    }

    fn delete_schema(&mut self, schema_id: &SchemaId) -> Option<Schema> {
        self.schemas.remove(schema_id)
    }

    fn has_table(&self, table_id: &TableId) -> bool {
        self.tables.contains_key(table_id)
    }

    fn add_table(&mut self, table: Table) {
        let table_id = TableId::from(&table.table_ref_id);
        self.tables.insert(table_id, table);
    }

    fn delete_table(&mut self, table_id: &TableId) -> Option<Table> {
        self.tables.remove(table_id)
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
                if *o.get() == 0 {
                    o.remove_entry();
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }
}
