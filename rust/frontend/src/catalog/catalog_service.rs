use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use risingwave_common::array::RwError;
use risingwave_common::error::Result;
use risingwave_common::types::DataType;
use risingwave_pb::meta::{Database, Schema, Table};
use risingwave_pb::plan::{ColumnDesc, DatabaseRefId, SchemaRefId, TableRefId};
use risingwave_rpc_client::MetaClient;

use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::table_catalog::{TableCatalog, ROWID_NAME};
use crate::catalog::{CatalogError, DatabaseId, SchemaId};

pub const DEFAULT_DATABASE_NAME: &str = "dev";
pub const DEFAULT_SCHEMA_NAME: &str = "dev";

struct CatalogCache {
    database_by_name: HashMap<String, Arc<DatabaseCatalog>>,
}

/// Root catalog of database catalog. Manage all database/schema/table in memory.
/// It can not be used outside from [`CatalogConnector`].
///
/// - catalog cache (root catalog)
///   - database catalog
///     - schema catalog
///       - table catalog
///        - column catalog
impl CatalogCache {
    fn new() -> Self {
        Self {
            database_by_name: HashMap::new(),
        }
    }

    fn create_database(&mut self, db_name: &str, db_id: DatabaseId) -> Result<()> {
        self.database_by_name
            .try_insert(db_name.to_string(), Arc::new(DatabaseCatalog::new(db_id)))
            .map(|_| ())
            .map_err(|_| CatalogError::Duplicated("database", db_name.to_string()).into())
    }

    fn get_database(&self, db_name: &str) -> Option<&DatabaseCatalog> {
        Some(self.database_by_name.get(db_name)?.as_ref())
    }

    fn get_database_mut(&mut self, db_name: &str) -> Option<&mut DatabaseCatalog> {
        Some(Arc::make_mut(self.database_by_name.get_mut(db_name)?))
    }

    fn get_database_snapshot(&self, db_name: &str) -> Option<Arc<DatabaseCatalog>> {
        Some(self.database_by_name.get(db_name)?.clone())
    }

    fn create_schema(
        &mut self,
        db_name: &str,
        schema_name: &str,
        schema_id: SchemaId,
    ) -> Result<()> {
        self.get_database_mut(db_name).map_or(
            Err(CatalogError::NotFound("schema", db_name.to_string()).into()),
            |db| db.create_schema_with_id(schema_name, schema_id),
        )
    }

    fn get_schema(&self, db_name: &str, schema_name: &str) -> Option<&SchemaCatalog> {
        self.get_database(db_name)
            .and_then(|db| db.get_schema(schema_name))
    }

    fn get_schema_mut(&mut self, db_name: &str, schema_name: &str) -> Option<&mut SchemaCatalog> {
        self.get_database_mut(db_name)
            .and_then(|db| db.get_schema_mut(schema_name))
    }

    fn create_table(&mut self, db_name: &str, schema_name: &str, table: &Table) -> Result<()> {
        self.get_schema_mut(db_name, schema_name).map_or(
            Err(CatalogError::NotFound("table", table.table_name.to_string()).into()),
            |schema| schema.create_table(table),
        )
    }

    fn get_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<&TableCatalog> {
        self.get_schema(db_name, schema_name)
            .and_then(|schema| schema.get_table(table_name))
    }

    fn drop_table(&mut self, db_name: &str, schema_name: &str, table_name: &str) -> Result<()> {
        self.get_schema_mut(db_name, schema_name).map_or(
            Err(CatalogError::NotFound("schema", schema_name.to_string()).into()),
            |schema| schema.drop_table(table_name),
        )
    }

    fn drop_schema(&mut self, db_name: &str, schema_name: &str) -> Result<()> {
        self.get_database_mut(db_name).map_or(
            Err(CatalogError::NotFound("database", db_name.to_string()).into()),
            |db| db.drop_schema(schema_name),
        )
    }

    fn drop_database(&mut self, db_name: &str) -> Result<()> {
        self.database_by_name.remove(db_name).ok_or_else(|| {
            RwError::from(CatalogError::NotFound("database", db_name.to_string()))
        })?;
        Ok(())
    }
}

/// For DDL (create table/schema/database), go through meta rpc first then update local catalog
/// cache. For get catalog request (get table/schema/database), check the root catalog cache only.
/// Should be used by DDL handler.
///
/// Some changes need to be done in future:
/// 1. Support more fields for ddl in future (#2473)
/// 2. MVCC of schema (`version` flag in message) (#2474).
#[derive(Clone)]
pub struct CatalogConnector {
    meta_client: MetaClient,
    catalog_cache: Arc<Mutex<CatalogCache>>,
}

impl CatalogConnector {
    pub fn new(meta_client: MetaClient) -> Self {
        Self {
            meta_client,
            catalog_cache: Arc::new(Mutex::new(CatalogCache::new())),
        }
    }

    pub async fn create_database(&self, db_name: &str) -> Result<()> {
        let id = self
            .meta_client
            .create_database(Database {
                database_name: db_name.to_string(),
                // Do not support MVCC DDL now.
                ..Default::default()
            })
            .await?;
        self.catalog_cache
            .lock()
            .create_database(db_name, id as DatabaseId)?;
        Ok(())
    }

    pub async fn create_schema(&self, db_name: &str, schema_name: &str) -> Result<()> {
        let database_id = self
            .catalog_cache
            .lock()
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id();
        let schema_id = self
            .meta_client
            .create_schema(Schema {
                schema_name: schema_name.to_string(),
                version: 0,
                schema_ref_id: Some(SchemaRefId {
                    database_ref_id: Some(DatabaseRefId {
                        database_id: database_id as i32,
                    }),
                    schema_id: 0,
                }),
            })
            .await?;
        self.catalog_cache
            .lock()
            .create_schema(db_name, schema_name, schema_id as SchemaId)?;
        Ok(())
    }

    pub async fn create_table(
        &self,
        db_name: &str,
        schema_name: &str,
        mut table: Table,
    ) -> Result<()> {
        let database_id = self
            .catalog_cache
            .lock()
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id() as i32;
        let schema_id = self
            .catalog_cache
            .lock()
            .get_schema(db_name, schema_name)
            .ok_or_else(|| {
                RwError::from(CatalogError::NotFound("schema", schema_name.to_string()))
            })?
            .id() as i32;
        let schema_ref_id = Some(SchemaRefId {
            database_ref_id: Some(DatabaseRefId { database_id }),
            schema_id,
        });
        table.table_ref_id = Some(TableRefId {
            schema_ref_id: schema_ref_id.clone(),
            table_id: 0,
        });
        // Append hidden column ROWID.
        table.column_descs.insert(
            0,
            ColumnDesc {
                name: ROWID_NAME.to_string(),
                column_type: Some(DataType::Int64.to_protobuf()?),
                ..Default::default()
            },
        );
        let table_id = self.meta_client.create_table(table.clone()).await?;
        // Create table locally.
        table.table_ref_id = Some(TableRefId::from(&table_id));
        self.catalog_cache
            .lock()
            .create_table(db_name, schema_name, &table)
    }

    pub async fn drop_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<()> {
        let table_id = self
            .catalog_cache
            .lock()
            .get_table(db_name, schema_name, table_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("table", table_name.to_string())))?
            .id();

        let table_ref_id = TableRefId::from(&table_id);
        self.meta_client.drop_table(table_ref_id).await?;
        // Drop table locally.
        self.catalog_cache
            .lock()
            .drop_table(db_name, schema_name, table_name)
            .unwrap();
        Ok(())
    }

    pub async fn drop_schema(&self, db_name: &str, schema_name: &str) -> Result<()> {
        let database_id = self
            .catalog_cache
            .lock()
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id() as i32;
        let schema_id = self
            .catalog_cache
            .lock()
            .get_schema(db_name, schema_name)
            .ok_or_else(|| {
                RwError::from(CatalogError::NotFound("schema", schema_name.to_string()))
            })?
            .id() as i32;

        let schema_ref_id = SchemaRefId {
            database_ref_id: Some(DatabaseRefId { database_id }),
            schema_id,
        };
        self.meta_client.drop_schema(schema_ref_id).await?;
        // Drop schema locally.
        self.catalog_cache
            .lock()
            .drop_schema(db_name, schema_name)
            .unwrap();
        Ok(())
    }

    pub async fn drop_database(&self, db_name: &str) -> Result<()> {
        let database_id = self
            .catalog_cache
            .lock()
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id() as i32;
        let database_ref_id = DatabaseRefId { database_id };
        self.meta_client.drop_database(database_ref_id).await?;
        // Drop database locally.
        self.catalog_cache.lock().drop_database(db_name).unwrap();
        Ok(())
    }

    pub fn get_database_snapshot(&self, db_name: &str) -> Option<Arc<DatabaseCatalog>> {
        self.catalog_cache.lock().get_database_snapshot(db_name)
    }

    /// Get catalog will not query meta service. The sync of schema is done by periodically push of
    /// meta. Frontend should not pull and update the catalog voluntarily.
    #[cfg(test)]
    pub fn get_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<TableCatalog> {
        self.catalog_cache
            .lock()
            .get_table(db_name, schema_name, table_name)
            .cloned()
    }

    #[cfg(test)]
    pub fn get_database(&self, db_name: &str) -> Option<DatabaseCatalog> {
        self.catalog_cache.lock().get_database(db_name).cloned()
    }

    #[cfg(test)]
    pub fn get_schema(&self, db_name: &str, schema_name: &str) -> Option<SchemaCatalog> {
        self.catalog_cache
            .lock()
            .get_schema(db_name, schema_name)
            .cloned()
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::types::DataType;
    use risingwave_meta::test_utils::LocalMeta;
    use risingwave_pb::plan::ColumnDesc;

    use crate::catalog::catalog_service::{
        CatalogConnector, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };

    fn create_test_table(test_table_name: &str, columns: Vec<(String, DataType)>) -> Table {
        let column_descs = columns
            .iter()
            .map(|c| ColumnDesc {
                name: c.0.clone(),
                column_type: Some(c.1.to_protobuf().unwrap()),
                ..Default::default()
            })
            .collect();
        Table {
            table_name: test_table_name.to_string(),
            column_descs,
            ..Default::default()
        }
    }

    use risingwave_pb::meta::{GetCatalogRequest, Table};

    use crate::catalog::table_catalog::ROWID_NAME;

    #[tokio::test]
    #[serial_test::serial]
    async fn test_create_and_drop_table() {
        // Init meta and catalog.
        let meta = LocalMeta::start().await;
        let mut meta_client = LocalMeta::create_client().await;
        let catalog_mgr = CatalogConnector::new(meta_client.clone());

        // Create db and schema.
        catalog_mgr
            .create_database(DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        assert!(catalog_mgr.get_database(DEFAULT_DATABASE_NAME).is_some());
        catalog_mgr
            .create_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .await
            .unwrap();
        assert!(catalog_mgr
            .get_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .is_some());

        // Create table.
        let test_table_name = "t";
        let table = create_test_table(
            test_table_name,
            vec![
                ("v1".to_string(), DataType::Int32),
                ("v2".to_string(), DataType::Int32),
            ],
        );
        catalog_mgr
            .create_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, table)
            .await
            .unwrap();
        assert!(catalog_mgr
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .is_some());

        // Get catalog from meta and check the table info.
        let req = GetCatalogRequest { node_id: 0 };
        let response = meta_client
            .catalog_client
            .get_catalog(req.clone())
            .await
            .unwrap();
        let catalog = response.get_ref().catalog.as_ref().unwrap();
        assert_eq!(catalog.tables.len(), 1);
        assert_eq!(catalog.tables[0].table_name, test_table_name);
        let expected_table = create_test_table(
            test_table_name,
            vec![
                (ROWID_NAME.to_string(), DataType::Int64),
                ("v1".to_string(), DataType::Int32),
                ("v2".to_string(), DataType::Int32),
            ],
        );
        assert_eq!(catalog.tables[0].column_descs, expected_table.column_descs);

        // -----  test drop table, schema and database  -----

        catalog_mgr
            .drop_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .await
            .unwrap();
        // Ensure the table has been dropped from cache.
        assert!(catalog_mgr
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .is_none());
        // Ensure the table has been dropped from meta.
        meta_client
            .catalog_client
            .get_catalog(req.clone())
            .await
            .unwrap()
            .get_ref()
            .catalog
            .as_ref()
            .map(|catalog| {
                assert_eq!(catalog.tables.len(), 0);
            })
            .unwrap();

        catalog_mgr
            .drop_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .await
            .unwrap();
        // Ensure the schema has been dropped from cache.
        assert!(catalog_mgr
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .is_none());
        // Ensure the schema has been dropped from meta.
        meta_client
            .catalog_client
            .get_catalog(req.clone())
            .await
            .unwrap()
            .get_ref()
            .catalog
            .as_ref()
            .map(|catalog| {
                assert_eq!(catalog.schemas.len(), 0);
            })
            .unwrap();

        catalog_mgr
            .drop_database(DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        // Ensure the db has been dropped from cache.
        assert!(catalog_mgr
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
            .is_none());
        // Ensure the db has been dropped from meta.
        meta_client
            .catalog_client
            .get_catalog(req.clone())
            .await
            .unwrap()
            .get_ref()
            .catalog
            .as_ref()
            .map(|catalog| {
                assert_eq!(catalog.databases.len(), 0);
            })
            .unwrap();

        meta.stop().await;
    }
}
