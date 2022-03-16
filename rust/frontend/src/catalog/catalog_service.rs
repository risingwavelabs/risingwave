use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, RwError};
use risingwave_common::types::DataType;
use risingwave_meta::manager::SourceId;
use risingwave_pb::catalog::{
    Database as ProstDatabase, Schema as ProstSchema, Source as ProstSource, Table as ProstTable,
};
use risingwave_pb::meta::{Catalog, Database as OldProstDatabase};
use risingwave_pb::plan::{ColumnDesc, DatabaseRefId, SchemaRefId, TableRefId};
use risingwave_rpc_client::MetaClient;
use tokio::sync::watch::Receiver;

use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::table_catalog::{TableCatalog, ROWID_NAME};
use crate::catalog::{CatalogError, DatabaseId, SchemaId};

pub const DEFAULT_DATABASE_NAME: &str = "dev";
pub const DEFAULT_SCHEMA_NAME: &str = "dev";

pub struct CatalogCache {
    catalog_version: CatalogVersion,
    database_by_name: HashMap<String, DatabaseCatalog>,
    db_name_by_id: HashMap<DatabaseId, String>,
}

impl Default for CatalogCache {
    fn default() -> Self {
        Self {
            catalog_version: 0,
            database_by_name: HashMap::new(),
            db_name_by_id: HashMap::new(),
        }
    }
}

/// Root catalog of database catalog. Manage all database/schema/table in memory.
/// It is used in [`CatalogConnector`] and [`ObserverManager`].
/// `pub` create/delete methods are used in [`ObserverManager`].
///
/// - catalog cache (root catalog)
///   - database catalog
///     - schema catalog
///       - table catalog
///        - column catalog
impl CatalogCache {
    fn get_database_mut(&mut self, db_id: DatabaseId) -> Option<&mut DatabaseCatalog> {
        let name = self.db_name_by_id.get(&db_id)?;
        self.database_by_name.get_mut(name)
    }

    pub fn create_database(&mut self, db: &ProstDatabase) {
        let name = db.name;
        let id = db.id.into();

        self.database_by_name.try_insert(name, db.into()).unwrap();
        self.db_name_by_id.try_insert(id, name).unwrap();
    }

    pub fn create_schema(&mut self, proto: &ProstSchema) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .create_schema(proto);
    }

    pub fn create_table(&mut self, proto: &ProstTable) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_table(proto);
    }
    pub fn create_source(&mut self, proto: ProstSource) {
        self.get_database_mut(proto.database_id)
            .unwrap()
            .get_schema_mut(proto.schema_id)
            .unwrap()
            .create_source(proto);
    }

    pub fn drop_database(&mut self, db_id: DatabaseId) {
        let name = self.db_name_by_id.remove(&db_id).unwrap();
        let database = self.database_by_name.remove(&name).unwrap();
    }

    pub fn drop_schema(&mut self, schema_id: SchemaId, db_id: DatabaseId) {
        self.get_database_mut(db_id).unwrap().drop_schema(schema_id);
    }

    pub fn drop_table(&mut self, schema_id: SchemaId, db_id: DatabaseId, tb_id: TableId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_table(tb_id);
    }

    pub fn drop_source(&mut self, schema_id: SchemaId, db_id: DatabaseId, source_id: SourceId) {
        self.get_database_mut(db_id)
            .unwrap()
            .get_schema_mut(schema_id)
            .unwrap()
            .drop_source(source_id);
    }

    pub fn get_database_by_name(&self, db_name: &str) -> Option<&DatabaseCatalog> {
        self.database_by_name.get(db_name)
    }

    pub fn get_schema_by_name(&self, db_name: &str, schema_name: &str) -> Option<&SchemaCatalog> {
        self.get_database_by_name(db_name)?
            .get_schema_by_name(schema_name)
    }

    pub fn get_table_by_name(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<&TableCatalog> {
        self.get_schema_by_name(db_name, schema_name)?
            .get_table_by_name(table_name)
    }
}

/// For DDL (create table/schema/database), only send rpc to meta. Create and delete actions will be
/// done by `ObserverManager`. Should be used by DDL handler.
///
/// Some changes need to be done in future:
/// 1. Support more fields for ddl in future (#2473)
/// 2. MVCC of schema (`version` flag in message) (#2474).
#[derive(Clone)]
pub struct CatalogConnector {
    meta_client: MetaClient,
    catalog_cache: Arc<RwLock<CatalogCache>>,
    catalog_updated_rx: Receiver<CatalogVersion>,
}

impl CatalogConnector {
    pub fn new(
        meta_client: MetaClient,
        catalog_cache: Arc<RwLock<CatalogCache>>,
        catalog_updated_rx: Receiver<CatalogVersion>,
    ) -> Self {
        Self {
            meta_client,
            catalog_cache,
            catalog_updated_rx,
        }
    }

    async fn wait_version(&self, version: CatalogVersion) -> Result<()> {
        let mut rx = self.catalog_updated_rx.clone();
        while *rx.borrow_and_update() < version {
            rx.changed()
                .await
                .map_err(|e| RwError::from(InternalError(e.to_string())))?;
        }
        Ok(())
    }

    pub async fn create_database(&self, db_name: &str) -> Result<()> {
        let (_, version) = self
            .meta_client
            .create_database(ProstDatabase {
                name: db_name.to_string(),
                id: 0,
            })
            .await?;
        self.wait_version(version).await
    }

    pub async fn create_schema(&self, db_id: DatabaseId, schema_name: &str) -> Result<()> {
        let (_, version) = self
            .meta_client
            .create_schema(ProstSchema {
                id: 0,
                name: schema_name.to_string(),
                database_id: db_id,
            })
            .await?;
        self.wait_version(version).await
    }

    /// for the `CREATE TABLE statement`
    pub async fn create_materialized_table_source(&self, table: ProstTable) -> Result<()> {
        todo!()
    }

    // TODO: maybe here to pass a materialize plan node
    pub async fn create_materialized_view(
        &self,
        db_id: DatabaseId,
        schema_id: SchemaId,
    ) -> Result<()> {
        todo!()
    }
}

// #[cfg(test)]
// mod tests {

//     use std::sync::{Arc, RwLock};

//     use risingwave_common::types::DataType;
//     use risingwave_pb::meta::table::Info;
//     use risingwave_pb::plan::{ColumnDesc, TableSourceInfo};
//     use risingwave_rpc_client::MetaClient;
//     use tokio::sync::watch;

//     use crate::catalog::catalog_service::{
//         CatalogCache, CatalogConnector, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
//     };
//     use crate::observer::observer_manager::ObserverManager;
//     use crate::scheduler::schedule::WorkerNodeManager;
//     use crate::test_utils::FrontendMockMetaClient;

//     fn create_test_table(test_table_name: &str, columns: Vec<(String, DataType)>) -> Table {
//         let column_descs = columns
//             .iter()
//             .map(|c| ColumnDesc {
//                 name: c.0.clone(),
//                 column_type: Some(c.1.to_protobuf().unwrap()),
//                 ..Default::default()
//             })
//             .collect();
//         Table {
//             table_name: test_table_name.to_string(),
//             column_descs,
//             info: Info::TableSource(TableSourceInfo::default()).into(),
//             ..Default::default()
//         }
//     }

//     use risingwave_pb::meta::Table;

//     use crate::catalog::table_catalog::ROWID_NAME;

//     #[tokio::test]
//     async fn test_create_and_drop_table() {
//         // Init meta and catalog.
//         let meta_client = MetaClient::mock(FrontendMockMetaClient::new().await);

//         let (catalog_updated_tx, catalog_updated_rx) = watch::channel(0);
//         let catalog_cache = Arc::new(RwLock::new(
//             CatalogCache::new(meta_client.clone()).await.unwrap(),
//         ));
//         let catalog_mgr = CatalogConnector::new(
//             meta_client.clone(),
//             catalog_cache.clone(),
//             catalog_updated_rx,
//         );

//         let worker_node_manager =
//             Arc::new(WorkerNodeManager::new(meta_client.clone()).await.unwrap());

//         let observer_manager = ObserverManager::new(
//             meta_client.clone(),
//             "127.0.0.1:12345".parse().unwrap(), // Random value, not used here.
//             worker_node_manager,
//             catalog_cache,
//             catalog_updated_tx,
//         )
//         .await;
//         observer_manager.start();

//         // Create db and schema.
//         catalog_mgr
//             .create_database(DEFAULT_DATABASE_NAME)
//             .await
//             .unwrap();
//         assert!(catalog_mgr.get_database(DEFAULT_DATABASE_NAME).is_some());
//         catalog_mgr
//             .create_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
//             .await
//             .unwrap();
//         assert!(catalog_mgr
//             .get_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
//             .is_some());

//         // Create table.
//         let test_table_name = "t";
//         let table = create_test_table(
//             test_table_name,
//             vec![
//                 ("v1".to_string(), DataType::Int32),
//                 ("v2".to_string(), DataType::Int32),
//             ],
//         );
//         catalog_mgr
//             .create_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, table)
//             .await
//             .unwrap();
//         assert!(catalog_mgr
//             .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
//             .is_some());

//         // Get catalog from meta and check the table info.
//         let catalog = meta_client.get_catalog().await.unwrap();
//         assert_eq!(catalog.tables.len(), 1);
//         assert_eq!(catalog.tables[0].table_name, test_table_name);
//         let expected_table = create_test_table(
//             test_table_name,
//             vec![
//                 (ROWID_NAME.to_string(), DataType::Int64),
//                 ("v1".to_string(), DataType::Int32),
//                 ("v2".to_string(), DataType::Int32),
//             ],
//         );
//         assert_eq!(catalog.tables[0].column_descs, expected_table.column_descs);

//         // -----  test drop table, schema and database  -----

//         catalog_mgr
//             .drop_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
//             .await
//             .unwrap();
//         // Ensure the table has been dropped from cache.
//         assert!(catalog_mgr
//             .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
//             .is_none());
//         // Ensure the table has been dropped from meta.
//         let catalog = meta_client.get_catalog().await.unwrap();
//         assert_eq!(catalog.tables.len(), 0);

//         catalog_mgr
//             .drop_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
//             .await
//             .unwrap();
//         // Ensure the schema has been dropped from cache.
//         assert!(catalog_mgr
//             .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
//             .is_none());
//         // Ensure the schema has been dropped from meta.
//         let catalog = meta_client.get_catalog().await.unwrap();
//         assert_eq!(catalog.schemas.len(), 0);

//         catalog_mgr
//             .drop_database(DEFAULT_DATABASE_NAME)
//             .await
//             .unwrap();
//         // Ensure the db has been dropped from cache.
//         assert!(catalog_mgr
//             .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, test_table_name)
//             .is_none());
//         // Ensure the db has been dropped from meta.
//         let catalog = meta_client.get_catalog().await.unwrap();
//         assert_eq!(catalog.databases.len(), 0);
//     }
// }
