use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};

use risingwave_common::array::RwError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_pb::meta::catalog_service_client::CatalogServiceClient;
use risingwave_pb::meta::create_request::CatalogBody;
use risingwave_pb::meta::get_id_request::IdCategory;
use risingwave_pb::meta::id_generator_service_client::IdGeneratorServiceClient;
use risingwave_pb::meta::{CreateRequest, Database, GetIdRequest, GetIdResponse, Schema, Table};
use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};
use tonic::transport::Channel;
use tonic::Response;

use crate::catalog::create_table_info::CreateTableInfo;
use crate::catalog::database_catalog::DatabaseCatalog;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::table_catalog::TableCatalog;
use crate::catalog::{CatalogError, DatabaseId, SchemaId, TableId};

const DEFAULT_DATABASE_NAME: &str = "dev";
const DEFAULT_SCHEMA_NAME: &str = "dev";

struct LocalCatalogManager {
    next_database_id: AtomicU32,
    database_by_name: HashMap<String, DatabaseCatalog>,
}

/// Root catalog of database catalog. Manage all database/schema/table in memory.
///
/// For DDL, function with `local` suffix will use the local id generator (plan to remove in
/// future). Remote catalog manager should not use ``create_xxx_local`` but only use
/// ``create_xxx_with_id`` (e.g. ``create_database_with_id``).
///
///
/// - catalog manager (root catalog)
///   - database catalog
///     - schema catalog
///       - table catalog
///        - column catalog
impl LocalCatalogManager {
    pub fn new() -> Self {
        Self {
            next_database_id: AtomicU32::new(0),
            database_by_name: HashMap::new(),
        }
    }

    fn create_database_local(&mut self, db_name: &str) -> Result<()> {
        let db_id = self.next_database_id.fetch_add(1, Ordering::Relaxed);
        self.create_database_with_id(db_name, db_id)
    }

    fn create_database_with_id(&mut self, db_name: &str, db_id: DatabaseId) -> Result<()> {
        self.database_by_name
            .try_insert(db_name.to_string(), DatabaseCatalog::new(db_id))
            .map(|_| ())
            .map_err(|_| CatalogError::Duplicated("database", db_name.to_string()).into())
    }

    fn get_database(&self, db_name: &str) -> Option<&DatabaseCatalog> {
        self.database_by_name.get(db_name)
    }

    fn get_database_mut(&mut self, db_name: &str) -> Option<&mut DatabaseCatalog> {
        self.database_by_name.get_mut(db_name)
    }

    fn create_schema_local(&mut self, db_name: &str, schema_name: &str) -> Result<()> {
        self.get_database_mut(db_name).map_or(
            Err(CatalogError::NotFound("schema", schema_name.to_string()).into()),
            |db| db.create_schema_local(schema_name),
        )
    }

    fn create_schema_with_id(
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

    fn create_table_local(
        &mut self,
        db_name: &str,
        schema_name: &str,
        info: &CreateTableInfo,
    ) -> Result<()> {
        self.get_schema_mut(db_name, schema_name).map_or(
            Err(CatalogError::NotFound("table", info.get_name().to_string()).into()),
            |schema| schema.create_table_local(info),
        )
    }

    fn create_table_with_id(
        &mut self,
        db_name: &str,
        schema_name: &str,
        info: &CreateTableInfo,
        table_id: TableId,
    ) -> Result<()> {
        self.get_schema_mut(db_name, schema_name).map_or(
            Err(CatalogError::NotFound("table", info.get_name().to_string()).into()),
            |schema| schema.create_table_with_id(info, table_id),
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
}

/// NOTE: This is just a simple version remote catalog manager (can not handle complex case of
/// multi-frontend ddl).
///
/// For DDL (create table/schema/database), go through meta rpc first then update local catalog
/// manager.
///
/// Some changes need to be done in future:
/// 1. Do not use Id generator service (#2459).
/// 2. Support more fields for ddl in future (#2473)
/// 3. MVCC of schema (`version` flag in message) (#2474).
struct RemoteCatalogManager {
    catalog_client: CatalogServiceClient<Channel>,
    id_client: IdGeneratorServiceClient<Channel>,
    local_catalog_manager: LocalCatalogManager,
}

impl RemoteCatalogManager {
    pub fn new(
        catalog_client: CatalogServiceClient<Channel>,
        id_client: IdGeneratorServiceClient<Channel>,
    ) -> Self {
        Self {
            catalog_client,
            id_client,
            local_catalog_manager: LocalCatalogManager::new(),
        }
    }

    pub async fn create_database(&mut self, db_name: &str) -> Result<()> {
        let get_id_res = self.get_id_from_meta(IdCategory::Database).await?;
        let res_ref = get_id_res.get_ref();
        let ddl_request = CreateRequest {
            node_id: 0,
            catalog_body: Some(CatalogBody::Database(Database {
                database_name: db_name.to_string(),
                // Do not support MVCC DDL now.
                version: 0,
                database_ref_id: Some(DatabaseRefId {
                    database_id: res_ref.id,
                }),
            })),
        };
        self.catalog_client
            .create(ddl_request)
            .await
            .to_rw_result_with("create database from meta failed")?;
        self.local_catalog_manager.create_database_local(db_name)?;
        Ok(())
    }

    pub async fn create_schema(&mut self, db_name: &str, schema_name: &str) -> Result<()> {
        let database_id = self
            .local_catalog_manager
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id();
        let get_id_res = self.get_id_from_meta(IdCategory::Schema).await?;
        let res_ref = get_id_res.get_ref();
        let schema_id = res_ref.id;
        let ddl_request = CreateRequest {
            node_id: 0,
            catalog_body: Some(CatalogBody::Schema(Schema {
                schema_name: schema_name.to_string(),
                version: 0,
                schema_ref_id: Some(SchemaRefId {
                    database_ref_id: Some(DatabaseRefId {
                        database_id: database_id as i32,
                    }),
                    schema_id,
                }),
            })),
        };
        self.catalog_client.create(ddl_request).await.unwrap();
        self.local_catalog_manager.create_schema_with_id(
            db_name,
            schema_name,
            schema_id as SchemaId,
        )?;
        Ok(())
    }

    pub async fn create_table(
        &mut self,
        db_name: &str,
        schema_name: &str,
        table_info: &CreateTableInfo,
    ) -> Result<()> {
        let database_id = self
            .local_catalog_manager
            .get_database(db_name)
            .ok_or_else(|| RwError::from(CatalogError::NotFound("database", db_name.to_string())))?
            .id() as i32;
        let schema_id = self
            .local_catalog_manager
            .get_schema(db_name, schema_name)
            .ok_or_else(|| {
                RwError::from(CatalogError::NotFound("schema", schema_name.to_string()))
            })?
            .id() as i32;
        // Request Id from meta service.
        let get_id_res = self.get_id_from_meta(IdCategory::Table).await?;
        let res_ref = get_id_res.get_ref();
        let table_id = res_ref.id;
        let ddl_request = CreateRequest {
            node_id: 0,
            catalog_body: Some(CatalogBody::Table(Table {
                table_ref_id: Some(TableRefId {
                    table_id,
                    schema_ref_id: Some(SchemaRefId {
                        schema_id,
                        database_ref_id: Some(DatabaseRefId { database_id }),
                    }),
                }),
                table_name: table_info.get_name().to_string(),
                column_descs: table_info.get_col_desc_prost()?,
                is_materialized_view: false,
                is_source: false,
                append_only: false,
                // Below args do not support.
                pk_columns: vec![],
                dist_type: 0,
                properties: HashMap::new(),
                row_format: "".to_string(),
                row_schema_location: "".to_string(),
                column_orders: vec![],
                version: 0,
            })),
        };
        // Create table locally.
        self.catalog_client.create(ddl_request).await.unwrap();
        self.local_catalog_manager
            .create_table_with_id(db_name, schema_name, table_info, table_id as TableId)
            .unwrap();
        Ok(())
    }

    /// Get catalog will not query meta service. The sync of schema is done by periodically push of
    /// meta. Frontend should not pull and update the catalog voluntarily.
    pub async fn get_table(
        &self,
        db_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Option<&TableCatalog> {
        self.local_catalog_manager
            .get_table(db_name, schema_name, table_name)
    }

    /// Get one id (database/schema/table) from meta service.
    async fn get_id_from_meta(
        &mut self,
        id_category: IdCategory,
    ) -> Result<Response<GetIdResponse>> {
        let get_id_request = GetIdRequest {
            category: id_category as i32,
            interval: 1,
        };

        self.id_client
            .get_id(get_id_request)
            .await
            .to_rw_result_with("get id from meta failed")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use risingwave_common::types::{DataTypeKind, Int32Type};
    use risingwave_common::util::addr::get_host_port;

    use crate::catalog::catalog_service::{
        LocalCatalogManager, RemoteCatalogManager, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    };
    use crate::catalog::column_catalog::ColumnDesc;
    use crate::catalog::create_table_info::CreateTableInfo;

    #[test]
    fn test_create_table() {
        let mut catalog_manager = LocalCatalogManager::new();
        catalog_manager
            .create_database_local(DEFAULT_DATABASE_NAME)
            .unwrap();
        let db = catalog_manager.get_database(DEFAULT_DATABASE_NAME).unwrap();
        assert_eq!(db.id(), 0);
        catalog_manager
            .create_schema_local(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .unwrap();
        let schema = catalog_manager
            .get_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .unwrap();
        assert_eq!(schema.id(), 0);
        let columns = vec![
            (
                "v1".to_string(),
                ColumnDesc::new(Arc::new(Int32Type::new(true)), false),
            ),
            (
                "v2".to_string(),
                ColumnDesc::new(Arc::new(Int32Type::new(false)), false),
            ),
        ];
        catalog_manager
            .create_table_local(
                DEFAULT_DATABASE_NAME,
                DEFAULT_SCHEMA_NAME,
                &CreateTableInfo::new("t", columns),
            )
            .unwrap();
        let table = catalog_manager
            .get_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, "t")
            .unwrap();
        let col2 = table.get_column_by_id(1).unwrap();
        let col1 = table.get_column_by_id(0).unwrap();
        assert!(col1.is_nullable());
        assert_eq!(col1.id(), 0);
        assert_eq!(col1.data_type_clone().data_type_kind(), DataTypeKind::Int32);
        assert_eq!(col2.name(), "v2");
        assert_eq!(col2.id(), 1);
    }

    use std::time::Duration;

    use risingwave_common::error::ErrorCode::InternalError;
    use risingwave_common::error::ToRwResult;
    use risingwave_meta::rpc::server::{rpc_serve, MetaStoreBackend};
    use risingwave_pb::meta::catalog_service_client::CatalogServiceClient;
    use risingwave_pb::meta::id_generator_service_client::IdGeneratorServiceClient;
    use risingwave_pb::meta::GetCatalogRequest;
    use tonic::transport::Endpoint;

    #[tokio::test]
    async fn test_create_table_remote() {
        let addr = get_host_port("127.0.0.1:9526").unwrap();
        // Run a meta node server.
        let (_join_handle, _shutdown) = rpc_serve(
            addr,
            None,
            None,
            MetaStoreBackend::Sled(tempfile::tempdir().unwrap().into_path()),
        )
        .await;
        let endpoint = Endpoint::from_shared(format!("http://{}", addr));
        let mut catalog_client = CatalogServiceClient::new(
            endpoint
                .map_err(|e| InternalError(format!("{}", e)))
                .unwrap()
                .connect_timeout(Duration::from_secs(10))
                .connect()
                .await
                .to_rw_result_with(format!("failed to connect to {}", 0))
                .unwrap(),
        );
        let endpoint = Endpoint::from_shared(format!("http://{}", addr));
        let id_client = IdGeneratorServiceClient::new(
            endpoint
                .map_err(|e| InternalError(format!("{}", e)))
                .unwrap()
                .connect_timeout(Duration::from_secs(10))
                .connect()
                .await
                .to_rw_result_with(format!("failed to connect to {}", 0))
                .unwrap(),
        );
        let columns = vec![
            (
                "v1".to_string(),
                ColumnDesc::new(Arc::new(Int32Type::new(true)), false),
            ),
            (
                "v2".to_string(),
                ColumnDesc::new(Arc::new(Int32Type::new(false)), false),
            ),
        ];
        let mut remote_catalog_manager =
            RemoteCatalogManager::new(catalog_client.clone(), id_client);
        remote_catalog_manager
            .create_database(DEFAULT_DATABASE_NAME)
            .await
            .unwrap();
        remote_catalog_manager
            .create_schema(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME)
            .await
            .unwrap();
        let table_info = CreateTableInfo::new("t", columns);
        remote_catalog_manager
            .create_table(DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME, &table_info)
            .await
            .unwrap();
        let req = GetCatalogRequest { node_id: 0 };
        let response = catalog_client.get_catalog(req).await.unwrap();
        let catalog = response.get_ref().catalog.as_ref();
        catalog
            .map(|catalog| {
                assert_eq!(catalog.tables.len(), 1);
                assert_eq!(catalog.tables[0].table_name, "t");
                assert_eq!(
                    catalog.tables[0].column_descs,
                    table_info.get_col_desc_prost().unwrap()
                );
            })
            .unwrap();
    }
}
