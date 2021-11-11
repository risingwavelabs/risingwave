use crate::metadata::{
    DatabaseMetaManager, Epoch, MetaManager, SchemaMetaManager, TableMetaManager,
};
use prost::Message;
use risingwave_common::array::RwError;
use risingwave_common::error::ErrorCode::ProtocolError;
use risingwave_pb::metadata::catalog_service_server::CatalogService;
use risingwave_pb::metadata::{
    CatalogType, ColumnTable, CreateRequest, CreateResponse, Database, DropRequest, DropResponse,
    GetCatalogRequest, GetCatalogResponse, RowTable, Schema,
};
use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Clone)]
pub struct CatalogServiceImpl {
    mmc: Arc<MetaManager>,
}

impl CatalogServiceImpl {
    pub fn new(mmc: Arc<MetaManager>) -> Self {
        CatalogServiceImpl { mmc }
    }
}

#[async_trait::async_trait]
impl CatalogService for CatalogServiceImpl {
    #[cfg(not(tarpaulin_include))]
    async fn get_catalog(
        &self,
        request: Request<GetCatalogRequest>,
    ) -> Result<Response<GetCatalogResponse>, Status> {
        let _req = request.into_inner();
        let mut response = GetCatalogResponse {
            databases: self
                .mmc
                .list_databases()
                .await
                .map_err(|e| e.to_grpc_status())?,
            schemas: self
                .mmc
                .list_schemas()
                .await
                .map_err(|e| e.to_grpc_status())?,
            column_tables: self
                .mmc
                .list_tables()
                .await
                .map_err(|e| e.to_grpc_status())?,
            row_tables: self
                .mmc
                .list_materialized_views()
                .await
                .map_err(|e| e.to_grpc_status())?,
            watermark: 0,
        };

        response.watermark = response
            .databases
            .iter()
            .map(|d| d.get_version())
            .chain(response.schemas.iter().map(|s| s.get_version()))
            .chain(response.column_tables.iter().map(|t| t.get_version()))
            .chain(response.row_tables.iter().map(|t| t.get_version()))
            .max_by(|x, y| x.cmp(y))
            .or(Some(0))
            .unwrap();

        if !self.mmc.catalog_wm.inited() {
            self.mmc.catalog_wm.update(Epoch::from(response.watermark))
        }

        Ok(Response::new(response))
    }

    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let req = request.into_inner();
        let result = match CatalogType::from_i32(req.catalog_type) {
            Some(CatalogType::Database) => {
                let database = Database::decode(req.body.unwrap().value.as_slice()).unwrap();
                self.mmc.create_database(database).await
            }
            Some(CatalogType::Schema) => {
                let schema = Schema::decode(req.body.unwrap().value.as_slice()).unwrap();
                self.mmc.create_schema(schema).await
            }
            Some(CatalogType::RowTable) => {
                let table = RowTable::decode(req.body.unwrap().value.as_slice()).unwrap();
                self.mmc.create_materialized_view(table).await
            }
            Some(CatalogType::ColumnTable) => {
                let table = ColumnTable::decode(req.body.unwrap().value.as_slice()).unwrap();
                self.mmc.create_table(table).await
            }
            None => Err(RwError::from(ProtocolError(
                "catalog type invalid".to_string(),
            ))),
        };

        match result {
            Ok(epoch) => {
                self.mmc.catalog_wm.update(epoch);
                Ok(Response::new(CreateResponse {
                    version: epoch.into_inner(),
                }))
            }
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    async fn drop(&self, request: Request<DropRequest>) -> Result<Response<DropResponse>, Status> {
        let req = request.into_inner();
        let result = match CatalogType::from_i32(req.catalog_type) {
            Some(CatalogType::Database) => {
                let database_ref_id =
                    DatabaseRefId::decode(req.body.unwrap().value.as_slice()).unwrap();
                self.mmc.drop_database(&database_ref_id).await
            }
            Some(CatalogType::Schema) => {
                let schema_ref_id =
                    SchemaRefId::decode(req.body.unwrap().value.as_slice()).unwrap();
                self.mmc.drop_schema(&schema_ref_id).await
            }
            Some(CatalogType::RowTable) => {
                let table_ref_id = TableRefId::decode(req.body.unwrap().value.as_slice()).unwrap();
                self.mmc.drop_materialized_view(&table_ref_id).await
            }
            Some(CatalogType::ColumnTable) => {
                let table_ref_id = TableRefId::decode(req.body.unwrap().value.as_slice()).unwrap();
                self.mmc.drop_table(&table_ref_id).await
            }
            None => Err(RwError::from(ProtocolError(
                "catalog type invalid".to_string(),
            ))),
        };

        match result {
            Ok(version) => {
                self.mmc.catalog_wm.update(version);
                Ok(Response::new(DropResponse {}))
            }
            Err(e) => Err(e.to_grpc_status()),
        }
    }
}
