use crate::metadata::{DatabaseMetaManager, MetaManager, SchemaMetaManager, TableMetaManager};
use risingwave_pb::metadata::catalog_service_server::CatalogService;
use risingwave_pb::metadata::create_request::CatalogBody;
use risingwave_pb::metadata::drop_request::CatalogId;
use risingwave_pb::metadata::{
    CreateRequest, CreateResponse, DropRequest, DropResponse, GetCatalogRequest, GetCatalogResponse,
};
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
        let _g = self.mmc.catalog_lock.read().await;
        Ok(Response::new(GetCatalogResponse {
            status: None,
            catalog: Some(
                self.mmc
                    .get_catalog()
                    .await
                    .map_err(|e| e.to_grpc_status())?,
            ),
        }))
    }

    #[cfg(not(tarpaulin_include))]
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let req = request.into_inner();
        let _g = self.mmc.catalog_lock.write().await;
        let result = match req.get_catalog_body() {
            CatalogBody::Database(database) => self.mmc.create_database(database.clone()).await,
            CatalogBody::Schema(schema) => self.mmc.create_schema(schema.clone()).await,
            CatalogBody::Table(table) => self.mmc.create_table(table.clone()).await,
        };

        match result {
            Ok(epoch) => Ok(Response::new(CreateResponse {
                status: None,
                version: epoch.into_inner(),
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn drop(&self, request: Request<DropRequest>) -> Result<Response<DropResponse>, Status> {
        let req = request.into_inner();
        let _g = self.mmc.catalog_lock.write().await;
        let result = match req.get_catalog_id() {
            CatalogId::DatabaseId(database_id) => self.mmc.drop_database(database_id).await,
            CatalogId::SchemaId(schema_id) => self.mmc.drop_schema(schema_id).await,
            CatalogId::TableId(table_id) => self.mmc.drop_table(table_id).await,
        };

        match result {
            Ok(_) => Ok(Response::new(DropResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }
}
