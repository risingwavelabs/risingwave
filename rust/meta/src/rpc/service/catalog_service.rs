use risingwave_pb::meta::catalog_service_server::CatalogService;
use risingwave_pb::meta::create_request::CatalogBody;
use risingwave_pb::meta::drop_request::CatalogId;
use risingwave_pb::meta::{
    CreateRequest, CreateResponse, DropRequest, DropResponse, GetCatalogRequest, GetCatalogResponse,
};
use tonic::{Request, Response, Status};

use crate::catalog::CatalogManagerRef;

#[derive(Clone)]
pub struct CatalogServiceImpl {
    cmr: CatalogManagerRef,
}

impl CatalogServiceImpl {
    pub fn new(cmr: CatalogManagerRef) -> Self {
        CatalogServiceImpl { cmr }
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
        Ok(Response::new(GetCatalogResponse {
            status: None,
            catalog: Some(
                self.cmr
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
        let result = match req.get_catalog_body() {
            CatalogBody::Database(database) => self.cmr.create_database(database.clone()).await,
            CatalogBody::Schema(schema) => self.cmr.create_schema(schema.clone()).await,
            CatalogBody::Table(table) => self.cmr.create_table(table.clone()).await,
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
        let result = match req.get_catalog_id() {
            CatalogId::DatabaseId(database_id) => self.cmr.drop_database(database_id).await,
            CatalogId::SchemaId(schema_id) => self.cmr.drop_schema(schema_id).await,
            CatalogId::TableId(table_id) => self.cmr.drop_table(table_id).await,
        };

        match result {
            Ok(_) => Ok(Response::new(DropResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }
}
