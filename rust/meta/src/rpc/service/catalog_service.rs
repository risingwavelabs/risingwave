use risingwave_common::error::tonic_err;
use risingwave_pb::meta::catalog_service_server::CatalogService;
use risingwave_pb::meta::create_request::CatalogBody;
use risingwave_pb::meta::drop_request::CatalogId;
use risingwave_pb::meta::{
    CreateRequest, CreateResponse, DropRequest, DropResponse, GetCatalogRequest, GetCatalogResponse,
};
use risingwave_pb::plan::DatabaseRefId;
use tonic::{Request, Response, Status};

use crate::catalog::CatalogManagerRef;
use crate::manager::{IdCategory, IdGeneratorManagerRef, MetaSrvEnv};

#[derive(Clone)]
pub struct CatalogServiceImpl {
    cmr: CatalogManagerRef,
    id_gen_manager: IdGeneratorManagerRef,
}

impl CatalogServiceImpl {
    pub fn new(cmr: CatalogManagerRef, env: MetaSrvEnv) -> Self {
        CatalogServiceImpl {
            cmr,
            id_gen_manager: env.id_gen_manager_ref(),
        }
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
        let id: i32;
        let result = match req.get_catalog_body().map_err(tonic_err)? {
            CatalogBody::Database(database) => {
                id = self
                    .id_gen_manager
                    .generate(IdCategory::Database)
                    .await
                    .map_err(|e| e.to_grpc_status())?;
                let mut database = database.clone();
                database.database_ref_id = Some(DatabaseRefId { database_id: id });
                self.cmr.create_database(database).await
            }
            CatalogBody::Schema(schema) => {
                id = self
                    .id_gen_manager
                    .generate(IdCategory::Schema)
                    .await
                    .map_err(|e| e.to_grpc_status())?;
                let mut schema_ref_id = schema.get_schema_ref_id().map_err(tonic_err)?.clone();
                schema_ref_id.schema_id = id;
                let mut schema = schema.clone();
                schema.schema_ref_id = Some(schema_ref_id);
                self.cmr.create_schema(schema).await
            }
            CatalogBody::Table(table) => {
                id = self
                    .id_gen_manager
                    .generate(IdCategory::Table)
                    .await
                    .map_err(|e| e.to_grpc_status())?;
                let mut table_ref_id = table.get_table_ref_id().map_err(tonic_err)?.clone();
                table_ref_id.table_id = id;
                let mut table = table.clone();
                table.table_ref_id = Some(table_ref_id);
                self.cmr.create_table(table).await
            }
        };

        match result {
            Ok(epoch) => Ok(Response::new(CreateResponse {
                status: None,
                id,
                version: epoch.into_inner(),
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn drop(&self, request: Request<DropRequest>) -> Result<Response<DropResponse>, Status> {
        let req = request.into_inner();
        let result = match req.get_catalog_id().map_err(tonic_err)? {
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
