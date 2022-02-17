use std::sync::Arc;

use risingwave_common::array::RwError;
use risingwave_common::error::tonic_err;
use risingwave_pb::meta::catalog_service_server::CatalogService;
use risingwave_pb::meta::create_request::CatalogBody;
use risingwave_pb::meta::drop_request::CatalogId;
use risingwave_pb::meta::{
    CreateRequest, CreateResponse, DropRequest, DropResponse, GetCatalogRequest, GetCatalogResponse,
};
use risingwave_pb::plan::DatabaseRefId;
use tonic::{Request, Response, Status};

use crate::manager::{
    EpochGeneratorRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv, StoredCatalogManager,
    StoredCatalogManagerRef,
};

#[derive(Clone)]
pub struct CatalogServiceImpl {
    id_gen_manager: IdGeneratorManagerRef,
    epoch_generator: EpochGeneratorRef,
    stored_catalog_manager: StoredCatalogManagerRef,
}

impl CatalogServiceImpl {
    pub async fn new(env: MetaSrvEnv) -> Result<Self, RwError> {
        Ok(CatalogServiceImpl {
            id_gen_manager: env.id_gen_manager_ref(),
            epoch_generator: env.epoch_generator_ref(),
            stored_catalog_manager: Arc::new(
                StoredCatalogManager::new(env.meta_store_ref()).await?,
            ),
        })
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
            catalog: Some(self.stored_catalog_manager.get_catalog().await),
        }))
    }

    #[cfg(not(tarpaulin_include))]
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let req = request.into_inner();
        let version = self
            .epoch_generator
            .generate()
            .map_err(|e| e.to_grpc_status())?;
        let id: i32;
        let result = match req.get_catalog_body().map_err(tonic_err)? {
            CatalogBody::Database(database) => {
                id = self
                    .id_gen_manager
                    .generate::<{ IdCategory::Database }>()
                    .await
                    .map_err(|e| e.to_grpc_status())?;
                let mut database = database.clone();
                database.database_ref_id = Some(DatabaseRefId { database_id: id });
                database.version = version.into_inner();
                self.stored_catalog_manager.create_database(database).await
            }
            CatalogBody::Schema(schema) => {
                id = self
                    .id_gen_manager
                    .generate::<{ IdCategory::Schema }>()
                    .await
                    .map_err(|e| e.to_grpc_status())?;
                let mut schema_ref_id = schema.get_schema_ref_id().map_err(tonic_err)?.clone();
                schema_ref_id.schema_id = id;
                let mut schema = schema.clone();
                schema.schema_ref_id = Some(schema_ref_id);
                schema.version = version.into_inner();
                self.stored_catalog_manager.create_schema(schema).await
            }
            CatalogBody::Table(table) => {
                id = self
                    .id_gen_manager
                    .generate::<{ IdCategory::Table }>()
                    .await
                    .map_err(|e| e.to_grpc_status())?;
                let mut table_ref_id = table.get_table_ref_id().map_err(tonic_err)?.clone();
                table_ref_id.table_id = id;
                let mut table = table.clone();
                table.table_ref_id = Some(table_ref_id);
                table.version = version.into_inner();
                self.stored_catalog_manager.create_table(table).await
            }
        };

        match result {
            Ok(_) => Ok(Response::new(CreateResponse {
                status: None,
                id,
                version: version.into_inner(),
            })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }

    #[cfg(not(tarpaulin_include))]
    async fn drop(&self, request: Request<DropRequest>) -> Result<Response<DropResponse>, Status> {
        let req = request.into_inner();
        let result = match req.get_catalog_id().map_err(tonic_err)? {
            CatalogId::DatabaseId(database_ref_id) => {
                self.stored_catalog_manager
                    .delete_database(database_ref_id)
                    .await
            }
            CatalogId::SchemaId(schema_ref_id) => {
                self.stored_catalog_manager
                    .delete_schema(schema_ref_id)
                    .await
            }
            CatalogId::TableId(table_ref_id) => {
                self.stored_catalog_manager.delete_table(table_ref_id).await
            }
        };

        match result {
            Ok(_) => Ok(Response::new(DropResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }
}
