use risingwave_common::error::tonic_err;
use risingwave_pb::meta::catalog_service_server::CatalogService;
use risingwave_pb::meta::create_request::CatalogBody;
use risingwave_pb::meta::drop_request::CatalogId;
use risingwave_pb::meta::{
    CreateRequest, CreateResponse, Database, DropRequest, DropResponse, GetCatalogRequest,
    GetCatalogResponse, Schema, Table,
};
use risingwave_pb::plan::DatabaseRefId;
use tonic::{Request, Response, Status};

use crate::manager::{EpochGeneratorRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::{Catalog, MetadataModel};
use crate::storage::MetaStoreRef;

#[derive(Clone)]
pub struct CatalogServiceImpl {
    meta_store_ref: MetaStoreRef,
    id_gen_manager: IdGeneratorManagerRef,
    epoch_generator: EpochGeneratorRef,
}

impl CatalogServiceImpl {
    pub fn new(env: MetaSrvEnv) -> Self {
        CatalogServiceImpl {
            meta_store_ref: env.meta_store_ref(),
            id_gen_manager: env.id_gen_manager_ref(),
            epoch_generator: env.epoch_generator_ref(),
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
                Catalog::get(&self.meta_store_ref)
                    .await
                    .map_err(|e| e.to_grpc_status())?
                    .inner(),
            ),
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
                database.create(&self.meta_store_ref).await
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
                schema.create(&self.meta_store_ref).await
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
                table.create(&self.meta_store_ref).await
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
            CatalogId::DatabaseId(database_id) => {
                Database::delete(&self.meta_store_ref, database_id).await
            }
            CatalogId::SchemaId(schema_id) => Schema::delete(&self.meta_store_ref, schema_id).await,
            CatalogId::TableId(table_id) => Table::delete(&self.meta_store_ref, table_id).await,
        };

        match result {
            Ok(_) => Ok(Response::new(DropResponse { status: None })),
            Err(e) => Err(e.to_grpc_status()),
        }
    }
}
