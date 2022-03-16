#![allow(dead_code)]
use risingwave_common::error::tonic_err;
use risingwave_pb::catalog::catalog_service_server::CatalogService;
use risingwave_pb::catalog::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateMaterializedSourceRequest,
    CreateMaterializedSourceResponse, CreateMaterializedViewRequest,
    CreateMaterializedViewResponse, CreateSchemaRequest, CreateSchemaResponse, CreateSourceRequest,
    CreateSourceResponse, DropDatabaseRequest, DropDatabaseResponse, DropMaterializedSourceRequest,
    DropMaterializedSourceResponse, DropMaterializedViewRequest, DropMaterializedViewResponse,
    DropSchemaRequest, DropSchemaResponse, DropSourceRequest, DropSourceResponse,
};
use tonic::{Request, Response, Status};

use crate::manager::{CatalogManagerRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv};
use crate::storage::MetaStore;
use crate::stream::StreamManagerRef;

#[derive(Clone)]
pub struct CatalogServiceImpl<S>
where
    S: MetaStore,
{
    id_gen_manager: IdGeneratorManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    stream_manager: StreamManagerRef<S>,
}

impl<S> CatalogServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        catalog_manager: CatalogManagerRef<S>,
        stream_manager: StreamManagerRef<S>,
    ) -> Self {
        Self {
            id_gen_manager: env.id_gen_manager_ref(),
            catalog_manager,
            stream_manager,
        }
    }
}

#[async_trait::async_trait]
impl<S> CatalogService for CatalogServiceImpl<S>
where
    S: MetaStore,
{
    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let req = request.into_inner();
        let id = self
            .id_gen_manager
            .generate::<{ IdCategory::Database }>()
            .await
            .map_err(|e| e.to_grpc_status())?;
        let mut database = req.get_db().map_err(tonic_err)?.clone();
        database.id = id as u32;
        let version = self
            .catalog_manager
            .create_database(&database)
            .await
            .map_err(|e| e.to_grpc_status())?;

        Ok(Response::new(CreateDatabaseResponse {
            status: None,
            database_id: id as u32,
            version,
        }))
    }

    async fn create_schema(
        &self,
        request: Request<CreateSchemaRequest>,
    ) -> Result<Response<CreateSchemaResponse>, Status> {
        let req = request.into_inner();
        let id = self
            .id_gen_manager
            .generate::<{ IdCategory::Schema }>()
            .await
            .map_err(|e| e.to_grpc_status())?;
        let mut schema = req.get_schema().map_err(tonic_err)?.clone();
        schema.id = id as u32;
        let version = self
            .catalog_manager
            .create_schema(&schema)
            .await
            .map_err(|e| e.to_grpc_status())?;

        Ok(Response::new(CreateSchemaResponse {
            status: None,
            schema_id: id as u32,
            version,
        }))
    }

    async fn create_source(
        &self,
        _request: Request<CreateSourceRequest>,
    ) -> Result<Response<CreateSourceResponse>, Status> {
        todo!()
    }

    async fn create_materialized_source(
        &self,
        _request: Request<CreateMaterializedSourceRequest>,
    ) -> Result<Response<CreateMaterializedSourceResponse>, Status> {
        todo!()
    }

    async fn create_materialized_view(
        &self,
        _request: Request<CreateMaterializedViewRequest>,
    ) -> Result<Response<CreateMaterializedViewResponse>, Status> {
        todo!()
    }

    async fn drop_database(
        &self,
        request: Request<DropDatabaseRequest>,
    ) -> Result<Response<DropDatabaseResponse>, Status> {
        let req = request.into_inner();
        let database_id = req.get_database_id();
        let version = self
            .catalog_manager
            .drop_database(database_id)
            .await
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(DropDatabaseResponse {
            status: None,
            version,
        }))
    }

    async fn drop_schema(
        &self,
        request: Request<DropSchemaRequest>,
    ) -> Result<Response<DropSchemaResponse>, Status> {
        let req = request.into_inner();
        let schema_id = req.get_schema_id();
        let version = self
            .catalog_manager
            .drop_schema(schema_id)
            .await
            .map_err(|e| e.to_grpc_status())?;
        Ok(Response::new(DropSchemaResponse {
            status: None,
            version,
        }))
    }

    async fn drop_source(
        &self,
        _request: Request<DropSourceRequest>,
    ) -> Result<Response<DropSourceResponse>, Status> {
        todo!()
    }

    async fn drop_materialized_source(
        &self,
        _request: Request<DropMaterializedSourceRequest>,
    ) -> Result<Response<DropMaterializedSourceResponse>, Status> {
        todo!()
    }

    async fn drop_materialized_view(
        &self,
        _request: Request<DropMaterializedViewRequest>,
    ) -> Result<Response<DropMaterializedViewResponse>, Status> {
        todo!()
    }
}
