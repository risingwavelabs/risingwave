#![allow(dead_code)]

use risingwave_common::catalog::TableId;
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
use risingwave_pb::common::ParallelUnitType;
use risingwave_pb::plan::TableRefId;
use tonic::{Request, Response, Status};
use risingwave_pb::catalog::source::Info;
use risingwave_pb::catalog::source::Info::StreamSource;


use crate::cluster::StoredClusterManagerRef;
use crate::manager::{CatalogManagerRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv};
use crate::model::TableFragments;
use crate::rpc::service::stream_service::TonicResponse;
use crate::storage::MetaStore;
use crate::stream::{FragmentManagerRef, SourceManagerRef, StreamFragmenter, StreamManagerRef};

#[derive(Clone)]
pub struct CatalogServiceImpl<S>
    where
        S: MetaStore,
{
    id_gen_manager: IdGeneratorManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    stream_manager: StreamManagerRef<S>,
    cluster_manager: StoredClusterManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
    source_manager: SourceManagerRef<S>,

}

impl<S> CatalogServiceImpl<S>
    where
        S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        catalog_manager: CatalogManagerRef<S>,
        stream_manager: StreamManagerRef<S>,
        cluster_manager: StoredClusterManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        source_manager: SourceManagerRef<S>,
    ) -> Self {
        Self {
            id_gen_manager: env.id_gen_manager_ref(),
            catalog_manager,
            stream_manager,
            cluster_manager,
            fragment_manager,
            source_manager,
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
            .map_err(tonic_err)? as u32;
        let mut database = req.get_db().map_err(tonic_err)?.clone();
        database.id = id;
        let version = self
            .catalog_manager
            .create_database(&database)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(CreateDatabaseResponse {
            status: None,
            database_id: id,
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
            .map_err(tonic_err)? as u32;
        let mut schema = req.get_schema().map_err(tonic_err)?.clone();
        schema.id = id;
        let version = self
            .catalog_manager
            .create_schema(&schema)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(CreateSchemaResponse {
            status: None,
            schema_id: id,
            version,
        }))
    }

    async fn create_source(
        &self,
        _request: Request<CreateSourceRequest>,
    ) -> TonicResponse<CreateSourceResponse> {
        let req = _request.into_inner();
        let source = req.source.ok_or(Status::invalid_argument("source"))?;


        let _source_info = match source.clone().info {
            Some(StreamSource(s)) => { s }
            _ => return Err(Status::invalid_argument("source info illegal")),
        };

        // let _resp = self.source_manager.create_source(CreateSourceContext {
        //     name: "".to_string(),
        //     table_id: Default::default(),
        //     discovery_new_split: false,
        //     properties: Default::default(),
        // }).await;


        match self.catalog_manager.create_source(&source).await {
            Ok(x) => {
                Ok(Response::new(CreateSourceResponse {
                    status: None,
                    source_id: 0,
                    version: 0,
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    async fn create_materialized_source(
        &self,
        _request: Request<CreateMaterializedSourceRequest>,
    ) -> Result<Response<CreateMaterializedSourceResponse>, Status> {
        todo!()
    }

    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> Result<Response<CreateMaterializedViewResponse>, Status> {
        use crate::stream::CreateMaterializedViewContext;
        let req = request.into_inner();
        let id = self
            .id_gen_manager
            .generate::<{ IdCategory::Table }>()
            .await
            .map_err(tonic_err)? as u32;

        // 1. create mv in stream manager
        let hash_parallel_count = self
            .cluster_manager
            .get_parallel_unit_count(Some(ParallelUnitType::Hash))
            .await;
        let mut ctx = CreateMaterializedViewContext::default();
        let mut fragmenter = StreamFragmenter::new(
            self.id_gen_manager.clone(),
            self.fragment_manager.clone(),
            hash_parallel_count as u32,
        );
        let graph = fragmenter
            .generate_graph(req.get_stream_node().map_err(tonic_err)?, &mut ctx)
            .await
            .map_err(tonic_err)?;
        let table_fragments = TableFragments::new(TableId::new(id), graph);
        self.stream_manager
            .create_materialized_view(table_fragments, ctx)
            .await
            .map_err(tonic_err)?;

        // 2. append the mv into the catalog
        let mut mview = req.get_materialized_view().map_err(tonic_err)?.clone();
        mview.id = id as u32;
        let version = self
            .catalog_manager
            .create_table(&mview)
            .await
            .map_err(tonic_err)?;
        Ok(Response::new(CreateMaterializedViewResponse {
            status: None,
            table_id: id,
            version,
        }))
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
            .map_err(tonic_err)?;
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
            .map_err(tonic_err)?;
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
        request: Request<DropMaterializedViewRequest>,
    ) -> Result<Response<DropMaterializedViewResponse>, Status> {
        let req = request.into_inner();
        let mview_id = req.get_table_id();
        // 1. drop table in catalog
        let version = self
            .catalog_manager
            .drop_table(mview_id)
            .await
            .map_err(tonic_err)?;

        // 2. drop mv in stream manager
        // TODO: maybe we should refactor this and use catalog_v2's TableId (u32)
        self.stream_manager
            .drop_materialized_view(&TableRefId::from(&TableId::new(mview_id)))
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(DropMaterializedViewResponse {
            status: None,
            version,
        }))
    }
}
