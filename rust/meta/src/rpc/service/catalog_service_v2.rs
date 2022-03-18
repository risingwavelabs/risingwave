#![allow(dead_code)]
use futures::future::try_join_all;
use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_common::error::{tonic_err, Result as RwResult, ToRwResult};
use risingwave_pb::catalog::catalog_service_server::CatalogService;
use risingwave_pb::catalog::*;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::stream_service::{
    CreateSourceRequest as ComputeNodeCreateSourceRequest,
    DropSourceRequest as ComputeNodeDropSourceRequest,
};
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManagerRef;
use crate::manager::{
    CatalogManagerRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv, SourceId, StreamClient,
    StreamClientsRef,
};
use crate::model::TableFragments;
use crate::storage::MetaStore;
use crate::stream::{FragmentManagerRef, StreamFragmenter, StreamManagerRef};

#[derive(Clone)]
pub struct CatalogServiceImpl<S> {
    id_gen_manager: IdGeneratorManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    stream_manager: StreamManagerRef<S>,
    cluster_manager: StoredClusterManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,

    /// Clients to stream service on compute nodes
    stream_clients: StreamClientsRef,
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
    ) -> Self {
        Self {
            id_gen_manager: env.id_gen_manager_ref(),
            catalog_manager,
            stream_manager,
            cluster_manager,
            fragment_manager,
            stream_clients: env.stream_clients_ref(),
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

    async fn create_source(
        &self,
        request: Request<CreateSourceRequest>,
    ) -> Result<Response<CreateSourceResponse>, Status> {
        let source = request.into_inner().source.unwrap();
        let (source_id, version) = self.create_source_inner(source).await.map_err(tonic_err)?;

        Ok(Response::new(CreateSourceResponse {
            status: None,
            source_id,
            version,
        }))
    }

    async fn drop_source(
        &self,
        request: Request<DropSourceRequest>,
    ) -> Result<Response<DropSourceResponse>, Status> {
        let source_id = request.into_inner().source_id;
        let version = self.drop_source_inner(source_id).await.map_err(tonic_err)?;

        Ok(Response::new(DropSourceResponse {
            status: None,
            version,
        }))
    }

    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> Result<Response<CreateMaterializedViewResponse>, Status> {
        let request = request.into_inner();
        let mview = request.materialized_view.unwrap();
        let stream_node = request.stream_node.unwrap();

        let (table_id, version) = self
            .create_materialized_view_inner(mview, stream_node)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(CreateMaterializedViewResponse {
            status: None,
            table_id,
            version,
        }))
    }

    async fn drop_materialized_view(
        &self,
        request: Request<DropMaterializedViewRequest>,
    ) -> Result<Response<DropMaterializedViewResponse>, Status> {
        let table_id = request.into_inner().table_id;
        let version = self
            .drop_materialized_view_inner(table_id)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(DropMaterializedViewResponse {
            status: None,
            version,
        }))
    }

    async fn create_materialized_source(
        &self,
        _request: Request<CreateMaterializedSourceRequest>,
    ) -> Result<Response<CreateMaterializedSourceResponse>, Status> {
        todo!()
    }

    async fn drop_materialized_source(
        &self,
        _request: Request<DropMaterializedSourceRequest>,
    ) -> Result<Response<DropMaterializedSourceResponse>, Status> {
        todo!()
    }
}

impl<S> CatalogServiceImpl<S>
where
    S: MetaStore,
{
    async fn all_stream_clients(&self) -> RwResult<impl Iterator<Item = StreamClient>> {
        let all_compute_nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, Some(Running))
            .await;

        let all_stream_clients = try_join_all(
            all_compute_nodes
                .iter()
                .map(|worker| self.stream_clients.get(worker)),
        )
        .await?
        .into_iter();

        Ok(all_stream_clients)
    }

    async fn create_source_inner(
        &self,
        mut source: Source,
    ) -> RwResult<(SourceId, CatalogVersion)> {
        // 0. Generate source id.
        let source = {
            let id = self
                .id_gen_manager
                .generate::<{ IdCategory::Table }>() // TODO: use a separated catagory for source ids
                .await? as SourceId;
            source.id = id;
            source
        };

        // 1. Create source on compute nodes.
        // TODO: restore the source on other nodes when scale out / fail over
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeCreateSourceRequest {
                    source: Some(source.clone()),
                };
                async move { client.create_source(request).await.to_rw_result() }
            });
        let _responses: Vec<_> = try_join_all(futures).await?;

        // 2. Update the source catalog.
        let version = self.catalog_manager.create_source(&source).await?;

        Ok((source.id, version))
    }

    async fn drop_source_inner(&self, source_id: SourceId) -> RwResult<CatalogVersion> {
        // 1. Drop source in catalog. Ref count will be checked.
        let version = self.catalog_manager.drop_source(source_id).await?;

        // 2. Drop source on compute nodes.
        // TODO: restore the source on other nodes when scale out / fail over
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeDropSourceRequest { source_id };
                async move { client.drop_source(request).await.to_rw_result() }
            });
        let _responses: Vec<_> = try_join_all(futures).await?;

        Ok(version)
    }

    async fn create_materialized_view_inner(
        &self,
        mut mview: Table,
        stream_node: StreamNode,
    ) -> RwResult<(u32, CatalogVersion)> {
        use crate::stream::CreateMaterializedViewContext;

        // 0. Generate mview id.
        let mview = {
            let id = self
                .id_gen_manager
                .generate::<{ IdCategory::Table }>()
                .await? as u32;
            mview.id = id;
            mview
        };

        // 1. Create mv in stream manager.
        let hash_mapping = self.cluster_manager.get_hash_mapping().await;
        let mut ctx = CreateMaterializedViewContext::default();
        let mut fragmenter = StreamFragmenter::new(
            self.id_gen_manager.clone(),
            self.fragment_manager.clone(),
            hash_mapping,
        );
        let graph = fragmenter.generate_graph(&stream_node, &mut ctx).await?;
        let table_fragments = TableFragments::new(TableId::new(mview.id), graph);
        self.stream_manager
            .create_materialized_view(table_fragments, ctx)
            .await?;

        // 2. Update the table catalog.
        let version = self.catalog_manager.create_table(&mview).await?;

        Ok((mview.id, version))
    }

    async fn drop_materialized_view_inner(&self, table_id: u32) -> RwResult<CatalogVersion> {
        // 1. Drop table in catalog. Ref count will be checked.
        let version = self.catalog_manager.drop_table(table_id).await?;

        // 2. drop mv in stream manager
        // TODO: maybe we should refactor this and use catalog_v2's TableId (u32)
        self.stream_manager
            .drop_materialized_view(&TableRefId::from(&TableId::new(table_id)))
            .await?;

        Ok(version)
    }
}
