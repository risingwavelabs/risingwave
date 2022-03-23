// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#![allow(dead_code)]
use futures::future::try_join_all;
use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::{tonic_err, Result as RwResult, ToRwResult};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::*;
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::*;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::StreamNode;
use risingwave_pb::stream_service::{
    CreateSourceRequest as ComputeNodeCreateSourceRequest,
    DropSourceRequest as ComputeNodeDropSourceRequest,
};
use tonic::{Request, Response, Status};

use crate::cluster::StoredClusterManagerRef;
use crate::manager::{
    CatalogManagerRef, IdCategory, IdGeneratorManagerRef, MetaSrvEnv, SourceId, StreamClient,
    StreamClientsRef, TableId,
};
use crate::model::TableFragments;
use crate::storage::MetaStore;
use crate::stream::{FragmentManagerRef, StreamFragmenter, StreamManagerRef};

#[derive(Clone)]
pub struct DdlServiceImpl<S> {
    id_gen_manager: IdGeneratorManagerRef<S>,
    catalog_manager: CatalogManagerRef<S>,
    stream_manager: StreamManagerRef<S>,
    cluster_manager: StoredClusterManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,

    /// Clients to stream service on compute nodes
    stream_clients: StreamClientsRef,
}

impl<S> DdlServiceImpl<S>
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
impl<S> DdlService for DdlServiceImpl<S>
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
        let mut source = request.into_inner().source.unwrap();

        let id = self
            .id_gen_manager
            .generate::<{ IdCategory::Table }>()
            .await
            .map_err(tonic_err)? as u32;

        self.catalog_manager
            .start_create_source_procedure(&source)
            .await
            .map_err(tonic_err)?;

        if let Err(e) = self.create_source_on_compute_node(&source).await {
            self.catalog_manager
                .cancel_create_source_procedure(&source)
                .await
                .map_err(tonic_err)?;
            return Err(e.to_grpc_status());
        }

        source.id = id;
        let version = self
            .catalog_manager
            .finish_create_source_procedure(&source)
            .await
            .map_err(tonic_err)?;
        Ok(Response::new(CreateSourceResponse {
            status: None,
            source_id: id,
            version,
        }))
    }

    async fn drop_source(
        &self,
        request: Request<DropSourceRequest>,
    ) -> Result<Response<DropSourceResponse>, Status> {
        let source_id = request.into_inner().source_id;

        // 1. Drop source in catalog. Ref count will be checked.
        let version = self
            .catalog_manager
            .drop_source(source_id)
            .await
            .map_err(tonic_err)?;

        // 2. Drop source on compute nodes.
        self.drop_source_on_compute_node(source_id)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(DropSourceResponse {
            status: None,
            version,
        }))
    }

    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> Result<Response<CreateMaterializedViewResponse>, Status> {
        let req = request.into_inner();
        let mut mview = req.get_materialized_view().map_err(tonic_err)?.clone();
        let stream_node = req.get_stream_node().map_err(tonic_err)?.clone();

        let id = self
            .id_gen_manager
            .generate::<{ IdCategory::Table }>()
            .await
            .map_err(tonic_err)? as u32;
        mview.id = id;

        // 1. Mark current mview as "creating" and add reference count to dependent tables
        self.catalog_manager
            .start_create_table_procedure(&mview)
            .await
            .map_err(tonic_err)?;

        // 2. Create mview in stream manager
        if let Err(e) = self.create_mview_on_compute_node(stream_node, id).await {
            self.catalog_manager
                .cancel_create_table_procedure(&mview)
                .await
                .map_err(tonic_err)?;
            return Err(e.to_grpc_status());
        }

        // Finally, update the catalog.
        let version = self
            .catalog_manager
            .finish_create_table_procedure(&mview)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(CreateMaterializedViewResponse {
            status: None,
            table_id: id,
            version,
        }))
    }

    async fn drop_materialized_view(
        &self,
        request: Request<DropMaterializedViewRequest>,
    ) -> Result<Response<DropMaterializedViewResponse>, Status> {
        let table_id = request.into_inner().table_id;
        // 1. Drop table in catalog. Ref count will be checked.
        let version = self
            .catalog_manager
            .drop_table(table_id)
            .await
            .map_err(tonic_err)?;

        // 2. drop mv in stream manager
        self.drop_mview_on_compute_node(table_id)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(DropMaterializedViewResponse {
            status: None,
            version,
        }))
    }

    async fn create_materialized_source(
        &self,
        request: Request<CreateMaterializedSourceRequest>,
    ) -> Result<Response<CreateMaterializedSourceResponse>, Status> {
        let request = request.into_inner();
        let source = request.source.unwrap();
        let mview = request.materialized_view.unwrap();
        let stream_node = request.stream_node.unwrap();

        let (source_id, table_id, version) = self
            .create_materialized_source_inner(source, mview, stream_node)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(CreateMaterializedSourceResponse {
            status: None,
            source_id,
            table_id,
            version,
        }))
    }

    async fn drop_materialized_source(
        &self,
        request: Request<DropMaterializedSourceRequest>,
    ) -> Result<Response<DropMaterializedSourceResponse>, Status> {
        let request = request.into_inner();
        let source_id = request.source_id;
        let table_id = request.table_id;

        let version = self
            .drop_materialized_source_inner(source_id, table_id)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(DropMaterializedSourceResponse {
            status: None,
            version,
        }))
    }
}

impl<S> DdlServiceImpl<S>
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

    async fn create_source_on_compute_node(&self, source: &Source) -> RwResult<()> {
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

        Ok(())
    }

    async fn drop_source_on_compute_node(&self, source_id: SourceId) -> RwResult<()> {
        let futures = self
            .all_stream_clients()
            .await?
            .into_iter()
            .map(|mut client| {
                let request = ComputeNodeDropSourceRequest { source_id };
                async move { client.drop_source(request).await.to_rw_result() }
            });
        let _responses: Vec<_> = try_join_all(futures).await?;

        Ok(())
    }

    async fn create_mview_on_compute_node(
        &self,
        mut stream_node: StreamNode,
        id: TableId,
    ) -> RwResult<()> {
        use risingwave_common::catalog::TableId;

        use crate::stream::CreateMaterializedViewContext;

        // Fill in the correct mview id for stream node.
        fn fill_mview_id(stream_node: &mut StreamNode, mview_id: TableId) -> usize {
            let mut mview_count = 0;
            if let Node::MaterializeNode(materialize_node) = stream_node.node.as_mut().unwrap() {
                materialize_node.table_ref_id = TableRefId::from(&mview_id).into();
                mview_count += 1;
            }
            for input in &mut stream_node.input {
                mview_count += fill_mview_id(input, mview_id);
            }
            mview_count
        }

        let mview_id = TableId::new(id);
        let mview_count = fill_mview_id(&mut stream_node, mview_id);
        assert_eq!(
            mview_count, 1,
            "require exactly 1 materialize node when creating materialized view"
        );

        // Resolve fragments.
        let hash_mapping = self.cluster_manager.get_hash_mapping().await;
        let mut ctx = CreateMaterializedViewContext::default();
        let mut fragmenter = StreamFragmenter::new(
            self.id_gen_manager.clone(),
            self.fragment_manager.clone(),
            hash_mapping,
        );
        let graph = fragmenter.generate_graph(&stream_node, &mut ctx).await?;
        let table_fragments = TableFragments::new(mview_id, graph);

        // Create on compute node.
        self.stream_manager
            .create_materialized_view(table_fragments, ctx)
            .await?;

        Ok(())
    }

    async fn drop_mview_on_compute_node(&self, table_id: u32) -> RwResult<()> {
        use risingwave_common::catalog::TableId;
        // TODO: maybe we should refactor this and use catalog_v2's TableId (u32)
        self.stream_manager
            .drop_materialized_view(&TableRefId::from(&TableId::new(table_id)))
            .await?;

        Ok(())
    }

    async fn create_materialized_source_inner(
        &self,
        mut source: Source,
        mut mview: Table,
        mut stream_node: StreamNode,
    ) -> RwResult<(SourceId, TableId, CatalogVersion)> {
        // Generate source id.
        let source_id = self
            .id_gen_manager
            .generate::<{ IdCategory::Table }>() // TODO: use source category
            .await? as u32;
        source.id = source_id;

        self.catalog_manager
            .start_create_materialized_source_procedure(&source, &mview)
            .await?;

        // Create source on compute node.
        if let Err(e) = self.create_source_on_compute_node(&source).await {
            self.catalog_manager
                .cancel_create_materialized_source_procedure(&source, &mview)
                .await?;
            return Err(e);
        }

        // Fill in the correct source id for stream node.
        fn fill_source_id(stream_node: &mut StreamNode, source_id: u32) -> usize {
            use risingwave_common::catalog::TableId;
            let mut source_count = 0;
            if let Node::SourceNode(source_node) = stream_node.node.as_mut().unwrap() {
                // TODO: refactor using source id.
                source_node.table_ref_id = TableRefId::from(&TableId::new(source_id)).into();
                source_count += 1;
            }
            for input in &mut stream_node.input {
                source_count += fill_source_id(input, source_id);
            }
            source_count
        }

        let source_count = fill_source_id(&mut stream_node, source_id);
        assert_eq!(
            source_count, 1,
            "require exactly 1 source node when creating materialized source"
        );

        // Fill in the correct source id for mview.
        mview.optional_associated_source_id =
            Some(OptionalAssociatedSourceId::AssociatedSourceId(source_id));

        // Generate mview id.
        let mview_id = self
            .id_gen_manager
            .generate::<{ IdCategory::Table }>()
            .await? as u32;
        mview.id = mview_id;

        // Create mview on compute node.
        if let Err(e) = self
            .create_mview_on_compute_node(stream_node, mview_id)
            .await
        {
            self.catalog_manager
                .cancel_create_materialized_source_procedure(&source, &mview)
                .await?;
            // drop previously created source
            self.drop_source_on_compute_node(source_id).await?;
            return Err(e);
        }

        // Finally, update the catalog.
        let version = self
            .catalog_manager
            .finish_create_materialized_source_procedure(&source, &mview)
            .await?;

        Ok((source_id, mview_id, version))
    }

    async fn drop_materialized_source_inner(
        &self,
        source_id: SourceId,
        table_id: TableId,
    ) -> RwResult<CatalogVersion> {
        // 1. Drop materialized source in catalog, source_id will be checked if it is
        // associated_source_id in mview.
        let version = self
            .catalog_manager
            .drop_materialized_source(source_id, table_id)
            .await?;

        // 2. Drop source and mv separately.
        self.drop_source_on_compute_node(source_id).await?;
        self.drop_mview_on_compute_node(table_id).await?;

        Ok(version)
    }
}
