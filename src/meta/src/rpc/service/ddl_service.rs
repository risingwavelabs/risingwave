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

#![allow(dead_code)]
use std::collections::HashSet;

use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::{tonic_err, Result as RwResult};
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::*;
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::*;
use risingwave_pb::plan::TableRefId;
use risingwave_pb::stream_plan::stream_node::Node;
use risingwave_pb::stream_plan::StreamNode;
use tonic::{Request, Response, Status};

use crate::cluster::ClusterManagerRef;
use crate::manager::{CatalogManagerRef, IdCategory, MetaSrvEnv, SourceId, TableId};
use crate::model::TableFragments;
use crate::storage::MetaStore;
use crate::stream::{
    FragmentManagerRef, GlobalStreamManagerRef, SourceManagerRef, StreamFragmenter,
};

#[derive(Clone)]
pub struct DdlServiceImpl<S: MetaStore> {
    env: MetaSrvEnv<S>,

    catalog_manager: CatalogManagerRef<S>,
    stream_manager: GlobalStreamManagerRef<S>,
    source_manager: SourceManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
}

impl<S> DdlServiceImpl<S>
where
    S: MetaStore,
{
    pub fn new(
        env: MetaSrvEnv<S>,
        catalog_manager: CatalogManagerRef<S>,
        stream_manager: GlobalStreamManagerRef<S>,
        source_manager: SourceManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
    ) -> Self {
        Self {
            env,
            catalog_manager,
            stream_manager,
            source_manager,
            cluster_manager,
            fragment_manager,
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
            .env
            .id_gen_manager()
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
            .env
            .id_gen_manager()
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
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::Table }>()
            .await
            .map_err(tonic_err)? as u32;

        self.catalog_manager
            .start_create_source_procedure(&source)
            .await
            .map_err(tonic_err)?;

        if let Err(e) = self.source_manager.create_source(&source).await {
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
        self.source_manager
            .drop_source(source_id)
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

        // 0. Generate an id from mview.
        let id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::Table }>()
            .await
            .map_err(tonic_err)? as u32;
        mview.id = id;

        // 1. Resolve the dependent relations.
        {
            // TODO: distinguish SourceId and TableId
            fn resolve_dependent_relations(
                stream_node: &StreamNode,
                dependent_relations: &mut HashSet<TableId>,
            ) -> RwResult<()> {
                match stream_node.node.as_ref().unwrap() {
                    Node::SourceNode(source_node) => {
                        dependent_relations.insert(source_node.get_table_ref_id()?.table_id as u32);
                    }
                    Node::ChainNode(chain_node) => {
                        dependent_relations.insert(chain_node.get_table_ref_id()?.table_id as u32);
                    }
                    _ => {}
                }
                for child in &stream_node.input {
                    resolve_dependent_relations(child, dependent_relations)?;
                }
                Ok(())
            }

            let mut dependent_relations = Default::default();
            resolve_dependent_relations(&stream_node, &mut dependent_relations)
                .map_err(tonic_err)?;
            assert!(
                !dependent_relations.is_empty(),
                "there should be at lease 1 dependent relation when creating materialized view"
            );
            mview.dependent_relations = dependent_relations.into_iter().collect();
        }

        // 2. Mark current mview as "creating" and add reference count to dependent relations.
        self.catalog_manager
            .start_create_table_procedure(&mview)
            .await
            .map_err(tonic_err)?;

        // 3. Create mview in stream manager. The id in stream node will be filled.
        if let Err(e) = self.create_mview_on_compute_node(stream_node, id).await {
            self.catalog_manager
                .cancel_create_table_procedure(&mview)
                .await
                .map_err(tonic_err)?;
            return Err(e.to_grpc_status());
        }

        // 4. Finally, update the catalog.
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
        use risingwave_common::catalog::TableId;

        let table_id = request.into_inner().table_id;
        // 1. Drop table in catalog. Ref count will be checked.
        let version = self
            .catalog_manager
            .drop_table(table_id)
            .await
            .map_err(tonic_err)?;

        // 2. drop mv in stream manager
        self.stream_manager
            .drop_materialized_view(&TableId::new(table_id))
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
            self.env.id_gen_manager_ref(),
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

    async fn create_materialized_source_inner(
        &self,
        mut source: Source,
        mut mview: Table,
        mut stream_node: StreamNode,
    ) -> RwResult<(SourceId, TableId, CatalogVersion)> {
        // Generate source id.
        let source_id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::Table }>() // TODO: use source category
            .await? as u32;
        source.id = source_id;

        self.catalog_manager
            .start_create_materialized_source_procedure(&source, &mview)
            .await?;

        // Create source on compute node.
        if let Err(e) = self.source_manager.create_source(&source).await {
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
            .env
            .id_gen_manager()
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
            self.source_manager.drop_source(source_id).await?;
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
        use risingwave_common::catalog::TableId;

        // 1. Drop materialized source in catalog, source_id will be checked if it is
        // associated_source_id in mview.
        let version = self
            .catalog_manager
            .drop_materialized_source(source_id, table_id)
            .await?;

        // 2. Drop source and mv separately.
        self.source_manager.drop_source(source_id).await?;
        self.stream_manager
            .drop_materialized_view(&TableId::new(table_id))
            .await?;

        Ok(version)
    }
}
