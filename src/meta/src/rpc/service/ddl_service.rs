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

use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::catalog::CatalogVersion;
use risingwave_common::error::{tonic_err, ErrorCode, Result as RwResult};
use risingwave_common::util::compress::compress_data;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::*;
use risingwave_pb::common::{ParallelUnitMapping, ParallelUnitType};
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::*;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{StreamFragmentGraph, StreamNode};
use tonic::{Request, Response, Status};

use crate::cluster::ClusterManagerRef;
use crate::manager::{CatalogManagerRef, IdCategory, MetaSrvEnv, SinkId, SourceId, TableId};
use crate::model::{FragmentId, TableFragments};
use crate::storage::MetaStore;
use crate::stream::{
    ActorGraphBuilder, CreateMaterializedViewContext, FragmentManagerRef, GlobalStreamManagerRef,
    SinkManagerRef, SourceManagerRef,
};

#[derive(Clone)]
pub struct DdlServiceImpl<S: MetaStore> {
    env: MetaSrvEnv<S>,

    catalog_manager: CatalogManagerRef<S>,
    stream_manager: GlobalStreamManagerRef<S>,
    source_manager: SourceManagerRef<S>,
    sink_manager: SinkManagerRef<S>,
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
        sink_manager: SinkManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
    ) -> Self {
        Self {
            env,
            catalog_manager,
            stream_manager,
            source_manager,
            sink_manager,
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
        source.id = id;

        self.catalog_manager
            .start_create_source_procedure(&source)
            .await
            .map_err(tonic_err)?;

        // QUESTION(patrick): why do we need to contact compute node on create source
        if let Err(e) = self.source_manager.create_source(&source).await {
            self.catalog_manager
                .cancel_create_source_procedure(&source)
                .await
                .map_err(tonic_err)?;
            return Err(e.into());
        }

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

    async fn create_sink(
        &self,
        request: Request<CreateSinkRequest>,
    ) -> Result<Response<CreateSinkResponse>, Status> {
        let req = request.into_inner();

        let sink = req.sink.unwrap();
        let fragment_graph = req.fragment_graph.unwrap();

        let (sink_id, version) = self
            .create_sink_inner(sink, fragment_graph)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(CreateSinkResponse {
            status: None,
            sink_id,
            version,
        }))
    }

    async fn drop_sink(
        &self,
        request: Request<DropSinkRequest>,
    ) -> Result<Response<DropSinkResponse>, Status> {
        let sink_id = request.into_inner().sink_id;

        // 1. Drop sink in catalog.
        let version = self
            .catalog_manager
            .drop_sink(sink_id)
            .await
            .map_err(tonic_err)?;

        // 2. Drop sink on compute nodes.
        self.sink_manager
            .drop_sink(sink_id)
            .await
            .map_err(tonic_err)?;

        Ok(Response::new(DropSinkResponse {
            status: None,
            version,
        }))
    }

    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> Result<Response<CreateMaterializedViewResponse>, Status> {
        self.env.idle_manager().record_activity();

        let req = request.into_inner();
        let mut mview = req.get_materialized_view().map_err(tonic_err)?.clone();
        let fragment_graph = req.get_fragment_graph().map_err(tonic_err)?.clone();
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
                match stream_node.node_body.as_ref().unwrap() {
                    NodeBody::Source(source_node) => {
                        dependent_relations.insert(source_node.get_table_id());
                    }
                    NodeBody::Chain(chain_node) => {
                        dependent_relations.insert(chain_node.get_table_id());
                    }
                    _ => {}
                }
                for child in &stream_node.input {
                    resolve_dependent_relations(child, dependent_relations)?;
                }
                Ok(())
            }

            let mut dependent_relations = Default::default();
            for fragment in fragment_graph.fragments.values() {
                resolve_dependent_relations(
                    fragment.node.as_ref().unwrap(),
                    &mut dependent_relations,
                )
                .map_err(tonic_err)?;
            }
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
        let mut ctx = CreateMaterializedViewContext {
            schema_id: mview.schema_id,
            database_id: mview.database_id,
            mview_name: mview.name.clone(),
            table_properties: mview.properties.clone(),
            affiliated_source: None,
            ..Default::default()
        };
        let internal_tables = match self
            .create_mview_on_compute_node(fragment_graph, id, &mut ctx)
            .await
        {
            Err(e) => {
                self.catalog_manager
                    .cancel_create_table_procedure(&mview)
                    .await
                    .map_err(tonic_err)?;
                return Err(e.into());
            }
            Ok(()) => {
                self.set_table_mapping(&mut mview).map_err(tonic_err)?;
                let mut internal_table = ctx
                    .internal_table_id_map
                    .iter()
                    .filter(|(_, table)| table.is_some())
                    .map(|(_, table)| table.clone().unwrap())
                    .collect_vec();

                for inner_table in &mut internal_table {
                    self.set_table_mapping(inner_table).map_err(tonic_err)?;
                }
                internal_table
            }
        };

        // tracing for checking the diff of catalog::Table and internal_table_id count
        tracing::info!(
            "create_materialized_view internal_table_count {} internal_table_id_count {}",
            internal_tables.len(),
            ctx.internal_table_id_map.len()
        );

        // 4. Finally, update the catalog.
        let version = self
            .catalog_manager
            .finish_create_table_procedure(internal_tables, &mview)
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

        self.env.idle_manager().record_activity();

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
        let fragment_graph = request.fragment_graph.unwrap();

        let (source_id, table_id, version) = self
            .create_materialized_source_inner(source, mview, fragment_graph)
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

    async fn risectl_list_state_tables(
        &self,
        _request: Request<RisectlListStateTablesRequest>,
    ) -> Result<Response<RisectlListStateTablesResponse>, Status> {
        use crate::model::MetadataModel;
        let tables = Table::list(self.env.meta_store())
            .await
            .map_err(tonic_err)?;
        Ok(Response::new(RisectlListStateTablesResponse { tables }))
    }
}

impl<S> DdlServiceImpl<S>
where
    S: MetaStore,
{
    async fn create_mview_on_compute_node(
        &self,
        mut fragment_graph: StreamFragmentGraph,
        id: TableId,
        ctx: &mut CreateMaterializedViewContext,
    ) -> RwResult<()> {
        use risingwave_common::catalog::TableId;

        // Fill in the correct mview id for stream node.
        fn fill_mview_id(stream_node: &mut StreamNode, mview_id: TableId) -> usize {
            let mut mview_count = 0;
            if let NodeBody::Materialize(materialize_node) = stream_node.node_body.as_mut().unwrap()
            {
                materialize_node.table_id = mview_id.table_id();
                mview_count += 1;
            }
            for input in &mut stream_node.input {
                mview_count += fill_mview_id(input, mview_id);
            }
            mview_count
        }

        let mview_id = TableId::new(id);
        let mut mview_count = 0;
        for fragment in fragment_graph.fragments.values_mut() {
            mview_count += fill_mview_id(fragment.node.as_mut().unwrap(), mview_id);
        }

        assert_eq!(
            mview_count, 1,
            "require exactly 1 materialize node when creating materialized view"
        );

        // Resolve fragments.
        let parallel_degree = self
            .cluster_manager
            .get_parallel_unit_count(Some(ParallelUnitType::Hash))
            .await;

        let mut actor_graph_builder =
            ActorGraphBuilder::new(self.env.id_gen_manager_ref(), &fragment_graph, ctx).await?;

        // TODO(Kexiang): now simply use Count(ParallelUnit) - 1 as parallelism of each fragment
        let parallelisms: HashMap<FragmentId, u32> = actor_graph_builder
            .list_fragment_ids()
            .into_iter()
            .map(|(fragment_id, is_singleton)| {
                if is_singleton {
                    (fragment_id, 1)
                } else {
                    (fragment_id, parallel_degree as u32)
                }
            })
            .collect();

        let graph = actor_graph_builder
            .generate_graph(
                self.env.id_gen_manager_ref(),
                self.fragment_manager.clone(),
                parallelisms,
                ctx,
            )
            .await?;

        let internal_table_id_set = ctx
            .internal_table_id_map
            .iter()
            .map(|(table_id, _)| *table_id)
            .collect::<HashSet<u32>>();

        assert_eq!(
            fragment_graph.table_ids_cnt,
            internal_table_id_set.len() as u32
        );

        let table_fragments = TableFragments::new(mview_id, graph, internal_table_id_set);

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
        mut fragment_graph: StreamFragmentGraph,
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
            let mut source_count = 0;
            if let NodeBody::Source(source_node) = stream_node.node_body.as_mut().unwrap() {
                // TODO: refactor using source id.
                source_node.table_id = source_id;
                source_count += 1;
            }
            for input in &mut stream_node.input {
                source_count += fill_source_id(input, source_id);
            }
            source_count
        }

        let mut source_count = 0;
        for fragment in fragment_graph.fragments.values_mut() {
            source_count += fill_source_id(fragment.node.as_mut().unwrap(), source_id);
        }
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
        // Noted that this progress relies on the source just created, so we pass it here.
        let mut ctx = CreateMaterializedViewContext {
            schema_id: source.schema_id,
            database_id: source.database_id,
            mview_name: source.name.clone(),
            table_properties: mview.properties.clone(),
            affiliated_source: Some(source.clone()),
            ..Default::default()
        };

        let internal_tables = match self
            .create_mview_on_compute_node(fragment_graph, mview_id, &mut ctx)
            .await
        {
            Err(e) => {
                self.catalog_manager
                    .cancel_create_materialized_source_procedure(&source, &mview)
                    .await?;
                // drop previously created source
                self.source_manager.drop_source(source_id).await?;
                return Err(e);
            }
            Ok(()) => {
                self.set_table_mapping(&mut mview).map_err(tonic_err)?;
                let mut internal_table = ctx
                    .internal_table_id_map
                    .iter()
                    .filter(|(_, table)| table.is_some())
                    .map(|(_, table)| table.clone().unwrap())
                    .collect_vec();

                for inner_table in &mut internal_table {
                    self.set_table_mapping(inner_table).map_err(tonic_err)?;
                }
                internal_table
            }
        };

        // Finally, update the catalog.
        let version = self
            .catalog_manager
            .finish_create_materialized_source_procedure(&source, &mview, internal_tables)
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
        // Note: we need to drop the materialized view to unmap the source_id to fragment_ids in
        // `SourceManager` before we can drop the source
        self.stream_manager
            .drop_materialized_view(&TableId::new(table_id))
            .await?;

        self.source_manager.drop_source(source_id).await?;

        Ok(version)
    }

    /// Fill in mview's vnode mapping so that frontend will know the data distribution.
    fn set_table_mapping(&self, table: &mut Table) -> RwResult<()> {
        let vnode_mapping = self
            .env
            .hash_mapping_manager_ref()
            .get_table_hash_mapping(&table.id);
        match vnode_mapping {
            Some(vnode_mapping) => {
                let (original_indices, data) = compress_data(&vnode_mapping);
                table.mapping = Some(ParallelUnitMapping {
                    table_id: table.id,
                    original_indices,
                    data,
                });
                Ok(())
            }
            None => Err(ErrorCode::InternalError(
                "no data distribution found for materialized view".to_string(),
            )
            .into()),
        }
    }

    async fn create_sink_inner(
        &self,
        mut sink: Sink,
        fragment_graph: StreamFragmentGraph,
    ) -> RwResult<(SinkId, u64)> {
        let sink_id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::Table }>()
            .await? as u32;
        sink.id = sink_id;

        // TODO(nanderstabel): Resolve code duplication (in fn create_materialized_view).
        // 1. Resolve the dependent relations.
        {
            fn resolve_dependent_relations(
                stream_node: &StreamNode,
                dependent_relations: &mut HashSet<TableId>,
            ) -> RwResult<()> {
                match stream_node.node_body.as_ref().unwrap() {
                    NodeBody::Source(source_node) => {
                        dependent_relations.insert(source_node.get_table_id());
                    }
                    NodeBody::Chain(chain_node) => {
                        dependent_relations.insert(chain_node.get_table_id());
                    }
                    _ => {}
                }
                for child in &stream_node.input {
                    resolve_dependent_relations(child, dependent_relations)?;
                }
                Ok(())
            }

            let mut dependent_relations = Default::default();
            for fragment in fragment_graph.fragments.values() {
                resolve_dependent_relations(
                    fragment.node.as_ref().unwrap(),
                    &mut dependent_relations,
                )
                .map_err(tonic_err)?;
            }
            assert!(
                !dependent_relations.is_empty(),
                "there should be at lease 1 dependent relation when creating sink"
            );
        }

        self.catalog_manager
            .start_create_sink_procedure(&sink)
            .await
            .map_err(tonic_err)?;

        if let Err(e) = self.sink_manager.create_sink(&sink).await {
            self.catalog_manager
                .cancel_create_sink_procedure(&sink)
                .await?;
            return Err(e);
        }

        let version = self
            .catalog_manager
            .finish_create_sink_procedure(&sink)
            .await
            .map_err(tonic_err)?;

        Ok((sink_id, version))
    }
}
