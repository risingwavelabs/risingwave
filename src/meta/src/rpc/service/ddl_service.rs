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

use std::collections::HashSet;
use std::sync::Arc;

use itertools::Itertools;
use risingwave_common::catalog::CatalogVersion;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::*;
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::*;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{StreamFragmentGraph, StreamNode};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::error::meta_error_to_tonic;
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, IdCategory, MetaSrvEnv, Relation,
    SourceId, TableId,
};
use crate::model::TableFragments;
use crate::storage::MetaStore;
use crate::stream::{
    ActorGraphBuilder, CreateMaterializedViewContext, GlobalStreamManagerRef, SourceManagerRef,
};
use crate::MetaResult;

#[derive(Clone)]
pub struct DdlServiceImpl<S: MetaStore> {
    env: MetaSrvEnv<S>,

    catalog_manager: CatalogManagerRef<S>,
    stream_manager: GlobalStreamManagerRef<S>,
    source_manager: SourceManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
    ddl_lock: Arc<RwLock<()>>,
}

impl<S> DdlServiceImpl<S>
where
    S: MetaStore,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        env: MetaSrvEnv<S>,
        catalog_manager: CatalogManagerRef<S>,
        stream_manager: GlobalStreamManagerRef<S>,
        source_manager: SourceManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        ddl_lock: Arc<RwLock<()>>,
    ) -> Self {
        Self {
            env,
            catalog_manager,
            stream_manager,
            source_manager,
            cluster_manager,
            fragment_manager,
            ddl_lock,
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
            .map_err(meta_error_to_tonic)? as u32;
        let mut database = req.get_db().map_err(meta_error_to_tonic)?.clone();
        database.id = id;
        let version = self.catalog_manager.create_database(&database).await?;

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
        let version = self.catalog_manager.drop_database(database_id).await?;
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
            .map_err(meta_error_to_tonic)? as u32;
        let mut schema = req.get_schema().map_err(meta_error_to_tonic)?.clone();
        schema.id = id;
        let version = self.catalog_manager.create_schema(&schema).await?;

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
        let version = self.catalog_manager.drop_schema(schema_id).await?;
        Ok(Response::new(DropSchemaResponse {
            status: None,
            version,
        }))
    }

    async fn create_source(
        &self,
        request: Request<CreateSourceRequest>,
    ) -> Result<Response<CreateSourceResponse>, Status> {
        let _ddl_lock = self.ddl_lock.read().await;
        let mut source = request.into_inner().source.unwrap();

        let id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::Table }>()
            .await
            .map_err(meta_error_to_tonic)? as u32;
        source.id = id;

        self.catalog_manager
            .start_create_source_procedure(&source)
            .await?;

        // QUESTION(patrick): why do we need to contact compute node on create source
        if let Err(e) = self.source_manager.create_source(&source).await {
            self.catalog_manager
                .cancel_create_source_procedure(&source)
                .await?;
            return Err(e.into());
        }

        let version = self
            .catalog_manager
            .finish_create_source_procedure(&source)
            .await?;
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
        let _ddl_lock = self.ddl_lock.read().await;
        let source_id = request.into_inner().source_id;

        // 1. Drop source in catalog. Ref count will be checked.
        let version = self.catalog_manager.drop_source(source_id).await?;

        // 2. Drop source on compute nodes.
        self.source_manager.drop_source(source_id).await?;

        Ok(Response::new(DropSourceResponse {
            status: None,
            version,
        }))
    }

    async fn create_sink(
        &self,
        request: Request<CreateSinkRequest>,
    ) -> Result<Response<CreateSinkResponse>, Status> {
        let _ddl_lock = self.ddl_lock.read().await;
        self.env.idle_manager().record_activity();

        let req = request.into_inner();
        let sink = req.get_sink().map_err(meta_error_to_tonic)?.clone();
        let fragment_graph = req
            .get_fragment_graph()
            .map_err(meta_error_to_tonic)?
            .clone();

        let (sink_id, version) = self
            .create_relation(&mut Relation::Sink(sink), fragment_graph)
            .await?;

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
        use risingwave_common::catalog::TableId;
        let sink_id = request.into_inner().sink_id;

        // 1. Drop sink in catalog.
        let version = self.catalog_manager.drop_sink(sink_id).await?;

        // 2. drop sink in stream manager
        let _table_fragments = self
            .stream_manager
            .drop_materialized_view(&TableId::new(sink_id))
            .await?;

        Ok(Response::new(DropSinkResponse {
            status: None,
            version,
        }))
    }

    async fn create_materialized_view(
        &self,
        request: Request<CreateMaterializedViewRequest>,
    ) -> Result<Response<CreateMaterializedViewResponse>, Status> {
        let _ddl_lock = self.ddl_lock.read().await;
        self.env.idle_manager().record_activity();

        let req = request.into_inner();
        let mview = req
            .get_materialized_view()
            .map_err(meta_error_to_tonic)?
            .clone();
        let fragment_graph = req
            .get_fragment_graph()
            .map_err(meta_error_to_tonic)?
            .clone();

        let (table_id, version) = self
            .create_relation(&mut Relation::Table(mview), fragment_graph)
            .await?;

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
        let _ddl_lock = self.ddl_lock.read().await;
        use risingwave_common::catalog::TableId;

        self.env.idle_manager().record_activity();

        let table_id = request.into_inner().table_id;
        let table_fragment = self
            .fragment_manager
            .select_table_fragments_by_table_id(&table_id.into())
            .await?;
        let internal_tables = table_fragment.internal_table_ids();
        // 1. Drop table in catalog. Ref count will be checked.
        let version = self
            .catalog_manager
            .drop_table(table_id, internal_tables)
            .await?;

        // 2. drop mv in stream manager
        let table_fragments = self
            .stream_manager
            .drop_materialized_view(&TableId::new(table_id))
            .await?;

        self.notify_table_mapping(&table_fragments, Operation::Delete)
            .await;

        Ok(Response::new(DropMaterializedViewResponse {
            status: None,
            version,
        }))
    }

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        let _ddl_lock = self.ddl_lock.read().await;
        self.env.idle_manager().record_activity();

        let req = request.into_inner();
        let index = req.get_index().map_err(meta_error_to_tonic)?.clone();
        let index_table = req.get_index_table().map_err(meta_error_to_tonic)?.clone();
        let fragment_graph = req
            .get_fragment_graph()
            .map_err(meta_error_to_tonic)?
            .clone();

        let (index_id, version) = self
            .create_relation(&mut Relation::Index(index, index_table), fragment_graph)
            .await?;

        Ok(Response::new(CreateIndexResponse {
            status: None,
            index_id,
            version,
        }))
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        let _ddl_lock = self.ddl_lock.read().await;
        use risingwave_common::catalog::TableId;

        self.env.idle_manager().record_activity();

        let index_id = request.into_inner().index_id;
        let index_table_id = self.catalog_manager.get_index_table(index_id).await?;
        let table_fragment = self
            .fragment_manager
            .select_table_fragments_by_table_id(&index_table_id.into())
            .await?;
        let internal_tables = table_fragment.internal_table_ids();

        // 1. Drop index in catalog. Ref count will be checked.
        let version = self
            .catalog_manager
            .drop_index(index_id, index_table_id, internal_tables)
            .await?;

        // 2. drop mv(index) in stream manager
        self.stream_manager
            .drop_materialized_view(&TableId::new(index_table_id))
            .await?;

        Ok(Response::new(DropIndexResponse {
            status: None,
            version,
        }))
    }

    async fn create_materialized_source(
        &self,
        request: Request<CreateMaterializedSourceRequest>,
    ) -> Result<Response<CreateMaterializedSourceResponse>, Status> {
        let _ddl_lock = self.ddl_lock.read().await;
        let request = request.into_inner();
        let source = request.source.unwrap();
        let mview = request.materialized_view.unwrap();
        let fragment_graph = request.fragment_graph.unwrap();

        let (source_id, table_id, version) = self
            .create_materialized_source_inner(source, mview, fragment_graph)
            .await?;

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
        let _ddl_lock = self.ddl_lock.read().await;
        let request = request.into_inner();
        let source_id = request.source_id;
        let table_id = request.table_id;

        let version = self
            .drop_materialized_source_inner(source_id, table_id)
            .await?;

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
            .map_err(meta_error_to_tonic)?;
        Ok(Response::new(RisectlListStateTablesResponse { tables }))
    }
}

impl<S> DdlServiceImpl<S>
where
    S: MetaStore,
{
    async fn notify_table_mapping(&self, table_fragment: &TableFragments, operation: Operation) {
        for table_id in table_fragment
            .internal_table_ids()
            .into_iter()
            .chain(std::iter::once(table_fragment.table_id().table_id))
        {
            let mapping = table_fragment
                .get_table_hash_mapping(table_id)
                .expect("no data distribution found");
            self.env
                .notification_manager()
                .notify_frontend(operation, Info::ParallelUnitMapping(mapping))
                .await;
        }
    }

    // Creates relation. `Relation` can be either a `Table` or a `Sink`.
    async fn create_relation(
        &self,
        relation: &mut Relation,
        fragment_graph: StreamFragmentGraph,
    ) -> MetaResult<(u32, u64)> {
        // 0. Generate an id from relation.
        let id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::Table }>()
            .await? as u32;
        relation.set_id(id);

        let mview_id = match relation {
            Relation::Table(table) => table.id,
            Relation::Index(_, table) => table.id,
            _ => 0,
        };

        // 1. Resolve the dependent relations.
        let dependent_relations = get_dependent_relations(&fragment_graph)?;
        assert!(
            !dependent_relations.is_empty(),
            "there should be at lease 1 dependent relation when creating sink"
        );
        relation.set_dependent_relations(dependent_relations);

        // 2. Mark current relation as "creating" and add reference count to dependent relations.
        self.catalog_manager
            .start_create_procedure(relation)
            .await?;

        // 3. Create relation in stream manager. The id in stream node will be filled.
        let mut ctx = CreateMaterializedViewContext {
            schema_id: relation.schema_id(),
            database_id: relation.database_id(),
            mview_name: relation.name(),
            table_properties: relation.properties(),
            affiliated_source: None,
            ..Default::default()
        };
        let res = self
            .create_relation_on_compute_node(relation, fragment_graph, id, &mut ctx)
            .await;
        if let Err(err) = res {
            self.stream_manager
                .remove_processing_table(
                    ctx.internal_table_ids()
                        .into_iter()
                        .chain(std::iter::once(mview_id))
                        .collect_vec(),
                    true,
                )
                .await;

            self.catalog_manager
                .cancel_create_procedure(relation)
                .await?;
            return Err(err);
        };
        let table_fragment = res?;

        // 4. Notify vnode mapping info to frontend.
        match relation {
            Relation::Table(_) | Relation::Index(..) => {
                self.notify_table_mapping(&table_fragment, Operation::Add)
                    .await
            }
            _ => {}
        }

        // 5. Finally, update the catalog.
        let version = self
            .catalog_manager
            .finish_create_procedure(
                match relation {
                    Relation::Table(_) | Relation::Index(..) => Some(ctx.internal_tables()),
                    Relation::Sink(_) => None,
                },
                relation,
            )
            .await;

        self.stream_manager
            .remove_processing_table(
                ctx.internal_table_ids()
                    .into_iter()
                    .chain(std::iter::once(mview_id))
                    .collect_vec(),
                false,
            )
            .await;

        Ok((id, version?))
    }

    async fn create_relation_on_compute_node(
        &self,
        relation: &Relation,
        mut fragment_graph: StreamFragmentGraph,
        id: TableId,
        ctx: &mut CreateMaterializedViewContext,
    ) -> MetaResult<TableFragments> {
        use risingwave_common::catalog::TableId;

        // Get relation_id and make fragment_graph immutable.
        let (relation_id, fragment_graph) = match relation {
            Relation::Table(_) | Relation::Index(..) => {
                // Fill in the correct mview id for stream node.
                fn fill_mview_id(stream_node: &mut StreamNode, mview_id: TableId) -> usize {
                    let mut mview_count = 0;
                    if let NodeBody::Materialize(materialize_node) =
                        stream_node.node_body.as_mut().unwrap()
                    {
                        materialize_node.table_id = mview_id.table_id();
                        materialize_node.table.as_mut().unwrap().id = mview_id.table_id();
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
                (mview_id, fragment_graph)
            }
            Relation::Sink(_) => (TableId::new(id), fragment_graph),
        };

        // Resolve fragments.
        let parallel_degree = self.cluster_manager.get_parallel_unit_count().await;

        let mut actor_graph_builder = ActorGraphBuilder::new(
            self.env.id_gen_manager_ref(),
            &fragment_graph,
            parallel_degree as u32,
            ctx,
        )
        .await?;

        let graph = actor_graph_builder
            .generate_graph(
                self.env.id_gen_manager_ref(),
                self.fragment_manager.clone(),
                ctx,
            )
            .await?;

        assert_eq!(
            fragment_graph.table_ids_cnt,
            ctx.internal_table_ids().len() as u32
        );

        let mut table_fragments = TableFragments::new(relation_id, graph);

        // Create on compute node.
        self.stream_manager
            .create_materialized_view(relation, &mut table_fragments, ctx)
            .await?;
        Ok(table_fragments)
    }

    async fn create_materialized_source_inner(
        &self,
        mut source: Source,
        mut mview: Table,
        mut fragment_graph: StreamFragmentGraph,
    ) -> MetaResult<(SourceId, TableId, CatalogVersion)> {
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
                source_node.source_id = source_id;
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

        let res = self
            .create_relation_on_compute_node(
                &Relation::Table(mview.clone()),
                fragment_graph,
                mview_id,
                &mut ctx,
            )
            .await;

        if let Err(err) = res {
            self.stream_manager
                .remove_processing_table(
                    ctx.internal_table_ids()
                        .into_iter()
                        .chain(std::iter::once(mview_id))
                        .collect_vec(),
                    true,
                )
                .await;

            self.catalog_manager
                .cancel_create_materialized_source_procedure(&source, &mview)
                .await?;
            self.source_manager.drop_source(source_id).await?;
            return Err(err);
        }
        let table_fragment = res?;

        // Notify vnode mapping info to frontend.
        self.notify_table_mapping(&table_fragment, Operation::Add)
            .await;

        // Finally, update the catalog.
        let version = self
            .catalog_manager
            .finish_create_materialized_source_procedure(&source, &mview, ctx.internal_tables())
            .await;

        self.stream_manager
            .remove_processing_table(
                ctx.internal_table_ids()
                    .into_iter()
                    .chain(std::iter::once(mview_id))
                    .collect_vec(),
                false,
            )
            .await;
        Ok((source_id, mview_id, version?))
    }

    async fn drop_materialized_source_inner(
        &self,
        source_id: SourceId,
        table_id: TableId,
    ) -> MetaResult<CatalogVersion> {
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
        let table_fragments = self
            .stream_manager
            .drop_materialized_view(&TableId::new(table_id))
            .await?;

        self.notify_table_mapping(&table_fragments, Operation::Delete)
            .await;

        self.source_manager.drop_source(source_id).await?;

        Ok(version)
    }
}

fn get_dependent_relations(fragment_graph: &StreamFragmentGraph) -> MetaResult<Vec<TableId>> {
    // TODO: distinguish SourceId and TableId
    fn resolve_dependent_relations(
        stream_node: &StreamNode,
        dependent_relations: &mut HashSet<TableId>,
    ) -> MetaResult<()> {
        match stream_node.node_body.as_ref().unwrap() {
            NodeBody::Source(source_node) => {
                dependent_relations.insert(source_node.get_source_id());
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
        resolve_dependent_relations(fragment.node.as_ref().unwrap(), &mut dependent_relations)?;
    }
    Ok(dependent_relations.into_iter().collect())
}
