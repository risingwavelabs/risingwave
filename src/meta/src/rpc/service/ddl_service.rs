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
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::WorkerType;
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::*;
use risingwave_pb::meta::subscribe_response::Operation;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{StreamFragmentGraph, StreamNode};
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};

use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, IdCategory, IdCategoryType,
    MetaSrvEnv, NotificationVersion, SourceId, StreamJobId, StreamingJob,
    StreamingJobBackgroundDeleterRef, TableId,
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
    table_background_deleter: StreamingJobBackgroundDeleterRef,
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
        table_background_deleter: StreamingJobBackgroundDeleterRef,
        ddl_lock: Arc<RwLock<()>>,
    ) -> Self {
        Self {
            env,
            catalog_manager,
            stream_manager,
            source_manager,
            cluster_manager,
            fragment_manager,
            table_background_deleter,
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
        let id = self.gen_unique_id::<{ IdCategory::Database }>().await?;
        let mut database = req.get_db()?.clone();
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
        let (version, catalog_ids) = self.catalog_manager.drop_database(database_id).await?;

        self.table_background_deleter.delete(catalog_ids);

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
        let id = self.gen_unique_id::<{ IdCategory::Schema }>().await?;
        let mut schema = req.get_schema()?.clone();
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
        let mut source = request.into_inner().get_source()?.clone();

        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
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

        // 2. Drop source in table background deleter asynchronously.
        self.table_background_deleter
            .delete(vec![StreamJobId::SourceId(source_id)]);

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
        let sink = req.get_sink()?.clone();
        let fragment_graph = req.get_fragment_graph()?.clone();

        let mut stream_job = StreamingJob::Sink(sink);
        let version = self
            .create_stream_job(&mut stream_job, fragment_graph)
            .await?;

        Ok(Response::new(CreateSinkResponse {
            status: None,
            sink_id: stream_job.id(),
            version,
        }))
    }

    async fn drop_sink(
        &self,
        request: Request<DropSinkRequest>,
    ) -> Result<Response<DropSinkResponse>, Status> {
        let sink_id = request.into_inner().sink_id;

        // 1. Drop sink in catalog.
        let version = self.catalog_manager.drop_sink(sink_id).await?;

        // 2. drop sink in table background deleter asynchronously.
        self.table_background_deleter
            .delete(vec![StreamJobId::SinkId(sink_id.into())]);

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
        let mview = req.get_materialized_view()?.clone();
        let fragment_graph = req.get_fragment_graph()?.clone();

        let mut stream_job = StreamingJob::MaterializedView(mview);
        let version = self
            .create_stream_job(&mut stream_job, fragment_graph)
            .await?;

        Ok(Response::new(CreateMaterializedViewResponse {
            status: None,
            table_id: stream_job.id(),
            version,
        }))
    }

    async fn drop_materialized_view(
        &self,
        request: Request<DropMaterializedViewRequest>,
    ) -> Result<Response<DropMaterializedViewResponse>, Status> {
        let _ddl_lock = self.ddl_lock.read().await;

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

        // 2. drop mv in table background deleter asynchronously.
        self.table_background_deleter
            .delete(vec![StreamJobId::TableId(table_id.into())]);

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
        let index = req.get_index()?.clone();
        let index_table = req.get_index_table()?.clone();
        let fragment_graph = req.get_fragment_graph()?.clone();

        let mut stream_job = StreamingJob::Index(index, index_table);
        let version = self
            .create_stream_job(&mut stream_job, fragment_graph)
            .await?;

        Ok(Response::new(CreateIndexResponse {
            status: None,
            index_id: stream_job.id(),
            version,
        }))
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        let _ddl_lock = self.ddl_lock.read().await;

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

        // 2. drop mv(index) in table background deleter asynchronously.
        self.table_background_deleter
            .delete(vec![StreamJobId::TableId(index_table_id.into())]);

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
        let tables = Table::list(self.env.meta_store()).await?;
        Ok(Response::new(RisectlListStateTablesResponse { tables }))
    }
}

impl<S> DdlServiceImpl<S>
where
    S: MetaStore,
{
    /// `create_stream_job` creates a stream job and returns the version of the catalog.
    async fn create_stream_job(
        &self,
        stream_job: &mut StreamingJob,
        fragment_graph: StreamFragmentGraph,
    ) -> MetaResult<NotificationVersion> {
        let (mut ctx, mut table_fragments) =
            self.prepare_stream_job(stream_job, fragment_graph).await?;
        match self
            .stream_manager
            .create_materialized_view(&mut table_fragments, &mut ctx)
            .await
        {
            Ok(_) => {
                self.finish_stream_job(stream_job, &table_fragments, &ctx)
                    .await
            }
            Err(err) => {
                self.cancel_stream_job(stream_job, &ctx).await?;
                Err(err)
            }
        }
    }

    /// `prepare_stream_job` prepares a stream job and returns the context and table fragments.
    async fn prepare_stream_job(
        &self,
        stream_job: &mut StreamingJob,
        fragment_graph: StreamFragmentGraph,
    ) -> MetaResult<(CreateMaterializedViewContext, TableFragments)> {
        // 1. assign a new id to the stream job.
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        // 2. resolve the dependent relations.
        let dependent_relations = get_dependent_relations(&fragment_graph)?;
        assert!(
            !dependent_relations.is_empty(),
            "there should be at lease 1 dependent relation when creating table or sink"
        );
        stream_job.set_dependent_relations(dependent_relations);

        // 3. Mark current relation as "creating" and add reference count to dependent relations.
        self.catalog_manager
            .start_create_stream_job_procedure(stream_job)
            .await?;

        // 4. build fragment graph.
        use risingwave_common::catalog::TableId;
        let dependent_table_ids = fragment_graph
            .dependent_table_ids
            .iter()
            .map(|table_id| TableId::new(*table_id))
            .collect();

        let mut ctx = CreateMaterializedViewContext {
            schema_id: stream_job.schema_id(),
            database_id: stream_job.database_id(),
            mview_name: stream_job.name(),
            table_properties: stream_job.properties(),
            table_sink_map: self
                .fragment_manager
                .get_build_graph_info(&dependent_table_ids)
                .await?
                .table_sink_actor_ids,
            dependent_table_ids,
            ..Default::default()
        };

        let table_ids_cnt = fragment_graph.table_ids_cnt;
        let default_parallelism = if self.env.opts.minimal_scheduling {
            self.cluster_manager
                .list_worker_node(WorkerType::ComputeNode, Some(State::Running))
                .await
                .len()
        } else {
            self.cluster_manager.get_active_parallel_unit_count().await
        };
        let mut actor_graph_builder = ActorGraphBuilder::new(
            self.env.id_gen_manager_ref(),
            fragment_graph,
            default_parallelism as u32,
            &mut ctx,
        )
        .await?;

        // fill correct table id in fragment graph and fill fragment id in table.
        if let StreamingJob::MaterializedView(table)
        | StreamingJob::Index(_, table)
        | StreamingJob::MaterializedSource(_, table) = stream_job
        {
            actor_graph_builder.fill_mview_id(table);
        }

        let graph = actor_graph_builder
            .generate_graph(self.env.id_gen_manager_ref(), &mut ctx)
            .await?;

        assert_eq!(table_ids_cnt, ctx.internal_table_ids().len() as u32);

        // 5. mark creating tables.
        let mut creating_tables = ctx
            .internal_table_id_map
            .iter()
            .map(|(id, table)| {
                table.clone().unwrap_or(Table {
                    id: *id,
                    ..Default::default()
                })
            })
            .collect_vec();
        match stream_job {
            StreamingJob::MaterializedView(table)
            | StreamingJob::Index(_, table)
            | StreamingJob::MaterializedSource(_, table) => creating_tables.push(table.clone()),
            _ => {}
        }

        self.catalog_manager
            .mark_creating_tables(&creating_tables)
            .await;

        Ok((ctx, TableFragments::new(id.into(), graph)))
    }

    /// `cancel_stream_job` cancels a stream job and clean some states.
    async fn cancel_stream_job(
        &self,
        stream_job: &StreamingJob,
        ctx: &CreateMaterializedViewContext,
    ) -> MetaResult<()> {
        let mut creating_internal_table_ids = ctx.internal_table_ids();
        // 1. cancel create procedure.
        match stream_job {
            StreamingJob::MaterializedView(table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .cancel_create_table_procedure(table)
                    .await?;
            }
            StreamingJob::Sink(sink) => {
                self.catalog_manager
                    .cancel_create_sink_procedure(sink)
                    .await?;
            }
            StreamingJob::MaterializedSource(source, table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .cancel_create_materialized_source_procedure(source, table)
                    .await?;
            }
            StreamingJob::Index(index, table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .cancel_create_index_procedure(index, table)
                    .await?;
            }
        }
        // 2. unmark creating tables.
        self.catalog_manager
            .unmark_creating_tables(&creating_internal_table_ids, true)
            .await;

        Ok(())
    }

    /// `finish_stream_job` finishes a stream job and clean some states.
    async fn finish_stream_job(
        &self,
        stream_job: &StreamingJob,
        table_fragments: &TableFragments,
        ctx: &CreateMaterializedViewContext,
    ) -> MetaResult<u64> {
        // 1. notify vnode mapping.
        self.fragment_manager
            .notify_fragment_mapping(table_fragments, Operation::Add)
            .await;

        // 2. finish procedure.
        let mut creating_internal_table_ids = ctx.internal_table_ids();
        let version = match stream_job {
            StreamingJob::MaterializedView(table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .finish_create_table_procedure(ctx.internal_tables(), table)
                    .await?
            }
            StreamingJob::Sink(sink) => {
                self.catalog_manager
                    .finish_create_sink_procedure(sink)
                    .await?
            }
            StreamingJob::MaterializedSource(source, table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .finish_create_materialized_source_procedure(
                        source,
                        table,
                        ctx.internal_tables(),
                    )
                    .await?
            }
            StreamingJob::Index(index, table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .finish_create_index_procedure(index, ctx.internal_tables(), table)
                    .await?
            }
        };

        // 3. unmark creating tables.
        self.catalog_manager
            .unmark_creating_tables(&creating_internal_table_ids, false)
            .await;

        Ok(version)
    }

    async fn create_materialized_source_inner(
        &self,
        mut source: Source,
        mut mview: Table,
        mut fragment_graph: StreamFragmentGraph,
    ) -> MetaResult<(SourceId, TableId, CatalogVersion)> {
        // Generate source id.
        let source_id = self.gen_unique_id::<{ IdCategory::Table }>().await?; // TODO: use source category
        source.id = source_id;

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

        let mut stream_job = StreamingJob::MaterializedSource(source.clone(), mview.clone());
        let (mut ctx, mut table_fragments) = self
            .prepare_stream_job(&mut stream_job, fragment_graph)
            .await?;

        // Create source on compute node.
        if let Err(e) = self.source_manager.create_source(&source).await {
            self.catalog_manager
                .cancel_create_materialized_source_procedure(&source, &mview)
                .await?;
            return Err(e);
        }

        match self
            .stream_manager
            .create_materialized_view(&mut table_fragments, &mut ctx)
            .await
        {
            Ok(_) => {
                let version = self
                    .finish_stream_job(&stream_job, &table_fragments, &ctx)
                    .await?;
                Ok((source_id, stream_job.id(), version))
            }
            Err(err) => {
                self.cancel_stream_job(&stream_job, &ctx).await?;
                Err(err)
            }
        }
    }

    async fn drop_materialized_source_inner(
        &self,
        source_id: SourceId,
        table_id: TableId,
    ) -> MetaResult<CatalogVersion> {
        // 1. Drop materialized source in catalog, source_id will be checked if it is
        // associated_source_id in mview.
        let version = self
            .catalog_manager
            .drop_materialized_source(source_id, table_id)
            .await?;

        // 2. Drop source and mv in table background deleter asynchronously.
        // Note: we need to drop the materialized view to unmap the source_id to fragment_ids before
        // we can drop the source.
        self.table_background_deleter.delete(vec![
            StreamJobId::TableId(table_id.into()),
            StreamJobId::SourceId(source_id),
        ]);

        Ok(version)
    }

    async fn gen_unique_id<const C: IdCategoryType>(&self) -> MetaResult<u32> {
        let id = self.env.id_gen_manager().generate::<C>().await? as u32;
        Ok(id)
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
