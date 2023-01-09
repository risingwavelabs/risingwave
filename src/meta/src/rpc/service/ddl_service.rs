// Copyright 2023 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;

use risingwave_common::catalog::CatalogVersion;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::*;
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::WorkerType;
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::drop_table_request::SourceId as ProstSourceId;
use risingwave_pb::ddl_service::*;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{StreamFragmentGraph, StreamNode};
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, IdCategory, IdCategoryType,
    MetaSrvEnv, NotificationVersion, SourceId, StreamingJob, TableId,
};
use crate::model::TableFragments;
use crate::storage::MetaStore;
use crate::stream::{
    ActorGraphBuilder, CreateStreamingJobContext, GlobalStreamManagerRef, SourceManagerRef,
};
use crate::{MetaError, MetaResult};

#[derive(Clone)]
pub struct DdlServiceImpl<S: MetaStore> {
    env: MetaSrvEnv<S>,

    catalog_manager: CatalogManagerRef<S>,
    stream_manager: GlobalStreamManagerRef<S>,
    source_manager: SourceManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
    barrier_manager: BarrierManagerRef<S>,
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
        barrier_manager: BarrierManagerRef<S>,
    ) -> Self {
        Self {
            env,
            catalog_manager,
            stream_manager,
            source_manager,
            cluster_manager,
            fragment_manager,
            barrier_manager,
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
        self.check_barrier_manager_status().await?;
        let req = request.into_inner();
        let database_id = req.get_database_id();

        // 1. drop all catalogs in this database.
        let (version, streaming_ids, source_ids) =
            self.catalog_manager.drop_database(database_id).await?;
        // 2. Unregister source connector worker.
        self.source_manager.unregister_sources(source_ids).await;
        // 3. drop streaming jobs.
        if !streaming_ids.is_empty() {
            self.stream_manager.drop_streaming_jobs(streaming_ids).await;
        }

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
        let mut source = request.into_inner().get_source()?.clone();

        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        source.id = id;

        self.catalog_manager
            .start_create_source_procedure(&source)
            .await?;

        if let Err(e) = self.source_manager.register_source(&source).await {
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
        let source_id = request.into_inner().source_id;

        // 1. Drop source in catalog.
        let version = self.catalog_manager.drop_source(source_id).await?;
        // 2. Unregister source connector worker.
        self.source_manager
            .unregister_sources(vec![source_id])
            .await;

        Ok(Response::new(DropSourceResponse {
            status: None,
            version,
        }))
    }

    async fn create_sink(
        &self,
        request: Request<CreateSinkRequest>,
    ) -> Result<Response<CreateSinkResponse>, Status> {
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
        self.check_barrier_manager_status().await?;
        let sink_id = request.into_inner().sink_id;

        // 1. Drop sink in catalog.
        let version = self.catalog_manager.drop_sink(sink_id).await?;
        // 2. drop streaming job of sink.
        self.stream_manager
            .drop_streaming_jobs(vec![sink_id.into()])
            .await;

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
        self.check_barrier_manager_status().await?;
        self.env.idle_manager().record_activity();

        let request = request.into_inner();
        let table_id = request.table_id;
        let table_fragment = self
            .fragment_manager
            .select_table_fragments_by_table_id(&table_id.into())
            .await?;
        let internal_tables = table_fragment.internal_table_ids();

        // 1. Drop table in catalog. Ref count will be checked.
        let (version, delete_jobs) = self
            .catalog_manager
            .drop_table(table_id, internal_tables)
            .await?;
        // 2. Drop streaming jobs.
        self.stream_manager.drop_streaming_jobs(delete_jobs).await;

        Ok(Response::new(DropMaterializedViewResponse {
            status: None,
            version,
        }))
    }

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
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
        self.check_barrier_manager_status().await?;
        self.env.idle_manager().record_activity();

        let index_id = request.into_inner().index_id;
        let index_table_id = self.catalog_manager.get_index_table(index_id).await?;

        // 1. Drop index in catalog. Ref count will be checked.
        let version = self
            .catalog_manager
            .drop_index(index_id, index_table_id)
            .await?;
        // 2. drop streaming jobs of the index tables.
        self.stream_manager
            .drop_streaming_jobs(vec![index_table_id.into()])
            .await;

        Ok(Response::new(DropIndexResponse {
            status: None,
            version,
        }))
    }

    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let request = request.into_inner();
        let source = request.source;
        let mview = request.materialized_view.unwrap();
        let fragment_graph = request.fragment_graph.unwrap();

        let (table_id, version) = self
            .create_table_inner(source, mview, fragment_graph)
            .await?;

        Ok(Response::new(CreateTableResponse {
            status: None,
            table_id,
            version,
        }))
    }

    async fn drop_table(
        &self,
        request: Request<DropTableRequest>,
    ) -> Result<Response<DropTableResponse>, Status> {
        self.check_barrier_manager_status().await?;
        let request = request.into_inner();
        let source_id = request.source_id;
        let table_id = request.table_id;

        let version = self
            .drop_table_inner(source_id.map(|ProstSourceId::Id(id)| id), table_id)
            .await?;

        Ok(Response::new(DropTableResponse {
            status: None,
            version,
        }))
    }

    async fn create_view(
        &self,
        request: Request<CreateViewRequest>,
    ) -> Result<Response<CreateViewResponse>, Status> {
        let req = request.into_inner();
        let mut view = req.get_view()?.clone();
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        view.id = id;

        let version = self.catalog_manager.create_view(&view).await?;

        Ok(Response::new(CreateViewResponse {
            status: None,
            view_id: id,
            version,
        }))
    }

    async fn drop_view(
        &self,
        request: Request<DropViewRequest>,
    ) -> Result<Response<DropViewResponse>, Status> {
        let req = request.into_inner();
        let view_id = req.get_view_id();
        let version = self.catalog_manager.drop_view(view_id).await?;
        Ok(Response::new(DropViewResponse {
            status: None,
            version,
        }))
    }

    async fn risectl_list_state_tables(
        &self,
        _request: Request<RisectlListStateTablesRequest>,
    ) -> Result<Response<RisectlListStateTablesResponse>, Status> {
        let tables = self.catalog_manager.list_tables().await;
        Ok(Response::new(RisectlListStateTablesResponse { tables }))
    }

    async fn replace_table_plan(
        &self,
        _request: Request<ReplaceTablePlanRequest>,
    ) -> Result<Response<ReplaceTablePlanResponse>, Status> {
        Err(Status::unimplemented(
            "replace table plan is not implemented yet",
        ))
    }
}

impl<S> DdlServiceImpl<S>
where
    S: MetaStore,
{
    /// `check_barrier_manager_status` checks the status of the barrier manager, return unavailable
    /// when it's not running.
    async fn check_barrier_manager_status(&self) -> MetaResult<()> {
        if !self.barrier_manager.is_running().await {
            return Err(MetaError::unavailable(
                "The cluster is starting or recovering".into(),
            ));
        }
        Ok(())
    }

    /// `create_stream_job` creates a stream job and returns the version of the catalog.
    async fn create_stream_job(
        &self,
        stream_job: &mut StreamingJob,
        fragment_graph: StreamFragmentGraph,
    ) -> MetaResult<NotificationVersion> {
        self.check_barrier_manager_status().await?;

        let (mut ctx, table_fragments) =
            self.prepare_stream_job(stream_job, fragment_graph).await?;
        match self
            .stream_manager
            .create_streaming_job(table_fragments, &mut ctx)
            .await
        {
            Ok(_) => self.finish_stream_job(stream_job, &ctx).await,
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
    ) -> MetaResult<(CreateStreamingJobContext, TableFragments)> {
        // 1. assign a new id to the stream job.
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        // 2. resolve the dependent relations.
        let dependent_relations = get_dependent_relations(&fragment_graph)?;
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

        let mut ctx = CreateStreamingJobContext {
            schema_id: stream_job.schema_id(),
            database_id: stream_job.database_id(),
            streaming_job_name: stream_job.name(),
            streaming_definition: stream_job.mview_definition(),
            table_properties: stream_job.properties(),
            table_mview_map: self
                .fragment_manager
                .get_build_graph_info(&dependent_table_ids)
                .await?
                .table_mview_actor_ids,
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
        match stream_job {
            StreamingJob::MaterializedView(table)
            | StreamingJob::Index(_, table)
            | StreamingJob::Table(_, table) => {
                table.fragment_id = actor_graph_builder.fill_database_object_id(
                    table.database_id,
                    table.schema_id,
                    table.id.into(),
                );
            }
            StreamingJob::Sink(sink) => {
                actor_graph_builder.fill_database_object_id(
                    sink.database_id,
                    sink.schema_id,
                    sink.id.into(),
                );
            }
        }

        let graph = actor_graph_builder
            .generate_graph(self.env.id_gen_manager_ref(), &mut ctx)
            .await?;

        assert_eq!(table_ids_cnt, ctx.internal_table_ids().len() as u32);

        // 5. mark creating tables.
        let mut creating_tables = ctx.internal_tables();
        match stream_job {
            StreamingJob::MaterializedView(table)
            | StreamingJob::Index(_, table)
            | StreamingJob::Table(_, table) => creating_tables.push(table.clone()),

            StreamingJob::Sink(_) => {
                // No need to mark it as creating. Do nothing.
            }
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
        ctx: &CreateStreamingJobContext,
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
            StreamingJob::Table(source, table) => {
                creating_internal_table_ids.push(table.id);
                if let Some(source) = source {
                    self.catalog_manager
                        .cancel_create_table_procedure_with_source(source, table)
                        .await?;
                } else {
                    self.catalog_manager
                        .cancel_create_table_procedure(table)
                        .await?;
                }
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
        ctx: &CreateStreamingJobContext,
    ) -> MetaResult<u64> {
        // 1. finish procedure.
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
            StreamingJob::Table(source, table) => {
                creating_internal_table_ids.push(table.id);
                if let Some(source) = source {
                    let internal_tables: [_; 1] = ctx.internal_tables().try_into().unwrap();
                    self.catalog_manager
                        .finish_create_table_procedure_with_source(
                            source,
                            table,
                            &internal_tables[0],
                        )
                        .await?
                } else {
                    let internal_tables = ctx.internal_tables();
                    assert!(internal_tables.is_empty());
                    // Though `internal_tables` is empty here, we pass it as a parameter to reuse
                    // the method.
                    self.catalog_manager
                        .finish_create_table_procedure(internal_tables, table)
                        .await?
                }
            }
            StreamingJob::Index(index, table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .finish_create_index_procedure(index, table)
                    .await?
            }
        };

        // 2. unmark creating tables.
        self.catalog_manager
            .unmark_creating_tables(&creating_internal_table_ids, false)
            .await;

        Ok(version)
    }

    /// If `source` is `None` here, then there is no external streaming source for this table.
    async fn create_table_inner(
        &self,
        mut source: Option<Source>,
        mut mview: Table,
        mut fragment_graph: StreamFragmentGraph,
    ) -> MetaResult<(TableId, CatalogVersion)> {
        self.check_barrier_manager_status().await?;

        if let Some(source) = &mut source {
            // Generate source id.
            let source_id = self.gen_unique_id::<{ IdCategory::Table }>().await?; // TODO: Use source category
            source.id = source_id;

            // Fill in the correct source id for stream node.
            fn fill_source_id(stream_node: &mut StreamNode, source_id: u32) -> usize {
                let mut source_count = 0;
                if let NodeBody::Source(source_node) = stream_node.node_body.as_mut().unwrap() {
                    // TODO: Refactor using source id.
                    source_node.source_inner.as_mut().unwrap().source_id = source_id;
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
                "require exactly 1 external stream source when creating table with a connector"
            );

            // Fill in the correct source id for mview.
            mview.optional_associated_source_id =
                Some(OptionalAssociatedSourceId::AssociatedSourceId(source_id));
        }

        let mut stream_job = StreamingJob::Table(source.clone(), mview.clone());
        let (mut ctx, table_fragments) = self
            .prepare_stream_job(&mut stream_job, fragment_graph)
            .await?;

        if let Some(source) = &source {
            if let Err(e) = self.source_manager.register_source(source).await {
                self.catalog_manager
                    .cancel_create_table_procedure_with_source(source, &mview)
                    .await?;
                return Err(e);
            }
        }

        match self
            .stream_manager
            .create_streaming_job(table_fragments, &mut ctx)
            .await
        {
            Ok(_) => {
                let version = self.finish_stream_job(&stream_job, &ctx).await?;
                Ok((stream_job.id(), version))
            }
            Err(err) => {
                self.cancel_stream_job(&stream_job, &ctx).await?;
                Err(err)
            }
        }
    }

    async fn drop_table_inner(
        &self,
        source_id: Option<SourceId>,
        table_id: TableId,
    ) -> MetaResult<CatalogVersion> {
        let table_fragment = self
            .fragment_manager
            .select_table_fragments_by_table_id(&table_id.into())
            .await?;
        let internal_table_ids = table_fragment.internal_table_ids();

        let (version, delete_jobs) = if let Some(source_id) = source_id {
            // Drop table and source in catalog. Check `source_id` if it is the table's
            // `associated_source_id`. Indexes also need to be dropped atomically.
            assert_eq!(internal_table_ids.len(), 1);
            let (version, delete_jobs) = self
                .catalog_manager
                .drop_table_with_source(source_id, table_id, internal_table_ids[0])
                .await?;
            // Unregister source connector worker.
            self.source_manager
                .unregister_sources(vec![source_id])
                .await;
            (version, delete_jobs)
        } else {
            assert!(internal_table_ids.is_empty());
            self.catalog_manager
                .drop_table(table_id, internal_table_ids)
                .await?
        };

        // Drop streaming jobs.
        self.stream_manager.drop_streaming_jobs(delete_jobs).await;

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
                if let Some(source) = &source_node.source_inner {
                    dependent_relations.insert(source.get_source_id());
                }
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
