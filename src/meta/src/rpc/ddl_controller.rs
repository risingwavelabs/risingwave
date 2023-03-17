// Copyright 2023 RisingWave Labs
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

use itertools::Itertools;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_pb::catalog::{Connection, Database, Function, Schema, Source, Table, View};
use risingwave_pb::ddl_service::alter_relation_name_request::Relation;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::stream_plan::StreamFragmentGraph as StreamFragmentGraphProto;

use crate::barrier::BarrierManagerRef;
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, DatabaseId, FragmentManagerRef, FunctionId, IdCategory,
    IndexId, MetaSrvEnv, NotificationVersion, SchemaId, SinkId, SourceId, StreamingJob, TableId,
    ViewId,
};
use crate::model::{StreamEnvironment, TableFragments};
use crate::storage::MetaStore;
use crate::stream::{
    validate_sink, ActorGraphBuildResult, ActorGraphBuilder, CompleteStreamFragmentGraph,
    CreateStreamingJobContext, GlobalStreamManagerRef, ReplaceTableContext, SourceManagerRef,
    StreamFragmentGraph,
};
use crate::{MetaError, MetaResult};

pub enum StreamingJobId {
    MaterializedView(TableId),
    Sink(SinkId),
    Table(Option<SourceId>, TableId),
    Index(IndexId),
}

impl StreamingJobId {
    fn id(&self) -> TableId {
        match self {
            StreamingJobId::MaterializedView(id)
            | StreamingJobId::Sink(id)
            | StreamingJobId::Table(_, id)
            | StreamingJobId::Index(id) => *id,
        }
    }
}

pub enum DdlCommand {
    CreateDatabase(Database),
    DropDatabase(DatabaseId),
    CreateSchema(Schema),
    DropSchema(SchemaId),
    CreateSource(Source),
    DropSource(SourceId),
    CreateFunction(Function),
    DropFunction(FunctionId),
    CreateView(View),
    DropView(ViewId),
    CreateStreamingJob(StreamingJob, StreamFragmentGraphProto),
    DropStreamingJob(StreamingJobId),
    ReplaceTable(StreamingJob, StreamFragmentGraphProto, ColIndexMapping),
    AlterRelationName(Relation, String),
    CreateConnection(Connection),
    DropConnection(String),
}

#[derive(Clone)]
pub struct DdlController<S: MetaStore> {
    env: MetaSrvEnv<S>,

    catalog_manager: CatalogManagerRef<S>,
    stream_manager: GlobalStreamManagerRef<S>,
    source_manager: SourceManagerRef<S>,
    cluster_manager: ClusterManagerRef<S>,
    fragment_manager: FragmentManagerRef<S>,
    barrier_manager: BarrierManagerRef<S>,
}

impl<S> DdlController<S>
where
    S: MetaStore,
{
    pub(crate) fn new(
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

    /// `run_command` spawns a tokio coroutine to execute the target ddl command. When the client
    /// has been interrupted during executing, the request will be cancelled by tonic. Since we have
    /// a lot of logic for revert, status management, notification and so on, ensuring consistency
    /// would be a huge hassle and pain if we don't spawn here.
    pub(crate) async fn run_command(&self, command: DdlCommand) -> MetaResult<NotificationVersion> {
        self.check_barrier_manager_status().await?;
        let ctrl = self.clone();
        let handler = tokio::spawn(async move {
            match command {
                DdlCommand::CreateDatabase(database) => ctrl.create_database(database).await,
                DdlCommand::DropDatabase(database_id) => ctrl.drop_database(database_id).await,
                DdlCommand::CreateSchema(schema) => ctrl.create_schema(schema).await,
                DdlCommand::DropSchema(schema_id) => ctrl.drop_schema(schema_id).await,
                DdlCommand::CreateSource(source) => ctrl.create_source(source).await,
                DdlCommand::DropSource(source_id) => ctrl.drop_source(source_id).await,
                DdlCommand::CreateFunction(function) => ctrl.create_function(function).await,
                DdlCommand::DropFunction(function_id) => ctrl.drop_function(function_id).await,
                DdlCommand::CreateView(view) => ctrl.create_view(view).await,
                DdlCommand::DropView(view_id) => ctrl.drop_view(view_id).await,
                DdlCommand::CreateStreamingJob(stream_job, fragment_graph) => {
                    ctrl.create_streaming_job(stream_job, fragment_graph).await
                }
                DdlCommand::DropStreamingJob(job_id) => ctrl.drop_streaming_job(job_id).await,
                DdlCommand::ReplaceTable(stream_job, fragment_graph, table_col_index_mapping) => {
                    ctrl.replace_table(stream_job, fragment_graph, table_col_index_mapping)
                        .await
                }
                DdlCommand::AlterRelationName(relation, name) => {
                    ctrl.alter_relation_table(relation, &name).await
                }
                DdlCommand::CreateConnection(connection) => {
                    ctrl.create_connection(connection).await
                }
                DdlCommand::DropConnection(conn_name) => ctrl.drop_connection(&conn_name).await,
            }
        });
        handler.await.unwrap()
    }

    pub(crate) async fn get_ddl_progress(&self) -> Vec<DdlProgress> {
        self.barrier_manager.get_ddl_progress().await
    }

    async fn create_database(&self, database: Database) -> MetaResult<NotificationVersion> {
        self.catalog_manager.create_database(&database).await
    }

    async fn drop_database(&self, database_id: DatabaseId) -> MetaResult<NotificationVersion> {
        // 1. drop all catalogs in this database.
        let (version, streaming_ids, source_ids) =
            self.catalog_manager.drop_database(database_id).await?;
        // 2. Unregister source connector worker.
        self.source_manager.unregister_sources(source_ids).await;
        // 3. drop streaming jobs.
        if !streaming_ids.is_empty() {
            self.stream_manager.drop_streaming_jobs(streaming_ids).await;
        }
        Ok(version)
    }

    async fn create_schema(&self, schema: Schema) -> MetaResult<NotificationVersion> {
        self.catalog_manager.create_schema(&schema).await
    }

    async fn drop_schema(&self, schema_id: SchemaId) -> MetaResult<NotificationVersion> {
        self.catalog_manager.drop_schema(schema_id).await
    }

    async fn create_source(&self, source: Source) -> MetaResult<NotificationVersion> {
        self.catalog_manager
            .start_create_source_procedure(&source)
            .await?;

        if let Err(e) = self.source_manager.register_source(&source).await {
            self.catalog_manager
                .cancel_create_source_procedure(&source)
                .await?;
            return Err(e);
        }

        self.catalog_manager
            .finish_create_source_procedure(&source)
            .await
    }

    async fn drop_source(&self, source_id: SourceId) -> MetaResult<NotificationVersion> {
        // 1. Drop source in catalog.
        let version = self.catalog_manager.drop_source(source_id).await?;
        // 2. Unregister source connector worker.
        self.source_manager
            .unregister_sources(vec![source_id])
            .await;

        Ok(version)
    }

    async fn create_function(&self, function: Function) -> MetaResult<NotificationVersion> {
        self.catalog_manager.create_function(&function).await
    }

    async fn drop_function(&self, function_id: FunctionId) -> MetaResult<NotificationVersion> {
        self.catalog_manager.drop_function(function_id).await
    }

    async fn create_view(&self, view: View) -> MetaResult<NotificationVersion> {
        self.catalog_manager.create_view(&view).await
    }

    async fn drop_view(&self, view_id: ViewId) -> MetaResult<NotificationVersion> {
        self.catalog_manager.drop_view(view_id).await
    }

    async fn create_connection(&self, connection: Connection) -> MetaResult<NotificationVersion> {
        self.catalog_manager.create_connection(connection).await
    }

    async fn drop_connection(&self, conn_name: &str) -> MetaResult<NotificationVersion> {
        self.catalog_manager.drop_connection(conn_name).await
    }

    async fn create_streaming_job(
        &self,
        mut stream_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
    ) -> MetaResult<NotificationVersion> {
        let env = StreamEnvironment::from_protobuf(fragment_graph.get_env().unwrap());
        let fragment_graph = self
            .prepare_stream_job(&mut stream_job, fragment_graph)
            .await?;

        let mut internal_tables = vec![];
        let result = try {
            let (ctx, table_fragments) = self
                .build_stream_job(env, &stream_job, fragment_graph)
                .await?;

            internal_tables = ctx.internal_tables();

            match &stream_job {
                StreamingJob::Table(Some(source), _) => {
                    // Register the source on the connector node.
                    self.source_manager.register_source(source).await?;
                }
                StreamingJob::Sink(sink) => {
                    // Validate the sink on the connector node.
                    validate_sink(sink, self.env.opts.connector_rpc_endpoint.clone()).await?;
                }
                _ => {}
            }

            self.stream_manager
                .create_streaming_job(table_fragments, ctx)
                .await?;
        };

        match result {
            Ok(_) => self.finish_stream_job(&stream_job, internal_tables).await,
            Err(err) => {
                self.cancel_stream_job(&stream_job, internal_tables).await;
                Err(err)
            }
        }
    }

    async fn drop_streaming_job(&self, job_id: StreamingJobId) -> MetaResult<NotificationVersion> {
        let table_fragments = self
            .fragment_manager
            .select_table_fragments_by_table_id(&job_id.id().into())
            .await?;
        let internal_table_ids = table_fragments.internal_table_ids();
        let (version, streaming_job_ids) = match job_id {
            StreamingJobId::MaterializedView(table_id) => {
                self.catalog_manager
                    .drop_table(table_id, internal_table_ids)
                    .await?
            }
            StreamingJobId::Sink(sink_id) => {
                let version = self
                    .catalog_manager
                    .drop_sink(sink_id, internal_table_ids)
                    .await?;
                (version, vec![sink_id.into()])
            }
            StreamingJobId::Table(source_id, table_id) => {
                self.drop_table_inner(source_id, table_id, internal_table_ids)
                    .await?
            }
            StreamingJobId::Index(index_id) => {
                let index_table_id = self.catalog_manager.get_index_table(index_id).await?;
                let version = self
                    .catalog_manager
                    .drop_index(index_id, index_table_id)
                    .await?;
                (version, vec![index_table_id.into()])
            }
        };

        self.stream_manager
            .drop_streaming_jobs(streaming_job_ids)
            .await;
        Ok(version)
    }

    /// `prepare_stream_job` prepares a stream job and returns the stream fragment graph.
    async fn prepare_stream_job(
        &self,
        stream_job: &mut StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
    ) -> MetaResult<StreamFragmentGraph> {
        // 1. Build fragment graph.
        let fragment_graph =
            StreamFragmentGraph::new(fragment_graph, self.env.id_gen_manager_ref(), &*stream_job)
                .await?;

        // 2. Set the graph-related fields and freeze the `stream_job`.
        stream_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        let dependent_relations = fragment_graph.dependent_relations().clone();
        stream_job.set_dependent_relations(dependent_relations);
        let stream_job = &*stream_job;

        // 3. Mark current relation as "creating" and add reference count to dependent relations.
        self.catalog_manager
            .start_create_stream_job_procedure(stream_job)
            .await?;

        Ok(fragment_graph)
    }

    /// `build_stream_job` builds a streaming job and returns the context and table fragments.
    async fn build_stream_job(
        &self,
        env: StreamEnvironment,
        stream_job: &StreamingJob,
        fragment_graph: StreamFragmentGraph,
    ) -> MetaResult<(CreateStreamingJobContext, TableFragments)> {
        let id = stream_job.id();
        let default_parallelism = fragment_graph.default_parallelism();
        let internal_tables = fragment_graph.internal_tables();

        // 1. Resolve the upstream fragments, extend the fragment graph to a complete graph that
        // contains all information needed for building the actor graph.
        let upstream_mview_fragments = self
            .fragment_manager
            .get_upstream_mview_fragments(fragment_graph.dependent_relations())
            .await;
        let upstream_mview_actors = upstream_mview_fragments
            .iter()
            .map(|(&table_id, fragment)| {
                (
                    table_id,
                    fragment.actors.iter().map(|a| a.actor_id).collect_vec(),
                )
            })
            .collect();

        let complete_graph =
            CompleteStreamFragmentGraph::with_upstreams(fragment_graph, upstream_mview_fragments)?;

        // 2. Build the actor graph.
        let cluster_info = self.cluster_manager.get_streaming_cluster_info().await;
        let actor_graph_builder =
            ActorGraphBuilder::new(complete_graph, cluster_info, default_parallelism)?;

        let ActorGraphBuildResult {
            graph,
            building_locations,
            existing_locations,
            dispatchers,
            merge_updates,
        } = actor_graph_builder
            .generate_graph(self.env.id_gen_manager_ref(), stream_job)
            .await?;
        assert!(merge_updates.is_empty());

        // 3. Build the table fragments structure that will be persisted in the stream manager,
        // and the context that contains all information needed for building the
        // actors on the compute nodes.
        let table_fragments =
            TableFragments::new(id.into(), graph, &building_locations.actor_locations, env);

        let ctx = CreateStreamingJobContext {
            dispatchers,
            upstream_mview_actors,
            internal_tables,
            building_locations,
            existing_locations,
            table_properties: stream_job.properties(),
            definition: stream_job.mview_definition(),
        };

        // 4. Mark creating tables, including internal tables and the table of the stream job.
        let creating_tables = ctx
            .internal_tables()
            .into_iter()
            .chain(stream_job.table().cloned())
            .collect_vec();

        self.catalog_manager
            .mark_creating_tables(&creating_tables)
            .await;

        Ok((ctx, table_fragments))
    }

    /// `cancel_stream_job` cancels a stream job and clean some states.
    async fn cancel_stream_job(&self, stream_job: &StreamingJob, internal_tables: Vec<Table>) {
        let mut creating_internal_table_ids =
            internal_tables.into_iter().map(|t| t.id).collect_vec();
        // 1. cancel create procedure.
        match stream_job {
            StreamingJob::MaterializedView(table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .cancel_create_table_procedure(table)
                    .await;
            }
            StreamingJob::Sink(sink) => {
                self.catalog_manager
                    .cancel_create_sink_procedure(sink)
                    .await;
            }
            StreamingJob::Table(source, table) => {
                creating_internal_table_ids.push(table.id);
                if let Some(source) = source {
                    self.catalog_manager
                        .cancel_create_table_procedure_with_source(source, table)
                        .await;
                } else {
                    self.catalog_manager
                        .cancel_create_table_procedure(table)
                        .await;
                }
            }
            StreamingJob::Index(index, table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .cancel_create_index_procedure(index, table)
                    .await;
            }
        }
        // 2. unmark creating tables.
        self.catalog_manager
            .unmark_creating_tables(&creating_internal_table_ids, true)
            .await;
    }

    /// `finish_stream_job` finishes a stream job and clean some states.
    async fn finish_stream_job(
        &self,
        stream_job: &StreamingJob,
        internal_tables: Vec<Table>,
    ) -> MetaResult<u64> {
        // 1. finish procedure.
        let mut creating_internal_table_ids = internal_tables.iter().map(|t| t.id).collect_vec();
        let version = match stream_job {
            StreamingJob::MaterializedView(table) => {
                creating_internal_table_ids.push(table.id);
                self.catalog_manager
                    .finish_create_table_procedure(internal_tables, table)
                    .await?
            }
            StreamingJob::Sink(sink) => {
                self.catalog_manager
                    .finish_create_sink_procedure(internal_tables, sink)
                    .await?
            }
            StreamingJob::Table(source, table) => {
                creating_internal_table_ids.push(table.id);
                if let Some(source) = source {
                    self.catalog_manager
                        .finish_create_table_procedure_with_source(source, table, internal_tables)
                        .await?
                } else {
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

    async fn drop_table_inner(
        &self,
        source_id: Option<SourceId>,
        table_id: TableId,
        internal_table_ids: Vec<TableId>,
    ) -> MetaResult<(
        NotificationVersion,
        Vec<risingwave_common::catalog::TableId>,
    )> {
        if let Some(source_id) = source_id {
            // Drop table and source in catalog. Check `source_id` if it is the table's
            // `associated_source_id`. Indexes also need to be dropped atomically.
            let (version, delete_jobs) = self
                .catalog_manager
                .drop_table_with_source(source_id, table_id, internal_table_ids)
                .await?;
            // Unregister source connector worker.
            self.source_manager
                .unregister_sources(vec![source_id])
                .await;
            Ok((version, delete_jobs))
        } else {
            assert!(internal_table_ids.is_empty());
            self.catalog_manager
                .drop_table(table_id, internal_table_ids)
                .await
        }
    }

    async fn replace_table(
        &self,
        mut stream_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        table_col_index_mapping: ColIndexMapping,
    ) -> MetaResult<NotificationVersion> {
        let env = StreamEnvironment::from_protobuf(fragment_graph.get_env().unwrap());

        let fragment_graph = self
            .prepare_replace_table(&mut stream_job, fragment_graph)
            .await?;

        let result = try {
            let (ctx, table_fragments) = self
                .build_replace_table(env, &stream_job, fragment_graph, table_col_index_mapping)
                .await?;

            self.stream_manager
                .replace_table(table_fragments, ctx)
                .await?;
        };

        match result {
            Ok(_) => self.finish_replace_table(&stream_job).await,
            Err(err) => {
                self.cancel_replace_table(&stream_job).await?;
                Err(err)
            }
        }
    }

    /// `prepare_replace_table` prepares a table replacement and returns the new stream fragment
    /// graph. This is basically the same as `prepare_stream_job`, except that it does more
    /// assertions and uses a different method to mark in the catalog.
    async fn prepare_replace_table(
        &self,
        stream_job: &mut StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
    ) -> MetaResult<StreamFragmentGraph> {
        // 1. Build fragment graph.
        let fragment_graph =
            StreamFragmentGraph::new(fragment_graph, self.env.id_gen_manager_ref(), &*stream_job)
                .await?;
        assert!(fragment_graph.internal_tables().is_empty());
        assert!(fragment_graph.dependent_relations().is_empty());

        // 2. Set the graph-related fields and freeze the `stream_job`.
        stream_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        let stream_job = &*stream_job;

        // 3. Mark current relation as "updating".
        self.catalog_manager
            .start_replace_table_procedure(stream_job.table().unwrap())
            .await?;

        Ok(fragment_graph)
    }

    /// `build_replace_table` builds a table replacement and returns the context and new table
    /// fragments.
    async fn build_replace_table(
        &self,
        env: StreamEnvironment,
        stream_job: &StreamingJob,
        fragment_graph: StreamFragmentGraph,
        table_col_index_mapping: ColIndexMapping,
    ) -> MetaResult<(ReplaceTableContext, TableFragments)> {
        let id = stream_job.id();
        let default_parallelism = fragment_graph.default_parallelism();

        // 1. Resolve the edges to the downstream fragments, extend the fragment graph to a complete
        // graph that contains all information needed for building the actor graph.
        let original_table_fragment = self.fragment_manager.get_mview_fragment(id.into()).await?;

        // Map the column indices in the dispatchers with the given mapping.
        let downstream_fragments = self
            .fragment_manager
            .get_downstream_chain_fragments(id.into())
            .await?
            .into_iter()
            .map(|(d, f)| Some((table_col_index_mapping.rewrite_dispatch_strategy(&d)?, f)))
            .collect::<Option<_>>()
            .ok_or_else(|| {
                // The `rewrite` only fails if some column is dropped.
                MetaError::invalid_parameter(
                    "unable to drop the column due to being referenced by downstream materialized views or sinks",
                )
            })?;

        let complete_graph = CompleteStreamFragmentGraph::with_downstreams(
            fragment_graph,
            original_table_fragment.fragment_id,
            downstream_fragments,
        )?;

        // 2. Build the actor graph.
        let cluster_info = self.cluster_manager.get_streaming_cluster_info().await;
        let actor_graph_builder =
            ActorGraphBuilder::new(complete_graph, cluster_info, default_parallelism)?;

        let ActorGraphBuildResult {
            graph,
            building_locations,
            existing_locations,
            dispatchers,
            merge_updates,
        } = actor_graph_builder
            .generate_graph(self.env.id_gen_manager_ref(), stream_job)
            .await?;
        assert!(dispatchers.is_empty());

        // 3. Assign a new dummy ID for the new table fragments.
        //
        // FIXME: we use a dummy table ID for new table fragments, so we can drop the old fragments
        // with the real table ID, then replace the dummy table ID with the real table ID. This is a
        // workaround for not having the version info in the fragment manager.
        let dummy_id = self
            .env
            .id_gen_manager()
            .generate::<{ IdCategory::Table }>()
            .await? as u32;

        // 4. Build the table fragments structure that will be persisted in the stream manager, and
        // the context that contains all information needed for building the actors on the compute
        // nodes.
        let table_fragments = TableFragments::new(
            dummy_id.into(),
            graph,
            &building_locations.actor_locations,
            env,
        );

        let old_table_fragments = self
            .fragment_manager
            .select_table_fragments_by_table_id(&id.into())
            .await?;

        let ctx = ReplaceTableContext {
            old_table_fragments,
            merge_updates,
            building_locations,
            existing_locations,
            table_properties: stream_job.properties(),
        };

        Ok((ctx, table_fragments))
    }

    async fn finish_replace_table(
        &self,
        stream_job: &StreamingJob,
    ) -> MetaResult<NotificationVersion> {
        let StreamingJob::Table(None, table) = stream_job else {
            unreachable!("unexpected job: {stream_job:?}")
        };

        self.catalog_manager
            .finish_replace_table_procedure(table)
            .await
    }

    async fn cancel_replace_table(&self, stream_job: &StreamingJob) -> MetaResult<()> {
        let StreamingJob::Table(None, table) = stream_job else {
            unreachable!("unexpected job: {stream_job:?}")
        };

        self.catalog_manager
            .cancel_replace_table_procedure(table)
            .await
    }

    async fn alter_relation_table(
        &self,
        relation: Relation,
        new_name: &str,
    ) -> MetaResult<NotificationVersion> {
        match relation {
            Relation::TableId(table_id) => {
                self.catalog_manager
                    .alter_table_name(table_id, new_name)
                    .await
            }
            Relation::ViewId(view_id) => {
                self.catalog_manager
                    .alter_view_name(view_id, new_name)
                    .await
            }
            Relation::IndexId(index_id) => {
                self.catalog_manager
                    .alter_index_name(index_id, new_name)
                    .await
            }
            Relation::SinkId(sink_id) => {
                self.catalog_manager
                    .alter_sink_name(sink_id, new_name)
                    .await
            }
        }
    }
}
