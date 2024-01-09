// Copyright 2024 RisingWave Labs
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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use itertools::Itertools;
use rand::Rng;
use risingwave_common::config::DefaultParallelism;
use risingwave_common::hash::{ParallelUnitMapping, VirtualNode};
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::epoch::Epoch;
use risingwave_common::util::stream_graph_visitor::{
    visit_fragment, visit_stream_node, visit_stream_node_cont,
};
use risingwave_common::{bail, current_cluster_version};
use risingwave_connector::dispatch_source_prop;
use risingwave_connector::source::cdc::CdcSourceType;
use risingwave_connector::source::{
    ConnectorProperties, SourceEnumeratorContext, SourceProperties, SplitEnumerator,
    UPSTREAM_SOURCE_KEY,
};
use risingwave_meta_model_v2::object::ObjectType;
use risingwave_meta_model_v2::ObjectId;
use risingwave_pb::catalog::connection::private_link_service::PbPrivateLinkProvider;
use risingwave_pb::catalog::connection::PrivateLinkService;
use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{
    connection, Comment, Connection, CreateType, Database, Function, PbSource, PbTable, Schema,
    Sink, Source, Table, View,
};
use risingwave_pb::ddl_service::alter_owner_request::Object;
use risingwave_pb::ddl_service::{
    alter_name_request, alter_set_schema_request, DdlProgress, TableJobType,
};
use risingwave_pb::meta::table_fragments::PbFragment;
use risingwave_pb::meta::PbTableParallelism;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::{
    Dispatcher, DispatcherType, FragmentTypeFlag, MergeNode, PbStreamFragmentGraph,
    StreamFragmentGraph as StreamFragmentGraphProto,
};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::log::warn;
use tracing::Instrument;

use crate::barrier::BarrierManagerRef;
use crate::controller::catalog::{CatalogControllerRef, ReleaseContext};
use crate::manager::{
    CatalogManagerRef, ConnectionId, DatabaseId, FragmentManagerRef, FunctionId, IdCategory,
    IdCategoryType, IndexId, LocalNotification, MetaSrvEnv, MetadataManager, MetadataManagerV1,
    NotificationVersion, RelationIdEnum, SchemaId, SinkId, SourceId, StreamingClusterInfo,
    StreamingJob, TableId, UserId, ViewId, IGNORED_NOTIFICATION_VERSION,
};
use crate::model::{FragmentId, StreamContext, TableFragments, TableParallelism};
use crate::rpc::cloud_provider::AwsEc2Client;
use crate::stream::{
    validate_sink, ActorGraphBuildResult, ActorGraphBuilder, CompleteStreamFragmentGraph,
    CreateStreamingJobContext, GlobalStreamManagerRef, ReplaceTableContext, SourceManagerRef,
    StreamFragmentGraph,
};
use crate::{MetaError, MetaResult};

#[derive(PartialEq)]
pub enum DropMode {
    Restrict,
    Cascade,
}

impl DropMode {
    pub fn from_request_setting(cascade: bool) -> DropMode {
        if cascade {
            DropMode::Cascade
        } else {
            DropMode::Restrict
        }
    }
}

pub enum StreamingJobId {
    MaterializedView(TableId),
    Sink(SinkId),
    Table(Option<SourceId>, TableId),
    Index(IndexId),
}

impl StreamingJobId {
    #[allow(dead_code)]
    fn id(&self) -> TableId {
        match self {
            StreamingJobId::MaterializedView(id)
            | StreamingJobId::Sink(id)
            | StreamingJobId::Table(_, id)
            | StreamingJobId::Index(id) => *id,
        }
    }
}

// It’s used to describe the information of the table that needs to be replaced and it will be used during replacing table and creating sink into table operations.
pub struct ReplaceTableInfo {
    pub streaming_job: StreamingJob,
    pub fragment_graph: StreamFragmentGraphProto,
    pub col_index_mapping: Option<ColIndexMapping>,
}

pub enum DdlCommand {
    CreateDatabase(Database),
    DropDatabase(DatabaseId),
    CreateSchema(Schema),
    DropSchema(SchemaId),
    CreateSource(Source),
    DropSource(SourceId, DropMode),
    CreateFunction(Function),
    DropFunction(FunctionId),
    CreateView(View),
    DropView(ViewId, DropMode),
    CreateStreamingJob(
        StreamingJob,
        StreamFragmentGraphProto,
        CreateType,
        Option<ReplaceTableInfo>,
    ),
    DropStreamingJob(StreamingJobId, DropMode, Option<ReplaceTableInfo>),
    AlterName(alter_name_request::Object, String),
    ReplaceTable(ReplaceTableInfo),
    AlterSourceColumn(Source),
    AlterObjectOwner(Object, UserId),
    AlterSetSchema(alter_set_schema_request::Object, SchemaId),
    CreateConnection(Connection),
    DropConnection(ConnectionId),
    CommentOn(Comment),
}

impl DdlCommand {
    fn allow_in_recovery(&self) -> bool {
        match self {
            DdlCommand::DropDatabase(_)
            | DdlCommand::DropSchema(_)
            | DdlCommand::DropSource(_, _)
            | DdlCommand::DropFunction(_)
            | DdlCommand::DropView(_, _)
            | DdlCommand::DropStreamingJob(_, _, _)
            | DdlCommand::DropConnection(_) => true,
            // Simply ban all other commands in recovery.
            _ => false,
        }
    }
}

#[derive(Clone)]
pub struct DdlController {
    pub(crate) env: MetaSrvEnv,

    pub(crate) metadata_manager: MetadataManager,
    pub(crate) stream_manager: GlobalStreamManagerRef,
    pub(crate) source_manager: SourceManagerRef,
    barrier_manager: BarrierManagerRef,

    aws_client: Arc<Option<AwsEc2Client>>,
    // The semaphore is used to limit the number of concurrent streaming job creation.
    pub(crate) creating_streaming_job_permits: Arc<CreatingStreamingJobPermit>,
}

#[derive(Clone)]
pub struct CreatingStreamingJobPermit {
    pub(crate) semaphore: Arc<Semaphore>,
}

impl CreatingStreamingJobPermit {
    async fn new(env: &MetaSrvEnv) -> Self {
        let mut permits = env
            .system_params_reader()
            .await
            .max_concurrent_creating_streaming_jobs() as usize;
        if permits == 0 {
            // if the system parameter is set to zero, use the max permitted value.
            permits = Semaphore::MAX_PERMITS;
        }
        let semaphore = Arc::new(Semaphore::new(permits));

        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        env.notification_manager()
            .insert_local_sender(local_notification_tx)
            .await;
        let semaphore_clone = semaphore.clone();
        tokio::spawn(async move {
            while let Some(notification) = local_notification_rx.recv().await {
                let LocalNotification::SystemParamsChange(p) = &notification else {
                    continue;
                };
                let mut new_permits = p.max_concurrent_creating_streaming_jobs() as usize;
                if new_permits == 0 {
                    new_permits = Semaphore::MAX_PERMITS;
                }
                match permits.cmp(&new_permits) {
                    Ordering::Less => {
                        semaphore_clone.add_permits(new_permits - permits);
                    }
                    Ordering::Equal => continue,
                    Ordering::Greater => {
                        semaphore_clone
                            .acquire_many((permits - new_permits) as u32)
                            .await
                            .unwrap()
                            .forget();
                    }
                }
                tracing::info!(
                    "max_concurrent_creating_streaming_jobs changed from {} to {}",
                    permits,
                    new_permits
                );
                permits = new_permits;
            }
        });

        Self { semaphore }
    }
}

impl DdlController {
    pub async fn new(
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        stream_manager: GlobalStreamManagerRef,
        source_manager: SourceManagerRef,
        barrier_manager: BarrierManagerRef,
        aws_client: Arc<Option<AwsEc2Client>>,
    ) -> Self {
        let creating_streaming_job_permits = Arc::new(CreatingStreamingJobPermit::new(&env).await);
        Self {
            env,
            metadata_manager,
            stream_manager,
            source_manager,
            barrier_manager,
            aws_client,
            creating_streaming_job_permits,
        }
    }

    async fn gen_unique_id<const C: IdCategoryType>(&self) -> MetaResult<u32> {
        let id = self.env.id_gen_manager().generate::<C>().await? as u32;
        Ok(id)
    }

    /// `run_command` spawns a tokio coroutine to execute the target ddl command. When the client
    /// has been interrupted during executing, the request will be cancelled by tonic. Since we have
    /// a lot of logic for revert, status management, notification and so on, ensuring consistency
    /// would be a huge hassle and pain if we don't spawn here.
    pub async fn run_command(&self, command: DdlCommand) -> MetaResult<NotificationVersion> {
        if !command.allow_in_recovery() {
            self.barrier_manager.check_status_running().await?;
        }
        let ctrl = self.clone();
        let fut = async move {
            match command {
                DdlCommand::CreateDatabase(database) => ctrl.create_database(database).await,
                DdlCommand::DropDatabase(database_id) => ctrl.drop_database(database_id).await,
                DdlCommand::CreateSchema(schema) => ctrl.create_schema(schema).await,
                DdlCommand::DropSchema(schema_id) => ctrl.drop_schema(schema_id).await,
                DdlCommand::CreateSource(source) => ctrl.create_source(source).await,
                DdlCommand::DropSource(source_id, drop_mode) => {
                    ctrl.drop_source(source_id, drop_mode).await
                }
                DdlCommand::CreateFunction(function) => ctrl.create_function(function).await,
                DdlCommand::DropFunction(function_id) => ctrl.drop_function(function_id).await,
                DdlCommand::CreateView(view) => ctrl.create_view(view).await,
                DdlCommand::DropView(view_id, drop_mode) => {
                    ctrl.drop_view(view_id, drop_mode).await
                }
                DdlCommand::CreateStreamingJob(
                    stream_job,
                    fragment_graph,
                    create_type,
                    affected_table_replace_info,
                ) => {
                    ctrl.create_streaming_job(
                        stream_job,
                        fragment_graph,
                        create_type,
                        affected_table_replace_info,
                    )
                    .await
                }
                DdlCommand::DropStreamingJob(job_id, drop_mode, target_replace_info) => {
                    ctrl.drop_streaming_job(job_id, drop_mode, target_replace_info)
                        .await
                }
                DdlCommand::ReplaceTable(ReplaceTableInfo {
                    streaming_job,
                    fragment_graph,
                    col_index_mapping,
                }) => {
                    ctrl.replace_table(streaming_job, fragment_graph, col_index_mapping)
                        .await
                }
                DdlCommand::AlterName(relation, name) => ctrl.alter_name(relation, &name).await,
                DdlCommand::AlterObjectOwner(object, owner_id) => {
                    ctrl.alter_owner(object, owner_id).await
                }
                DdlCommand::AlterSetSchema(object, new_schema_id) => {
                    ctrl.alter_set_schema(object, new_schema_id).await
                }
                DdlCommand::CreateConnection(connection) => {
                    ctrl.create_connection(connection).await
                }
                DdlCommand::DropConnection(connection_id) => {
                    ctrl.drop_connection(connection_id).await
                }
                DdlCommand::AlterSourceColumn(source) => ctrl.alter_source_column(source).await,
                DdlCommand::CommentOn(comment) => ctrl.comment_on(comment).await,
            }
        }
        .in_current_span();
        tokio::spawn(fut).await.unwrap()
    }

    pub async fn get_ddl_progress(&self) -> Vec<DdlProgress> {
        self.barrier_manager.get_ddl_progress().await
    }

    async fn create_database(&self, mut database: Database) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                database.id = self.gen_unique_id::<{ IdCategory::Database }>().await?;
                mgr.catalog_manager.create_database(&database).await
            }
            MetadataManager::V2(mgr) => mgr.catalog_controller.create_database(database).await,
        }
    }

    pub async fn alter_parallelism(
        &self,
        table_id: u32,
        parallelism: PbTableParallelism,
    ) -> MetaResult<()> {
        self.stream_manager
            .alter_table_parallelism(table_id, parallelism.into())
            .await
    }

    async fn drop_database_v1(
        &self,
        catalog_manager: &CatalogManagerRef,
        database_id: DatabaseId,
    ) -> MetaResult<NotificationVersion> {
        // 1. drop all catalogs in this database.
        let (version, streaming_ids, source_ids, connections_dropped) =
            catalog_manager.drop_database(database_id).await?;
        // 2. Unregister source connector worker.
        self.source_manager.unregister_sources(source_ids).await;
        // 3. drop streaming jobs.
        if !streaming_ids.is_empty() {
            self.stream_manager.drop_streaming_jobs(streaming_ids).await;
        }
        // 4. delete cloud resources if any
        for conn in connections_dropped {
            self.delete_vpc_endpoint(&conn).await?;
        }

        Ok(version)
    }

    async fn drop_database_v2(
        &self,
        catalog_controller: &CatalogControllerRef,
        database_id: DatabaseId,
    ) -> MetaResult<NotificationVersion> {
        let (
            ReleaseContext {
                streaming_jobs,
                source_ids,
                connections,
                ..
            },
            version,
        ) = catalog_controller.drop_database(database_id as _).await?;
        self.source_manager
            .unregister_sources(source_ids.into_iter().map(|id| id as _).collect())
            .await;
        self.stream_manager
            .drop_streaming_jobs(
                streaming_jobs
                    .into_iter()
                    .map(|id| (id as u32).into())
                    .collect(),
            )
            .await;
        for svc in connections {
            self.delete_vpc_endpoint_v2(svc.into_inner()).await?;
        }
        Ok(version)
    }

    async fn drop_database(&self, database_id: DatabaseId) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                self.drop_database_v1(&mgr.catalog_manager, database_id)
                    .await
            }
            MetadataManager::V2(mgr) => {
                self.drop_database_v2(&mgr.catalog_controller, database_id)
                    .await
            }
        }
    }

    async fn create_schema(&self, mut schema: Schema) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                schema.id = self.gen_unique_id::<{ IdCategory::Schema }>().await?;
                mgr.catalog_manager.create_schema(&schema).await
            }
            MetadataManager::V2(mgr) => mgr.catalog_controller.create_schema(schema).await,
        }
    }

    async fn drop_schema(&self, schema_id: SchemaId) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.drop_schema(schema_id).await,
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller
                    .drop_schema(schema_id as _, DropMode::Restrict)
                    .await
            }
        }
    }

    async fn create_source(&self, mut source: Source) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                source.id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
                // set the initialized_at_epoch to the current epoch.
                source.initialized_at_epoch = Some(Epoch::now().0);
                source.initialized_at_cluster_version = Some(current_cluster_version());

                mgr.catalog_manager
                    .start_create_source_procedure(&source)
                    .await?;

                if let Err(e) = self.source_manager.register_source(&source).await {
                    mgr.catalog_manager
                        .cancel_create_source_procedure(&source)
                        .await?;
                    return Err(e.into());
                }

                mgr.catalog_manager
                    .finish_create_source_procedure(source, vec![])
                    .await
            }
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller
                    .create_source(source, Some(self.source_manager.clone()))
                    .await
            }
        }
    }

    async fn drop_source(
        &self,
        source_id: SourceId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unimplemented!("support drop source in v2");
        };
        // 1. Drop source in catalog.
        let (version, streaming_job_ids) = mgr
            .catalog_manager
            .drop_relation(
                RelationIdEnum::Source(source_id),
                mgr.fragment_manager.clone(),
                drop_mode,
            )
            .await?;

        // 2. Unregister source connector worker.
        self.source_manager
            .unregister_sources(vec![source_id])
            .await;

        // 3. Drop streaming jobs if cascade
        self.stream_manager
            .drop_streaming_jobs(streaming_job_ids)
            .await;

        Ok(version)
    }

    // Maybe we can unify `alter_source_column` and `alter_source_name`.
    async fn alter_source_column(&self, source: Source) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.alter_source_column(source).await,
            MetadataManager::V2(mgr) => mgr.catalog_controller.alter_source_column(source).await,
        }
    }

    async fn create_function(&self, mut function: Function) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                function.id = self.gen_unique_id::<{ IdCategory::Function }>().await?;
                mgr.catalog_manager.create_function(&function).await
            }
            MetadataManager::V2(mgr) => mgr.catalog_controller.create_function(function).await,
        }
    }

    async fn drop_function(&self, function_id: FunctionId) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.drop_function(function_id).await,
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller.drop_function(function_id as _).await
            }
        }
    }

    async fn create_view(&self, mut view: View) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                view.id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
                mgr.catalog_manager.create_view(&view).await
            }
            MetadataManager::V2(mgr) => mgr.catalog_controller.create_view(view).await,
        }
    }

    async fn drop_view(
        &self,
        view_id: ViewId,
        drop_mode: DropMode,
    ) -> MetaResult<NotificationVersion> {
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unimplemented!("support drop view in v2");
        };
        let (version, streaming_job_ids) = mgr
            .catalog_manager
            .drop_relation(
                RelationIdEnum::View(view_id),
                mgr.fragment_manager.clone(),
                drop_mode,
            )
            .await?;
        self.stream_manager
            .drop_streaming_jobs(streaming_job_ids)
            .await;
        Ok(version)
    }

    async fn create_connection(
        &self,
        mut connection: Connection,
    ) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                connection.id = self.gen_unique_id::<{ IdCategory::Connection }>().await?;
                mgr.catalog_manager.create_connection(connection).await
            }
            MetadataManager::V2(mgr) => mgr.catalog_controller.create_connection(connection).await,
        }
    }

    async fn drop_connection(
        &self,
        connection_id: ConnectionId,
    ) -> MetaResult<NotificationVersion> {
        let (version, connection) = match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.drop_connection(connection_id).await?,
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller
                    .drop_connection(connection_id as _)
                    .await?
            }
        };

        self.delete_vpc_endpoint(&connection).await?;
        Ok(version)
    }

    async fn delete_vpc_endpoint(&self, connection: &Connection) -> MetaResult<()> {
        // delete AWS vpc endpoint
        if let Some(connection::Info::PrivateLinkService(svc)) = &connection.info
            && svc.get_provider()? == PbPrivateLinkProvider::Aws
        {
            if let Some(aws_cli) = self.aws_client.as_ref() {
                aws_cli.delete_vpc_endpoint(&svc.endpoint_id).await?;
            } else {
                warn!(
                    "AWS client is not initialized, skip deleting vpc endpoint {}",
                    svc.endpoint_id
                );
            }
        }
        Ok(())
    }

    async fn delete_vpc_endpoint_v2(&self, svc: PrivateLinkService) -> MetaResult<()> {
        // delete AWS vpc endpoint
        if svc.get_provider()? == PbPrivateLinkProvider::Aws {
            if let Some(aws_cli) = self.aws_client.as_ref() {
                aws_cli.delete_vpc_endpoint(&svc.endpoint_id).await?;
            } else {
                warn!(
                    "AWS client is not initialized, skip deleting vpc endpoint {}",
                    svc.endpoint_id
                );
            }
        }
        Ok(())
    }

    async fn create_streaming_job(
        &self,
        mut stream_job: StreamingJob,
        mut fragment_graph: StreamFragmentGraphProto,
        create_type: CreateType,
        affected_table_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<NotificationVersion> {
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unimplemented!("support create streaming job in v2");
        };
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        match &mut stream_job {
            StreamingJob::Table(Some(src), table, job_type) => {
                // If we're creating a table with connector, we should additionally fill its ID first.
                src.id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
                fill_table_stream_graph_info(src, table, *job_type, &mut fragment_graph);
            }
            StreamingJob::Source(_) => {
                // set the inner source id of source node.
                for fragment in fragment_graph.fragments.values_mut() {
                    visit_fragment(fragment, |node_body| {
                        if let NodeBody::Source(source_node) = node_body {
                            source_node.source_inner.as_mut().unwrap().source_id = id;
                        }
                    });
                }
            }
            _ => {}
        }

        tracing::debug!(
            id = stream_job.id(),
            definition = stream_job.definition(),
            "starting stream job",
        );
        let _permit = self
            .creating_streaming_job_permits
            .semaphore
            .acquire()
            .await
            .unwrap();
        let _reschedule_job_lock = self.stream_manager.reschedule_lock.read().await;

        let stream_ctx = StreamContext::from_protobuf(fragment_graph.get_ctx().unwrap());

        tracing::debug!(id = stream_job.id(), "preparing stream job");

        // 1. Build fragment graph.
        let fragment_graph =
            StreamFragmentGraph::new(&self.env, fragment_graph, &stream_job).await?;
        let internal_tables = fragment_graph.internal_tables().into_values().collect_vec();

        // 2. Set the graph-related fields and freeze the `stream_job`.
        stream_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        stream_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());
        stream_job.mark_initialized();

        // 3. Persist tables.
        mgr.catalog_manager
            .start_create_stream_job_procedure(&stream_job, internal_tables.clone())
            .await?;

        // 4. Build and persist stream job.
        let result = try {
            tracing::debug!(id = stream_job.id(), "building stream job");
            let (ctx, table_fragments) = self
                .build_stream_job(
                    stream_ctx,
                    &stream_job,
                    fragment_graph,
                    affected_table_replace_info,
                )
                .await?;

            // Do some type-specific work for each type of stream job.
            match stream_job {
                StreamingJob::Table(None, ref table, TableJobType::SharedCdcSource) => {
                    Self::validate_cdc_table(table, &table_fragments)
                        .await
                        .context("failed to validate CDC table")?;
                }
                StreamingJob::Table(Some(ref source), ..) => {
                    // Register the source on the connector node.
                    self.source_manager.register_source(source).await?;
                }
                StreamingJob::Sink(ref sink, ref mut target_table) => {
                    // When sinking into table occurs, some variables of the target table may be modified,
                    // such as `fragment_id` being altered by `prepare_replace_table`.
                    // At this point, it’s necessary to update the table info carried with the sink.
                    if let Some((StreamingJob::Table(source, table, _), ..)) =
                        &ctx.replace_table_job_info
                    {
                        *target_table = Some((table.clone(), source.clone()));
                    }

                    // Validate the sink on the connector node.
                    validate_sink(sink).await?;
                }
                StreamingJob::Source(ref source) => {
                    // Register the source on the connector node.
                    self.source_manager.register_source(source).await?;
                }
                _ => {}
            }

            (ctx, table_fragments)
        };

        let (ctx, table_fragments) = match result {
            Ok(r) => r,
            Err(e) => {
                self.cancel_stream_job(&stream_job, internal_tables, Some(&e))
                    .await?;
                return Err(e);
            }
        };

        match create_type {
            CreateType::Foreground | CreateType::Unspecified => {
                self.create_streaming_job_inner(
                    mgr,
                    stream_job,
                    table_fragments,
                    ctx,
                    internal_tables,
                )
                .await
            }
            CreateType::Background => {
                let ctrl = self.clone();
                let mgr = mgr.clone();
                let stream_job_id = stream_job.id();
                let fut = async move {
                    let result = ctrl
                        .create_streaming_job_inner(
                            &mgr,
                            stream_job,
                            table_fragments,
                            ctx,
                            internal_tables,
                        )
                        .await;
                    match result {
                        Err(e) => {
                            tracing::error!(id=stream_job_id, error = ?e, "finish stream job failed")
                        }
                        Ok(_) => {
                            tracing::info!(id = stream_job_id, "finish stream job succeeded")
                        }
                    }
                };
                tokio::spawn(fut);
                Ok(IGNORED_NOTIFICATION_VERSION)
            }
        }
    }

    /// Validates the connect properties in the `cdc_table_desc` stored in the `StreamCdcScan` node
    pub(crate) async fn validate_cdc_table(
        table: &Table,
        table_fragments: &TableFragments,
    ) -> anyhow::Result<()> {
        let stream_scan_fragment = table_fragments
            .fragments
            .values()
            .filter(|f| f.fragment_type_mask & FragmentTypeFlag::StreamScan as u32 != 0)
            .exactly_one()
            .ok()
            .with_context(|| {
                format!(
                    "expect exactly one stream scan fragment, got: {:?}",
                    table_fragments.fragments
                )
            })?;

        async fn new_enumerator_for_validate<P: SourceProperties>(
            source_props: P,
        ) -> Result<P::SplitEnumerator, anyhow::Error> {
            P::SplitEnumerator::new(source_props, SourceEnumeratorContext::default().into()).await
        }

        for actor in &stream_scan_fragment.actors {
            if let Some(NodeBody::StreamCdcScan(ref stream_cdc_scan)) =
                actor.nodes.as_ref().unwrap().node_body
                && let Some(ref cdc_table_desc) = stream_cdc_scan.cdc_table_desc
            {
                let properties: HashMap<String, String> = cdc_table_desc
                    .connect_properties
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                let mut props = ConnectorProperties::extract(properties, true)?;
                props.init_from_pb_cdc_table_desc(cdc_table_desc);

                dispatch_source_prop!(props, props, {
                    new_enumerator_for_validate(*props).await?;
                });
                tracing::debug!(?table.id, "validate cdc table success");
            }
        }
        Ok(())
    }

    // Here we modify the union node of the downstream table by the TableFragments of the to-be-created sink upstream.
    // The merge in the union has already been set up in the frontend and will be filled with specific upstream actors in this function.
    // Meanwhile, the Dispatcher corresponding to the upstream of the merge will also be added to the replace table context here.
    async fn inject_replace_table_job_for_table_sink(
        &self,
        mgr: &MetadataManagerV1,
        stream_ctx: StreamContext,
        sink: Option<&Sink>,
        creating_sink_table_fragments: Option<&TableFragments>,
        dropping_sink_id: Option<SinkId>,
        ReplaceTableInfo {
            mut streaming_job,
            fragment_graph,
            ..
        }: ReplaceTableInfo,
    ) -> MetaResult<(StreamingJob, ReplaceTableContext, TableFragments)> {
        let fragment_graph = self
            .prepare_replace_table(
                mgr.catalog_manager.clone(),
                &mut streaming_job,
                fragment_graph,
            )
            .await?;

        let (mut replace_table_ctx, mut table_fragments) = self
            .build_replace_table(mgr, stream_ctx, &streaming_job, fragment_graph, None)
            .await?;

        let mut union_fragment_id = None;

        for (fragment_id, fragment) in &mut table_fragments.fragments {
            for actor in &mut fragment.actors {
                if let Some(node) = &mut actor.nodes {
                    visit_stream_node(node, |body| {
                        if let NodeBody::Union(_) = body {
                            if let Some(union_fragment_id) = union_fragment_id.as_mut() {
                                // The union fragment should be unique.
                                assert_eq!(*union_fragment_id, *fragment_id);
                            } else {
                                union_fragment_id = Some(*fragment_id);
                            }
                        }
                    })
                };
            }
        }

        let table = streaming_job.table().unwrap();

        let target_fragment_id =
            union_fragment_id.expect("fragment of placeholder merger not found");

        if let Some(creating_sink_table_fragments) = creating_sink_table_fragments {
            let sink_fragment = creating_sink_table_fragments.sink_fragment().unwrap();

            Self::inject_replace_table_plan_for_sink(
                sink.map(|sink| sink.id),
                &sink_fragment,
                table,
                &mut replace_table_ctx,
                &mut table_fragments,
                target_fragment_id,
            );
        }

        let [table_catalog]: [_; 1] = mgr
            .catalog_manager
            .get_tables(&[table.id])
            .await
            .try_into()
            .expect("Target table should exist in sink into table");

        assert_eq!(table_catalog.incoming_sinks, table.incoming_sinks);

        {
            let guard = mgr.fragment_manager.get_fragment_read_guard().await;

            for sink_id in &table_catalog.incoming_sinks {
                if let Some(dropping_sink_id) = dropping_sink_id
                    && *sink_id == dropping_sink_id
                {
                    continue;
                };

                let sink_table_fragments = guard
                    .table_fragments()
                    .get(&risingwave_common::catalog::TableId::new(*sink_id))
                    .unwrap();

                let sink_fragment = sink_table_fragments.sink_fragment().unwrap();

                Self::inject_replace_table_plan_for_sink(
                    Some(*sink_id),
                    &sink_fragment,
                    table,
                    &mut replace_table_ctx,
                    &mut table_fragments,
                    target_fragment_id,
                );
            }
        }

        // check if the union fragment is fully assigned.
        for fragment in table_fragments.fragments.values_mut() {
            for actor in &mut fragment.actors {
                if let Some(node) = &mut actor.nodes {
                    visit_stream_node(node, |node| {
                        if let NodeBody::Merge(merge_node) = node {
                            assert!(!merge_node.upstream_actor_id.is_empty(), "All the mergers for the union should have been fully assigned beforehand.");
                        }
                    });
                }
            }
        }

        Ok((streaming_job, replace_table_ctx, table_fragments))
    }

    fn inject_replace_table_plan_for_sink(
        sink_id: Option<u32>,
        sink_fragment: &PbFragment,
        table: &Table,
        replace_table_ctx: &mut ReplaceTableContext,
        table_fragments: &mut TableFragments,
        target_fragment_id: FragmentId,
    ) {
        let sink_actor_ids = sink_fragment
            .actors
            .iter()
            .map(|a| a.actor_id)
            .collect_vec();

        let union_fragment = table_fragments
            .fragments
            .get_mut(&target_fragment_id)
            .unwrap();

        let downstream_actor_ids = union_fragment
            .actors
            .iter()
            .map(|actor| actor.actor_id)
            .collect_vec();

        let output_indices = table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, _)| idx as _)
            .collect_vec();

        let dist_key_indices = table.distribution_key.iter().map(|i| *i as _).collect_vec();

        let mapping = downstream_actor_ids
            .iter()
            .map(|id| {
                let actor_status = table_fragments.actor_status.get(id).unwrap();
                let parallel_unit_id = actor_status.parallel_unit.as_ref().unwrap().id;

                (parallel_unit_id, *id)
            })
            .collect();

        let actor_mapping =
            ParallelUnitMapping::from_protobuf(union_fragment.vnode_mapping.as_ref().unwrap())
                .to_actor(&mapping);

        let upstream_actors = sink_fragment.get_actors();

        for actor in upstream_actors {
            replace_table_ctx.dispatchers.insert(
                actor.actor_id,
                vec![Dispatcher {
                    r#type: DispatcherType::Hash as _,
                    dist_key_indices: dist_key_indices.clone(),
                    output_indices: output_indices.clone(),
                    hash_mapping: Some(actor_mapping.to_protobuf()),
                    dispatcher_id: union_fragment.fragment_id as _,
                    downstream_actor_id: downstream_actor_ids.clone(),
                }],
            );
        }

        let upstream_fragment_id = sink_fragment.fragment_id;
        for actor in &mut union_fragment.actors {
            if let Some(node) = &mut actor.nodes {
                let fields = node.fields.clone();

                visit_stream_node_cont(node, |node| {
                    if let Some(NodeBody::Union(_)) = &mut node.node_body {
                        for input in &mut node.input {
                            if let Some(NodeBody::Merge(merge_node)) = &mut input.node_body
                                && merge_node.upstream_actor_id.is_empty()
                            {
                                if let Some(sink_id) = sink_id {
                                    input.identity =
                                        format!("MergeExecutor(from sink {})", sink_id);
                                }

                                *merge_node = MergeNode {
                                    upstream_actor_id: sink_actor_ids.clone(),
                                    upstream_fragment_id,
                                    upstream_dispatcher_type: DispatcherType::Hash as _,
                                    fields: fields.clone(),
                                };

                                return false;
                            }
                        }
                    }
                    true
                });
            }
        }

        union_fragment
            .upstream_fragment_ids
            .push(upstream_fragment_id);
    }

    /// Let the stream manager to create the actors, and do some cleanup work after it fails or finishes.
    async fn create_streaming_job_inner(
        &self,
        mgr: &MetadataManagerV1,
        stream_job: StreamingJob,
        table_fragments: TableFragments,
        ctx: CreateStreamingJobContext,
        internal_tables: Vec<Table>,
    ) -> MetaResult<NotificationVersion> {
        let job_id = stream_job.id();
        tracing::debug!(id = job_id, "creating stream job");

        let result = try {
            // Add table fragments to meta store with state: `State::Initial`.
            mgr.fragment_manager
                .start_create_table_fragments(table_fragments.clone())
                .await?;

            self.stream_manager
                .create_streaming_job(table_fragments, ctx)
                .await?
        };

        if let Err(e) = result {
            match stream_job.create_type() {
                CreateType::Background => {
                    tracing::error!(id = job_id, error = ?e, "finish stream job failed");
                    let should_cancel = match mgr
                        .fragment_manager
                        .select_table_fragments_by_table_id(&job_id.into())
                        .await
                    {
                        Err(err) => err.is_fragment_not_found(),
                        Ok(table_fragments) => table_fragments.is_initial(),
                    };
                    if should_cancel {
                        // If the table fragments are not found or in initial state, it means that the stream job has not been created.
                        // We need to cancel the stream job.
                        self.cancel_stream_job(&stream_job, internal_tables, Some(&e))
                            .await?;
                    } else {
                        // NOTE: This assumes that we will trigger recovery,
                        // and recover stream job progress.
                    }
                }
                _ => {
                    self.cancel_stream_job(&stream_job, internal_tables, Some(&e))
                        .await?;
                }
            }
            return Err(e);
        };

        tracing::debug!(id = job_id, "finishing stream job");
        let version = self
            .finish_stream_job(mgr, stream_job, internal_tables)
            .await?;
        tracing::debug!(id = job_id, "finished stream job");

        Ok(version)
    }

    async fn drop_streaming_job(
        &self,
        job_id: StreamingJobId,
        drop_mode: DropMode,
        target_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<NotificationVersion> {
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unimplemented!("support drop streaming job in v2");
        };
        let _reschedule_job_lock = self.stream_manager.reschedule_lock.read().await;
        let (mut version, streaming_job_ids) = match job_id {
            StreamingJobId::MaterializedView(table_id) => {
                mgr.catalog_manager
                    .drop_relation(
                        RelationIdEnum::Table(table_id),
                        mgr.fragment_manager.clone(),
                        drop_mode,
                    )
                    .await?
            }
            StreamingJobId::Sink(sink_id) => {
                mgr.catalog_manager
                    .drop_relation(
                        RelationIdEnum::Sink(sink_id),
                        mgr.fragment_manager.clone(),
                        drop_mode,
                    )
                    .await?
            }
            StreamingJobId::Table(source_id, table_id) => {
                self.drop_table_inner(
                    source_id,
                    table_id,
                    mgr.catalog_manager.clone(),
                    mgr.fragment_manager.clone(),
                    drop_mode,
                )
                .await?
            }
            StreamingJobId::Index(index_id) => {
                mgr.catalog_manager
                    .drop_relation(
                        RelationIdEnum::Index(index_id),
                        mgr.fragment_manager.clone(),
                        drop_mode,
                    )
                    .await?
            }
        };

        if let Some(replace_table_info) = target_replace_info {
            let stream_ctx =
                StreamContext::from_protobuf(replace_table_info.fragment_graph.get_ctx().unwrap());

            let StreamingJobId::Sink(sink_id) = job_id else {
                panic!("additional replace table event only occurs when dropping sink into table")
            };

            let (streaming_job, context, table_fragments) = self
                .inject_replace_table_job_for_table_sink(
                    mgr,
                    stream_ctx,
                    None,
                    None,
                    Some(sink_id),
                    replace_table_info,
                )
                .await?;

            // Add table fragments to meta store with state: `State::Initial`.
            mgr.fragment_manager
                .start_create_table_fragments(table_fragments.clone())
                .await?;

            self.stream_manager
                .replace_table(table_fragments, context)
                .await?;

            version = self
                .finish_replace_table(
                    mgr.catalog_manager.clone(),
                    &streaming_job,
                    None,
                    None,
                    Some(sink_id),
                )
                .await?;
        }

        self.stream_manager
            .drop_streaming_jobs(streaming_job_ids)
            .await;

        Ok(version)
    }

    fn resolve_stream_parallelism(
        &self,
        default_parallelism: Option<NonZeroUsize>,
        cluster_info: &StreamingClusterInfo,
    ) -> MetaResult<NonZeroUsize> {
        if cluster_info.parallel_units.is_empty() {
            return Err(MetaError::unavailable(
                "No available parallel units to schedule",
            ));
        }

        let available_parallel_units =
            NonZeroUsize::new(cluster_info.parallel_units.len()).unwrap();
        // Use configured parallel units if no default parallelism is specified.
        let parallelism = default_parallelism.unwrap_or(match &self.env.opts.default_parallelism {
            DefaultParallelism::Full => {
                if available_parallel_units.get() > VirtualNode::COUNT {
                    tracing::warn!(
                        "Too many parallel units, use {} instead",
                        VirtualNode::COUNT
                    );
                    NonZeroUsize::new(VirtualNode::COUNT).unwrap()
                } else {
                    available_parallel_units
                }
            }
            DefaultParallelism::Default(num) => *num,
        });

        if parallelism > available_parallel_units {
            return Err(MetaError::unavailable(format!(
                "Not enough parallel units to schedule, required: {}, available: {}",
                parallelism, available_parallel_units
            )));
        }

        Ok(parallelism)
    }

    /// Builds the actor graph:
    /// - Schedule the fragments based on their distribution
    /// - Expand each fragment into one or several actors
    pub(crate) async fn build_stream_job(
        &self,
        stream_ctx: StreamContext,
        stream_job: &StreamingJob,
        fragment_graph: StreamFragmentGraph,
        affected_table_replace_info: Option<ReplaceTableInfo>,
    ) -> MetaResult<(CreateStreamingJobContext, TableFragments)> {
        let id = stream_job.id();
        let default_parallelism = fragment_graph.default_parallelism();
        let internal_tables = fragment_graph.internal_tables();
        let expr_context = stream_ctx.to_expr_context();

        // 1. Resolve the upstream fragments, extend the fragment graph to a complete graph that
        // contains all information needed for building the actor graph.

        let upstream_root_fragments = self
            .metadata_manager
            .get_upstream_root_fragments(
                fragment_graph.dependent_table_ids(),
                stream_job.table_job_type(),
            )
            .await?;

        let upstream_actors: HashMap<_, _> = upstream_root_fragments
            .iter()
            .map(|(&table_id, fragment)| {
                (
                    table_id,
                    fragment.actors.iter().map(|a| a.actor_id).collect_vec(),
                )
            })
            .collect();

        let complete_graph = CompleteStreamFragmentGraph::with_upstreams(
            fragment_graph,
            upstream_root_fragments,
            stream_job.table_job_type(),
        )?;

        // 2. Build the actor graph.
        let cluster_info = self.metadata_manager.get_streaming_cluster_info().await?;

        let parallelism = self.resolve_stream_parallelism(default_parallelism, &cluster_info)?;

        let actor_graph_builder =
            ActorGraphBuilder::new(id, complete_graph, cluster_info, parallelism)?;

        let ActorGraphBuildResult {
            graph,
            building_locations,
            existing_locations,
            dispatchers,
            merge_updates,
        } = actor_graph_builder
            .generate_graph(&self.env, stream_job, expr_context)
            .await?;
        assert!(merge_updates.is_empty());

        // 3. Build the table fragments structure that will be persisted in the stream manager,
        // and the context that contains all information needed for building the
        // actors on the compute nodes.

        let table_parallelism = match default_parallelism {
            None => TableParallelism::Auto,
            Some(parallelism) => TableParallelism::Fixed(parallelism.get()),
        };

        let table_fragments = TableFragments::new(
            id.into(),
            graph,
            &building_locations.actor_locations,
            stream_ctx.clone(),
            table_parallelism,
        );

        let replace_table_job_info = match affected_table_replace_info {
            Some(replace_table_info) => {
                let StreamingJob::Sink(s, _) = stream_job else {
                    bail!("additional replace table event only occurs when sinking into table");
                };
                let MetadataManager::V1(mgr) = &self.metadata_manager else {
                    unimplemented!("support replace table in v2");
                };

                Some(
                    self.inject_replace_table_job_for_table_sink(
                        mgr,
                        stream_ctx,
                        Some(s),
                        Some(&table_fragments),
                        None,
                        replace_table_info,
                    )
                    .await?,
                )
            }
            None => None,
        };

        let ctx = CreateStreamingJobContext {
            dispatchers,
            upstream_mview_actors: upstream_actors,
            internal_tables,
            building_locations,
            existing_locations,
            table_properties: stream_job.properties(),
            definition: stream_job.definition(),
            mv_table_id: stream_job.mv_table(),
            create_type: stream_job.create_type(),
            ddl_type: stream_job.into(),
            replace_table_job_info,
        };

        // 4. Mark tables as creating, including internal tables and the table of the stream job.
        let creating_tables = ctx
            .internal_tables()
            .into_iter()
            .chain(stream_job.table().cloned())
            .collect_vec();

        if let MetadataManager::V1(mgr) = &self.metadata_manager {
            mgr.catalog_manager
                .mark_creating_tables(&creating_tables)
                .await;
        }

        Ok((ctx, table_fragments))
    }

    /// This is NOT used by `CANCEL JOBS`.
    /// It is used internally by `DdlController` to cancel and cleanup stream job.
    async fn cancel_stream_job(
        &self,
        stream_job: &StreamingJob,
        internal_tables: Vec<Table>,
        error: Option<&impl ToString>,
    ) -> MetaResult<()> {
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unimplemented!("support cancel streaming job in v2");
        };
        let error = error.map(ToString::to_string).unwrap_or_default();
        let event = risingwave_pb::meta::event_log::EventCreateStreamJobFail {
            id: stream_job.id(),
            name: stream_job.name(),
            definition: stream_job.definition(),
            error,
        };
        self.env.event_log_manager_ref().add_event_logs(vec![
            risingwave_pb::meta::event_log::Event::CreateStreamJobFail(event),
        ]);

        let mut creating_internal_table_ids =
            internal_tables.into_iter().map(|t| t.id).collect_vec();
        // 1. cancel create procedure.
        match stream_job {
            StreamingJob::MaterializedView(table) => {
                // barrier manager will do the cleanup.
                let result = mgr
                    .catalog_manager
                    .cancel_create_table_procedure(table.id, creating_internal_table_ids.clone())
                    .await;
                creating_internal_table_ids.push(table.id);
                if let Err(e) = result {
                    tracing::warn!("Failed to cancel create table procedure, perhaps barrier manager has already cleaned it. Reason: {e:#?}");
                }
            }
            StreamingJob::Sink(sink, target_table) => {
                mgr.catalog_manager
                    .cancel_create_sink_procedure(sink, target_table)
                    .await;
            }
            StreamingJob::Table(source, table, ..) => {
                if let Some(source) = source {
                    mgr.catalog_manager
                        .cancel_create_table_procedure_with_source(source, table)
                        .await;
                } else {
                    let result = mgr
                        .catalog_manager
                        .cancel_create_table_procedure(
                            table.id,
                            creating_internal_table_ids.clone(),
                        )
                        .await;
                    if let Err(e) = result {
                        tracing::warn!("Failed to cancel create table procedure, perhaps barrier manager has already cleaned it. Reason: {e:#?}");
                    }
                }
                creating_internal_table_ids.push(table.id);
            }
            StreamingJob::Index(index, table) => {
                creating_internal_table_ids.push(table.id);
                mgr.catalog_manager
                    .cancel_create_index_procedure(index, table)
                    .await;
            }
            StreamingJob::Source(source) => {
                mgr.catalog_manager
                    .cancel_create_source_procedure(source)
                    .await?;
            }
        }
        // 2. unmark creating tables.
        mgr.catalog_manager
            .unmark_creating_tables(&creating_internal_table_ids, true)
            .await;
        Ok(())
    }

    /// `finish_stream_job` finishes a stream job and clean some states.
    async fn finish_stream_job(
        &self,
        mgr: &MetadataManagerV1,
        mut stream_job: StreamingJob,
        internal_tables: Vec<Table>,
    ) -> MetaResult<u64> {
        // 1. finish procedure.
        let mut creating_internal_table_ids = internal_tables.iter().map(|t| t.id).collect_vec();

        // Update the corresponding 'created_at' field.
        stream_job.mark_created();

        let version = match stream_job {
            StreamingJob::MaterializedView(table) => {
                creating_internal_table_ids.push(table.id);
                mgr.catalog_manager
                    .finish_create_table_procedure(internal_tables, table)
                    .await?
            }
            StreamingJob::Sink(sink, target_table) => {
                let sink_id = sink.id;

                let mut version = mgr
                    .catalog_manager
                    .finish_create_sink_procedure(internal_tables, sink)
                    .await?;

                if let Some((table, source)) = target_table {
                    let streaming_job =
                        StreamingJob::Table(source, table, TableJobType::Unspecified);

                    version = self
                        .finish_replace_table(
                            mgr.catalog_manager.clone(),
                            &streaming_job,
                            None,
                            Some(sink_id),
                            None,
                        )
                        .await?;
                }

                version
            }
            StreamingJob::Table(source, table, ..) => {
                creating_internal_table_ids.push(table.id);
                if let Some(source) = source {
                    mgr.catalog_manager
                        .finish_create_table_procedure_with_source(source, table, internal_tables)
                        .await?
                } else {
                    mgr.catalog_manager
                        .finish_create_table_procedure(internal_tables, table)
                        .await?
                }
            }
            StreamingJob::Index(index, table) => {
                creating_internal_table_ids.push(table.id);
                mgr.catalog_manager
                    .finish_create_index_procedure(internal_tables, index, table)
                    .await?
            }
            StreamingJob::Source(source) => {
                mgr.catalog_manager
                    .finish_create_source_procedure(source, internal_tables)
                    .await?
            }
        };

        // 2. unmark creating tables.
        mgr.catalog_manager
            .unmark_creating_tables(&creating_internal_table_ids, false)
            .await;

        Ok(version)
    }

    async fn drop_table_inner(
        &self,
        source_id: Option<SourceId>,
        table_id: TableId,
        catalog_manager: CatalogManagerRef,
        fragment_manager: FragmentManagerRef,
        drop_mode: DropMode,
    ) -> MetaResult<(
        NotificationVersion,
        Vec<risingwave_common::catalog::TableId>,
    )> {
        if let Some(source_id) = source_id {
            // Drop table and source in catalog. Check `source_id` if it is the table's
            // `associated_source_id`. Indexes also need to be dropped atomically.
            let (version, delete_jobs) = catalog_manager
                .drop_relation(
                    RelationIdEnum::Table(table_id),
                    fragment_manager.clone(),
                    drop_mode,
                )
                .await?;
            // Unregister source connector worker.
            self.source_manager
                .unregister_sources(vec![source_id])
                .await;
            Ok((version, delete_jobs))
        } else {
            catalog_manager
                .drop_relation(RelationIdEnum::Table(table_id), fragment_manager, drop_mode)
                .await
        }
    }

    async fn replace_table(
        &self,
        mut stream_job: StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
        table_col_index_mapping: Option<ColIndexMapping>,
    ) -> MetaResult<NotificationVersion> {
        let MetadataManager::V1(mgr) = &self.metadata_manager else {
            unimplemented!("support replace table in v2");
        };
        let _reschedule_job_lock = self.stream_manager.reschedule_lock.read().await;
        let stream_ctx = StreamContext::from_protobuf(fragment_graph.get_ctx().unwrap());

        let fragment_graph = self
            .prepare_replace_table(mgr.catalog_manager.clone(), &mut stream_job, fragment_graph)
            .await?;

        let result = try {
            let (ctx, table_fragments) = self
                .build_replace_table(
                    mgr,
                    stream_ctx,
                    &stream_job,
                    fragment_graph,
                    table_col_index_mapping.clone(),
                )
                .await?;

            // Add table fragments to meta store with state: `State::Initial`.
            mgr.fragment_manager
                .start_create_table_fragments(table_fragments.clone())
                .await?;

            self.stream_manager
                .replace_table(table_fragments, ctx)
                .await?;
        };

        match result {
            Ok(_) => {
                self.finish_replace_table(
                    mgr.catalog_manager.clone(),
                    &stream_job,
                    table_col_index_mapping,
                    None,
                    None,
                )
                .await
            }
            Err(err) => {
                self.cancel_replace_table(mgr.catalog_manager.clone(), &stream_job)
                    .await?;
                Err(err)
            }
        }
    }

    /// `prepare_replace_table` prepares a table replacement and returns the new stream fragment
    /// graph. This is basically the same as `prepare_stream_job`, except that it does more
    /// assertions and uses a different method to mark in the catalog.
    async fn prepare_replace_table(
        &self,
        catalog_manager: CatalogManagerRef,
        stream_job: &mut StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
    ) -> MetaResult<StreamFragmentGraph> {
        // 1. Build fragment graph.
        let fragment_graph =
            StreamFragmentGraph::new(&self.env, fragment_graph, stream_job).await?;

        // 2. Set the graph-related fields and freeze the `stream_job`.
        stream_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        stream_job.set_dml_fragment_id(fragment_graph.dml_fragment_id());
        let stream_job = &*stream_job;

        // 3. Mark current relation as "updating".
        catalog_manager
            .start_replace_table_procedure(stream_job)
            .await?;

        Ok(fragment_graph)
    }

    /// `build_replace_table` builds a table replacement and returns the context and new table
    /// fragments.
    async fn build_replace_table(
        &self,
        mgr: &MetadataManagerV1,
        stream_ctx: StreamContext,
        stream_job: &StreamingJob,
        mut fragment_graph: StreamFragmentGraph,
        table_col_index_mapping: Option<ColIndexMapping>,
    ) -> MetaResult<(ReplaceTableContext, TableFragments)> {
        let id = stream_job.id();
        let default_parallelism = fragment_graph.default_parallelism();
        let expr_context = stream_ctx.to_expr_context();

        let old_table_fragments = mgr
            .fragment_manager
            .select_table_fragments_by_table_id(&id.into())
            .await?;
        let old_internal_table_ids = old_table_fragments.internal_table_ids();
        let old_internal_tables = mgr
            .catalog_manager
            .get_tables(&old_internal_table_ids)
            .await;

        fragment_graph.fit_internal_table_ids(old_internal_tables)?;

        // 1. Resolve the edges to the downstream fragments, extend the fragment graph to a complete
        // graph that contains all information needed for building the actor graph.
        let original_table_fragment = mgr.fragment_manager.get_mview_fragment(id.into()).await?;

        // Map the column indices in the dispatchers with the given mapping.
        let downstream_fragments = mgr
                .fragment_manager
                .get_downstream_fragments(id.into())
                .await?
                .into_iter()
                .map(|(d, f)|
                    if let Some(mapping) = &table_col_index_mapping {
                        Some((mapping.rewrite_dispatch_strategy(&d)?, f))
                    } else {
                        Some((d, f))
                    })
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
        let cluster_info = mgr.cluster_manager.get_streaming_cluster_info().await;

        let parallelism = self.resolve_stream_parallelism(default_parallelism, &cluster_info)?;
        let actor_graph_builder =
            ActorGraphBuilder::new(id, complete_graph, cluster_info, parallelism)?;

        let ActorGraphBuildResult {
            graph,
            building_locations,
            existing_locations,
            dispatchers,
            merge_updates,
        } = actor_graph_builder
            .generate_graph(&self.env, stream_job, expr_context)
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

        let table_parallelism = match default_parallelism {
            None => TableParallelism::Auto,
            Some(parallelism) => TableParallelism::Fixed(parallelism.get()),
        };

        // 4. Build the table fragments structure that will be persisted in the stream manager, and
        // the context that contains all information needed for building the actors on the compute
        // nodes.
        let table_fragments = TableFragments::new(
            dummy_id.into(),
            graph,
            &building_locations.actor_locations,
            stream_ctx,
            // todo: shall we use the old table fragments' parallelism
            table_parallelism,
        );

        let ctx = ReplaceTableContext {
            old_table_fragments,
            merge_updates,
            dispatchers,
            building_locations,
            existing_locations,
            table_properties: stream_job.properties(),
        };

        Ok((ctx, table_fragments))
    }

    async fn finish_replace_table(
        &self,
        catalog_manager: CatalogManagerRef,
        stream_job: &StreamingJob,
        table_col_index_mapping: Option<ColIndexMapping>,
        creating_sink_id: Option<SinkId>,
        dropping_sink_id: Option<SinkId>,
    ) -> MetaResult<NotificationVersion> {
        let StreamingJob::Table(source, table, ..) = stream_job else {
            unreachable!("unexpected job: {stream_job:?}")
        };

        catalog_manager
            .finish_replace_table_procedure(
                source,
                table,
                table_col_index_mapping,
                creating_sink_id,
                dropping_sink_id,
            )
            .await
    }

    async fn cancel_replace_table(
        &self,
        catalog_manager: CatalogManagerRef,
        stream_job: &StreamingJob,
    ) -> MetaResult<()> {
        catalog_manager
            .cancel_replace_table_procedure(stream_job)
            .await
    }

    async fn alter_name(
        &self,
        relation: alter_name_request::Object,
        new_name: &str,
    ) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => match relation {
                alter_name_request::Object::TableId(table_id) => {
                    mgr.catalog_manager
                        .alter_table_name(table_id, new_name)
                        .await
                }
                alter_name_request::Object::ViewId(view_id) => {
                    mgr.catalog_manager.alter_view_name(view_id, new_name).await
                }
                alter_name_request::Object::IndexId(index_id) => {
                    mgr.catalog_manager
                        .alter_index_name(index_id, new_name)
                        .await
                }
                alter_name_request::Object::SinkId(sink_id) => {
                    mgr.catalog_manager.alter_sink_name(sink_id, new_name).await
                }
                alter_name_request::Object::SourceId(source_id) => {
                    mgr.catalog_manager
                        .alter_source_name(source_id, new_name)
                        .await
                }
                alter_name_request::Object::SchemaId(schema_id) => {
                    mgr.catalog_manager
                        .alter_schema_name(schema_id, new_name)
                        .await
                }
                alter_name_request::Object::DatabaseId(database_id) => {
                    mgr.catalog_manager
                        .alter_database_name(database_id, new_name)
                        .await
                }
            },
            MetadataManager::V2(mgr) => {
                let (obj_type, id) = match relation {
                    alter_name_request::Object::TableId(id) => (ObjectType::Table, id as ObjectId),
                    alter_name_request::Object::ViewId(id) => (ObjectType::View, id as ObjectId),
                    alter_name_request::Object::IndexId(id) => (ObjectType::Index, id as ObjectId),
                    alter_name_request::Object::SinkId(id) => (ObjectType::Sink, id as ObjectId),
                    alter_name_request::Object::SourceId(id) => {
                        (ObjectType::Source, id as ObjectId)
                    }
                    alter_name_request::Object::SchemaId(id) => {
                        (ObjectType::Schema, id as ObjectId)
                    }
                    alter_name_request::Object::DatabaseId(id) => {
                        (ObjectType::Database, id as ObjectId)
                    }
                };
                mgr.catalog_controller
                    .alter_name(obj_type, id, new_name)
                    .await
            }
        }
    }

    async fn alter_owner(
        &self,
        object: Object,
        owner_id: UserId,
    ) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                mgr.catalog_manager
                    .alter_owner(mgr.fragment_manager.clone(), object, owner_id)
                    .await
            }
            MetadataManager::V2(mgr) => {
                let (obj_type, id) = match object {
                    Object::TableId(id) => (ObjectType::Table, id as ObjectId),
                    Object::ViewId(id) => (ObjectType::View, id as ObjectId),
                    Object::SourceId(id) => (ObjectType::Source, id as ObjectId),
                    Object::SinkId(id) => (ObjectType::Sink, id as ObjectId),
                    Object::SchemaId(id) => (ObjectType::Schema, id as ObjectId),
                    Object::DatabaseId(id) => (ObjectType::Database, id as ObjectId),
                };
                mgr.catalog_controller
                    .alter_owner(obj_type, id, owner_id as _)
                    .await
            }
        }
    }

    async fn alter_set_schema(
        &self,
        object: alter_set_schema_request::Object,
        new_schema_id: SchemaId,
    ) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                mgr.catalog_manager
                    .alter_set_schema(mgr.fragment_manager.clone(), object, new_schema_id)
                    .await
            }
            MetadataManager::V2(mgr) => {
                let (obj_type, id) = match object {
                    alter_set_schema_request::Object::TableId(id) => {
                        (ObjectType::Table, id as ObjectId)
                    }
                    alter_set_schema_request::Object::ViewId(id) => {
                        (ObjectType::View, id as ObjectId)
                    }
                    alter_set_schema_request::Object::SourceId(id) => {
                        (ObjectType::Source, id as ObjectId)
                    }
                    alter_set_schema_request::Object::SinkId(id) => {
                        (ObjectType::Sink, id as ObjectId)
                    }
                    alter_set_schema_request::Object::FunctionId(id) => {
                        (ObjectType::Function, id as ObjectId)
                    }
                    alter_set_schema_request::Object::ConnectionId(id) => {
                        (ObjectType::Connection, id as ObjectId)
                    }
                };
                mgr.catalog_controller
                    .alter_schema(obj_type, id, new_schema_id as _)
                    .await
            }
        }
    }

    pub async fn wait(&self) -> MetaResult<()> {
        let timeout_secs = 30 * 60;
        for _ in 0..timeout_secs {
            match &self.metadata_manager {
                MetadataManager::V1(mgr) => {
                    if mgr
                        .catalog_manager
                        .list_creating_background_mvs()
                        .await
                        .is_empty()
                    {
                        return Ok(());
                    }
                }
                MetadataManager::V2(mgr) => {
                    if mgr
                        .catalog_controller
                        .list_background_creating_mviews()
                        .await?
                        .is_empty()
                    {
                        return Ok(());
                    }
                }
            }

            sleep(Duration::from_secs(1)).await;
        }
        Err(MetaError::cancelled(format!(
            "timeout after {timeout_secs}s"
        )))
    }

    async fn comment_on(&self, comment: Comment) -> MetaResult<NotificationVersion> {
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.comment_on(comment).await,
            MetadataManager::V2(mgr) => mgr.catalog_controller.comment_on(comment).await,
        }
    }
}

/// Fill in necessary information for table stream graph.
pub fn fill_table_stream_graph_info(
    source: &mut PbSource,
    table: &mut PbTable,
    table_job_type: TableJobType,
    fragment_graph: &mut PbStreamFragmentGraph,
) {
    let mut source_count = 0;
    // Fill in the correct table id for source.
    source.optional_associated_table_id =
        Some(OptionalAssociatedTableId::AssociatedTableId(table.id));
    // Fill in the correct source id for mview.
    table.optional_associated_source_id =
        Some(OptionalAssociatedSourceId::AssociatedSourceId(source.id));

    for fragment in fragment_graph.fragments.values_mut() {
        visit_fragment(fragment, |node_body| {
            if let NodeBody::Source(source_node) = node_body {
                if source_node.source_inner.is_none() {
                    // skip empty source for dml node
                    return;
                }

                // If we're creating a table with connector, we should additionally fill its ID first.
                source_node.source_inner.as_mut().unwrap().source_id = source.id;
                source_count += 1;

                // Generate a random server id for mysql cdc source if needed
                // `server.id` (in the range from 1 to 2^32 - 1). This value MUST be unique across whole replication
                // group (that is, different from any other server id being used by any master or slave)
                if let Some(connector) = source.with_properties.get(UPSTREAM_SOURCE_KEY)
                    && matches!(
                        CdcSourceType::from(connector.as_str()),
                        CdcSourceType::Mysql
                    )
                {
                    let props = &mut source_node.source_inner.as_mut().unwrap().with_properties;
                    let rand_server_id = rand::thread_rng().gen_range(1..u32::MAX);
                    props
                        .entry("server.id".to_string())
                        .or_insert(rand_server_id.to_string());

                    // make these two `Source` consistent
                    props.clone_into(&mut source.with_properties);
                }
            }

            // fill table id for cdc backfill
            if let NodeBody::StreamCdcScan(node) = node_body
                && table_job_type == TableJobType::SharedCdcSource
            {
                if let Some(table_desc) = node.cdc_table_desc.as_mut() {
                    table_desc.table_id = table.id;
                }
            }
        });
    }
    assert_eq!(
        source_count, 1,
        "require exactly 1 external stream source when creating table with a connector"
    );
}
