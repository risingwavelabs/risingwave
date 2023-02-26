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

use std::collections::HashMap;

use anyhow::anyhow;
use itertools::Itertools;
use risingwave_common::catalog::CatalogVersion;
use risingwave_connector::common::AwsPrivateLinks;
use risingwave_connector::source::kafka::{KAFKA_PROPS_BROKER_KEY, KAFKA_PROPS_BROKER_KEY_ALIAS};
use risingwave_connector::source::KAFKA_CONNECTOR;
use risingwave_pb::catalog;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{connection, Connection, Source, Table};
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::drop_table_request::SourceId as ProstSourceId;
use risingwave_pb::ddl_service::*;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::StreamFragmentGraph as StreamFragmentGraphProto;
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, IdCategory, IdCategoryType,
    MetaSrvEnv, NotificationVersion, SourceId, StreamingJob, TableId,
};
use crate::model::{StreamEnvironment, TableFragments};
use crate::rpc::cloud_provider::AwsEc2Client;
use crate::storage::MetaStore;
use crate::stream::{
    visit_fragment, ActorGraphBuildResult, ActorGraphBuilder, CompleteStreamFragmentGraph,
    CreateStreamingJobContext, GlobalStreamManagerRef, ReplaceTableContext, SourceManagerRef,
    StreamFragmentGraph,
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

    aws_client: Option<AwsEc2Client>,
}

impl<S> DdlServiceImpl<S>
where
    S: MetaStore,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        env: MetaSrvEnv<S>,
        aws_client: Option<AwsEc2Client>,
        catalog_manager: CatalogManagerRef<S>,
        stream_manager: GlobalStreamManagerRef<S>,
        source_manager: SourceManagerRef<S>,
        cluster_manager: ClusterManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        barrier_manager: BarrierManagerRef<S>,
    ) -> Self {
        Self {
            env,
            aws_client,
            catalog_manager,
            stream_manager,
            source_manager,
            cluster_manager,
            fragment_manager,
            barrier_manager,
        }
    }
}

#[inline(always)]
fn is_kafka_source(with_properties: &HashMap<String, String>) -> bool {
    const UPSTREAM_SOURCE_KEY: &str = "connector";
    with_properties
        .get(UPSTREAM_SOURCE_KEY)
        .unwrap_or(&"".to_string())
        .to_lowercase()
        .eq(KAFKA_CONNECTOR)
}

#[inline(always)]
fn kafka_props_broker_key(with_properties: &HashMap<String, String>) -> &str {
    if with_properties.contains_key(KAFKA_PROPS_BROKER_KEY) {
        KAFKA_PROPS_BROKER_KEY
    } else {
        KAFKA_PROPS_BROKER_KEY_ALIAS
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

        self.resolve_stream_source_info(&mut source).await?;

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
        let table_fragment = self
            .fragment_manager
            .select_table_fragments_by_table_id(&sink_id.into())
            .await?;
        let internal_tables = table_fragment.internal_table_ids();
        // 1. Drop sink in catalog.
        let version = self
            .catalog_manager
            .drop_sink(sink_id, internal_tables)
            .await?;
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

    async fn create_function(
        &self,
        request: Request<CreateFunctionRequest>,
    ) -> Result<Response<CreateFunctionResponse>, Status> {
        let req = request.into_inner();
        let id = self.gen_unique_id::<{ IdCategory::Function }>().await?;
        let mut function = req.get_function()?.clone();
        function.id = id;
        let version = self.catalog_manager.create_function(&function).await?;

        Ok(Response::new(CreateFunctionResponse {
            status: None,
            function_id: id,
            version,
        }))
    }

    async fn drop_function(
        &self,
        request: Request<DropFunctionRequest>,
    ) -> Result<Response<DropFunctionResponse>, Status> {
        self.check_barrier_manager_status().await?;
        let request = request.into_inner();

        let version = self
            .catalog_manager
            .drop_function(request.function_id)
            .await?;

        Ok(Response::new(DropFunctionResponse {
            status: None,
            version,
        }))
    }

    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let request = request.into_inner();
        let mut source = request.source;
        let mut mview = request.materialized_view.unwrap();
        let mut fragment_graph = request.fragment_graph.unwrap();

        // If we're creating a table with connector, we should additionally fill its ID first.
        if let Some(source) = &mut source {
            // Generate source id.
            let source_id = self.gen_unique_id::<{ IdCategory::Table }>().await?; // TODO: Use source category
            source.id = source_id;

            let mut source_count = 0;
            for fragment in fragment_graph.fragments.values_mut() {
                visit_fragment(fragment, |node_body| {
                    if let NodeBody::Source(source_node) = node_body {
                        // TODO: Refactor using source id.
                        source_node.source_inner.as_mut().unwrap().source_id = source_id;
                        source_count += 1;
                    }
                });
            }
            assert_eq!(
                source_count, 1,
                "require exactly 1 external stream source when creating table with a connector"
            );

            // Fill in the correct source id for mview.
            mview.optional_associated_source_id =
                Some(OptionalAssociatedSourceId::AssociatedSourceId(source_id));
        }

        let mut stream_job = StreamingJob::Table(source, mview);
        let version = self
            .create_stream_job(&mut stream_job, fragment_graph)
            .await?;

        Ok(Response::new(CreateTableResponse {
            status: None,
            table_id: stream_job.id(),
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
        request: Request<ReplaceTablePlanRequest>,
    ) -> Result<Response<ReplaceTablePlanResponse>, Status> {
        let req = request.into_inner();

        let mut stream_job = StreamingJob::Table(None, req.table.unwrap());
        let fragment_graph = req.fragment_graph.unwrap();

        let (_ctx, _table_fragments) = self
            .prepare_replace_table(&mut stream_job, fragment_graph)
            .await?;

        Err(Status::unimplemented(
            "replace table plan is not implemented yet",
        ))
    }

    async fn get_table(
        &self,
        request: Request<GetTableRequest>,
    ) -> Result<Response<GetTableResponse>, Status> {
        let req = request.into_inner();
        let database = self
            .catalog_manager
            .list_databases()
            .await
            .into_iter()
            .find(|db| db.name == req.database_name);
        if let Some(db) = database {
            let table = self
                .catalog_manager
                .list_tables()
                .await
                .into_iter()
                .find(|t| t.name == req.table_name && t.database_id == db.id);
            Ok(Response::new(GetTableResponse { table }))
        } else {
            Ok(Response::new(GetTableResponse { table: None }))
        }
    }

    async fn get_ddl_progress(
        &self,
        _request: Request<GetDdlProgressRequest>,
    ) -> Result<Response<GetDdlProgressResponse>, Status> {
        Ok(Response::new(GetDdlProgressResponse {
            ddl_progress: self.barrier_manager.get_ddl_progress().await,
        }))
    }

    async fn create_connection(
        &self,
        request: Request<CreateConnectionRequest>,
    ) -> Result<Response<CreateConnectionResponse>, Status> {
        if self.aws_client.is_none() {
            return Err(Status::from(MetaError::unavailable(
                "AWS client is not configured".into(),
            )));
        }

        let req = request.into_inner();
        if req.payload.is_none() {
            return Err(Status::invalid_argument("request is empty"));
        }

        match req.payload.unwrap() {
            create_connection_request::Payload::PrivateLink(link) => {
                // connection info will be updated if it already exists (upsert)
                let cli = self.aws_client.as_ref().unwrap();
                let private_link_svc = cli
                    .create_aws_private_link(&link.service_name, &link.availability_zones)
                    .await?;

                let id = self.gen_unique_id::<{ IdCategory::Connection }>().await?;
                let connection = Connection {
                    id,
                    name: link.service_name.clone(),
                    info: Some(connection::Info::PrivateLinkService(private_link_svc)),
                };

                // save private link info to catalog
                self.catalog_manager
                    .create_connection(&link.service_name, connection)
                    .await?;

                Ok(Response::new(CreateConnectionResponse {
                    connection_id: id,
                    version: 0,
                }))
            }
        }
    }

    async fn list_connections(
        &self,
        _request: Request<ListConnectionRequest>,
    ) -> Result<Response<ListConnectionResponse>, Status> {
        let conns = self.catalog_manager.list_connections().await;
        Ok(Response::new(ListConnectionResponse {
            connections: conns,
            version: 0,
        }))
    }
}

impl<S> DdlServiceImpl<S>
where
    S: MetaStore,
{
    async fn resolve_stream_source_info(&self, source: &mut Source) -> MetaResult<()> {
        let mut dns_entries = vec![];
        const UPSTREAM_SOURCE_PRIVATE_LINK_KEY: &str = "private.links";
        if let Some(prop) = source.properties.get(UPSTREAM_SOURCE_PRIVATE_LINK_KEY) {
            let links: AwsPrivateLinks = serde_json::from_str(prop).unwrap();

            // if private link is required, get connection info from catalog
            for link in links.infos.iter() {
                let conn = self
                    .catalog_manager
                    .get_connection_by_name(&link.service_name)
                    .await?;

                if let Some(info) = conn.info {
                    match info {
                        connection::Info::PrivateLinkService(svc) => {
                            link.availability_zones.iter().for_each(|az| {
                                svc.dns_entries.get(az).map_or((), |dns_name| {
                                    dns_entries.push(format!("{}:{}", dns_name.clone(), link.port));
                                });
                            })
                        }
                    }
                }
            }
        }

        // store the rewrite rules in Source
        if is_kafka_source(&source.properties) {
            let broker_key = kafka_props_broker_key(&source.properties);
            let servers = source
                .properties
                .get(broker_key)
                .cloned()
                .ok_or(MetaError::from(anyhow!(
                    "Must specify brokers in WITH clause",
                )))?;

            let broker_addrs = servers.split(',').collect::<Vec<&str>>();
            if broker_addrs.len() != dns_entries.len() {
                return Err(MetaError::from(anyhow!(
                    "The number of private link dns entries does not match the number of kafka brokers.\
                     dns entries: {:?}, kafka brokers: {:?}",
                    dns_entries,
                    broker_addrs,
                )));
            }

            // save private link dns names into source properties, which
            // will be extracted into KafkaProperties
            tracing::info!("private link broker address: {:?}", dns_entries);
            const PRIVATE_LINK_DNS_KEY: &str = "private.links.dns.names";
            source
                .properties
                .insert(PRIVATE_LINK_DNS_KEY.to_string(), dns_entries.join(","));

            // store the rewrite mapping in Source
            let broker_rewrite_map = broker_addrs
                .into_iter()
                .map(|str| str.to_string())
                .zip_eq(dns_entries.into_iter())
                .collect::<HashMap<String, String>>();

            if let Some(info) = source.info.as_mut() {
                info.kafka_rewrite_map = broker_rewrite_map;
            }
        }
        Ok(())
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

    /// `create_stream_job` creates a stream job and returns the version of the catalog.
    async fn create_stream_job(
        &self,
        stream_job: &mut StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
    ) -> MetaResult<NotificationVersion> {
        self.check_barrier_manager_status().await?;

        let (ctx, table_fragments) = self.prepare_stream_job(stream_job, fragment_graph).await?;

        let internal_tables = ctx.internal_tables();
        let result = try {
            if let Some(source) = stream_job.source() {
                self.source_manager.register_source(source).await?;
            }
            self.stream_manager
                .create_streaming_job(table_fragments, ctx)
                .await?;
        };

        match result {
            Ok(_) => self.finish_stream_job(stream_job, internal_tables).await,
            Err(err) => {
                self.cancel_stream_job(stream_job, internal_tables).await?;
                Err(err)
            }
        }
    }

    /// `prepare_stream_job` prepares a stream job and returns the context and table fragments.
    async fn prepare_stream_job(
        &self,
        stream_job: &mut StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
    ) -> MetaResult<(CreateStreamingJobContext, TableFragments)> {
        // 1. Assign a new id to the stream job.
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        // 2. Get the env for streaming jobs.
        let env = StreamEnvironment::from_protobuf(fragment_graph.get_env().unwrap());

        // 3. Build fragment graph.
        let fragment_graph =
            StreamFragmentGraph::new(fragment_graph, self.env.id_gen_manager_ref(), &*stream_job)
                .await?;
        let default_parallelism = fragment_graph.default_parallelism();
        let internal_tables = fragment_graph.internal_tables();

        // 4. Set the graph-related fields and freeze the `stream_job`.
        stream_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        let dependent_relations = fragment_graph.dependent_relations().clone();
        stream_job.set_dependent_relations(dependent_relations.clone());

        let stream_job = &*stream_job;

        // 5. Mark current relation as "creating" and add reference count to dependent relations.
        self.catalog_manager
            .start_create_stream_job_procedure(stream_job)
            .await?;

        // 6. Resolve the upstream fragments, extend the fragment graph to a complete graph that
        // contains all information needed for building the actor graph.
        let upstream_mview_fragments = self
            .fragment_manager
            .get_upstream_mview_fragments(&dependent_relations)
            .await?;
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

        // 7. Build the actor graph.
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

        // 8. Build the table fragments structure that will be persisted in the stream manager, and
        // the context that contains all information needed for building the actors on the compute
        // nodes.
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

        // 9. Mark creating tables, including internal tables and the table of the stream job.
        // Note(bugen): should we take `Sink` into account as well?
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
    async fn cancel_stream_job(
        &self,
        stream_job: &StreamingJob,
        internal_tables: Vec<Table>,
    ) -> MetaResult<()> {
        let mut creating_internal_table_ids =
            internal_tables.into_iter().map(|t| t.id).collect_vec();
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
                    let internal_tables: [_; 1] = internal_tables.try_into().unwrap();
                    self.catalog_manager
                        .finish_create_table_procedure_with_source(
                            source,
                            table,
                            &internal_tables[0],
                        )
                        .await?
                } else {
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

    /// Prepares a table replacement and returns the context and table fragments.
    async fn prepare_replace_table(
        &self,
        stream_job: &mut StreamingJob,
        fragment_graph: StreamFragmentGraphProto,
    ) -> MetaResult<(ReplaceTableContext, TableFragments)> {
        let id = stream_job.id();

        // 1. Get the env for streaming jobs.
        let env = StreamEnvironment::from_protobuf(fragment_graph.get_env().unwrap());

        // 2. Build fragment graph.
        let fragment_graph =
            StreamFragmentGraph::new(fragment_graph, self.env.id_gen_manager_ref(), &*stream_job)
                .await?;
        let default_parallelism = fragment_graph.default_parallelism();
        assert!(fragment_graph.internal_tables().is_empty());

        // 3. Set the graph-related fields and freeze the `stream_job`.
        stream_job.set_table_fragment_id(fragment_graph.table_fragment_id());
        let stream_job = &*stream_job;

        // TODO: 4. Mark current relation as "updating".

        // 5. Resolve the downstream fragments, extend the fragment graph to a complete graph that
        // contains all information needed for building the actor graph.
        let downstream_fragments = self
            .fragment_manager
            .get_downstream_chain_fragments(id.into())
            .await?;

        let complete_graph =
            CompleteStreamFragmentGraph::with_downstreams(fragment_graph, downstream_fragments)?;

        // 6. Build the actor graph.
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

        // 7. Build the table fragments structure that will be persisted in the stream manager, and
        // the context that contains all information needed for building the actors on the compute
        // nodes.
        let table_fragments =
            TableFragments::new(id.into(), graph, &building_locations.actor_locations, env);

        let ctx = ReplaceTableContext {
            merge_updates,
            building_locations,
            existing_locations,
            table_properties: stream_job.properties(),
        };

        Ok((ctx, table_fragments))
    }

    async fn gen_unique_id<const C: IdCategoryType>(&self) -> MetaResult<u32> {
        let id = self.env.id_gen_manager().generate::<C>().await? as u32;
        Ok(id)
    }
}
