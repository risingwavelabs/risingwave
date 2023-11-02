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
use std::sync::Arc;

use anyhow::anyhow;
use rand::Rng;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::stream_graph_visitor::visit_fragment;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_connector::source::cdc::CdcSourceType;
use risingwave_connector::source::UPSTREAM_SOURCE_KEY;
use risingwave_pb::catalog::connection::private_link_service::{
    PbPrivateLinkProvider, PrivateLinkProvider,
};
use risingwave_pb::catalog::connection::PbPrivateLinkService;
use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{connection, Comment, Connection, CreateType, PbSource, PbTable};
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::drop_table_request::PbSourceId;
use risingwave_pb::ddl_service::*;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use risingwave_pb::stream_plan::PbStreamFragmentGraph;
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, ConnectionId, FragmentManagerRef, IdCategory,
    IdCategoryType, MetaSrvEnv, StreamingJob,
};
use crate::rpc::cloud_provider::AwsEc2Client;
use crate::rpc::ddl_controller::{DdlCommand, DdlController, DropMode, StreamingJobId};
use crate::stream::{GlobalStreamManagerRef, SourceManagerRef};
use crate::{MetaError, MetaResult};

#[derive(Clone)]
pub struct DdlServiceImpl {
    env: MetaSrvEnv,

    catalog_manager: CatalogManagerRef,
    sink_manager: SinkCoordinatorManager,
    ddl_controller: DdlController,
    aws_client: Arc<Option<AwsEc2Client>>,
}

impl DdlServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        env: MetaSrvEnv,
        aws_client: Option<AwsEc2Client>,
        catalog_manager: CatalogManagerRef,
        stream_manager: GlobalStreamManagerRef,
        source_manager: SourceManagerRef,
        cluster_manager: ClusterManagerRef,
        fragment_manager: FragmentManagerRef,
        barrier_manager: BarrierManagerRef,
        sink_manager: SinkCoordinatorManager,
    ) -> Self {
        let aws_cli_ref = Arc::new(aws_client);
        let ddl_controller = DdlController::new(
            env.clone(),
            catalog_manager.clone(),
            stream_manager,
            source_manager,
            cluster_manager,
            fragment_manager,
            barrier_manager,
            aws_cli_ref.clone(),
        )
        .await;
        Self {
            env,
            catalog_manager,
            ddl_controller,
            aws_client: aws_cli_ref,
            sink_manager,
        }
    }
}

#[async_trait::async_trait]
impl DdlService for DdlServiceImpl {
    async fn create_database(
        &self,
        request: Request<CreateDatabaseRequest>,
    ) -> Result<Response<CreateDatabaseResponse>, Status> {
        let req = request.into_inner();
        let id = self.gen_unique_id::<{ IdCategory::Database }>().await?;
        let mut database = req.get_db()?.clone();
        database.id = id;
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateDatabase(database))
            .await?;

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
            .ddl_controller
            .run_command(DdlCommand::DropDatabase(database_id))
            .await?;

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
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateSchema(schema))
            .await?;

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
            .ddl_controller
            .run_command(DdlCommand::DropSchema(schema_id))
            .await?;
        Ok(Response::new(DropSchemaResponse {
            status: None,
            version,
        }))
    }

    async fn create_source(
        &self,
        request: Request<CreateSourceRequest>,
    ) -> Result<Response<CreateSourceResponse>, Status> {
        let req = request.into_inner();
        let mut source = req.get_source()?.clone();

        // validate connection before starting the DDL procedure
        if let Some(connection_id) = source.connection_id {
            self.validate_connection(connection_id).await?;
        }

        let source_id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        source.id = source_id;

        match req.fragment_graph {
            None => {
                let version = self
                    .ddl_controller
                    .run_command(DdlCommand::CreateSource(source))
                    .await?;
                Ok(Response::new(CreateSourceResponse {
                    status: None,
                    source_id,
                    version,
                }))
            }
            Some(mut fragment_graph) => {
                for fragment in fragment_graph.fragments.values_mut() {
                    visit_fragment(fragment, |node_body| {
                        if let NodeBody::Source(source_node) = node_body {
                            source_node.source_inner.as_mut().unwrap().source_id = source_id;
                        }
                    });
                }

                // The id of stream job has been set above
                let stream_job = StreamingJob::Source(source);
                let version = self
                    .ddl_controller
                    .run_command(DdlCommand::CreateStreamingJob(
                        stream_job,
                        fragment_graph,
                        CreateType::Foreground,
                    ))
                    .await?;
                Ok(Response::new(CreateSourceResponse {
                    status: None,
                    source_id,
                    version,
                }))
            }
        }
    }

    async fn drop_source(
        &self,
        request: Request<DropSourceRequest>,
    ) -> Result<Response<DropSourceResponse>, Status> {
        let request = request.into_inner();
        let source_id = request.source_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropSource(source_id, drop_mode))
            .await?;

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

        // validate connection before starting the DDL procedure
        if let Some(connection_id) = sink.connection_id {
            self.validate_connection(connection_id).await?;
        }

        let mut stream_job = StreamingJob::Sink(sink);
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(
                stream_job,
                fragment_graph,
                CreateType::Foreground,
            ))
            .await?;

        Ok(Response::new(CreateSinkResponse {
            status: None,
            sink_id: id,
            version,
        }))
    }

    async fn drop_sink(
        &self,
        request: Request<DropSinkRequest>,
    ) -> Result<Response<DropSinkResponse>, Status> {
        let request = request.into_inner();
        let sink_id = request.sink_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob(
                StreamingJobId::Sink(sink_id),
                drop_mode,
            ))
            .await?;

        self.sink_manager
            .stop_sink_coordinator(SinkId::from(sink_id))
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
        let create_type = mview.get_create_type().unwrap_or(CreateType::Foreground);
        let fragment_graph = req.get_fragment_graph()?.clone();

        let mut stream_job = StreamingJob::MaterializedView(mview);
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(
                stream_job,
                fragment_graph,
                create_type,
            ))
            .await?;

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
        self.env.idle_manager().record_activity();

        let request = request.into_inner();
        let table_id = request.table_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob(
                StreamingJobId::MaterializedView(table_id),
                drop_mode,
            ))
            .await?;

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
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(
                stream_job,
                fragment_graph,
                CreateType::Foreground,
            ))
            .await?;

        Ok(Response::new(CreateIndexResponse {
            status: None,
            index_id: id,
            version,
        }))
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        self.env.idle_manager().record_activity();

        let request = request.into_inner();
        let index_id = request.index_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob(
                StreamingJobId::Index(index_id),
                drop_mode,
            ))
            .await?;

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
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateFunction(function))
            .await?;

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
        let request = request.into_inner();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropFunction(request.function_id))
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
        let job_type = request.get_job_type().unwrap_or_default();
        let mut source = request.source;
        let mut mview = request.materialized_view.unwrap();
        let mut fragment_graph = request.fragment_graph.unwrap();
        let table_id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        // If we're creating a table with connector, we should additionally fill its ID first.
        if let Some(source) = &mut source {
            // Generate source id.
            let source_id = self.gen_unique_id::<{ IdCategory::Table }>().await?; // TODO: Use source category
            fill_table_source(source, source_id, &mut mview, table_id, &mut fragment_graph);

            // Modify properties for cdc sources if needed
            if let Some(connector) = source.properties.get(UPSTREAM_SOURCE_KEY) {
                if matches!(
                    CdcSourceType::from(connector.as_str()),
                    CdcSourceType::Mysql
                ) {
                    fill_cdc_mysql_server_id(&mut fragment_graph);
                }
            }
        }

        let mut stream_job = StreamingJob::Table(source, mview, job_type);

        stream_job.set_id(table_id);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(
                stream_job,
                fragment_graph,
                CreateType::Foreground,
            ))
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
        let request = request.into_inner();
        let source_id = request.source_id;
        let table_id = request.table_id;

        let drop_mode = DropMode::from_request_setting(request.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob(
                StreamingJobId::Table(source_id.map(|PbSourceId::Id(id)| id), table_id),
                drop_mode,
            ))
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

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateView(view))
            .await?;

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
        let request = request.into_inner();
        let view_id = request.get_view_id();
        let drop_mode = DropMode::from_request_setting(request.cascade);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropView(view_id, drop_mode))
            .await?;
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

        let mut source = req.source;
        let mut fragment_graph = req.fragment_graph.unwrap();
        let mut table = req.table.unwrap();
        if let Some(OptionalAssociatedSourceId::AssociatedSourceId(source_id)) =
            table.optional_associated_source_id
        {
            let source = source.as_mut().unwrap();
            let table_id = table.id;
            fill_table_source(source, source_id, &mut table, table_id, &mut fragment_graph);
        }
        let table_col_index_mapping =
            ColIndexMapping::from_protobuf(&req.table_col_index_mapping.unwrap());
        let stream_job = StreamingJob::Table(source, table, TableJobType::General);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::ReplaceTable(
                stream_job,
                fragment_graph,
                table_col_index_mapping,
            ))
            .await?;

        Ok(Response::new(ReplaceTablePlanResponse {
            status: None,
            version,
        }))
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

    async fn alter_relation_name(
        &self,
        request: Request<AlterRelationNameRequest>,
    ) -> Result<Response<AlterRelationNameResponse>, Status> {
        let AlterRelationNameRequest { relation, new_name } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterRelationName(relation.unwrap(), new_name))
            .await?;
        Ok(Response::new(AlterRelationNameResponse {
            status: None,
            version,
        }))
    }

    async fn alter_source(
        &self,
        request: Request<AlterSourceRequest>,
    ) -> Result<Response<AlterSourceResponse>, Status> {
        let AlterSourceRequest { source } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterSourceColumn(source.unwrap()))
            .await?;
        Ok(Response::new(AlterSourceResponse {
            status: None,
            version,
        }))
    }

    async fn get_ddl_progress(
        &self,
        _request: Request<GetDdlProgressRequest>,
    ) -> Result<Response<GetDdlProgressResponse>, Status> {
        Ok(Response::new(GetDdlProgressResponse {
            ddl_progress: self.ddl_controller.get_ddl_progress().await,
        }))
    }

    async fn create_connection(
        &self,
        request: Request<CreateConnectionRequest>,
    ) -> Result<Response<CreateConnectionResponse>, Status> {
        let req = request.into_inner();
        if req.payload.is_none() {
            return Err(Status::invalid_argument("request is empty"));
        }

        match req.payload.unwrap() {
            create_connection_request::Payload::PrivateLink(link) => {
                // currently we only support AWS
                let private_link_svc = match link.get_provider()? {
                    PbPrivateLinkProvider::Mock => PbPrivateLinkService {
                        provider: link.provider,
                        service_name: String::new(),
                        endpoint_id: String::new(),
                        endpoint_dns_name: String::new(),
                        dns_entries: HashMap::new(),
                    },
                    PbPrivateLinkProvider::Aws => {
                        if let Some(aws_cli) = self.aws_client.as_ref() {
                            let tags_env = self
                                .env
                                .opts
                                .privatelink_endpoint_default_tags
                                .as_ref()
                                .map(|tags| {
                                    tags.iter()
                                        .map(|(key, val)| (key.as_str(), val.as_str()))
                                        .collect()
                                });
                            aws_cli
                                .create_aws_private_link(
                                    &link.service_name,
                                    link.tags.as_deref(),
                                    tags_env,
                                )
                                .await?
                        } else {
                            return Err(Status::from(MetaError::unavailable(
                                "AWS client is not configured".into(),
                            )));
                        }
                    }
                    PbPrivateLinkProvider::Unspecified => {
                        return Err(Status::invalid_argument("Privatelink provider unspecified"));
                    }
                };
                let id = self.gen_unique_id::<{ IdCategory::Connection }>().await?;
                let connection = Connection {
                    id,
                    schema_id: req.schema_id,
                    database_id: req.database_id,
                    name: req.name,
                    owner: req.owner_id,
                    info: Some(connection::Info::PrivateLinkService(private_link_svc)),
                };

                // save private link info to catalog
                let version = self
                    .ddl_controller
                    .run_command(DdlCommand::CreateConnection(connection))
                    .await?;

                Ok(Response::new(CreateConnectionResponse {
                    connection_id: id,
                    version,
                }))
            }
        }
    }

    async fn list_connections(
        &self,
        _request: Request<ListConnectionsRequest>,
    ) -> Result<Response<ListConnectionsResponse>, Status> {
        let conns = self.catalog_manager.list_connections().await;
        Ok(Response::new(ListConnectionsResponse {
            connections: conns,
        }))
    }

    async fn drop_connection(
        &self,
        request: Request<DropConnectionRequest>,
    ) -> Result<Response<DropConnectionResponse>, Status> {
        let req = request.into_inner();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropConnection(req.connection_id))
            .await?;

        Ok(Response::new(DropConnectionResponse {
            status: None,
            version,
        }))
    }

    async fn comment_on(
        &self,
        request: Request<CommentOnRequest>,
    ) -> Result<Response<CommentOnResponse>, Status> {
        let req = request.into_inner();
        let comment = req.get_comment()?.clone();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CommentOn(Comment {
                table_id: comment.table_id,
                schema_id: comment.schema_id,
                database_id: comment.database_id,
                column_index: comment.column_index,
                description: comment.description,
            }))
            .await?;

        Ok(Response::new(CommentOnResponse {
            status: None,
            version,
        }))
    }

    #[cfg_attr(coverage, coverage(off))]
    async fn get_tables(
        &self,
        request: Request<GetTablesRequest>,
    ) -> Result<Response<GetTablesResponse>, Status> {
        let ret = self
            .catalog_manager
            .get_tables(&request.into_inner().table_ids)
            .await;
        let mut tables = HashMap::default();
        for table in ret {
            tables.insert(table.id, table);
        }
        Ok(Response::new(GetTablesResponse { tables }))
    }

    async fn wait(&self, _request: Request<WaitRequest>) -> Result<Response<WaitResponse>, Status> {
        self.ddl_controller.wait().await?;
        Ok(Response::new(WaitResponse {}))
    }
}

impl DdlServiceImpl {
    async fn gen_unique_id<const C: IdCategoryType>(&self) -> MetaResult<u32> {
        let id = self.env.id_gen_manager().generate::<C>().await? as u32;
        Ok(id)
    }

    async fn validate_connection(&self, connection_id: ConnectionId) -> MetaResult<()> {
        let connection = self
            .catalog_manager
            .get_connection_by_id(connection_id)
            .await?;
        if let Some(connection::Info::PrivateLinkService(svc)) = &connection.info {
            // skip all checks for mock connection
            if svc.get_provider()? == PrivateLinkProvider::Mock {
                return Ok(());
            }

            // check whether private link is ready
            if let Some(aws_cli) = self.aws_client.as_ref() {
                if !aws_cli.is_vpc_endpoint_ready(&svc.endpoint_id).await? {
                    return Err(MetaError::from(anyhow!(
                        "Private link endpoint {} is not ready",
                        svc.endpoint_id
                    )));
                }
            }
        }
        Ok(())
    }
}

fn fill_table_source(
    source: &mut PbSource,
    source_id: u32,
    table: &mut PbTable,
    table_id: u32,
    fragment_graph: &mut PbStreamFragmentGraph,
) {
    // If we're creating a table with connector, we should additionally fill its ID first.
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

    // Fill in the correct table id for source.
    source.optional_associated_table_id =
        Some(OptionalAssociatedTableId::AssociatedTableId(table_id));

    // Fill in the correct source id for mview.
    table.optional_associated_source_id =
        Some(OptionalAssociatedSourceId::AssociatedSourceId(source_id));
}

// `server.id` (in the range from 1 to 2^32 - 1). This value MUST be unique across whole replication
// group (that is, different from any other server id being used by any master or slave)
fn fill_cdc_mysql_server_id(fragment_graph: &mut PbStreamFragmentGraph) {
    for fragment in fragment_graph.fragments.values_mut() {
        visit_fragment(fragment, |node_body| {
            if let NodeBody::Source(source_node) = node_body {
                let props = &mut source_node.source_inner.as_mut().unwrap().properties;
                let rand_server_id = rand::thread_rng().gen_range(1..u32::MAX);
                props
                    .entry("server.id".to_string())
                    .or_insert(rand_server_id.to_string());
            }
        });
    }
}
