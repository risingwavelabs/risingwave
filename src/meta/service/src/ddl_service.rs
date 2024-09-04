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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::anyhow;
use rand::seq::SliceRandom;
use rand::thread_rng;
use risingwave_common::catalog::ColumnCatalog;
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_connector::sink::catalog::SinkId;
use risingwave_meta::manager::{EventLogManagerRef, MetadataManager};
use risingwave_meta::rpc::ddl_controller::fill_table_stream_graph_info;
use risingwave_meta::rpc::metrics::MetaMetrics;
use risingwave_pb::catalog::connection::private_link_service::{
    PbPrivateLinkProvider, PrivateLinkProvider,
};
use risingwave_pb::catalog::connection::PbPrivateLinkService;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{connection, Comment, Connection, CreateType, Secret, Table};
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::WorkerType;
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::drop_table_request::PbSourceId;
use risingwave_pb::ddl_service::*;
use risingwave_pb::frontend_service::GetTableReplacePlanRequest;
use risingwave_pb::meta::event_log;
use thiserror_ext::AsReport;
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{ConnectionId, MetaSrvEnv, StreamingJob};
use crate::rpc::cloud_provider::AwsEc2Client;
use crate::rpc::ddl_controller::{
    DdlCommand, DdlController, DropMode, ReplaceTableInfo, StreamingJobId,
};
use crate::stream::{GlobalStreamManagerRef, SourceManagerRef};
use crate::{MetaError, MetaResult};

#[derive(Clone)]
pub struct DdlServiceImpl {
    env: MetaSrvEnv,

    metadata_manager: MetadataManager,
    sink_manager: SinkCoordinatorManager,
    ddl_controller: DdlController,
    aws_client: Arc<Option<AwsEc2Client>>,
    meta_metrics: Arc<MetaMetrics>,
}

impl DdlServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        env: MetaSrvEnv,
        aws_client: Option<AwsEc2Client>,
        metadata_manager: MetadataManager,
        stream_manager: GlobalStreamManagerRef,
        source_manager: SourceManagerRef,
        barrier_manager: BarrierManagerRef,
        sink_manager: SinkCoordinatorManager,
        meta_metrics: Arc<MetaMetrics>,
    ) -> Self {
        let aws_cli_ref = Arc::new(aws_client);
        let ddl_controller = DdlController::new(
            env.clone(),
            metadata_manager.clone(),
            stream_manager,
            source_manager,
            barrier_manager,
            aws_cli_ref.clone(),
        )
        .await;
        Self {
            env,
            metadata_manager,
            ddl_controller,
            aws_client: aws_cli_ref,
            sink_manager,
            meta_metrics,
        }
    }

    fn extract_replace_table_info(change: ReplaceTablePlan) -> ReplaceTableInfo {
        let job_type = change.get_job_type().unwrap_or_default();
        let mut source = change.source;
        let mut fragment_graph = change.fragment_graph.unwrap();
        let mut table = change.table.unwrap();
        if let Some(OptionalAssociatedSourceId::AssociatedSourceId(source_id)) =
            table.optional_associated_source_id
        {
            source.as_mut().unwrap().id = source_id;
            fill_table_stream_graph_info(&mut source, &mut table, job_type, &mut fragment_graph);
        }
        let table_col_index_mapping = change
            .table_col_index_mapping
            .as_ref()
            .map(ColIndexMapping::from_protobuf);

        let stream_job = StreamingJob::Table(source, table, job_type);
        ReplaceTableInfo {
            streaming_job: stream_job,
            fragment_graph,
            col_index_mapping: table_col_index_mapping,
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
        let database = req.get_db()?.clone();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateDatabase(database))
            .await?;

        Ok(Response::new(CreateDatabaseResponse {
            status: None,
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

    async fn create_secret(
        &self,
        request: Request<CreateSecretRequest>,
    ) -> Result<Response<CreateSecretResponse>, Status> {
        let req = request.into_inner();
        let pb_secret = Secret {
            id: 0,
            name: req.get_name().clone(),
            database_id: req.get_database_id(),
            value: req.get_value().clone(),
            owner: req.get_owner_id(),
            schema_id: req.get_schema_id(),
        };
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateSecret(pb_secret))
            .await?;

        Ok(Response::new(CreateSecretResponse { version }))
    }

    async fn drop_secret(
        &self,
        request: Request<DropSecretRequest>,
    ) -> Result<Response<DropSecretResponse>, Status> {
        let req = request.into_inner();
        let secret_id = req.get_secret_id();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropSecret(secret_id))
            .await?;

        Ok(Response::new(DropSecretResponse { version }))
    }

    async fn create_schema(
        &self,
        request: Request<CreateSchemaRequest>,
    ) -> Result<Response<CreateSchemaResponse>, Status> {
        let req = request.into_inner();
        let schema = req.get_schema()?.clone();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateSchema(schema))
            .await?;

        Ok(Response::new(CreateSchemaResponse {
            status: None,
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
        let source = req.get_source()?.clone();

        // validate connection before starting the DDL procedure
        if let Some(connection_id) = source.connection_id {
            self.validate_connection(connection_id).await?;
        }

        match req.fragment_graph {
            None => {
                let version = self
                    .ddl_controller
                    .run_command(DdlCommand::CreateSource(source))
                    .await?;
                Ok(Response::new(CreateSourceResponse {
                    status: None,
                    version,
                }))
            }
            Some(fragment_graph) => {
                // The id of stream job has been set above
                let stream_job = StreamingJob::Source(source);
                let version = self
                    .ddl_controller
                    .run_command(DdlCommand::CreateStreamingJob(
                        stream_job,
                        fragment_graph,
                        CreateType::Foreground,
                        None,
                    ))
                    .await?;
                Ok(Response::new(CreateSourceResponse {
                    status: None,
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
        let affected_table_change = req.get_affected_table_change().cloned().ok();

        // validate connection before starting the DDL procedure
        if let Some(connection_id) = sink.connection_id {
            self.validate_connection(connection_id).await?;
        }

        let stream_job = match &affected_table_change {
            None => StreamingJob::Sink(sink, None),
            Some(change) => {
                let table = change.table.clone().unwrap();
                let source = change.source.clone();
                StreamingJob::Sink(sink, Some((table, source)))
            }
        };

        let command = DdlCommand::CreateStreamingJob(
            stream_job,
            fragment_graph,
            CreateType::Foreground,
            affected_table_change.map(Self::extract_replace_table_info),
        );

        let version = self.ddl_controller.run_command(command).await?;

        Ok(Response::new(CreateSinkResponse {
            status: None,
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

        let command = DdlCommand::DropStreamingJob(
            StreamingJobId::Sink(sink_id),
            drop_mode,
            request
                .affected_table_change
                .map(Self::extract_replace_table_info),
        );

        let version = self.ddl_controller.run_command(command).await?;

        self.sink_manager
            .stop_sink_coordinator(SinkId::from(sink_id))
            .await;

        Ok(Response::new(DropSinkResponse {
            status: None,
            version,
        }))
    }

    async fn create_subscription(
        &self,
        request: Request<CreateSubscriptionRequest>,
    ) -> Result<Response<CreateSubscriptionResponse>, Status> {
        self.env.idle_manager().record_activity();

        let req = request.into_inner();

        let subscription = req.get_subscription()?.clone();
        let command = DdlCommand::CreateSubscription(subscription);

        let version = self.ddl_controller.run_command(command).await?;

        Ok(Response::new(CreateSubscriptionResponse {
            status: None,
            version,
        }))
    }

    async fn drop_subscription(
        &self,
        request: Request<DropSubscriptionRequest>,
    ) -> Result<Response<DropSubscriptionResponse>, Status> {
        let request = request.into_inner();
        let subscription_id = request.subscription_id;
        let drop_mode = DropMode::from_request_setting(request.cascade);

        let command = DdlCommand::DropSubscription(subscription_id, drop_mode);

        let version = self.ddl_controller.run_command(command).await?;

        Ok(Response::new(DropSubscriptionResponse {
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

        let stream_job = StreamingJob::MaterializedView(mview);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(
                stream_job,
                fragment_graph,
                create_type,
                None,
            ))
            .await?;

        Ok(Response::new(CreateMaterializedViewResponse {
            status: None,
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
                None,
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

        let stream_job = StreamingJob::Index(index, index_table);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(
                stream_job,
                fragment_graph,
                CreateType::Foreground,
                None,
            ))
            .await?;

        Ok(Response::new(CreateIndexResponse {
            status: None,
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
                None,
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
        let function = req.get_function()?.clone();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateFunction(function))
            .await?;

        Ok(Response::new(CreateFunctionResponse {
            status: None,
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
        let source = request.source;
        let mview = request.materialized_view.unwrap();
        let fragment_graph = request.fragment_graph.unwrap();

        let stream_job = StreamingJob::Table(source, mview, job_type);
        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(
                stream_job,
                fragment_graph,
                CreateType::Foreground,
                None,
            ))
            .await?;

        Ok(Response::new(CreateTableResponse {
            status: None,
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
                None,
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
        let view = req.get_view()?.clone();

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateView(view))
            .await?;

        Ok(Response::new(CreateViewResponse {
            status: None,
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
        let tables = match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_tables().await,
            MetadataManager::V2(mgr) => mgr.catalog_controller.list_all_state_tables().await?,
        };
        Ok(Response::new(RisectlListStateTablesResponse { tables }))
    }

    async fn replace_table_plan(
        &self,
        request: Request<ReplaceTablePlanRequest>,
    ) -> Result<Response<ReplaceTablePlanResponse>, Status> {
        let req = request.into_inner().get_plan().cloned()?;

        let version = self
            .ddl_controller
            .run_command(DdlCommand::ReplaceTable(Self::extract_replace_table_info(
                req,
            )))
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
        let table = match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let database = mgr
                    .catalog_manager
                    .list_databases()
                    .await
                    .into_iter()
                    .find(|db| db.name == req.database_name);
                if let Some(db) = database {
                    mgr.catalog_manager
                        .list_tables()
                        .await
                        .into_iter()
                        .find(|t| t.name == req.table_name && t.database_id == db.id)
                } else {
                    None
                }
            }
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller
                    .get_table_by_name(&req.database_name, &req.table_name)
                    .await?
            }
        };

        Ok(Response::new(GetTableResponse { table }))
    }

    async fn alter_name(
        &self,
        request: Request<AlterNameRequest>,
    ) -> Result<Response<AlterNameResponse>, Status> {
        let AlterNameRequest { object, new_name } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterName(object.unwrap(), new_name))
            .await?;
        Ok(Response::new(AlterNameResponse {
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

    async fn alter_owner(
        &self,
        request: Request<AlterOwnerRequest>,
    ) -> Result<Response<AlterOwnerResponse>, Status> {
        let AlterOwnerRequest { object, owner_id } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterObjectOwner(object.unwrap(), owner_id))
            .await?;
        Ok(Response::new(AlterOwnerResponse {
            status: None,
            version,
        }))
    }

    async fn alter_set_schema(
        &self,
        request: Request<AlterSetSchemaRequest>,
    ) -> Result<Response<AlterSetSchemaResponse>, Status> {
        let AlterSetSchemaRequest {
            object,
            new_schema_id,
        } = request.into_inner();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::AlterSetSchema(object.unwrap(), new_schema_id))
            .await?;
        Ok(Response::new(AlterSetSchemaResponse {
            status: None,
            version,
        }))
    }

    async fn get_ddl_progress(
        &self,
        _request: Request<GetDdlProgressRequest>,
    ) -> Result<Response<GetDdlProgressResponse>, Status> {
        Ok(Response::new(GetDdlProgressResponse {
            ddl_progress: self.ddl_controller.get_ddl_progress().await?,
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
                                "AWS client is not configured",
                            )));
                        }
                    }
                    PbPrivateLinkProvider::Unspecified => {
                        return Err(Status::invalid_argument("Privatelink provider unspecified"));
                    }
                };
                let connection = Connection {
                    id: 0,
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

                Ok(Response::new(CreateConnectionResponse { version }))
            }
        }
    }

    async fn list_connections(
        &self,
        _request: Request<ListConnectionsRequest>,
    ) -> Result<Response<ListConnectionsResponse>, Status> {
        let conns = match &self.metadata_manager {
            MetadataManager::V1(mgr) => mgr.catalog_manager.list_connections().await,
            MetadataManager::V2(mgr) => mgr.catalog_controller.list_connections().await?,
        };

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
        let ret = match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                mgr.catalog_manager
                    .get_tables(&request.into_inner().table_ids)
                    .await
            }
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller
                    .get_table_by_ids(
                        request
                            .into_inner()
                            .table_ids
                            .into_iter()
                            .map(|id| id as _)
                            .collect(),
                    )
                    .await?
            }
        };

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

    async fn alter_parallelism(
        &self,
        request: Request<AlterParallelismRequest>,
    ) -> Result<Response<AlterParallelismResponse>, Status> {
        let req = request.into_inner();

        let table_id = req.get_table_id();
        let parallelism = *req.get_parallelism()?;
        let deferred = req.get_deferred();

        self.ddl_controller
            .alter_parallelism(table_id, parallelism, deferred)
            .await?;

        Ok(Response::new(AlterParallelismResponse {}))
    }

    /// Auto schema change for cdc sources,
    /// called by the source parser when a schema change is detected.
    async fn auto_schema_change(
        &self,
        request: Request<AutoSchemaChangeRequest>,
    ) -> Result<Response<AutoSchemaChangeResponse>, Status> {
        let req = request.into_inner();

        // randomly select a frontend worker to get the replace table plan
        let workers = self
            .metadata_manager
            .list_worker_node(Some(WorkerType::Frontend), Some(State::Running))
            .await?;
        let worker = workers
            .choose(&mut thread_rng())
            .ok_or_else(|| MetaError::from(anyhow!("no frontend worker available")))?;

        let client = self
            .env
            .frontend_client_pool()
            .get(worker)
            .await
            .map_err(MetaError::from)?;

        let Some(schema_change) = req.schema_change else {
            return Err(Status::invalid_argument(
                "schema change message is required",
            ));
        };

        for table_change in schema_change.table_changes {
            // get the table catalog corresponding to the cdc table
            let tables: Vec<Table> = self
                .metadata_manager
                .get_table_catalog_by_cdc_table_id(&table_change.cdc_table_id)
                .await?;

            for table in tables {
                // Since we only support `ADD` and `DROP` column, we check whether the new columns and the original columns
                // is a subset of the other.
                let original_columns: HashSet<(String, DataType)> =
                    HashSet::from_iter(table.columns.iter().map(|col| {
                        let col = ColumnCatalog::from(col.clone());
                        let data_type = col.data_type().clone();
                        (col.column_desc.name, data_type)
                    }));
                let new_columns: HashSet<(String, DataType)> =
                    HashSet::from_iter(table_change.columns.iter().map(|col| {
                        let col = ColumnCatalog::from(col.clone());
                        let data_type = col.data_type().clone();
                        (col.column_desc.name, data_type)
                    }));

                if !(original_columns.is_subset(&new_columns)
                    || original_columns.is_superset(&new_columns))
                {
                    tracing::warn!(target: "auto_schema_change",
                                    table_id = table.id,
                                    cdc_table_id = table.cdc_table_id,
                                    upstraem_ddl = table_change.upstream_ddl,
                                    original_columns = ?original_columns,
                                    new_columns = ?new_columns,
                                    "New columns should be a subset or superset of the original columns, since only `ADD COLUMN` and `DROP COLUMN` is supported");
                    return Err(Status::invalid_argument(
                        "New columns should be a subset or superset of the original columns",
                    ));
                }
                // skip the schema change if there is no change to original columns
                if original_columns == new_columns {
                    tracing::warn!(target: "auto_schema_change",
                                   table_id = table.id,
                                   cdc_table_id = table.cdc_table_id,
                                   upstraem_ddl = table_change.upstream_ddl,
                                    original_columns = ?original_columns,
                                    new_columns = ?new_columns,
                                   "No change to columns, skipping the schema change");
                    continue;
                }

                let latency_timer = self
                    .meta_metrics
                    .auto_schema_change_latency
                    .with_guarded_label_values(&[&table.id.to_string(), &table.name])
                    .start_timer();
                // send a request to the frontend to get the ReplaceTablePlan
                // will retry with exponential backoff if the request fails
                let resp = client
                    .get_table_replace_plan(GetTableReplacePlanRequest {
                        database_id: table.database_id,
                        owner: table.owner,
                        table_name: table.name.clone(),
                        table_change: Some(table_change.clone()),
                    })
                    .await;

                match resp {
                    Ok(resp) => {
                        let resp = resp.into_inner();
                        if let Some(plan) = resp.replace_plan {
                            plan.table.as_ref().inspect(|t| {
                                tracing::info!(
                                    target: "auto_schema_change",
                                    table_id = t.id,
                                    cdc_table_id = t.cdc_table_id,
                                    upstraem_ddl = table_change.upstream_ddl,
                                    "Start the replace config change")
                            });
                            // start the schema change procedure
                            let replace_res = self
                                .ddl_controller
                                .run_command(DdlCommand::ReplaceTable(
                                    Self::extract_replace_table_info(plan),
                                ))
                                .await;

                            match replace_res {
                                Ok(_) => {
                                    tracing::info!(
                                        target: "auto_schema_change",
                                        table_id = table.id,
                                        cdc_table_id = table.cdc_table_id,
                                        "Table replaced success");

                                    self.meta_metrics
                                        .auto_schema_change_success_cnt
                                        .with_guarded_label_values(&[
                                            &table.id.to_string(),
                                            &table.name,
                                        ])
                                        .inc();
                                    latency_timer.observe_duration();
                                }
                                Err(e) => {
                                    tracing::error!(
                                        target: "auto_schema_change",
                                        error = %e.as_report(),
                                        table_id = table.id,
                                        cdc_table_id = table.cdc_table_id,
                                        upstraem_ddl = table_change.upstream_ddl,
                                        "failed to replace the table",
                                    );
                                    add_auto_schema_change_fail_event_log(
                                        &self.meta_metrics,
                                        table.id,
                                        table.name.clone(),
                                        table_change.cdc_table_id.clone(),
                                        table_change.upstream_ddl.clone(),
                                        &self.env.event_log_manager_ref(),
                                    );
                                }
                            };
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            target: "auto_schema_change",
                            error = %e.as_report(),
                            table_id = table.id,
                            cdc_table_id = table.cdc_table_id,
                            "failed to get replace table plan",
                        );
                        add_auto_schema_change_fail_event_log(
                            &self.meta_metrics,
                            table.id,
                            table.name.clone(),
                            table_change.cdc_table_id.clone(),
                            table_change.upstream_ddl.clone(),
                            &self.env.event_log_manager_ref(),
                        );
                    }
                };
            }
        }

        Ok(Response::new(AutoSchemaChangeResponse {}))
    }
}

impl DdlServiceImpl {
    async fn validate_connection(&self, connection_id: ConnectionId) -> MetaResult<()> {
        let connection = match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                mgr.catalog_manager
                    .get_connection_by_id(connection_id)
                    .await?
            }
            MetadataManager::V2(mgr) => {
                mgr.catalog_controller
                    .get_connection_by_id(connection_id as _)
                    .await?
            }
        };
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

fn add_auto_schema_change_fail_event_log(
    meta_metrics: &Arc<MetaMetrics>,
    table_id: u32,
    table_name: String,
    cdc_table_id: String,
    upstream_ddl: String,
    event_log_manager: &EventLogManagerRef,
) {
    meta_metrics
        .auto_schema_change_failure_cnt
        .with_guarded_label_values(&[&table_id.to_string(), &table_name])
        .inc();
    let event = event_log::EventAutoSchemaChangeFail {
        table_id,
        table_name,
        cdc_table_id,
        upstream_ddl,
    };
    event_log_manager.add_event_logs(vec![event_log::Event::AutoSchemaChangeFail(event)]);
}
