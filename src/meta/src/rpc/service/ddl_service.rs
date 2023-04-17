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
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_connector::common::AwsPrivateLinkItem;
use risingwave_connector::source::kafka::{KAFKA_PROPS_BROKER_KEY, KAFKA_PROPS_BROKER_KEY_ALIAS};
use risingwave_connector::source::KAFKA_CONNECTOR;
use risingwave_pb::catalog::connection::private_link_service::{
    PbPrivateLinkProvider, PrivateLinkProvider,
};
use risingwave_pb::catalog::connection::PbPrivateLinkService;
use risingwave_pb::catalog::source::OptionalAssociatedTableId;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{connection, Connection};
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::drop_table_request::PbSourceId;
use risingwave_pb::ddl_service::*;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, ConnectionId, FragmentManagerRef, IdCategory,
    IdCategoryType, MetaSrvEnv, StreamingJob,
};
use crate::rpc::cloud_provider::AwsEc2Client;
use crate::rpc::ddl_controller::{DdlCommand, DdlController, StreamingJobId};
use crate::storage::MetaStore;
use crate::stream::{visit_fragment, GlobalStreamManagerRef, SourceManagerRef};
use crate::{MetaError, MetaResult};

#[derive(Clone)]
pub struct DdlServiceImpl<S: MetaStore> {
    env: MetaSrvEnv<S>,

    catalog_manager: CatalogManagerRef<S>,
    ddl_controller: DdlController<S>,
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
        let ddl_controller = DdlController::new(
            env.clone(),
            catalog_manager.clone(),
            stream_manager,
            source_manager,
            cluster_manager,
            fragment_manager,
            barrier_manager,
        );
        Self {
            env,
            catalog_manager,
            ddl_controller,
            aws_client,
        }
    }
}

#[inline(always)]
fn is_kafka_connector(with_properties: &HashMap<String, String>) -> bool {
    const UPSTREAM_SOURCE_KEY: &str = "connector";
    with_properties
        .get(UPSTREAM_SOURCE_KEY)
        .unwrap_or(&"".to_string())
        .to_lowercase()
        .eq_ignore_ascii_case(KAFKA_CONNECTOR)
}

#[inline(always)]
fn kafka_props_broker_key(with_properties: &HashMap<String, String>) -> &str {
    if with_properties.contains_key(KAFKA_PROPS_BROKER_KEY) {
        KAFKA_PROPS_BROKER_KEY
    } else {
        KAFKA_PROPS_BROKER_KEY_ALIAS
    }
}

#[inline(always)]
fn get_property_required(
    with_properties: &HashMap<String, String>,
    property: &str,
) -> MetaResult<String> {
    with_properties
        .get(property)
        .map(|s| s.to_lowercase())
        .ok_or(MetaError::from(anyhow!(
            "Required property \"{property}\" is not provided"
        )))
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
        let mut source = request.into_inner().get_source()?.clone();

        // resolve private links before starting the DDL procedure
        if let Some(connection_id) = source.connection_id {
            self.resolve_private_link_info(connection_id, &mut source.properties)
                .await?;
        }

        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        source.id = id;

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateSource(source))
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
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropSource(source_id))
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
        let mut sink = req.get_sink()?.clone();
        let fragment_graph = req.get_fragment_graph()?.clone();

        // resolve private links before starting the DDL procedure
        if let Some(connection_id) = sink.connection_id {
            self.resolve_private_link_info(connection_id, &mut sink.properties)
                .await?;
        }

        let mut stream_job = StreamingJob::Sink(sink);
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(stream_job, fragment_graph))
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
        let sink_id = request.into_inner().sink_id;

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob(StreamingJobId::Sink(sink_id)))
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
        self.env.idle_manager().record_activity();

        let req = request.into_inner();
        let mview = req.get_materialized_view()?.clone();
        let fragment_graph = req.get_fragment_graph()?.clone();

        let mut stream_job = StreamingJob::MaterializedView(mview);
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(stream_job, fragment_graph))
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

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob(
                StreamingJobId::MaterializedView(table_id),
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
            .run_command(DdlCommand::CreateStreamingJob(stream_job, fragment_graph))
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

        let index_id = request.into_inner().index_id;
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob(StreamingJobId::Index(
                index_id,
            )))
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
        let mut source = request.source;
        let mut mview = request.materialized_view.unwrap();
        let mut fragment_graph = request.fragment_graph.unwrap();
        let table_id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
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

            // Fill in the correct table id for source.
            source.optional_associated_table_id =
                Some(OptionalAssociatedTableId::AssociatedTableId(table_id));

            // Fill in the correct source id for mview.
            mview.optional_associated_source_id =
                Some(OptionalAssociatedSourceId::AssociatedSourceId(source_id));
        }

        let mut stream_job = StreamingJob::Table(source, mview);

        stream_job.set_id(table_id);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreateStreamingJob(stream_job, fragment_graph))
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

        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropStreamingJob(StreamingJobId::Table(
                source_id.map(|PbSourceId::Id(id)| id),
                table_id,
            )))
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
        let req = request.into_inner();
        let view_id = req.get_view_id();
        let version = self
            .ddl_controller
            .run_command(DdlCommand::DropView(view_id))
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

        let stream_job = StreamingJob::Table(None, req.table.unwrap());
        let fragment_graph = req.fragment_graph.unwrap();
        let table_col_index_mapping =
            ColIndexMapping::from_protobuf(&req.table_col_index_mapping.unwrap());

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
                        if self.aws_client.is_none() {
                            return Err(Status::from(MetaError::unavailable(
                                "AWS client is not configured".into(),
                            )));
                        }
                        let cli = self.aws_client.as_ref().unwrap();
                        cli.create_aws_private_link(&link.service_name).await?
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
}

impl<S> DdlServiceImpl<S>
where
    S: MetaStore,
{
    async fn gen_unique_id<const C: IdCategoryType>(&self) -> MetaResult<u32> {
        let id = self.env.id_gen_manager().generate::<C>().await? as u32;
        Ok(id)
    }

    async fn resolve_private_link_info(
        &self,
        connection_id: ConnectionId,
        properties: &mut HashMap<String, String>,
    ) -> MetaResult<()> {
        let mut broker_rewrite_map = HashMap::new();
        const PRIVATE_LINK_TARGETS_KEY: &str = "privatelink.targets";
        let conn = self
            .catalog_manager
            .get_connection_by_id(connection_id)
            .await?;
        if let Some(connection::Info::PrivateLinkService(svc)) = &conn.info {
            if !is_kafka_connector(properties) {
                return Err(MetaError::from(anyhow!(
                    "Private link is only supported for Kafka connector",
                )));
            }
            // skip all checks for mock connection
            if svc.get_provider()? == PrivateLinkProvider::Mock {
                return Ok(());
            }
            let link_target_value = get_property_required(properties, PRIVATE_LINK_TARGETS_KEY)?;
            let servers = get_property_required(properties, kafka_props_broker_key(properties))?;
            let broker_addrs = servers.split(',').collect_vec();
            let link_targets: Vec<AwsPrivateLinkItem> =
                serde_json::from_str(link_target_value.as_str()).map_err(|e| anyhow!(e))?;
            if broker_addrs.len() != link_targets.len() {
                return Err(MetaError::from(anyhow!(
                    "The number of broker addrs {} does not match the number of private link targets {}",
                    broker_addrs.len(),
                    link_targets.len()
                )));
            }
            // check whether private link is ready
            let cli = self.aws_client.as_ref().unwrap();
            if !cli.is_vpc_endpoint_ready(&svc.endpoint_id).await? {
                return Err(MetaError::from(anyhow!(
                    "Private link endpoint {} is not ready",
                    svc.endpoint_id
                )));
            }

            for (link, broker) in link_targets.iter().zip_eq_fast(broker_addrs.into_iter()) {
                if let Some(connection::Info::PrivateLinkService(svc)) = &conn.info {
                    if svc.dns_entries.is_empty() {
                        return Err(MetaError::from(anyhow!(
                            "No available private link endpoints for Kafka broker {}",
                            broker
                        )));
                    }
                    // rewrite the broker address to the dns name w/o az
                    // requires the NLB has enabled the cross-zone load balancing
                    broker_rewrite_map.insert(
                        broker.to_string(),
                        format!("{}:{}", &svc.endpoint_dns_name, link.port),
                    );
                }
            }

            // save private link dns names into source properties, which
            // will be extracted into KafkaProperties
            let json = serde_json::to_string(&broker_rewrite_map).map_err(|e| anyhow!(e))?;
            const BROKER_REWRITE_MAP_KEY: &str = "broker.rewrite.endpoints";
            properties.insert(BROKER_REWRITE_MAP_KEY.to_string(), json);
        }
        Ok(())
    }
}
