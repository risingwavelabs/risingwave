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
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_connector::common::AwsPrivateLinks;
use risingwave_connector::source::kafka::{KAFKA_PROPS_BROKER_KEY, KAFKA_PROPS_BROKER_KEY_ALIAS};
use risingwave_connector::source::KAFKA_CONNECTOR;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{connection, Connection};
use risingwave_pb::ddl_service::ddl_service_server::DdlService;
use risingwave_pb::ddl_service::drop_table_request::SourceId as ProstSourceId;
use risingwave_pb::ddl_service::*;
use risingwave_pb::stream_plan::stream_node::NodeBody;
use tonic::{Request, Response, Status};

use crate::barrier::BarrierManagerRef;
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, IdCategory, IdCategoryType,
    MetaSrvEnv, StreamingJob,
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

        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        source.id = id;

        // resolve private links before starting the DDL procedure
        self.resolve_private_link_info(&mut source.properties)
            .await?;

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
        let sink = req.get_sink()?.clone();
        let fragment_graph = req.get_fragment_graph()?.clone();

        let mut stream_job = StreamingJob::Sink(sink);
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreatingStreamingJob(stream_job, fragment_graph))
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
            .run_command(DdlCommand::CreatingStreamingJob(stream_job, fragment_graph))
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
            .run_command(DdlCommand::CreatingStreamingJob(stream_job, fragment_graph))
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
        let id = self.gen_unique_id::<{ IdCategory::Table }>().await?;
        stream_job.set_id(id);

        let version = self
            .ddl_controller
            .run_command(DdlCommand::CreatingStreamingJob(stream_job, fragment_graph))
            .await?;

        Ok(Response::new(CreateTableResponse {
            status: None,
            table_id: id,
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
                source_id.map(|ProstSourceId::Id(id)| id),
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
    async fn gen_unique_id<const C: IdCategoryType>(&self) -> MetaResult<u32> {
        let id = self.env.id_gen_manager().generate::<C>().await? as u32;
        Ok(id)
    }

    async fn resolve_private_link_info(
        &self,
        properties: &mut HashMap<String, String>,
    ) -> MetaResult<()> {
        let mut dns_entries = vec![];
        const UPSTREAM_SOURCE_PRIVATE_LINK_KEY: &str = "private.links";
        if let Some(prop) = properties.get(UPSTREAM_SOURCE_PRIVATE_LINK_KEY) {
            let links: AwsPrivateLinks = serde_json::from_str(prop).map_err(|e| anyhow!(e))?;

            // if private link is required, get connection info from catalog
            for link in &links.infos {
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

        // store the rewrite rules in properties
        if is_kafka_source(properties) && !dns_entries.is_empty() {
            let broker_key = kafka_props_broker_key(&properties);
            let servers = properties
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
            properties.insert(PRIVATE_LINK_DNS_KEY.to_string(), dns_entries.join(","));
        }
        Ok(())
    }
}
