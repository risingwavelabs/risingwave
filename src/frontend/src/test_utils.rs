// Copyright 2025 RisingWave Labs
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
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use futures_async_stream::for_await;
use parking_lot::RwLock;
use pgwire::net::{Address, AddressRef};
use pgwire::pg_response::StatementType;
use pgwire::pg_server::{BoxedError, SessionId, SessionManager, UserAuthenticator};
use pgwire::types::Row;
use risingwave_common::catalog::{
    FunctionId, IndexId, ObjectId, TableId, DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME,
    DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID, NON_RESERVED_USER_ID, PG_CATALOG_SCHEMA_NAME,
    RW_CATALOG_SCHEMA_NAME,
};
use risingwave_common::hash::{VirtualNode, VnodeCount, VnodeCountCompat};
use risingwave_common::session_config::SessionConfig;
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::util::cluster_limit::ClusterLimit;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_common::util::worker_util::DEFAULT_RESOURCE_GROUP;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_hummock_sdk::{HummockVersionId, INVALID_VERSION_ID};
use risingwave_pb::backup_service::MetaSnapshotMetadata;
use risingwave_pb::catalog::table::OptionalAssociatedSourceId;
use risingwave_pb::catalog::{
    PbComment, PbDatabase, PbFunction, PbIndex, PbSchema, PbSink, PbSource, PbStreamJobStatus,
    PbSubscription, PbTable, PbView, Table,
};
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::alter_owner_request::Object;
use risingwave_pb::ddl_service::{
    alter_name_request, alter_set_schema_request, alter_swap_rename_request,
    create_connection_request, DdlProgress, PbTableJobType, ReplaceJobPlan, TableJobType,
};
use risingwave_pb::hummock::write_limits::WriteLimit;
use risingwave_pb::hummock::{
    BranchedObject, CompactTaskAssignment, CompactTaskProgress, CompactionGroupInfo,
};
use risingwave_pb::meta::cancel_creating_jobs_request::PbJobs;
use risingwave_pb::meta::list_actor_splits_response::ActorSplit;
use risingwave_pb::meta::list_actor_states_response::ActorState;
use risingwave_pb::meta::list_fragment_distribution_response::FragmentDistribution;
use risingwave_pb::meta::list_object_dependencies_response::PbObjectDependencies;
use risingwave_pb::meta::list_rate_limits_response::RateLimitInfo;
use risingwave_pb::meta::list_streaming_job_states_response::StreamingJobState;
use risingwave_pb::meta::list_table_fragments_response::TableFragmentInfo;
use risingwave_pb::meta::{
    EventLog, PbTableParallelism, PbThrottleTarget, RecoveryStatus, SystemParams,
};
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::{GrantPrivilege, UserInfo};
use risingwave_rpc_client::error::Result as RpcResult;
use tempfile::{Builder, NamedTempFile};

use crate::catalog::catalog_service::CatalogWriter;
use crate::catalog::root_catalog::Catalog;
use crate::catalog::{ConnectionId, DatabaseId, SchemaId, SecretId};
use crate::error::{ErrorCode, Result};
use crate::handler::RwPgResponse;
use crate::meta_client::FrontendMetaClient;
use crate::scheduler::HummockSnapshotManagerRef;
use crate::session::{AuthContext, FrontendEnv, SessionImpl};
use crate::user::user_manager::UserInfoManager;
use crate::user::user_service::UserInfoWriter;
use crate::user::UserId;
use crate::FrontendOpts;

/// An embedded frontend without starting meta and without starting frontend as a tcp server.
pub struct LocalFrontend {
    pub opts: FrontendOpts,
    env: FrontendEnv,
}

impl SessionManager for LocalFrontend {
    type Session = SessionImpl;

    fn create_dummy_session(
        &self,
        _database_id: u32,
        _user_name: u32,
    ) -> std::result::Result<Arc<Self::Session>, BoxedError> {
        unreachable!()
    }

    fn connect(
        &self,
        _database: &str,
        _user_name: &str,
        _peer_addr: AddressRef,
    ) -> std::result::Result<Arc<Self::Session>, BoxedError> {
        Ok(self.session_ref())
    }

    fn cancel_queries_in_session(&self, _session_id: SessionId) {
        unreachable!()
    }

    fn cancel_creating_jobs_in_session(&self, _session_id: SessionId) {
        unreachable!()
    }

    fn end_session(&self, _session: &Self::Session) {
        unreachable!()
    }
}

impl LocalFrontend {
    #[expect(clippy::unused_async)]
    pub async fn new(opts: FrontendOpts) -> Self {
        let env = FrontendEnv::mock();
        Self { opts, env }
    }

    pub async fn run_sql(
        &self,
        sql: impl Into<String>,
    ) -> std::result::Result<RwPgResponse, Box<dyn std::error::Error + Send + Sync>> {
        let sql: Arc<str> = Arc::from(sql.into());
        self.session_ref().run_statement(sql, vec![]).await
    }

    pub async fn run_sql_with_session(
        &self,
        session_ref: Arc<SessionImpl>,
        sql: impl Into<String>,
    ) -> std::result::Result<RwPgResponse, Box<dyn std::error::Error + Send + Sync>> {
        let sql: Arc<str> = Arc::from(sql.into());
        session_ref.run_statement(sql, vec![]).await
    }

    pub async fn run_user_sql(
        &self,
        sql: impl Into<String>,
        database: String,
        user_name: String,
        user_id: UserId,
    ) -> std::result::Result<RwPgResponse, Box<dyn std::error::Error + Send + Sync>> {
        let sql: Arc<str> = Arc::from(sql.into());
        self.session_user_ref(database, user_name, user_id)
            .run_statement(sql, vec![])
            .await
    }

    pub async fn query_formatted_result(&self, sql: impl Into<String>) -> Vec<String> {
        let mut rsp = self.run_sql(sql).await.unwrap();
        let mut res = vec![];
        #[for_await]
        for row_set in rsp.values_stream() {
            for row in row_set.unwrap() {
                res.push(format!("{:?}", row));
            }
        }
        res
    }

    pub async fn get_explain_output(&self, sql: impl Into<String>) -> String {
        let mut rsp = self.run_sql(sql).await.unwrap();
        assert_eq!(rsp.stmt_type(), StatementType::EXPLAIN);
        let mut res = String::new();
        #[for_await]
        for row_set in rsp.values_stream() {
            for row in row_set.unwrap() {
                let row: Row = row;
                let row = row.values()[0].as_ref().unwrap();
                res += std::str::from_utf8(row).unwrap();
                res += "\n";
            }
        }
        res
    }

    /// Creates a new session
    pub fn session_ref(&self) -> Arc<SessionImpl> {
        self.session_user_ref(
            DEFAULT_DATABASE_NAME.to_owned(),
            DEFAULT_SUPER_USER.to_owned(),
            DEFAULT_SUPER_USER_ID,
        )
    }

    pub fn session_user_ref(
        &self,
        database: String,
        user_name: String,
        user_id: UserId,
    ) -> Arc<SessionImpl> {
        Arc::new(SessionImpl::new(
            self.env.clone(),
            AuthContext::new(database, user_name, user_id),
            UserAuthenticator::None,
            // Local Frontend use a non-sense id.
            (0, 0),
            Address::Tcp(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                6666,
            ))
            .into(),
            Default::default(),
        ))
    }
}

pub async fn get_explain_output(mut rsp: RwPgResponse) -> String {
    if rsp.stmt_type() != StatementType::EXPLAIN {
        panic!("RESPONSE INVALID: {rsp:?}");
    }
    let mut res = String::new();
    #[for_await]
    for row_set in rsp.values_stream() {
        for row in row_set.unwrap() {
            let row: Row = row;
            let row = row.values()[0].as_ref().unwrap();
            res += std::str::from_utf8(row).unwrap();
            res += "\n";
        }
    }
    res
}

pub struct MockCatalogWriter {
    catalog: Arc<RwLock<Catalog>>,
    id: AtomicU32,
    table_id_to_schema_id: RwLock<HashMap<u32, SchemaId>>,
    schema_id_to_database_id: RwLock<HashMap<u32, DatabaseId>>,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
}

#[async_trait::async_trait]
impl CatalogWriter for MockCatalogWriter {
    async fn create_database(
        &self,
        db_name: &str,
        owner: UserId,
        resource_group: &str,
    ) -> Result<()> {
        let database_id = self.gen_id();
        self.catalog.write().create_database(&PbDatabase {
            name: db_name.to_owned(),
            id: database_id,
            owner,
            resource_group: resource_group.to_owned(),
        });
        self.create_schema(database_id, DEFAULT_SCHEMA_NAME, owner)
            .await?;
        self.create_schema(database_id, PG_CATALOG_SCHEMA_NAME, owner)
            .await?;
        self.create_schema(database_id, RW_CATALOG_SCHEMA_NAME, owner)
            .await?;
        Ok(())
    }

    async fn create_schema(
        &self,
        db_id: DatabaseId,
        schema_name: &str,
        owner: UserId,
    ) -> Result<()> {
        let id = self.gen_id();
        self.catalog.write().create_schema(&PbSchema {
            id,
            name: schema_name.to_owned(),
            database_id: db_id,
            owner,
        });
        self.add_schema_id(id, db_id);
        Ok(())
    }

    async fn create_materialized_view(
        &self,
        mut table: PbTable,
        _graph: StreamFragmentGraph,
        _dependencies: HashSet<ObjectId>,
        _specific_resource_group: Option<String>,
    ) -> Result<()> {
        table.id = self.gen_id();
        table.stream_job_status = PbStreamJobStatus::Created as _;
        table.maybe_vnode_count = VnodeCount::for_test().to_protobuf();
        self.catalog.write().create_table(&table);
        self.add_table_or_source_id(table.id, table.schema_id, table.database_id);
        self.hummock_snapshot_manager
            .add_table_for_test(TableId::new(table.id));
        Ok(())
    }

    async fn create_view(&self, mut view: PbView) -> Result<()> {
        view.id = self.gen_id();
        self.catalog.write().create_view(&view);
        self.add_table_or_source_id(view.id, view.schema_id, view.database_id);
        Ok(())
    }

    async fn create_table(
        &self,
        source: Option<PbSource>,
        mut table: PbTable,
        graph: StreamFragmentGraph,
        _job_type: PbTableJobType,
    ) -> Result<()> {
        if let Some(source) = source {
            let source_id = self.create_source_inner(source)?;
            table.optional_associated_source_id =
                Some(OptionalAssociatedSourceId::AssociatedSourceId(source_id));
        }
        self.create_materialized_view(table, graph, HashSet::new(), None)
            .await?;
        Ok(())
    }

    async fn replace_table(
        &self,
        _source: Option<PbSource>,
        mut table: PbTable,
        _graph: StreamFragmentGraph,
        _mapping: ColIndexMapping,
        _job_type: TableJobType,
    ) -> Result<()> {
        table.stream_job_status = PbStreamJobStatus::Created as _;
        assert_eq!(table.vnode_count(), VirtualNode::COUNT_FOR_TEST);
        self.catalog.write().update_table(&table);
        Ok(())
    }

    async fn replace_source(
        &self,
        source: PbSource,
        _graph: StreamFragmentGraph,
        _mapping: ColIndexMapping,
    ) -> Result<()> {
        self.catalog.write().update_source(&source);
        Ok(())
    }

    async fn create_source(
        &self,
        source: PbSource,
        _graph: Option<StreamFragmentGraph>,
    ) -> Result<()> {
        self.create_source_inner(source).map(|_| ())
    }

    async fn create_sink(
        &self,
        sink: PbSink,
        graph: StreamFragmentGraph,
        _affected_table_change: Option<ReplaceJobPlan>,
        _dependencies: HashSet<ObjectId>,
    ) -> Result<()> {
        self.create_sink_inner(sink, graph)
    }

    async fn create_subscription(&self, subscription: PbSubscription) -> Result<()> {
        self.create_subscription_inner(subscription)
    }

    async fn create_index(
        &self,
        mut index: PbIndex,
        mut index_table: PbTable,
        _graph: StreamFragmentGraph,
    ) -> Result<()> {
        index_table.id = self.gen_id();
        index_table.stream_job_status = PbStreamJobStatus::Created as _;
        index_table.maybe_vnode_count = VnodeCount::for_test().to_protobuf();
        self.catalog.write().create_table(&index_table);
        self.add_table_or_index_id(
            index_table.id,
            index_table.schema_id,
            index_table.database_id,
        );

        index.id = index_table.id;
        index.index_table_id = index_table.id;
        self.catalog.write().create_index(&index);
        Ok(())
    }

    async fn create_function(&self, _function: PbFunction) -> Result<()> {
        unreachable!()
    }

    async fn create_connection(
        &self,
        _connection_name: String,
        _database_id: u32,
        _schema_id: u32,
        _owner_id: u32,
        _connection: create_connection_request::Payload,
    ) -> Result<()> {
        unreachable!()
    }

    async fn create_secret(
        &self,
        _secret_name: String,
        _database_id: u32,
        _schema_id: u32,
        _owner_id: u32,
        _payload: Vec<u8>,
    ) -> Result<()> {
        unreachable!()
    }

    async fn comment_on(&self, _comment: PbComment) -> Result<()> {
        unreachable!()
    }

    async fn drop_table(
        &self,
        source_id: Option<u32>,
        table_id: TableId,
        cascade: bool,
    ) -> Result<()> {
        if cascade {
            return Err(ErrorCode::NotSupported(
                "drop cascade in MockCatalogWriter is unsupported".to_owned(),
                "use drop instead".to_owned(),
            )
            .into());
        }
        if let Some(source_id) = source_id {
            self.drop_table_or_source_id(source_id);
        }
        let (database_id, schema_id) = self.drop_table_or_source_id(table_id.table_id);
        let indexes =
            self.catalog
                .read()
                .get_all_indexes_related_to_object(database_id, schema_id, table_id);
        for index in indexes {
            self.drop_index(index.id, cascade).await?;
        }
        self.catalog
            .write()
            .drop_table(database_id, schema_id, table_id);
        if let Some(source_id) = source_id {
            self.catalog
                .write()
                .drop_source(database_id, schema_id, source_id);
        }
        Ok(())
    }

    async fn drop_view(&self, _view_id: u32, _cascade: bool) -> Result<()> {
        unreachable!()
    }

    async fn drop_materialized_view(&self, table_id: TableId, cascade: bool) -> Result<()> {
        if cascade {
            return Err(ErrorCode::NotSupported(
                "drop cascade in MockCatalogWriter is unsupported".to_owned(),
                "use drop instead".to_owned(),
            )
            .into());
        }
        let (database_id, schema_id) = self.drop_table_or_source_id(table_id.table_id);
        let indexes =
            self.catalog
                .read()
                .get_all_indexes_related_to_object(database_id, schema_id, table_id);
        for index in indexes {
            self.drop_index(index.id, cascade).await?;
        }
        self.catalog
            .write()
            .drop_table(database_id, schema_id, table_id);
        Ok(())
    }

    async fn drop_source(&self, source_id: u32, cascade: bool) -> Result<()> {
        if cascade {
            return Err(ErrorCode::NotSupported(
                "drop cascade in MockCatalogWriter is unsupported".to_owned(),
                "use drop instead".to_owned(),
            )
            .into());
        }
        let (database_id, schema_id) = self.drop_table_or_source_id(source_id);
        self.catalog
            .write()
            .drop_source(database_id, schema_id, source_id);
        Ok(())
    }

    async fn drop_sink(
        &self,
        sink_id: u32,
        cascade: bool,
        _target_table_change: Option<ReplaceJobPlan>,
    ) -> Result<()> {
        if cascade {
            return Err(ErrorCode::NotSupported(
                "drop cascade in MockCatalogWriter is unsupported".to_owned(),
                "use drop instead".to_owned(),
            )
            .into());
        }
        let (database_id, schema_id) = self.drop_table_or_sink_id(sink_id);
        self.catalog
            .write()
            .drop_sink(database_id, schema_id, sink_id);
        Ok(())
    }

    async fn drop_subscription(&self, subscription_id: u32, cascade: bool) -> Result<()> {
        if cascade {
            return Err(ErrorCode::NotSupported(
                "drop cascade in MockCatalogWriter is unsupported".to_owned(),
                "use drop instead".to_owned(),
            )
            .into());
        }
        let (database_id, schema_id) = self.drop_table_or_subscription_id(subscription_id);
        self.catalog
            .write()
            .drop_subscription(database_id, schema_id, subscription_id);
        Ok(())
    }

    async fn drop_index(&self, index_id: IndexId, cascade: bool) -> Result<()> {
        if cascade {
            return Err(ErrorCode::NotSupported(
                "drop cascade in MockCatalogWriter is unsupported".to_owned(),
                "use drop instead".to_owned(),
            )
            .into());
        }
        let &schema_id = self
            .table_id_to_schema_id
            .read()
            .get(&index_id.index_id)
            .unwrap();
        let database_id = self.get_database_id_by_schema(schema_id);

        let index = {
            let catalog_reader = self.catalog.read();
            let schema_catalog = catalog_reader
                .get_schema_by_id(&database_id, &schema_id)
                .unwrap();
            schema_catalog.get_index_by_id(&index_id).unwrap().clone()
        };

        let index_table_id = index.index_table.id;
        let (database_id, schema_id) = self.drop_table_or_index_id(index_id.index_id);
        self.catalog
            .write()
            .drop_index(database_id, schema_id, index_id);
        self.catalog
            .write()
            .drop_table(database_id, schema_id, index_table_id);
        Ok(())
    }

    async fn drop_function(&self, _function_id: FunctionId) -> Result<()> {
        unreachable!()
    }

    async fn drop_connection(&self, _connection_id: ConnectionId) -> Result<()> {
        unreachable!()
    }

    async fn drop_secret(&self, _secret_id: SecretId) -> Result<()> {
        unreachable!()
    }

    async fn drop_database(&self, database_id: u32) -> Result<()> {
        self.catalog.write().drop_database(database_id);
        Ok(())
    }

    async fn drop_schema(&self, schema_id: u32, _cascade: bool) -> Result<()> {
        let database_id = self.drop_schema_id(schema_id);
        self.catalog.write().drop_schema(database_id, schema_id);
        Ok(())
    }

    async fn alter_name(
        &self,
        object_id: alter_name_request::Object,
        object_name: &str,
    ) -> Result<()> {
        match object_id {
            alter_name_request::Object::TableId(table_id) => {
                self.catalog
                    .write()
                    .alter_table_name_by_id(&table_id.into(), object_name);
                Ok(())
            }
            _ => {
                unimplemented!()
            }
        }
    }

    async fn alter_source(&self, source: PbSource) -> Result<()> {
        self.catalog.write().update_source(&source);
        Ok(())
    }

    async fn alter_owner(&self, object: Object, owner_id: u32) -> Result<()> {
        for database in self.catalog.read().iter_databases() {
            for schema in database.iter_schemas() {
                match object {
                    Object::TableId(table_id) => {
                        if let Some(table) =
                            schema.get_created_table_by_id(&TableId::from(table_id))
                        {
                            let mut pb_table = table.to_prost();
                            pb_table.owner = owner_id;
                            self.catalog.write().update_table(&pb_table);
                            return Ok(());
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        Err(ErrorCode::ItemNotFound(format!("object not found: {:?}", object)).into())
    }

    async fn alter_set_schema(
        &self,
        object: alter_set_schema_request::Object,
        new_schema_id: u32,
    ) -> Result<()> {
        match object {
            alter_set_schema_request::Object::TableId(table_id) => {
                let mut pb_table = {
                    let reader = self.catalog.read();
                    let table = reader.get_any_table_by_id(&table_id.into())?.to_owned();
                    table.to_prost()
                };
                pb_table.schema_id = new_schema_id;
                self.catalog.write().update_table(&pb_table);
                self.table_id_to_schema_id
                    .write()
                    .insert(table_id, new_schema_id);
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    async fn alter_parallelism(
        &self,
        _table_id: u32,
        _parallelism: PbTableParallelism,
        _deferred: bool,
    ) -> Result<()> {
        todo!()
    }

    async fn alter_swap_rename(&self, _object: alter_swap_rename_request::Object) -> Result<()> {
        todo!()
    }

    async fn alter_secret(
        &self,
        _secret_id: u32,
        _secret_name: String,
        _database_id: u32,
        _schema_id: u32,
        _owner_id: u32,
        _payload: Vec<u8>,
    ) -> Result<()> {
        unreachable!()
    }

    async fn alter_resource_group(
        &self,
        _table_id: u32,
        _resource_group: Option<String>,
        _deferred: bool,
    ) -> Result<()> {
        todo!()
    }
}

impl MockCatalogWriter {
    pub fn new(
        catalog: Arc<RwLock<Catalog>>,
        hummock_snapshot_manager: HummockSnapshotManagerRef,
    ) -> Self {
        catalog.write().create_database(&PbDatabase {
            id: 0,
            name: DEFAULT_DATABASE_NAME.to_owned(),
            owner: DEFAULT_SUPER_USER_ID,
            resource_group: DEFAULT_RESOURCE_GROUP.to_owned(),
        });
        catalog.write().create_schema(&PbSchema {
            id: 1,
            name: DEFAULT_SCHEMA_NAME.to_owned(),
            database_id: 0,
            owner: DEFAULT_SUPER_USER_ID,
        });
        catalog.write().create_schema(&PbSchema {
            id: 2,
            name: PG_CATALOG_SCHEMA_NAME.to_owned(),
            database_id: 0,
            owner: DEFAULT_SUPER_USER_ID,
        });
        catalog.write().create_schema(&PbSchema {
            id: 3,
            name: RW_CATALOG_SCHEMA_NAME.to_owned(),
            database_id: 0,
            owner: DEFAULT_SUPER_USER_ID,
        });
        let mut map: HashMap<u32, DatabaseId> = HashMap::new();
        map.insert(1_u32, 0_u32);
        map.insert(2_u32, 0_u32);
        map.insert(3_u32, 0_u32);
        Self {
            catalog,
            id: AtomicU32::new(3),
            table_id_to_schema_id: Default::default(),
            schema_id_to_database_id: RwLock::new(map),
            hummock_snapshot_manager,
        }
    }

    fn gen_id(&self) -> u32 {
        // Since the 0 value is `dev` schema and database, so jump out the 0 value.
        self.id.fetch_add(1, Ordering::SeqCst) + 1
    }

    fn add_table_or_source_id(&self, table_id: u32, schema_id: SchemaId, _database_id: DatabaseId) {
        self.table_id_to_schema_id
            .write()
            .insert(table_id, schema_id);
    }

    fn drop_table_or_source_id(&self, table_id: u32) -> (DatabaseId, SchemaId) {
        let schema_id = self
            .table_id_to_schema_id
            .write()
            .remove(&table_id)
            .unwrap();
        (self.get_database_id_by_schema(schema_id), schema_id)
    }

    fn add_table_or_sink_id(&self, table_id: u32, schema_id: SchemaId, _database_id: DatabaseId) {
        self.table_id_to_schema_id
            .write()
            .insert(table_id, schema_id);
    }

    fn add_table_or_subscription_id(
        &self,
        table_id: u32,
        schema_id: SchemaId,
        _database_id: DatabaseId,
    ) {
        self.table_id_to_schema_id
            .write()
            .insert(table_id, schema_id);
    }

    fn add_table_or_index_id(&self, table_id: u32, schema_id: SchemaId, _database_id: DatabaseId) {
        self.table_id_to_schema_id
            .write()
            .insert(table_id, schema_id);
    }

    fn drop_table_or_sink_id(&self, table_id: u32) -> (DatabaseId, SchemaId) {
        let schema_id = self
            .table_id_to_schema_id
            .write()
            .remove(&table_id)
            .unwrap();
        (self.get_database_id_by_schema(schema_id), schema_id)
    }

    fn drop_table_or_subscription_id(&self, table_id: u32) -> (DatabaseId, SchemaId) {
        let schema_id = self
            .table_id_to_schema_id
            .write()
            .remove(&table_id)
            .unwrap();
        (self.get_database_id_by_schema(schema_id), schema_id)
    }

    fn drop_table_or_index_id(&self, table_id: u32) -> (DatabaseId, SchemaId) {
        let schema_id = self
            .table_id_to_schema_id
            .write()
            .remove(&table_id)
            .unwrap();
        (self.get_database_id_by_schema(schema_id), schema_id)
    }

    fn add_schema_id(&self, schema_id: u32, database_id: DatabaseId) {
        self.schema_id_to_database_id
            .write()
            .insert(schema_id, database_id);
    }

    fn drop_schema_id(&self, schema_id: u32) -> DatabaseId {
        self.schema_id_to_database_id
            .write()
            .remove(&schema_id)
            .unwrap()
    }

    fn create_source_inner(&self, mut source: PbSource) -> Result<u32> {
        source.id = self.gen_id();
        self.catalog.write().create_source(&source);
        self.add_table_or_source_id(source.id, source.schema_id, source.database_id);
        Ok(source.id)
    }

    fn create_sink_inner(&self, mut sink: PbSink, _graph: StreamFragmentGraph) -> Result<()> {
        sink.id = self.gen_id();
        self.catalog.write().create_sink(&sink);
        self.add_table_or_sink_id(sink.id, sink.schema_id, sink.database_id);
        Ok(())
    }

    fn create_subscription_inner(&self, mut subscription: PbSubscription) -> Result<()> {
        subscription.id = self.gen_id();
        self.catalog.write().create_subscription(&subscription);
        self.add_table_or_subscription_id(
            subscription.id,
            subscription.schema_id,
            subscription.database_id,
        );
        Ok(())
    }

    fn get_database_id_by_schema(&self, schema_id: u32) -> DatabaseId {
        *self
            .schema_id_to_database_id
            .read()
            .get(&schema_id)
            .unwrap()
    }
}

pub struct MockUserInfoWriter {
    id: AtomicU32,
    user_info: Arc<RwLock<UserInfoManager>>,
}

#[async_trait::async_trait]
impl UserInfoWriter for MockUserInfoWriter {
    async fn create_user(&self, user: UserInfo) -> Result<()> {
        let mut user = user;
        user.id = self.gen_id();
        self.user_info.write().create_user(user);
        Ok(())
    }

    async fn drop_user(&self, id: UserId) -> Result<()> {
        self.user_info.write().drop_user(id);
        Ok(())
    }

    async fn update_user(
        &self,
        update_user: UserInfo,
        update_fields: Vec<UpdateField>,
    ) -> Result<()> {
        let mut lock = self.user_info.write();
        let id = update_user.get_id();
        let Some(old_name) = lock.get_user_name_by_id(id) else {
            return Ok(());
        };
        let mut user_info = lock.get_user_by_name(&old_name).unwrap().to_prost();
        update_fields.into_iter().for_each(|field| match field {
            UpdateField::Super => user_info.is_super = update_user.is_super,
            UpdateField::Login => user_info.can_login = update_user.can_login,
            UpdateField::CreateDb => user_info.can_create_db = update_user.can_create_db,
            UpdateField::CreateUser => user_info.can_create_user = update_user.can_create_user,
            UpdateField::AuthInfo => user_info.auth_info.clone_from(&update_user.auth_info),
            UpdateField::Rename => user_info.name.clone_from(&update_user.name),
            UpdateField::Unspecified => unreachable!(),
        });
        lock.update_user(update_user);
        Ok(())
    }

    /// In `MockUserInfoWriter`, we don't support expand privilege with `GrantAllTables` and
    /// `GrantAllSources` when grant privilege to user.
    async fn grant_privilege(
        &self,
        users: Vec<UserId>,
        privileges: Vec<GrantPrivilege>,
        with_grant_option: bool,
        _grantor: UserId,
    ) -> Result<()> {
        let privileges = privileges
            .into_iter()
            .map(|mut p| {
                p.action_with_opts
                    .iter_mut()
                    .for_each(|ao| ao.with_grant_option = with_grant_option);
                p
            })
            .collect::<Vec<_>>();
        for user_id in users {
            if let Some(u) = self.user_info.write().get_user_mut(user_id) {
                u.extend_privileges(privileges.clone());
            }
        }
        Ok(())
    }

    /// In `MockUserInfoWriter`, we don't support expand privilege with `RevokeAllTables` and
    /// `RevokeAllSources` when revoke privilege from user.
    async fn revoke_privilege(
        &self,
        users: Vec<UserId>,
        privileges: Vec<GrantPrivilege>,
        _granted_by: UserId,
        _revoke_by: UserId,
        revoke_grant_option: bool,
        _cascade: bool,
    ) -> Result<()> {
        for user_id in users {
            if let Some(u) = self.user_info.write().get_user_mut(user_id) {
                u.revoke_privileges(privileges.clone(), revoke_grant_option);
            }
        }
        Ok(())
    }
}

impl MockUserInfoWriter {
    pub fn new(user_info: Arc<RwLock<UserInfoManager>>) -> Self {
        user_info.write().create_user(UserInfo {
            id: DEFAULT_SUPER_USER_ID,
            name: DEFAULT_SUPER_USER.to_owned(),
            is_super: true,
            can_create_db: true,
            can_create_user: true,
            can_login: true,
            ..Default::default()
        });
        Self {
            user_info,
            id: AtomicU32::new(NON_RESERVED_USER_ID as u32),
        }
    }

    fn gen_id(&self) -> u32 {
        self.id.fetch_add(1, Ordering::SeqCst)
    }
}

pub struct MockFrontendMetaClient {}

#[async_trait::async_trait]
impl FrontendMetaClient for MockFrontendMetaClient {
    async fn try_unregister(&self) {}

    async fn flush(&self, _database_id: DatabaseId) -> RpcResult<HummockVersionId> {
        Ok(INVALID_VERSION_ID)
    }

    async fn wait(&self) -> RpcResult<()> {
        Ok(())
    }

    async fn cancel_creating_jobs(&self, _infos: PbJobs) -> RpcResult<Vec<u32>> {
        Ok(vec![])
    }

    async fn list_table_fragments(
        &self,
        _table_ids: &[u32],
    ) -> RpcResult<HashMap<u32, TableFragmentInfo>> {
        Ok(HashMap::default())
    }

    async fn list_streaming_job_states(&self) -> RpcResult<Vec<StreamingJobState>> {
        Ok(vec![])
    }

    async fn list_fragment_distribution(&self) -> RpcResult<Vec<FragmentDistribution>> {
        Ok(vec![])
    }

    async fn list_actor_states(&self) -> RpcResult<Vec<ActorState>> {
        Ok(vec![])
    }

    async fn list_actor_splits(&self) -> RpcResult<Vec<ActorSplit>> {
        Ok(vec![])
    }

    async fn list_object_dependencies(&self) -> RpcResult<Vec<PbObjectDependencies>> {
        Ok(vec![])
    }

    async fn list_meta_snapshots(&self) -> RpcResult<Vec<MetaSnapshotMetadata>> {
        Ok(vec![])
    }

    async fn get_system_params(&self) -> RpcResult<SystemParamsReader> {
        Ok(SystemParams::default().into())
    }

    async fn set_system_param(
        &self,
        _param: String,
        _value: Option<String>,
    ) -> RpcResult<Option<SystemParamsReader>> {
        Ok(Some(SystemParams::default().into()))
    }

    async fn get_session_params(&self) -> RpcResult<SessionConfig> {
        Ok(Default::default())
    }

    async fn set_session_param(&self, _param: String, _value: Option<String>) -> RpcResult<String> {
        Ok("".to_owned())
    }

    async fn get_ddl_progress(&self) -> RpcResult<Vec<DdlProgress>> {
        Ok(vec![])
    }

    async fn get_tables(&self, _table_ids: &[u32]) -> RpcResult<HashMap<u32, Table>> {
        Ok(HashMap::new())
    }

    async fn list_hummock_pinned_versions(&self) -> RpcResult<Vec<(u32, u64)>> {
        unimplemented!()
    }

    async fn get_hummock_current_version(&self) -> RpcResult<HummockVersion> {
        Ok(HummockVersion::default())
    }

    async fn get_hummock_checkpoint_version(&self) -> RpcResult<HummockVersion> {
        unimplemented!()
    }

    async fn list_version_deltas(&self) -> RpcResult<Vec<HummockVersionDelta>> {
        unimplemented!()
    }

    async fn list_branched_objects(&self) -> RpcResult<Vec<BranchedObject>> {
        unimplemented!()
    }

    async fn list_hummock_compaction_group_configs(&self) -> RpcResult<Vec<CompactionGroupInfo>> {
        unimplemented!()
    }

    async fn list_hummock_active_write_limits(&self) -> RpcResult<HashMap<u64, WriteLimit>> {
        unimplemented!()
    }

    async fn list_hummock_meta_configs(&self) -> RpcResult<HashMap<String, String>> {
        unimplemented!()
    }

    async fn list_event_log(&self) -> RpcResult<Vec<EventLog>> {
        unimplemented!()
    }

    async fn list_compact_task_assignment(&self) -> RpcResult<Vec<CompactTaskAssignment>> {
        unimplemented!()
    }

    async fn list_all_nodes(&self) -> RpcResult<Vec<WorkerNode>> {
        Ok(vec![])
    }

    async fn list_compact_task_progress(&self) -> RpcResult<Vec<CompactTaskProgress>> {
        unimplemented!()
    }

    async fn recover(&self) -> RpcResult<()> {
        unimplemented!()
    }

    async fn apply_throttle(
        &self,
        _kind: PbThrottleTarget,
        _id: u32,
        _rate_limit: Option<u32>,
    ) -> RpcResult<()> {
        unimplemented!()
    }

    async fn get_cluster_recovery_status(&self) -> RpcResult<RecoveryStatus> {
        Ok(RecoveryStatus::StatusRunning)
    }

    async fn get_cluster_limits(&self) -> RpcResult<Vec<ClusterLimit>> {
        Ok(vec![])
    }

    async fn list_rate_limits(&self) -> RpcResult<Vec<RateLimitInfo>> {
        Ok(vec![])
    }

    async fn get_meta_store_endpoint(&self) -> RpcResult<String> {
        unimplemented!()
    }
}

#[cfg(test)]
pub static PROTO_FILE_DATA: &str = r#"
    syntax = "proto3";
    package test;
    message TestRecord {
      int32 id = 1;
      Country country = 3;
      int64 zipcode = 4;
      float rate = 5;
    }
    message TestRecordAlterType {
        string id = 1;
        Country country = 3;
        int32 zipcode = 4;
        float rate = 5;
      }
    message TestRecordExt {
      int32 id = 1;
      Country country = 3;
      int64 zipcode = 4;
      float rate = 5;
      string name = 6;
    }
    message Country {
      string address = 1;
      City city = 2;
      string zipcode = 3;
    }
    message City {
      string address = 1;
      string zipcode = 2;
    }"#;

/// Returns the file.
/// (`NamedTempFile` will automatically delete the file when it goes out of scope.)
pub fn create_proto_file(proto_data: &str) -> NamedTempFile {
    let in_file = Builder::new()
        .prefix("temp")
        .suffix(".proto")
        .rand_bytes(8)
        .tempfile()
        .unwrap();

    let out_file = Builder::new()
        .prefix("temp")
        .suffix(".pb")
        .rand_bytes(8)
        .tempfile()
        .unwrap();

    let mut file = in_file.as_file();
    file.write_all(proto_data.as_ref())
        .expect("writing binary to test file");
    file.flush().expect("flush temp file failed");
    let include_path = in_file
        .path()
        .parent()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let out_path = out_file.path().to_string_lossy().into_owned();
    let in_path = in_file.path().to_string_lossy().into_owned();
    let mut compile = std::process::Command::new("protoc");

    let out = compile
        .arg("--include_imports")
        .arg("-I")
        .arg(include_path)
        .arg(format!("--descriptor_set_out={}", out_path))
        .arg(in_path)
        .output()
        .expect("failed to compile proto");
    if !out.status.success() {
        panic!("compile proto failed \n output: {:?}", out);
    }
    out_file
}
