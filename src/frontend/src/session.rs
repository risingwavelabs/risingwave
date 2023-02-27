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
use std::io::{Error, ErrorKind};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use parking_lot::{RwLock, RwLockReadGuard};
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::PgResponse;
use pgwire::pg_server::{BoxedError, Session, SessionId, SessionManager, UserAuthenticator};
use pgwire::types::Format;
use rand::RngCore;
use risingwave_common::array::DataChunk;
use risingwave_common::catalog::DEFAULT_SCHEMA_NAME;
#[cfg(test)]
use risingwave_common::catalog::{
    DEFAULT_DATABASE_NAME, DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID,
};
use risingwave_common::config::{load_config, BatchConfig};
use risingwave_common::error::{Result, RwError};
use risingwave_common::monitor::process_linux::monitor_process;
use risingwave_common::session_config::ConfigMap;
use risingwave_common::types::DataType;
use risingwave_common::util::addr::HostAddr;
use risingwave_common::util::stream_cancel::{stream_tripwire, Trigger, Tripwire};
use risingwave_common::{GIT_SHA, RW_VERSION};
use risingwave_common_service::observer_manager::ObserverManager;
use risingwave_common_service::MetricsManager;
use risingwave_connector::source::monitor::SourceMetrics;
use risingwave_pb::common::WorkerType;
use risingwave_pb::health::health_server::HealthServer;
use risingwave_pb::user::auth_info::EncryptionType;
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_rpc_client::{ComputeClientPool, ComputeClientPoolRef, MetaClient};
use risingwave_sqlparser::ast::{ObjectName, ShowObject, Statement};
use risingwave_sqlparser::parser::Parser;
use tokio::sync::oneshot::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::info;

use crate::binder::Binder;
use crate::catalog::catalog_service::{CatalogReader, CatalogWriter, CatalogWriterImpl};
use crate::catalog::root_catalog::Catalog;
use crate::catalog::{check_schema_writable, DatabaseId, SchemaId};
use crate::handler::privilege::ObjectCheckItem;
use crate::handler::util::to_pg_field;
use crate::handler::{handle, HandlerArgs};
use crate::health_service::HealthServiceImpl;
use crate::meta_client::{FrontendMetaClient, FrontendMetaClientImpl};
use crate::monitor::FrontendMetrics;
use crate::observer::FrontendObserverNode;
use crate::optimizer::OptimizerContext;
use crate::planner::Planner;
use crate::scheduler::streaming_manager::{StreamingJobTracker, StreamingJobTrackerRef};
use crate::scheduler::worker_node_manager::{WorkerNodeManager, WorkerNodeManagerRef};
use crate::scheduler::SchedulerError::QueryCancelError;
use crate::scheduler::{
    DistributedQueryMetrics, HummockSnapshotManager, HummockSnapshotManagerRef, QueryManager,
};
use crate::user::user_authentication::md5_hash_with_salt;
use crate::user::user_manager::UserInfoManager;
use crate::user::user_service::{UserInfoReader, UserInfoWriter, UserInfoWriterImpl};
use crate::user::UserId;
use crate::{FrontendOpts, PgResponseStream};

/// The global environment for the frontend server.
#[derive(Clone)]
pub struct FrontendEnv {
    // Different session may access catalog at the same time and catalog is protected by a
    // RwLock.
    meta_client: Arc<dyn FrontendMetaClient>,
    catalog_writer: Arc<dyn CatalogWriter>,
    catalog_reader: CatalogReader,
    user_info_writer: Arc<dyn UserInfoWriter>,
    user_info_reader: UserInfoReader,
    worker_node_manager: WorkerNodeManagerRef,
    query_manager: QueryManager,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
    server_addr: HostAddr,
    client_pool: ComputeClientPoolRef,

    /// Each session is identified by (process_id,
    /// secret_key). When Cancel Request received, find corresponding session and cancel all
    /// running queries.
    sessions_map: SessionMapRef,

    pub frontend_metrics: Arc<FrontendMetrics>,

    source_metrics: Arc<SourceMetrics>,

    batch_config: BatchConfig,

    /// Track creating streaming jobs, used to cancel creating streaming job when cancel request
    /// received.
    creating_streaming_job_tracker: StreamingJobTrackerRef,
}

type SessionMapRef = Arc<Mutex<HashMap<(i32, i32), Arc<SessionImpl>>>>;

impl FrontendEnv {
    pub fn mock() -> Self {
        use crate::test_utils::{MockCatalogWriter, MockFrontendMetaClient, MockUserInfoWriter};

        let catalog = Arc::new(RwLock::new(Catalog::default()));
        let catalog_writer = Arc::new(MockCatalogWriter::new(catalog.clone()));
        let catalog_reader = CatalogReader::new(catalog);
        let user_info_manager = Arc::new(RwLock::new(UserInfoManager::default()));
        let user_info_writer = Arc::new(MockUserInfoWriter::new(user_info_manager.clone()));
        let user_info_reader = UserInfoReader::new(user_info_manager);
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(vec![]));
        let meta_client = Arc::new(MockFrontendMetaClient {});
        let hummock_snapshot_manager = Arc::new(HummockSnapshotManager::new(meta_client.clone()));
        let compute_client_pool = Arc::new(ComputeClientPool::default());
        let query_manager = QueryManager::new(
            worker_node_manager.clone(),
            hummock_snapshot_manager.clone(),
            compute_client_pool,
            catalog_reader.clone(),
            Arc::new(DistributedQueryMetrics::for_test()),
            None,
        );
        let server_addr = HostAddr::try_from("127.0.0.1:4565").unwrap();
        let client_pool = Arc::new(ComputeClientPool::default());
        let creating_streaming_tracker = StreamingJobTracker::new(meta_client.clone());
        Self {
            meta_client,
            catalog_writer,
            catalog_reader,
            user_info_writer,
            user_info_reader,
            worker_node_manager,
            query_manager,
            hummock_snapshot_manager,
            server_addr,
            client_pool,
            sessions_map: Arc::new(Mutex::new(HashMap::new())),
            frontend_metrics: Arc::new(FrontendMetrics::for_test()),
            batch_config: BatchConfig::default(),
            source_metrics: Arc::new(SourceMetrics::default()),
            creating_streaming_job_tracker: Arc::new(creating_streaming_tracker),
        }
    }

    pub async fn init(
        opts: FrontendOpts,
    ) -> Result<(Self, JoinHandle<()>, JoinHandle<()>, Sender<()>)> {
        let config = load_config(&opts.config_path, Some(opts.override_opts));
        info!("Starting frontend node");
        info!("> config: {:?}", config);
        info!(
            "> debug assertions: {}",
            if cfg!(debug_assertions) { "on" } else { "off" }
        );
        info!("> version: {} ({})", RW_VERSION, GIT_SHA);

        let batch_config = config.batch;

        let frontend_address: HostAddr = opts
            .advertise_addr
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("advertise addr is not specified, defaulting to listen_addr");
                &opts.listen_addr
            })
            .parse()
            .unwrap();
        info!("advertise addr is {}", frontend_address);

        // Register in meta by calling `AddWorkerNode` RPC.
        let (meta_client, _) = MetaClient::register_new(
            opts.meta_addr.clone().as_str(),
            WorkerType::Frontend,
            &frontend_address,
            0,
        )
        .await?;

        let (heartbeat_join_handle, heartbeat_shutdown_sender) = MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval_ms as u64),
            Duration::from_secs(config.server.max_heartbeat_interval_secs as u64),
            vec![],
        );

        let (catalog_updated_tx, catalog_updated_rx) = watch::channel(0);
        let catalog = Arc::new(RwLock::new(Catalog::default()));
        let catalog_writer = Arc::new(CatalogWriterImpl::new(
            meta_client.clone(),
            catalog_updated_rx,
        ));
        let catalog_reader = CatalogReader::new(catalog.clone());

        let worker_node_manager = Arc::new(WorkerNodeManager::new());

        let registry = prometheus::Registry::new();
        monitor_process(&registry).unwrap();

        let frontend_meta_client = Arc::new(FrontendMetaClientImpl(meta_client.clone()));
        let hummock_snapshot_manager =
            Arc::new(HummockSnapshotManager::new(frontend_meta_client.clone()));
        let compute_client_pool =
            Arc::new(ComputeClientPool::new(config.server.connection_pool_size));
        let query_manager = QueryManager::new(
            worker_node_manager.clone(),
            hummock_snapshot_manager.clone(),
            compute_client_pool,
            catalog_reader.clone(),
            Arc::new(DistributedQueryMetrics::new(registry.clone())),
            batch_config.distributed_query_limit,
        );

        let user_info_manager = Arc::new(RwLock::new(UserInfoManager::default()));
        let (user_info_updated_tx, user_info_updated_rx) = watch::channel(0);
        let user_info_reader = UserInfoReader::new(user_info_manager.clone());
        let user_info_writer = Arc::new(UserInfoWriterImpl::new(
            meta_client.clone(),
            user_info_updated_rx,
        ));

        let frontend_observer_node = FrontendObserverNode::new(
            worker_node_manager.clone(),
            catalog,
            catalog_updated_tx,
            user_info_manager,
            user_info_updated_tx,
            hummock_snapshot_manager.clone(),
        );
        let observer_manager =
            ObserverManager::new_with_meta_client(meta_client.clone(), frontend_observer_node)
                .await;
        let observer_join_handle = observer_manager.start().await;

        meta_client.activate(&frontend_address).await?;

        let client_pool = Arc::new(ComputeClientPool::new(config.server.connection_pool_size));

        let frontend_metrics = Arc::new(FrontendMetrics::new(registry.clone()));
        let source_metrics = Arc::new(SourceMetrics::new(registry.clone()));

        if config.server.metrics_level > 0 {
            MetricsManager::boot_metrics_service(opts.prometheus_listener_addr.clone(), registry);
        }

        let health_srv = HealthServiceImpl::new();
        let host = opts.health_check_listener_addr.clone();
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(HealthServer::new(health_srv))
                .serve(host.parse().unwrap())
                .await
                .unwrap();
        });
        info!(
            "Health Check RPC Listener is set up on {}",
            opts.health_check_listener_addr.clone()
        );

        let creating_streaming_job_tracker =
            Arc::new(StreamingJobTracker::new(frontend_meta_client.clone()));

        Ok((
            Self {
                catalog_reader,
                catalog_writer,
                user_info_reader,
                user_info_writer,
                worker_node_manager,
                meta_client: frontend_meta_client,
                query_manager,
                hummock_snapshot_manager,
                server_addr: frontend_address,
                client_pool,
                frontend_metrics,
                sessions_map: Arc::new(Mutex::new(HashMap::new())),
                batch_config,
                source_metrics,
                creating_streaming_job_tracker,
            },
            observer_join_handle,
            heartbeat_join_handle,
            heartbeat_shutdown_sender,
        ))
    }

    /// Get a reference to the frontend env's catalog writer.
    pub fn catalog_writer(&self) -> &dyn CatalogWriter {
        &*self.catalog_writer
    }

    /// Get a reference to the frontend env's catalog reader.
    pub fn catalog_reader(&self) -> &CatalogReader {
        &self.catalog_reader
    }

    /// Get a reference to the frontend env's user info writer.
    pub fn user_info_writer(&self) -> &dyn UserInfoWriter {
        &*self.user_info_writer
    }

    /// Get a reference to the frontend env's user info reader.
    pub fn user_info_reader(&self) -> &UserInfoReader {
        &self.user_info_reader
    }

    pub fn worker_node_manager(&self) -> &WorkerNodeManager {
        &self.worker_node_manager
    }

    pub fn worker_node_manager_ref(&self) -> WorkerNodeManagerRef {
        self.worker_node_manager.clone()
    }

    pub fn meta_client(&self) -> &dyn FrontendMetaClient {
        &*self.meta_client
    }

    pub fn meta_client_ref(&self) -> Arc<dyn FrontendMetaClient> {
        self.meta_client.clone()
    }

    pub fn query_manager(&self) -> &QueryManager {
        &self.query_manager
    }

    pub fn hummock_snapshot_manager(&self) -> &HummockSnapshotManagerRef {
        &self.hummock_snapshot_manager
    }

    pub fn server_address(&self) -> &HostAddr {
        &self.server_addr
    }

    pub fn client_pool(&self) -> ComputeClientPoolRef {
        self.client_pool.clone()
    }

    pub fn batch_config(&self) -> &BatchConfig {
        &self.batch_config
    }

    pub fn source_metrics(&self) -> Arc<SourceMetrics> {
        self.source_metrics.clone()
    }

    pub fn creating_streaming_job_tracker(&self) -> &StreamingJobTrackerRef {
        &self.creating_streaming_job_tracker
    }
}

pub struct AuthContext {
    pub database: String,
    pub user_name: String,
    pub user_id: UserId,
}

impl AuthContext {
    pub fn new(database: String, user_name: String, user_id: UserId) -> Self {
        Self {
            database,
            user_name,
            user_id,
        }
    }
}

pub struct SessionImpl {
    env: FrontendEnv,
    auth_context: Arc<AuthContext>,
    // Used for user authentication.
    user_authenticator: UserAuthenticator,
    /// Stores the value of configurations.
    config_map: RwLock<ConfigMap>,

    /// Identified by process_id, secret_key. Corresponds to SessionManager.
    id: (i32, i32),

    /// Query cancel flag.
    /// This flag is set only when current query is executed in local mode, and used to cancel
    /// local query.
    current_query_cancel_flag: Mutex<Option<Trigger>>,
}

impl SessionImpl {
    pub fn new(
        env: FrontendEnv,
        auth_context: Arc<AuthContext>,
        user_authenticator: UserAuthenticator,
        id: SessionId,
    ) -> Self {
        Self {
            env,
            auth_context,
            user_authenticator,
            config_map: Default::default(),
            id,
            current_query_cancel_flag: Mutex::new(None),
        }
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        Self {
            env: FrontendEnv::mock(),
            auth_context: Arc::new(AuthContext::new(
                DEFAULT_DATABASE_NAME.to_string(),
                DEFAULT_SUPER_USER.to_string(),
                DEFAULT_SUPER_USER_ID,
            )),
            user_authenticator: UserAuthenticator::None,
            config_map: Default::default(),
            // Mock session use non-sense id.
            id: (0, 0),
            current_query_cancel_flag: Mutex::new(None),
        }
    }

    pub fn env(&self) -> &FrontendEnv {
        &self.env
    }

    pub fn auth_context(&self) -> Arc<AuthContext> {
        self.auth_context.clone()
    }

    pub fn database(&self) -> &str {
        &self.auth_context.database
    }

    pub fn user_name(&self) -> &str {
        &self.auth_context.user_name
    }

    pub fn user_id(&self) -> UserId {
        self.auth_context.user_id
    }

    pub fn config(&self) -> RwLockReadGuard<'_, ConfigMap> {
        self.config_map.read()
    }

    pub fn set_config(&self, key: &str, value: Vec<String>) -> Result<()> {
        self.config_map.write().set(key, value)
    }

    pub fn session_id(&self) -> SessionId {
        self.id
    }

    pub fn check_relation_name_duplicated(&self, name: ObjectName) -> Result<()> {
        let db_name = self.database();
        let catalog_reader = self.env().catalog_reader().read_guard();
        let (schema_name, view_name) = {
            let (schema_name, table_name) = Binder::resolve_schema_qualified_name(db_name, name)?;
            let search_path = self.config().get_search_path();
            let user_name = &self.auth_context().user_name;
            let schema_name = match schema_name {
                Some(schema_name) => schema_name,
                None => catalog_reader
                    .first_valid_schema(db_name, &search_path, user_name)?
                    .name(),
            };
            (schema_name, table_name)
        };
        catalog_reader
            .check_relation_name_duplicated(db_name, &schema_name, &view_name)
            .map_err(RwError::from)
    }

    /// Also check if the user has the privilege to create in the schema.
    pub fn get_database_and_schema_id_for_create(
        &self,
        schema_name: Option<String>,
    ) -> Result<(DatabaseId, SchemaId)> {
        let db_name = self.database();

        let search_path = self.config().get_search_path();
        let user_name = &self.auth_context().user_name;

        let catalog_reader = self.env().catalog_reader().read_guard();
        let schema = match schema_name {
            Some(schema_name) => catalog_reader.get_schema_by_name(db_name, &schema_name)?,
            None => catalog_reader.first_valid_schema(db_name, &search_path, user_name)?,
        };

        check_schema_writable(&schema.name())?;
        if schema.name() != DEFAULT_SCHEMA_NAME {
            self.check_privileges(&[ObjectCheckItem::new(
                schema.owner(),
                Action::Create,
                Object::SchemaId(schema.id()),
            )])?;
        }

        let db_id = catalog_reader.get_database_by_name(db_name)?.id();
        Ok((db_id, schema.id()))
    }

    pub fn clear_cancel_query_flag(&self) {
        let mut flag = self.current_query_cancel_flag.lock().unwrap();
        *flag = None;
    }

    pub fn reset_cancel_query_flag(&self) -> Tripwire<std::result::Result<DataChunk, BoxedError>> {
        let mut flag = self.current_query_cancel_flag.lock().unwrap();
        let (trigger, tripwire) = stream_tripwire(|| Err(Box::new(QueryCancelError) as BoxedError));
        *flag = Some(trigger);
        tripwire
    }

    pub fn cancel_current_query(&self) {
        let mut flag_guard = self.current_query_cancel_flag.lock().unwrap();
        if let Some(trigger) = flag_guard.take() {
            info!("Trying to cancel query in local mode.");
            // Current running query is in local mode
            trigger.abort();
            info!("Cancel query request sent.");
        } else {
            info!("Trying to cancel query in distributed mode.");
            self.env.query_manager().cancel_queries_in_session(self.id)
        }
    }

    pub fn cancel_current_creating_job(&self) {
        self.env.creating_streaming_job_tracker.abort_jobs(self.id);
    }
}

pub struct SessionManagerImpl {
    env: FrontendEnv,
    _observer_join_handle: JoinHandle<()>,
    _heartbeat_join_handle: JoinHandle<()>,
    _heartbeat_shutdown_sender: Sender<()>,
    number: AtomicI32,
}

impl SessionManager<PgResponseStream> for SessionManagerImpl {
    type Session = SessionImpl;

    fn connect(
        &self,
        database: &str,
        user_name: &str,
    ) -> std::result::Result<Arc<Self::Session>, BoxedError> {
        let catalog_reader = self.env.catalog_reader();
        let reader = catalog_reader.read_guard();
        let database_id = reader
            .get_database_by_name(database)
            .map_err(|_| {
                Box::new(Error::new(
                    ErrorKind::InvalidInput,
                    format!("Not found database name: {}", database),
                ))
            })?
            .id();
        let user_reader = self.env.user_info_reader();
        let reader = user_reader.read_guard();
        if let Some(user) = reader.get_user_by_name(user_name) {
            if !user.can_login {
                return Err(Box::new(Error::new(
                    ErrorKind::InvalidInput,
                    format!("User {} is not allowed to login", user_name),
                )));
            }
            let has_privilege = user.grant_privileges.iter().any(|privilege| {
                privilege.object == Some(Object::DatabaseId(database_id))
                    && privilege
                        .action_with_opts
                        .iter()
                        .any(|ao| ao.action == Action::Connect as i32)
            });
            if !user.is_super && !has_privilege {
                return Err(Box::new(Error::new(
                    ErrorKind::PermissionDenied,
                    "User does not have CONNECT privilege.",
                )));
            }
            let user_authenticator = match &user.auth_info {
                None => UserAuthenticator::None,
                Some(auth_info) => {
                    if auth_info.encryption_type == EncryptionType::Plaintext as i32 {
                        UserAuthenticator::ClearText(auth_info.encrypted_value.clone())
                    } else if auth_info.encryption_type == EncryptionType::Md5 as i32 {
                        let mut salt = [0; 4];
                        let mut rng = rand::thread_rng();
                        rng.fill_bytes(&mut salt);
                        UserAuthenticator::Md5WithSalt {
                            encrypted_password: md5_hash_with_salt(
                                &auth_info.encrypted_value,
                                &salt,
                            ),
                            salt,
                        }
                    } else {
                        return Err(Box::new(Error::new(
                            ErrorKind::Unsupported,
                            format!("Unsupported auth type: {}", auth_info.encryption_type),
                        )));
                    }
                }
            };

            // Assign a session id and insert into sessions map (for cancel request).
            let secret_key = self.number.fetch_add(1, Ordering::Relaxed);
            // Use a trivial strategy: process_id and secret_key are equal.
            let id = (secret_key, secret_key);
            let session_impl: Arc<SessionImpl> = SessionImpl::new(
                self.env.clone(),
                Arc::new(AuthContext::new(
                    database.to_string(),
                    user_name.to_string(),
                    user.id,
                )),
                user_authenticator,
                id,
            )
            .into();
            self.insert_session(session_impl.clone());

            Ok(session_impl)
        } else {
            Err(Box::new(Error::new(
                ErrorKind::InvalidInput,
                format!("Role {} does not exist", user_name),
            )))
        }
    }

    /// Used when cancel request happened.
    fn cancel_queries_in_session(&self, session_id: SessionId) {
        let guard = self.env.sessions_map.lock().unwrap();
        if let Some(session) = guard.get(&session_id) {
            session.cancel_current_query()
        } else {
            info!("Current session finished, ignoring cancel query request")
        }
    }

    fn cancel_creating_jobs_in_session(&self, session_id: SessionId) {
        let guard = self.env.sessions_map.lock().unwrap();
        if let Some(session) = guard.get(&session_id) {
            session.cancel_current_creating_job()
        } else {
            info!("Current session finished, ignoring cancel creating request")
        }
    }

    fn end_session(&self, session: &Self::Session) {
        self.delete_session(&session.session_id());
    }
}

impl SessionManagerImpl {
    pub async fn new(opts: FrontendOpts) -> Result<Self> {
        let (env, join_handle, heartbeat_join_handle, heartbeat_shutdown_sender) =
            FrontendEnv::init(opts).await?;
        Ok(Self {
            env,
            _observer_join_handle: join_handle,
            _heartbeat_join_handle: heartbeat_join_handle,
            _heartbeat_shutdown_sender: heartbeat_shutdown_sender,
            number: AtomicI32::new(0),
        })
    }

    fn insert_session(&self, session: Arc<SessionImpl>) {
        let mut write_guard = self.env.sessions_map.lock().unwrap();
        write_guard.insert(session.id(), session);
    }

    fn delete_session(&self, session_id: &SessionId) {
        let mut write_guard = self.env.sessions_map.lock().unwrap();
        write_guard.remove(session_id);
    }
}

#[async_trait::async_trait]
impl Session<PgResponseStream> for SessionImpl {
    async fn run_statement(
        self: Arc<Self>,
        sql: &str,
        formats: Vec<Format>,
    ) -> std::result::Result<PgResponse<PgResponseStream>, BoxedError> {
        // Parse sql.
        let mut stmts = Parser::parse_sql(sql)
            .inspect_err(|e| tracing::error!("failed to parse sql:\n{}:\n{}", sql, e))?;
        if stmts.is_empty() {
            return Ok(PgResponse::empty_result(
                pgwire::pg_response::StatementType::EMPTY,
            ));
        }
        if stmts.len() > 1 {
            return Ok(PgResponse::empty_result_with_notice(
                pgwire::pg_response::StatementType::EMPTY,
                "cannot insert multiple commands into statement".to_string(),
            ));
        }
        let stmt = stmts.swap_remove(0);
        let rsp = {
            let mut handle_fut = Box::pin(handle(self, stmt, sql, formats));
            if cfg!(debug_assertions) {
                // Report the SQL in the log periodically if the query is slow.
                const SLOW_QUERY_LOG_PERIOD: Duration = Duration::from_secs(60);
                loop {
                    match tokio::time::timeout(SLOW_QUERY_LOG_PERIOD, &mut handle_fut).await {
                        Ok(result) => break result,
                        Err(_) => tracing::warn!(
                            target: "risingwave_frontend_slow_query_log",
                            sql,
                            "slow query has been running for another {SLOW_QUERY_LOG_PERIOD:?}"
                        ),
                    }
                }
            } else {
                handle_fut.await
            }
        }
        .inspect_err(|e| tracing::error!("failed to handle sql:\n{}:\n{}", sql, e))?;
        Ok(rsp)
    }

    /// A copy of run_statement but exclude the parser part so each run must be at most one
    /// statement. The str sql use the to_string of AST. Consider Reuse later.
    async fn run_one_query(
        self: Arc<Self>,
        stmt: Statement,
        format: Format,
    ) -> std::result::Result<PgResponse<PgResponseStream>, BoxedError> {
        let sql_str = stmt.to_string();
        let rsp = {
            let mut handle_fut = Box::pin(handle(self, stmt, &sql_str, vec![format]));
            if cfg!(debug_assertions) {
                // Report the SQL in the log periodically if the query is slow.
                const SLOW_QUERY_LOG_PERIOD: Duration = Duration::from_secs(60);
                loop {
                    match tokio::time::timeout(SLOW_QUERY_LOG_PERIOD, &mut handle_fut).await {
                        Ok(result) => break result,
                        Err(_) => tracing::warn!(
                            sql_str,
                            "slow query has been running for another {SLOW_QUERY_LOG_PERIOD:?}"
                        ),
                    }
                }
            } else {
                handle_fut.await
            }
        }
        .inspect_err(|e| tracing::error!("failed to handle sql:\n{}:\n{}", sql_str, e))?;
        Ok(rsp)
    }

    async fn infer_return_type(
        self: Arc<Self>,
        sql: &str,
    ) -> std::result::Result<Vec<PgFieldDescriptor>, BoxedError> {
        // Parse sql.
        let mut stmts = Parser::parse_sql(sql)
            .inspect_err(|e| tracing::error!("failed to parse sql:\n{}:\n{}", sql, e))?;
        if stmts.is_empty() {
            return Ok(vec![]);
        }
        if stmts.len() > 1 {
            return Err(Box::new(Error::new(
                ErrorKind::InvalidInput,
                "cannot insert multiple commands into statement",
            )));
        }
        let stmt = stmts.swap_remove(0);
        // This part refers from src/frontend/handler/ so the Vec<PgFieldDescriptor> is same as
        // result of run_statement().
        let rsp = match stmt {
            Statement::Query(_) => infer(self, stmt, sql)
                .inspect_err(|e| tracing::error!("failed to handle sql:\n{}:\n{}", sql, e))?,
            Statement::ShowObjects(show_object) => match show_object {
                ShowObject::Columns { table: _ } => {
                    vec![
                        PgFieldDescriptor::new(
                            "Name".to_owned(),
                            DataType::VARCHAR.to_oid(),
                            DataType::VARCHAR.type_len(),
                        ),
                        PgFieldDescriptor::new(
                            "Type".to_owned(),
                            DataType::VARCHAR.to_oid(),
                            DataType::VARCHAR.type_len(),
                        ),
                    ]
                }
                _ => {
                    vec![PgFieldDescriptor::new(
                        "Name".to_owned(),
                        DataType::VARCHAR.to_oid(),
                        DataType::VARCHAR.type_len(),
                    )]
                }
            },
            Statement::ShowCreateObject { .. } => {
                vec![
                    PgFieldDescriptor::new(
                        "Name".to_owned(),
                        DataType::VARCHAR.to_oid(),
                        DataType::VARCHAR.type_len(),
                    ),
                    PgFieldDescriptor::new(
                        "Create Sql".to_owned(),
                        DataType::VARCHAR.to_oid(),
                        DataType::VARCHAR.type_len(),
                    ),
                ]
            }
            Statement::ShowVariable { variable } => {
                let name = &variable[0].real_value().to_lowercase();
                if name.eq_ignore_ascii_case("ALL") {
                    vec![
                        PgFieldDescriptor::new(
                            "Name".to_string(),
                            DataType::VARCHAR.to_oid(),
                            DataType::VARCHAR.type_len(),
                        ),
                        PgFieldDescriptor::new(
                            "Setting".to_string(),
                            DataType::VARCHAR.to_oid(),
                            DataType::VARCHAR.type_len(),
                        ),
                        PgFieldDescriptor::new(
                            "Description".to_string(),
                            DataType::VARCHAR.to_oid(),
                            DataType::VARCHAR.type_len(),
                        ),
                    ]
                } else {
                    vec![PgFieldDescriptor::new(
                        name.to_ascii_lowercase(),
                        DataType::VARCHAR.to_oid(),
                        DataType::VARCHAR.type_len(),
                    )]
                }
            }
            Statement::Describe { name: _ } => {
                vec![
                    PgFieldDescriptor::new(
                        "Name".to_owned(),
                        DataType::VARCHAR.to_oid(),
                        DataType::VARCHAR.type_len(),
                    ),
                    PgFieldDescriptor::new(
                        "Type".to_owned(),
                        DataType::VARCHAR.to_oid(),
                        DataType::VARCHAR.type_len(),
                    ),
                ]
            }
            Statement::Explain { .. } => {
                vec![PgFieldDescriptor::new(
                    "QUERY PLAN".to_owned(),
                    DataType::VARCHAR.to_oid(),
                    DataType::VARCHAR.type_len(),
                )]
            }
            _ => {
                panic!("infer_return_type only support query statement");
            }
        };
        Ok(rsp)
    }

    fn user_authenticator(&self) -> &UserAuthenticator {
        &self.user_authenticator
    }

    fn id(&self) -> SessionId {
        self.id
    }
}

/// Returns row description of the statement
fn infer(session: Arc<SessionImpl>, stmt: Statement, sql: &str) -> Result<Vec<PgFieldDescriptor>> {
    let context = OptimizerContext::from_handler_args(HandlerArgs::new(session, &stmt, sql)?);
    let session = context.session_ctx().clone();

    let bound = {
        let mut binder = Binder::new(&session);
        binder.bind(stmt)?
    };

    let root = Planner::new(context.into()).plan(bound)?;

    let pg_descs = root
        .schema()
        .fields()
        .iter()
        .map(to_pg_field)
        .collect::<Vec<PgFieldDescriptor>>();

    Ok(pg_descs)
}
