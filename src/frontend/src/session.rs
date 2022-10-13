// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Formatter;
use std::io::{Error, ErrorKind};
use std::marker::Sync;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
// use tokio::sync::Mutex;
use std::time::Duration;

use parking_lot::{RwLock, RwLockReadGuard};
use pgwire::error::PsqlResult;
use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::pg_response::PgResponse;
use pgwire::pg_server::{BoxedError, Session, SessionId, SessionManager, UserAuthenticator};
use rand::RngCore;
#[cfg(test)]
use risingwave_common::catalog::{
    DEFAULT_DATABASE_NAME, DEFAULT_SUPER_USER, DEFAULT_SUPER_USER_ID,
};
use risingwave_common::config::load_config;
use risingwave_common::error::Result;
use risingwave_common::monitor::process_linux::monitor_process;
use risingwave_common::session_config::ConfigMap;
use risingwave_common::util::addr::HostAddr;
use risingwave_common_service::observer_manager::ObserverManager;
use risingwave_common_service::MetricsManager;
use risingwave_pb::common::WorkerType;
use risingwave_pb::user::auth_info::EncryptionType;
use risingwave_pb::user::grant_privilege::{Action, Object};
use risingwave_rpc_client::{ComputeClientPool, ComputeClientPoolRef, MetaClient};
use risingwave_sqlparser::ast::{ShowObject, Statement};
use risingwave_sqlparser::parser::Parser;
use tokio::sync::oneshot::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::binder::Binder;
use crate::catalog::catalog_service::{CatalogReader, CatalogWriter, CatalogWriterImpl};
use crate::catalog::root_catalog::Catalog;
use crate::expr::CorrelatedId;
use crate::handler::handle;
use crate::handler::util::{to_pg_field, DataChunkToRowSetAdapter};
use crate::meta_client::{FrontendMetaClient, FrontendMetaClientImpl};
use crate::monitor::FrontendMetrics;
use crate::observer::observer_manager::FrontendObserverNode;
use crate::optimizer::plan_node::PlanNodeId;
use crate::planner::Planner;
use crate::scheduler::worker_node_manager::{WorkerNodeManager, WorkerNodeManagerRef};
use crate::scheduler::{HummockSnapshotManager, HummockSnapshotManagerRef, QueryManager};
use crate::user::user_authentication::md5_hash_with_salt;
use crate::user::user_manager::UserInfoManager;
use crate::user::user_service::{UserInfoReader, UserInfoWriter, UserInfoWriterImpl};
use crate::user::UserId;
use crate::utils::WithOptions;
use crate::{FrontendConfig, FrontendOpts, PgResponseStream};
pub struct OptimizerContext {
    pub session_ctx: Arc<SessionImpl>,
    // We use `AtomicI32` here because `Arc<T>` implements `Send` only when `T: Send + Sync`.
    pub next_id: AtomicI32,
    /// For debugging purposes, store the SQL string in Context
    pub sql: Arc<str>,

    /// it indicates whether the explain mode is verbose for explain statement
    pub explain_verbose: AtomicBool,

    /// it indicates whether the explain mode is trace for explain statement
    pub explain_trace: AtomicBool,
    /// Store the trace of optimizer
    pub optimizer_trace: Arc<Mutex<Vec<String>>>,
    /// Store correlated id
    pub next_correlated_id: AtomicU32,
    /// Store options or properties from the `with` clause
    pub with_options: WithOptions,
}

#[derive(Clone, Debug)]
pub struct OptimizerContextRef {
    inner: Arc<OptimizerContext>,
}

impl !Sync for OptimizerContextRef {}

impl From<OptimizerContext> for OptimizerContextRef {
    fn from(inner: OptimizerContext) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl OptimizerContextRef {
    pub fn inner(&self) -> &OptimizerContext {
        &self.inner
    }

    pub fn next_plan_node_id(&self) -> PlanNodeId {
        // It's safe to use `fetch_add` and `Relaxed` ordering since we have marked
        // `QueryContextRef` not `Sync`.
        let next_id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
        PlanNodeId(next_id)
    }

    pub fn next_correlated_id(&self) -> CorrelatedId {
        self.inner
            .next_correlated_id
            .fetch_add(1, Ordering::Relaxed)
    }

    pub fn is_explain_verbose(&self) -> bool {
        self.inner.explain_verbose.load(Ordering::Acquire)
    }

    pub fn is_explain_trace(&self) -> bool {
        self.inner.explain_trace.load(Ordering::Acquire)
    }

    pub fn trace(&self, str: impl Into<String>) {
        let mut guard = self.inner.optimizer_trace.lock().unwrap();
        guard.push(str.into());
        guard.push("\n".to_string());
    }

    pub fn take_trace(&self) -> Vec<String> {
        let mut guard = self.inner.optimizer_trace.lock().unwrap();
        guard.drain(..).collect()
    }
}

impl OptimizerContext {
    pub fn new(session_ctx: Arc<SessionImpl>, sql: Arc<str>, with_options: WithOptions) -> Self {
        Self {
            session_ctx,
            next_id: AtomicI32::new(0),
            sql,
            explain_verbose: AtomicBool::new(false),
            explain_trace: AtomicBool::new(false),
            optimizer_trace: Arc::new(Mutex::new(vec![])),
            next_correlated_id: AtomicU32::new(1),
            with_options,
        }
    }

    // TODO(TaoWu): Remove the async.
    #[cfg(test)]
    #[expect(clippy::unused_async)]
    pub async fn mock() -> OptimizerContextRef {
        Self {
            session_ctx: Arc::new(SessionImpl::mock()),
            next_id: AtomicI32::new(0),
            sql: Arc::from(""),
            explain_verbose: AtomicBool::new(false),
            explain_trace: AtomicBool::new(false),
            optimizer_trace: Arc::new(Mutex::new(vec![])),
            next_correlated_id: AtomicU32::new(1),
            with_options: Default::default(),
        }
        .into()
    }
}

impl std::fmt::Debug for OptimizerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QueryContext {{ current id = {} }}",
            self.next_id.load(Ordering::Relaxed)
        )
    }
}

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
}

/// TODO: Find a way to delete session from map when session is closed.
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
        );
        let server_addr = HostAddr::try_from("127.0.0.1:4565").unwrap();
        let client_pool = Arc::new(ComputeClientPool::default());
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
        }
    }

    pub async fn init(
        opts: &FrontendOpts,
    ) -> Result<(Self, JoinHandle<()>, JoinHandle<()>, Sender<()>)> {
        let config: FrontendConfig = load_config(&opts.config_path).unwrap();
        tracing::info!("Starting frontend node with config {:?}", config);

        let frontend_address: HostAddr = opts
            .client_address
            .as_ref()
            .unwrap_or_else(|| {
                tracing::warn!("Client address is not specified, defaulting to host address");
                &opts.host
            })
            .parse()
            .unwrap();
        tracing::info!("Client address is {}", frontend_address);

        // Register in meta by calling `AddWorkerNode` RPC.
        let meta_client = MetaClient::register_new(
            opts.meta_addr.clone().as_str(),
            WorkerType::Frontend,
            &frontend_address,
            0,
        )
        .await?;

        let (heartbeat_join_handle, heartbeat_shutdown_sender) = MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval_ms as u64),
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
        let observer_join_handle = observer_manager.start().await?;

        meta_client.activate(&frontend_address).await?;

        let client_pool = Arc::new(ComputeClientPool::new(config.server.connection_pool_size));

        let registry = prometheus::Registry::new();
        monitor_process(&registry).unwrap();
        let frontend_metrics = Arc::new(FrontendMetrics::new(registry.clone()));

        if opts.metrics_level > 0 {
            MetricsManager::boot_metrics_service(opts.prometheus_listener_addr.clone(), registry);
        }

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
            },
            observer_join_handle,
            heartbeat_join_handle,
            heartbeat_shutdown_sender,
        ))
    }

    /// Get a reference to the frontend env's catalog writer.
    #[expect(clippy::explicit_auto_deref)]
    pub fn catalog_writer(&self) -> &dyn CatalogWriter {
        &*self.catalog_writer
    }

    /// Get a reference to the frontend env's catalog reader.
    pub fn catalog_reader(&self) -> &CatalogReader {
        &self.catalog_reader
    }

    /// Get a reference to the frontend env's user info writer.
    #[expect(clippy::explicit_auto_deref)]
    pub fn user_info_writer(&self) -> &dyn UserInfoWriter {
        &*self.user_info_writer
    }

    /// Get a reference to the frontend env's user info reader.
    pub fn user_info_reader(&self) -> &UserInfoReader {
        &self.user_info_reader
    }

    #[expect(clippy::explicit_auto_deref)]
    pub fn worker_node_manager(&self) -> &WorkerNodeManager {
        &*self.worker_node_manager
    }

    pub fn worker_node_manager_ref(&self) -> WorkerNodeManagerRef {
        self.worker_node_manager.clone()
    }

    #[expect(clippy::explicit_auto_deref)]
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
            config_map: RwLock::new(Default::default()),
            id,
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

    pub fn set_config(&self, key: &str, value: &str) -> Result<()> {
        self.config_map.write().set(key, value)
    }

    pub fn session_id(&self) -> SessionId {
        self.id
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

    /// Used when cancel request happened, returned corresponding session ref.
    fn cancel_queries_in_session(&self, session_id: SessionId) {
        self.env.query_manager.cancel_queries_in_session(session_id);
    }

    fn end_session(&self, session: &Self::Session) {
        self.delete_session(&session.session_id());
    }
}

impl SessionManagerImpl {
    pub async fn new(opts: &FrontendOpts) -> Result<Self> {
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

        // format: indicate the query PgResponse format (Only meaningful for SELECT queries).
        // false: TEXT
        // true: BINARY
        format: bool,
    ) -> std::result::Result<PgResponse<PgResponseStream>, BoxedError> {
        // Parse sql.
        let mut stmts = Parser::parse_sql(sql).map_err(|e| {
            tracing::error!("failed to parse sql:\n{}:\n{}", sql, e);
            e
        })?;
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
        let rsp = handle(self, stmt, sql, format).await.map_err(|e| {
            tracing::error!("failed to handle sql:\n{}:\n{}", sql, e);
            e
        })?;
        Ok(rsp)
    }

    async fn infer_return_type(
        self: Arc<Self>,
        sql: &str,
    ) -> std::result::Result<Vec<PgFieldDescriptor>, BoxedError> {
        // Parse sql.
        let mut stmts = Parser::parse_sql(sql).map_err(|e| {
            tracing::error!("failed to parse sql:\n{}:\n{}", sql, e);
            e
        })?;
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
        // This part refers from src/frontend/handler/ so the Vec<PgFieldDescripyor> is same as
        // result of run_statement().
        let rsp = match stmt {
            Statement::Query(_) => infer(self, stmt, sql).map_err(|e| {
                tracing::error!("failed to handle sql:\n{}:\n{}", sql, e);
                e
            })?,
            Statement::ShowObjects(show_object) => match show_object {
                ShowObject::Columns { table: _ } => {
                    vec![
                        PgFieldDescriptor::new("Name".to_owned(), TypeOid::Varchar),
                        PgFieldDescriptor::new("Type".to_owned(), TypeOid::Varchar),
                    ]
                }
                _ => {
                    vec![PgFieldDescriptor::new("Name".to_owned(), TypeOid::Varchar)]
                }
            },
            Statement::ShowVariable { variable } => {
                let name = &variable[0].value.to_lowercase();
                if name.eq_ignore_ascii_case("ALL") {
                    vec![
                        PgFieldDescriptor::new("Name".to_string(), TypeOid::Varchar),
                        PgFieldDescriptor::new("Setting".to_string(), TypeOid::Varchar),
                        PgFieldDescriptor::new("Description".to_string(), TypeOid::Varchar),
                    ]
                } else {
                    vec![PgFieldDescriptor::new(
                        name.to_ascii_lowercase(),
                        TypeOid::Varchar,
                    )]
                }
            }
            Statement::Describe { name: _ } => {
                vec![
                    PgFieldDescriptor::new("Name".to_owned(), TypeOid::Varchar),
                    PgFieldDescriptor::new("Type".to_owned(), TypeOid::Varchar),
                ]
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

    fn end_query(&self, value_stream: &PgResponseStream) -> PsqlResult<()> {
        if let PgResponseStream::DistributedQuery(DataChunkToRowSetAdapter {
            chunk_stream, ..
        }) = value_stream
        {
            let query_id = chunk_stream.query_id();
            self.env().query_manager().delete_query(query_id);
        }
        Ok(())
    }
}

/// Returns row description of the statement
fn infer(session: Arc<SessionImpl>, stmt: Statement, sql: &str) -> Result<Vec<PgFieldDescriptor>> {
    let context = OptimizerContext::new(session, Arc::from(sql), WithOptions::try_from(&stmt)?);
    let session = context.session_ctx.clone();

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

#[cfg(test)]
mod tests {
    use assert_impl::assert_impl;

    use crate::session::OptimizerContextRef;

    #[test]
    fn check_query_context_ref() {
        assert_impl!(Send: OptimizerContextRef);
        assert_impl!(!Sync: OptimizerContextRef);
    }
}
