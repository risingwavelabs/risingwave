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
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use pgwire::pg_field_descriptor::PgFieldDescriptor;
use pgwire::pg_response::PgResponse;
use pgwire::pg_server::{BoxedError, Session, SessionManager, UserAuthenticator};
use rand::RngCore;
#[cfg(test)]
use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SUPPER_USER};
use risingwave_common::config::FrontendConfig;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_common::session_config::{DELTA_JOIN, IMPLICIT_FLUSH, QUERY_MODE};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_pb::user::auth_info::EncryptionType;
use risingwave_rpc_client::{ComputeClientPool, ComputeClientPoolRef, MetaClient};
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::Parser;
use tokio::sync::oneshot::Sender;
use tokio::sync::{watch, OnceCell};
use tokio::task::JoinHandle;

use crate::binder::Binder;
use crate::catalog::catalog_service::{CatalogReader, CatalogWriter, CatalogWriterImpl};
use crate::catalog::root_catalog::Catalog;
use crate::handler::handle;
use crate::handler::util::to_pg_field;
use crate::meta_client::{FrontendMetaClient, FrontendMetaClientImpl};
use crate::observer::observer_manager::ObserverManager;
use crate::optimizer::plan_node::PlanNodeId;
use crate::planner::Planner;
use crate::scheduler::worker_node_manager::{WorkerNodeManager, WorkerNodeManagerRef};
use crate::scheduler::{HummockSnapshotManager, HummockSnapshotManagerRef, QueryManager};
use crate::test_utils::MockUserInfoWriter;
use crate::user::user_authentication::md5_hash_with_salt;
use crate::user::user_manager::UserInfoManager;
use crate::user::user_service::{UserInfoReader, UserInfoWriter, UserInfoWriterImpl};
use crate::FrontendOpts;

pub struct OptimizerContext {
    pub session_ctx: Arc<SessionImpl>,
    // We use `AtomicI32` here because  `Arc<T>` implements `Send` only when `T: Send + Sync`.
    pub next_id: AtomicI32,
    /// For debugging purposes, store the SQL string in Context
    pub sql: Arc<str>,
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
}

impl OptimizerContext {
    pub fn new(session_ctx: Arc<SessionImpl>, sql: Arc<str>) -> Self {
        Self {
            session_ctx,
            next_id: AtomicI32::new(0),
            sql,
        }
    }

    // TODO(TaoWu): Remove the async.
    #[cfg(test)]
    pub async fn mock() -> OptimizerContextRef {
        Self {
            session_ctx: Arc::new(SessionImpl::mock()),
            next_id: AtomicI32::new(0),
            sql: Arc::from(""),
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

fn load_config(opts: &FrontendOpts) -> FrontendConfig {
    if opts.config_path.is_empty() {
        return FrontendConfig::default();
    }

    let config_path = PathBuf::from(opts.config_path.to_owned());
    FrontendConfig::init(config_path).unwrap()
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
    hummock_snapshot_manager: HummockSnapshotManagerRef,
    query_manager: Arc<OnceCell<QueryManager>>,
    server_addr: HostAddr,
    compute_client_pool: ComputeClientPoolRef,
}

impl FrontendEnv {
    pub async fn init(
        opts: &FrontendOpts,
    ) -> Result<(Self, JoinHandle<()>, JoinHandle<()>, Sender<()>)> {
        let meta_client = MetaClient::new(opts.meta_addr.clone().as_str()).await?;
        Self::with_meta_client(meta_client, opts).await
    }

    pub fn mock() -> Self {
        use crate::test_utils::{MockCatalogWriter, MockFrontendMetaClient};

        let catalog = Arc::new(RwLock::new(Catalog::default()));
        let catalog_writer = Arc::new(MockCatalogWriter::new(catalog.clone()));
        let catalog_reader = CatalogReader::new(catalog);
        let user_info_manager = Arc::new(RwLock::new(UserInfoManager::default()));
        let user_info_writer = Arc::new(MockUserInfoWriter::new(user_info_manager.clone()));
        let user_info_reader = UserInfoReader::new(user_info_manager);
        let worker_node_manager = Arc::new(WorkerNodeManager::mock(vec![]));
        let meta_client = Arc::new(MockFrontendMetaClient {});
        let hummock_snapshot_manager = Arc::new(HummockSnapshotManager::new(meta_client.clone()));
        let compute_client_pool = Arc::new(ComputeClientPool::new(u64::MAX));
        let server_addr = HostAddr::try_from("127.0.0.1:4565").unwrap();
        Self {
            meta_client,
            catalog_writer,
            catalog_reader,
            user_info_writer,
            user_info_reader,
            worker_node_manager,
            hummock_snapshot_manager,
            query_manager: Arc::new(OnceCell::new()),
            server_addr,
            compute_client_pool,
        }
    }

    pub async fn with_meta_client(
        mut meta_client: MetaClient,
        opts: &FrontendOpts,
    ) -> Result<(Self, JoinHandle<()>, JoinHandle<()>, Sender<()>)> {
        let config = load_config(opts);
        tracing::info!("Starting frontend node with config {:?}", config);

        let frontend_address: HostAddr = opts
            .client_address
            .as_ref()
            .unwrap_or(&opts.host)
            .parse()
            .unwrap();
        // Register in meta by calling `AddWorkerNode` RPC.
        meta_client
            .register(&frontend_address, WorkerType::Frontend)
            .await?;

        let (heartbeat_join_handle, heartbeat_shutdown_sender) = MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval_ms as u64),
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
        let compute_client_pool = Arc::new(ComputeClientPool::new(u64::MAX));

        let user_info_manager = Arc::new(RwLock::new(UserInfoManager::default()));
        let (user_info_updated_tx, user_info_updated_rx) = watch::channel(0);
        let user_info_reader = UserInfoReader::new(user_info_manager.clone());
        let user_info_writer = Arc::new(UserInfoWriterImpl::new(
            meta_client.clone(),
            user_info_updated_rx,
        ));

        let observer_manager = ObserverManager::new(
            meta_client.clone(),
            frontend_address.clone(),
            worker_node_manager.clone(),
            catalog,
            catalog_updated_tx,
            user_info_manager,
            user_info_updated_tx,
            hummock_snapshot_manager.clone(),
        )
        .await;
        let observer_join_handle = observer_manager.start().await?;

        meta_client.activate(&frontend_address).await?;

        Ok((
            Self {
                catalog_reader,
                catalog_writer,
                user_info_reader,
                user_info_writer,
                worker_node_manager,
                meta_client: frontend_meta_client,
                hummock_snapshot_manager,
                query_manager: Arc::new(OnceCell::new()),
                server_addr: frontend_address,
                compute_client_pool,
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
        &*self.worker_node_manager
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

    pub fn hummock_snapshot_manager(&self) -> &HummockSnapshotManager {
        &*self.hummock_snapshot_manager
    }

    pub fn hummock_snapshot_manager_ref(&self) -> HummockSnapshotManagerRef {
        self.hummock_snapshot_manager.clone()
    }

    pub fn server_address(&self) -> &HostAddr {
        &self.server_addr
    }

    pub async fn query_manager(&self) -> &QueryManager {
        self.query_manager
            .get_or_init(|| async move { QueryManager::new(self.clone()) })
            .await as _
    }

    pub fn compute_client_pool(&self) -> &ComputeClientPool {
        &*self.compute_client_pool
    }

    pub fn compute_client_pool_ref(&self) -> ComputeClientPoolRef {
        self.compute_client_pool.clone()
    }
}

pub struct SessionImpl {
    env: FrontendEnv,
    database: String,
    user_name: String,
    // Used for user authentication.
    user_authenticator: UserAuthenticator,
    /// Stores the value of configurations.
    config_map: RwLock<HashMap<String, ConfigEntry>>,
}

#[derive(Clone)]
pub struct ConfigEntry {
    str_val: String,
}

impl ConfigEntry {
    pub fn new(str_val: String) -> Self {
        ConfigEntry { str_val }
    }

    /// Only used for boolean configurations.
    pub fn is_set(&self, default: bool) -> bool {
        self.str_val.parse().unwrap_or(default)
    }

    pub fn get_val<V>(&self, default: V) -> V
    where
        for<'a> V: TryFrom<&'a str, Error = RwError>,
    {
        V::try_from(&self.str_val).unwrap_or(default)
    }
}

fn build_default_session_config_map() -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert(IMPLICIT_FLUSH.to_ascii_lowercase(), "false".to_string());
    m.insert(DELTA_JOIN.to_ascii_lowercase(), "false".to_string());
    m.insert(QUERY_MODE.to_ascii_lowercase(), "distributed".to_string());
    m
}

lazy_static::lazy_static! {
    static ref DEFAULT_SESSION_CONFIG_MAP: HashMap<String, String> = {
        build_default_session_config_map()
    };
}

impl SessionImpl {
    pub fn new(
        env: FrontendEnv,
        database: String,
        user_name: String,
        user_authenticator: UserAuthenticator,
    ) -> Self {
        Self {
            env,
            database,
            user_name,
            user_authenticator,
            config_map: Self::init_config_map(),
        }
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        Self {
            env: FrontendEnv::mock(),
            database: DEFAULT_DATABASE_NAME.to_string(),
            user_name: DEFAULT_SUPPER_USER.to_string(),
            user_authenticator: UserAuthenticator::None,
            config_map: Self::init_config_map(),
        }
    }

    pub fn env(&self) -> &FrontendEnv {
        &self.env
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn user_name(&self) -> &str {
        &self.user_name
    }

    /// Set configuration values in this session.
    /// For example, `set_config("RW_IMPLICIT_FLUSH", true)` will implicit flush for every inserts.
    pub fn set_config(&self, key: &str, val: &str) -> Result<()> {
        let lower_key = key.to_ascii_lowercase();
        self.config_map
            .read()
            .get(&lower_key)
            .ok_or_else(|| ErrorCode::UnrecognizedConfigurationParameter(key.to_string()))?;
        self.config_map
            .write()
            .insert(lower_key, ConfigEntry::new(val.to_string()));
        Ok(())
    }

    /// Get configuration values in this session.
    pub fn get_config(&self, key: &str) -> Option<ConfigEntry> {
        let key = key.to_ascii_lowercase();
        let reader = self.config_map.read();
        reader.get(&key).cloned()
    }

    fn init_config_map() -> RwLock<HashMap<String, ConfigEntry>> {
        let mut map = HashMap::new();
        for (key, value) in &*DEFAULT_SESSION_CONFIG_MAP {
            map.insert(key.clone(), ConfigEntry::new(value.clone()));
        }
        RwLock::new(map)
    }
}

pub struct SessionManagerImpl {
    env: FrontendEnv,
    observer_join_handle: JoinHandle<()>,
    heartbeat_join_handle: JoinHandle<()>,
    _heartbeat_shutdown_sender: Sender<()>,
}

impl SessionManager for SessionManagerImpl {
    type Session = SessionImpl;

    fn connect(
        &self,
        database: &str,
        user_name: &str,
    ) -> std::result::Result<Arc<Self::Session>, BoxedError> {
        let catalog_reader = self.env.catalog_reader();
        let reader = catalog_reader.read_guard();
        if reader.get_database_by_name(database).is_err() {
            return Err(Box::new(Error::new(
                ErrorKind::InvalidInput,
                format!("Not found database name: {}", database),
            )));
        }
        let user_reader = self.env.user_info_reader();
        let reader = user_reader.read_guard();
        if let Some(user) = reader.get_user_by_name(user_name) {
            if !user.can_login {
                return Err(Box::new(Error::new(
                    ErrorKind::InvalidInput,
                    format!("User {} is not allowed to login", user_name),
                )));
            }
            let authenticator = match &user.auth_info {
                None => UserAuthenticator::None,
                Some(auth_info) => {
                    if auth_info.encryption_type == EncryptionType::Plaintext as i32 {
                        UserAuthenticator::ClearText(auth_info.encrypted_value.clone())
                    } else if auth_info.encryption_type == EncryptionType::Md5 as i32 {
                        let mut salt = [0; 4];
                        let mut rng = rand::thread_rng();
                        rng.fill_bytes(&mut salt);
                        UserAuthenticator::MD5WithSalt {
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

            Ok(SessionImpl::new(
                self.env.clone(),
                database.to_string(),
                user_name.to_string(),
                authenticator,
            )
            .into())
        } else {
            Err(Box::new(Error::new(
                ErrorKind::InvalidInput,
                format!("Role {} does not exist", user_name),
            )))
        }
    }
}

impl SessionManagerImpl {
    pub async fn new(opts: &FrontendOpts) -> Result<Self> {
        let (env, join_handle, heartbeat_join_handle, heartbeat_shutdown_sender) =
            FrontendEnv::init(opts).await?;
        Ok(Self {
            env,
            observer_join_handle: join_handle,
            heartbeat_join_handle,
            _heartbeat_shutdown_sender: heartbeat_shutdown_sender,
        })
    }

    /// Used in unit test. Called before `LocalMeta::stop`.
    pub fn terminate(&self) {
        self.observer_join_handle.abort();
        self.heartbeat_join_handle.abort();
    }
}

#[async_trait::async_trait]
impl Session for SessionImpl {
    async fn run_statement(
        self: Arc<Self>,
        sql: &str,
    ) -> std::result::Result<PgResponse, BoxedError> {
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
        let rsp = handle(self, stmt, sql).await.map_err(|e| {
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
        let rsp = infer(self, stmt, sql).map_err(|e| {
            tracing::error!("failed to handle sql:\n{}:\n{}", sql, e);
            e
        })?;
        Ok(rsp)
    }

    fn user_authenticator(&self) -> &UserAuthenticator {
        &self.user_authenticator
    }
}

/// Returns row description of the statement
fn infer(session: Arc<SessionImpl>, stmt: Statement, sql: &str) -> Result<Vec<PgFieldDescriptor>> {
    let context = OptimizerContext::new(session, Arc::from(sql));
    let session = context.session_ctx.clone();

    let bound = {
        let mut binder = Binder::new(
            session.env().catalog_reader().read_guard(),
            session.database().to_string(),
        );
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
