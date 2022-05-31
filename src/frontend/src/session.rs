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
use std::marker::Sync;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use pgwire::pg_response::PgResponse;
use pgwire::pg_server::{BoxedError, Session, SessionManager};
use risingwave_common::config::FrontendConfig;
use risingwave_common::error::{Result, RwError};
use risingwave_common::util::addr::HostAddr;
use risingwave_pb::common::WorkerType;
use risingwave_rpc_client::{ComputeClientPool, MetaClient};
use risingwave_sqlparser::parser::Parser;
use tokio::sync::oneshot::Sender;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::catalog::catalog_service::{CatalogReader, CatalogWriter, CatalogWriterImpl};
use crate::catalog::root_catalog::Catalog;
use crate::handler::dml::IMPLICIT_FLUSH;
use crate::handler::handle;
use crate::meta_client::{FrontendMetaClient, FrontendMetaClientImpl};
use crate::observer::observer_manager::ObserverManager;
use crate::optimizer::plan_node::PlanNodeId;
use crate::scheduler::worker_node_manager::{WorkerNodeManager, WorkerNodeManagerRef};
use crate::scheduler::{HummockSnapshotManager, HummockSnapshotManagerRef, QueryManager};
use crate::test_utils::MockUserInfoWriter;
use crate::user::root_user::UserInfoManager;
use crate::user::user_service::{UserInfoReader, UserInfoWriter, UserInfoWriterImpl};
use crate::FrontendOpts;

pub struct OptimizerContext {
    pub session_ctx: Arc<SessionImpl>,
    // We use `AtomicI32` here because  `Arc<T>` implements `Send` only when `T: Send + Sync`.
    pub next_id: AtomicI32,
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
    pub fn new(session_ctx: Arc<SessionImpl>) -> Self {
        Self {
            session_ctx,
            next_id: AtomicI32::new(0),
        }
    }

    // TODO(TaoWu): Remove the async.
    #[cfg(test)]
    pub async fn mock() -> OptimizerContextRef {
        Self {
            session_ctx: Arc::new(SessionImpl::mock()),
            next_id: AtomicI32::new(0),
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
    query_manager: QueryManager,
    hummock_snapshot_manager: HummockSnapshotManagerRef,
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
        let query_manager = QueryManager::new(
            worker_node_manager.clone(),
            hummock_snapshot_manager.clone(),
            compute_client_pool,
        );
        Self {
            meta_client,
            catalog_writer,
            catalog_reader,
            user_info_writer,
            user_info_reader,
            worker_node_manager,
            query_manager,
            hummock_snapshot_manager,
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

        let worker_node_manager = Arc::new(WorkerNodeManager::new(meta_client.clone()).await?);

        let frontend_meta_client = Arc::new(FrontendMetaClientImpl(meta_client.clone()));
        let hummock_snapshot_manager =
            Arc::new(HummockSnapshotManager::new(frontend_meta_client.clone()));
        let compute_client_pool = Arc::new(ComputeClientPool::new(u64::MAX));
        let query_manager = QueryManager::new(
            worker_node_manager.clone(),
            hummock_snapshot_manager.clone(),
            compute_client_pool,
        );

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
                query_manager,
                hummock_snapshot_manager,
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

    pub fn query_manager(&self) -> &QueryManager {
        &self.query_manager
    }

    pub fn hummock_snapshot_manager(&self) -> &HummockSnapshotManagerRef {
        &self.hummock_snapshot_manager
    }
}

pub struct SessionImpl {
    env: FrontendEnv,
    database: String,
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

impl SessionImpl {
    pub fn new(env: FrontendEnv, database: String) -> Self {
        Self {
            env,
            database,
            config_map: Self::init_config_map(),
        }
    }

    #[cfg(test)]
    pub fn mock() -> Self {
        Self {
            env: FrontendEnv::mock(),
            database: "dev".to_string(),
            config_map: Self::init_config_map(),
        }
    }

    pub fn env(&self) -> &FrontendEnv {
        &self.env
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    /// Set configuration values in this session.
    /// For example, `set_config("RW_IMPLICIT_FLUSH", true)` will implicit flush for every inserts.
    pub fn set_config(&self, key: &str, val: &str) {
        self.config_map
            .write()
            .insert(key.to_string(), ConfigEntry::new(val.to_string()));
    }

    /// Get configuration values in this session.
    pub fn get_config(&self, key: &str) -> Option<ConfigEntry> {
        let reader = self.config_map.read();
        reader.get(key).cloned()
    }

    fn init_config_map() -> RwLock<HashMap<String, ConfigEntry>> {
        let mut map = HashMap::new();
        // FIXME: May need better init way + default config.
        map.insert(
            IMPLICIT_FLUSH.to_string(),
            ConfigEntry::new("false".to_string()),
        );
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

    fn connect(&self, database: &str) -> std::result::Result<Arc<Self::Session>, BoxedError> {
        Ok(SessionImpl::new(self.env.clone(), database.to_string()).into())
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
        // With pgwire, there would be at most 1 statement in the vec.
        assert!(stmts.len() <= 1);
        if stmts.is_empty() {
            return Ok(PgResponse::new(
                pgwire::pg_response::StatementType::EMPTY,
                0,
                vec![],
                vec![],
            ));
        }
        let stmt = stmts.swap_remove(0);
        let rsp = handle(self, stmt).await.map_err(|e| {
            tracing::error!("failed to handle sql:\n{}:\n{}", sql, e);
            e
        })?;
        Ok(rsp)
    }
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
