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

use std::cell::RefCell;
use std::error::Error;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use pgwire::pg_response::PgResponse;
use pgwire::pg_server::{Session, SessionManager};
use risingwave_common::config::FrontendConfig;
use risingwave_common::error::Result;
use risingwave_pb::common::WorkerType;
use risingwave_rpc_client::MetaClient;
use risingwave_sqlparser::parser::Parser;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::catalog::catalog_service::{CatalogReader, CatalogWriter};
use crate::catalog::root_catalog::Catalog;
use crate::handler::handle;
use crate::observer::observer_manager::ObserverManager;
use crate::optimizer::plan_node::PlanNodeId;
use crate::scheduler::schedule::WorkerNodeManager;
use crate::FrontendOpts;

pub struct QueryContext {
    pub session_ctx: Arc<SessionImpl>,
    pub next_id: i32,
}
/// The reference of `QueryContext`, our system assumes that frontend will not parallel for a query,
/// so we use `RefCell` here.
pub type QueryContextRef = Rc<RefCell<QueryContext>>;

impl QueryContext {
    pub fn new(session_ctx: Arc<SessionImpl>) -> Self {
        Self {
            session_ctx,
            next_id: 0,
        }
    }

    pub fn get_id(&mut self) -> PlanNodeId {
        let ret = PlanNodeId(self.next_id);
        self.next_id += 1;
        ret
    }

    #[cfg(test)]
    pub async fn mock() -> Self {
        Self {
            session_ctx: Arc::new(SessionImpl::mock().await),
            next_id: 0,
        }
    }
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "QueryContext {{ current id = {} }}", self.next_id)
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
    meta_client: MetaClient,
    // Different session may access catalog at the same time and catalog is protected by a
    // RwLock.
    catalog_writer: CatalogWriter,
    catalog_reader: CatalogReader,
}

impl FrontendEnv {
    pub async fn init(
        opts: &FrontendOpts,
    ) -> Result<(Self, JoinHandle<()>, JoinHandle<()>, UnboundedSender<()>)> {
        let meta_client = MetaClient::new(opts.meta_addr.clone().as_str()).await?;
        Self::with_meta_client(meta_client, opts).await
    }

    #[cfg(test)]
    pub async fn mock() -> Self {
        use crate::test_utils::FrontendMockMetaClient;
        let meta_client = MetaClient::mock(FrontendMockMetaClient::new().await);
        let (_catalog_updated_tx, catalog_updated_rx) = watch::channel(0);
        let catalog = Arc::new(RwLock::new(Catalog::default()));
        let catalog_writer = CatalogWriter::new(meta_client.clone(), catalog_updated_rx);
        let catalog_reader = CatalogReader::new(catalog);
        Self {
            meta_client,
            catalog_writer,
            catalog_reader,
        }
    }

    pub async fn with_meta_client(
        mut meta_client: MetaClient,
        opts: &FrontendOpts,
    ) -> Result<(Self, JoinHandle<()>, JoinHandle<()>, UnboundedSender<()>)> {
        let config = load_config(opts);
        tracing::info!("Starting compute node with config {:?}", config);

        let host = opts.host.parse().unwrap();

        // Register in meta by calling `AddWorkerNode` RPC.
        meta_client.register(host, WorkerType::Frontend).await?;

        let (heartbeat_join_handle, heartbeat_shutdown_sender) = MetaClient::start_heartbeat_loop(
            meta_client.clone(),
            Duration::from_millis(config.server.heartbeat_interval as u64),
        );

        let (catalog_updated_tx, catalog_updated_rx) = watch::channel(0);
        let catalog = Arc::new(RwLock::new(Catalog::default()));
        let catalog_writer = CatalogWriter::new(meta_client.clone(), catalog_updated_rx);
        let catalog_reader = CatalogReader::new(catalog.clone());

        let worker_node_manager = Arc::new(WorkerNodeManager::new(meta_client.clone()).await?);

        let observer_manager = ObserverManager::new(
            meta_client.clone(),
            host,
            worker_node_manager,
            catalog,
            catalog_updated_tx,
        )
        .await;
        let observer_join_handle = observer_manager.start().await;

        meta_client.activate(host).await?;

        Ok((
            Self {
                meta_client,
                catalog_reader,
                catalog_writer,
            },
            observer_join_handle,
            heartbeat_join_handle,
            heartbeat_shutdown_sender,
        ))
    }

    pub fn meta_client(&self) -> &MetaClient {
        &self.meta_client
    }

    /// Get a reference to the frontend env's catalog writer.
    pub fn catalog_writer(&self) -> &CatalogWriter {
        &self.catalog_writer
    }

    /// Get a reference to the frontend env's catalog reader.
    pub fn catalog_reader(&self) -> &CatalogReader {
        &self.catalog_reader
    }
}

pub struct SessionImpl {
    env: FrontendEnv,
    database: String,
}

impl SessionImpl {
    pub fn new(env: FrontendEnv, database: String) -> Self {
        Self { env, database }
    }

    #[cfg(test)]
    pub async fn mock() -> Self {
        Self {
            env: FrontendEnv::mock().await,
            database: "dev".to_string(),
        }
    }

    pub fn env(&self) -> &FrontendEnv {
        &self.env
    }

    pub fn database(&self) -> &str {
        &self.database
    }
}

pub struct SessionManagerImpl {
    env: FrontendEnv,
    observer_join_handle: JoinHandle<()>,
    heartbeat_join_handle: JoinHandle<()>,
    _heartbeat_shutdown_sender: UnboundedSender<()>,
}

impl SessionManager for SessionManagerImpl {
    fn connect(
        &self,
        database: &str,
    ) -> std::result::Result<Arc<dyn Session>, Box<dyn Error + Send + Sync>> {
        Ok(Arc::new(SessionImpl::new(
            self.env.clone(),
            database.to_string(),
        )))
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
    ) -> std::result::Result<PgResponse, Box<dyn std::error::Error + Send + Sync>> {
        // Parse sql.
        let mut stmts = Parser::parse_sql(sql)?;
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
        let rsp = handle(self, stmt).await?;
        Ok(rsp)
    }
}

// TODO: with a good MockMeta and then we can open the tests.
// #[cfg(test)]
// mod tests {

//     #[tokio::test]
//     async fn test_run_statement() {
//         use std::ffi::OsString;

//         use clap::StructOpt;
//         use risingwave_meta::test_utils::LocalMeta;

//         use super::*;

//         let meta = LocalMeta::start(12008).await;
//         let args: [OsString; 0] = []; // No argument.
//         let mut opts = FrontendOpts::parse_from(args);
//         opts.meta_addr = format!("http://{}", meta.meta_addr());
//         let mgr = SessionManagerImpl::new(&opts).await.unwrap();
//         // Check default database is created.
//         assert!(mgr
//             .env
//             .catalog_manager
//             .get_database(DEFAULT_DATABASE_NAME)
//             .is_some());
//         let session = mgr.connect();
//         assert!(session.run_statement("select * from t").await.is_err());

//         mgr.terminate();
//         meta.stop().await;
//     }
// }
