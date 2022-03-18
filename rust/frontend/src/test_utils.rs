use std::any::Any;
use std::cell::RefCell;
use std::ffi::OsString;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{default, mem};

use pgwire::pg_response::PgResponse;
use pgwire::pg_server::{Session, SessionManager};
use risingwave_common::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};
use risingwave_common::error::{Result, ToRwResult};
use risingwave_meta::cluster::StoredClusterManager;
use risingwave_meta::manager::{
    MemEpochGenerator, MetaSrvEnv, NotificationManager, NotificationManagerRef,
    StoredCatalogManager,
};
use risingwave_meta::rpc::{CatalogServiceImpl, ClusterServiceImpl, NotificationServiceImpl};
use risingwave_meta::storage::MemStore;
use risingwave_pb::catalog::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateMaterializedSourceRequest,
    CreateMaterializedSourceResponse, CreateMaterializedViewRequest,
    CreateMaterializedViewResponse, CreateSchemaRequest, CreateSchemaResponse,
    Database as ProstDatabase, Schema as ProstSchema,
};
use risingwave_pb::common::worker_node::State;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::meta::cluster_service_server::ClusterService;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::{
    subscribe_response, ActivateWorkerNodeRequest, ActivateWorkerNodeResponse,
    AddWorkerNodeRequest, AddWorkerNodeResponse, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse,
    ListAllNodesRequest, ListAllNodesResponse, SubscribeRequest, SubscribeResponse,
};
use risingwave_rpc_client::{MetaClient, MetaClientInner, NotificationStream};
use risingwave_sqlparser::ast::Statement;
use risingwave_sqlparser::parser::Parser;
use tokio::sync::mpsc::{
    self, unbounded_channel, Receiver, Sender, UnboundedReceiver, UnboundedSender, UnboundedSender,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tonic::Request;

use crate::binder::Binder;
use crate::optimizer::PlanRef;
use crate::planner::Planner;
use crate::session::{FrontendEnv, QueryContext, SessionImpl};
use crate::FrontendOpts;

/// LocalFrontend is an embedded frontend without starting meta and without
/// starting frontend as a tcp server. a mock ['SessionManagerImpl'](super::SessionManagerImpl) for
/// test
pub struct LocalFrontend {
    pub opts: FrontendOpts,
    pub observer_join_handle: JoinHandle<()>,
    _heartbeat_join_handle: JoinHandle<()>,
    _heartbeat_shutdown_sender: UnboundedSender<()>,
    env: FrontendEnv,
}

impl SessionManager for LocalFrontend {
    fn connect(&self) -> Arc<dyn Session> {
        self.session_ref()
    }
}

impl LocalFrontend {
    pub async fn new(opts: FrontendOpts) -> Self {
        let meta_client = MetaClient::mock(FrontendMockMetaClient::new().await);
        let (env, observer_join_handle, heartbeat_join_handle, heartbeat_shutdown_sender) =
            FrontendEnv::with_meta_client(meta_client, &opts)
                .await
                .unwrap();
        Self {
            opts,
            env,
            observer_join_handle,
            _heartbeat_join_handle: heartbeat_join_handle,
            _heartbeat_shutdown_sender: heartbeat_shutdown_sender,
        }
    }

    pub async fn run_sql(
        &self,
        sql: impl Into<String>,
    ) -> std::result::Result<PgResponse, Box<dyn std::error::Error + Send + Sync>> {
        let sql = sql.into();
        self.session_ref().run_statement(sql.as_str()).await
    }

    /// Convert a sql (must be an `Query`) into an unoptimized batch plan.
    pub async fn to_batch_plan(&self, sql: impl Into<String>) -> Result<PlanRef> {
        let statements = Parser::parse_sql(&sql.into()).unwrap();
        let statement = statements.get(0).unwrap();
        if let Statement::Query(query) = statement {
            let session = self.session_ref().clone();

            let bound = {
                let mut binder = Binder::new(
                    session.env().catalog_reader().read_guard(),
                    session.database().to_string(),
                );
                binder.bind(Statement::Query(query.clone()))?
            };
            Ok(
                Planner::new(Rc::new(RefCell::new(QueryContext::new(session))))
                    .plan(bound)
                    .unwrap()
                    .gen_batch_query_plan(),
            )
        } else {
            unreachable!()
        }
    }

    pub fn session_ref(&self) -> Arc<SessionImpl> {
        Arc::new(SessionImpl::new(
            self.env.clone(),
            DEFAULT_DATABASE_NAME.to_string(),
        ))
    }
}

pub struct FrontendMockMetaClient {
    // cluster_srv: ClusterServiceImpl<MemStore>,
    mock_catalog: Mutex<SingleFrontendMockNotifyService>,
    // TODO: now every thing use common id generator and may can't trigger some bug.
    mock_id: AtomicU32,
}

impl FrontendMockMetaClient {
    fn gen_id(&self) -> u32 {
        self.mock_id.fetch_add(1, Ordering::SeqCst)
    }

    pub async fn new() -> Self {
        // let meta_store = Arc::new(MemStore::default());
        // let epoch_generator = Arc::new(MemEpochGenerator::default());
        // let env = MetaSrvEnv::<MemStore>::new(meta_store.clone(), epoch_generator.clone()).await;

        // let notification_manager = Arc::new(NotificationManager::new());

        // let cluster_manager = Arc::new(
        //     StoredClusterManager::new(
        //         env.clone(),
        //         None,
        //         notification_manager.clone(),
        //         Duration::from_secs(3600),
        //     )
        //     .await
        //     .unwrap(),
        // );

        let meta = Self {
            // cluster_srv: ClusterServiceImpl::<MemStore>::new(cluster_manager),
            mock_catalog: Mutex::new(SingleFrontendMockNotifyService::new()),
            mock_id: AtomicU32::new(1),
        };
        let CreateDatabaseResponse {
            status: _,
            database_id,
            version: _,
        } = meta
            .create_database(CreateDatabaseRequest {
                db: Some(ProstDatabase {
                    id: 0,
                    name: DEFAULT_DATABASE_NAME.to_string(),
                }),
            })
            .await
            .unwrap();
        meta.create_schema(CreateSchemaRequest {
            schema: Some(ProstSchema {
                id: 0,
                name: DEFAULT_SCHEMA_NAME.to_string(),
                database_id,
            }),
        })
        .await
        .unwrap();

        meta
    }
}

#[async_trait::async_trait]
impl MetaClientInner for FrontendMockMetaClient {
    async fn list_all_nodes(&self, req: ListAllNodesRequest) -> Result<ListAllNodesResponse> {
        Ok(ListAllNodesResponse::default())
        // Ok(self
        //     .cluster_srv
        //     .list_all_nodes(Request::new(req))
        //     .await
        //     .to_rw_result()?
        //     .into_inner())
    }

    async fn add_worker_node(&self, req: AddWorkerNodeRequest) -> Result<AddWorkerNodeResponse> {
        let node = WorkerNode {
            id: self.gen_id(),
            r#type: req.worker_type,
            host: req.host,
            ..Default::default()
        };
        Ok(AddWorkerNodeResponse {
            status: None,
            node: Some(node),
        })
        // Ok(self
        //     .cluster_srv
        //     .add_worker_node(Request::new(req))
        //     .await
        //     .to_rw_result()?
        //     .into_inner())
    }

    async fn activate_worker_node(
        &self,
        req: ActivateWorkerNodeRequest,
    ) -> Result<ActivateWorkerNodeResponse> {
        Ok(ActivateWorkerNodeResponse::default())
        // Ok(self
        //     .cluster_srv
        //     .activate_worker_node(Request::new(req))
        //     .await
        //     .to_rw_result()?
        //     .into_inner())
    }

    async fn delete_worker_node(
        &self,
        req: DeleteWorkerNodeRequest,
    ) -> Result<DeleteWorkerNodeResponse> {
        Ok(DeleteWorkerNodeResponse::default())
        // Ok(self
        //     .cluster_srv
        //     .delete_worker_node(Request::new(req))
        //     .await
        //     .to_rw_result()?
        //     .into_inner())
    }

    async fn subscribe(&self, req: SubscribeRequest) -> Result<Box<dyn NotificationStream>> {
        Ok(self.mock_catalog.lock().await.subscribe().await)
    }

    // TODO:
    async fn create_database(&self, req: CreateDatabaseRequest) -> Result<CreateDatabaseResponse> {
        let database_id = self.gen_id();
        let mut db = req.db.unwrap();
        db.id = database_id;
        let version = self
            .mock_catalog
            .lock()
            .await
            .notify(Operation::Add, &Info::DatabaseV2(db))
            .await;
        Ok(CreateDatabaseResponse {
            status: None,
            database_id,
            version,
        })
    }

    async fn create_schema(&self, req: CreateSchemaRequest) -> Result<CreateSchemaResponse> {
        let schema_id = self.gen_id();
        let mut schema = req.schema.unwrap();
        schema.id = schema_id;
        let version = self
            .mock_catalog
            .lock()
            .await
            .notify(Operation::Add, &Info::SchemaV2(schema))
            .await;
        Ok(CreateSchemaResponse {
            status: None,
            version,
            schema_id,
        })
    }

    async fn create_materialized_source(
        &self,
        req: CreateMaterializedSourceRequest,
    ) -> Result<CreateMaterializedSourceResponse> {
        let CreateMaterializedSourceRequest {
            source,
            materialized_view: mv,
            stream_node: _,
        } = req;
        let source_id = self.gen_id();
        let table_id = self.gen_id();
        let mut source = source.unwrap();
        let mut table = mv.unwrap();
        source.id = source_id;
        table.id = table_id;
        self.mock_catalog
            .lock()
            .await
            .notify(Operation::Add, &Info::TableV2(table))
            .await;
        let version = self
            .mock_catalog
            .lock()
            .await
            .notify(Operation::Add, &Info::Source(source))
            .await;
        Ok(CreateMaterializedSourceResponse {
            status: None,
            source_id,
            table_id,
            version,
        })
    }

    async fn create_materialized_view(
        &self,
        req: CreateMaterializedViewRequest,
    ) -> Result<CreateMaterializedViewResponse> {
        let CreateMaterializedViewRequest {
            materialized_view: mv,
            stream_node: _,
        } = req;
        let table_id = self.gen_id();
        let mut table = mv.unwrap();
        table.id = table_id;
        let version = self
            .mock_catalog
            .lock()
            .await
            .notify(Operation::Add, &Info::TableV2(table))
            .await;

        Ok(CreateMaterializedViewResponse {
            status: None,
            table_id,
            version,
        })
    }
}

pub type Notification = std::result::Result<SubscribeResponse, tonic::Status>;

/// it is just correct when only one frontend and will do nothing check on catalog request, and you
/// might to use `Mutex<SingleFrontendMockNotifyService>`, we will remove it soon.
struct SingleFrontendMockNotifyService {
    cur_version: u64,
    notify_tx: Sender<Notification>,
    notify_rx: Option<Receiver<Notification>>,
}
impl SingleFrontendMockNotifyService {
    pub fn new() -> Self {
        let (notify_tx, notify_rx) = mpsc::channel(123456);
        Self {
            cur_version: 0,
            notify_tx,
            notify_rx: Some(notify_rx),
        }
    }

    pub async fn subscribe(&mut self) -> Box<dyn NotificationStream> {
        Box::new(
            mem::take(&mut self.notify_rx)
                .expect("there should be only one frontend use this mock"),
        )
    }

    // return the version id
    pub async fn notify(&mut self, operation: Operation, info: &Info) -> u64 {
        self.cur_version += 1;
        self.notify_tx
            .send(Ok(SubscribeResponse {
                status: None,
                operation: operation as i32,
                info: Some(info.clone()),
                // TODO: pass the version when call notify
                version: self.cur_version,
            }))
            .await
            .unwrap();
        self.cur_version
    }

    fn gen_version(&mut self) -> u64 {
        self.cur_version += 1;
        self.cur_version
    }
}
