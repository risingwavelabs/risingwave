use std::ffi::OsString;
use std::sync::Arc;
use std::time::Duration;

use clap::StructOpt;
use pgwire::pg_response::PgResponse;
use pgwire::pg_server::Session;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_meta::cluster::StoredClusterManager;
use risingwave_meta::manager::{
    MemEpochGenerator, MetaSrvEnv, NotificationManager, StoredCatalogManager,
};
use risingwave_meta::rpc::{CatalogServiceImpl, ClusterServiceImpl, NotificationServiceImpl};
use risingwave_meta::storage::MemStore;

use risingwave_pb::meta::cluster_service_server::ClusterService;
use risingwave_pb::meta::notification_service_server::NotificationService;
use risingwave_pb::meta::{
    ActivateWorkerNodeRequest, ActivateWorkerNodeResponse, AddWorkerNodeRequest,
    AddWorkerNodeResponse, DeleteWorkerNodeRequest,
    DeleteWorkerNodeResponse,
    ListAllNodesRequest, ListAllNodesResponse, SubscribeRequest,
};
use risingwave_rpc_client::{MetaClient, MetaClientInner, NotificationStream};
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tonic::Request;

use crate::session::{FrontendEnv, SessionImpl};
use crate::FrontendOpts;

pub struct LocalFrontend {
    pub opts: FrontendOpts,
    pub session: Arc<SessionImpl>,
    pub observer_join_handle: JoinHandle<()>,
    _heartbeat_join_handle: JoinHandle<()>,
    _heartbeat_shutdown_sender: UnboundedSender<()>,
}

impl LocalFrontend {
    pub async fn new() -> Self {
        let args: [OsString; 0] = []; // No argument.
        let opts: FrontendOpts = FrontendOpts::parse_from(args);
        let meta_client = MetaClient::mock(FrontendMockMetaClient::new().await);
        let (env, observer_join_handle, heartbeat_join_handle, heartbeat_shutdown_sender) =
            FrontendEnv::with_meta_client(meta_client, &opts)
                .await
                .unwrap();
        let session = Arc::new(SessionImpl::new(env, "dev".to_string()));
        Self {
            opts,
            session,
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
        self.session.clone().run_statement(sql.as_str()).await
    }

    pub fn session(&self) -> &SessionImpl {
        &self.session
    }

    pub fn session_ref(&self) -> Arc<SessionImpl> {
        self.session.clone()
    }
}

pub struct FrontendMockMetaClient {
    catalog_srv: CatalogServiceImpl<MemStore>,
    cluster_srv: ClusterServiceImpl<MemStore>,
    notification_srv: NotificationServiceImpl,
}

impl FrontendMockMetaClient {
    pub async fn new() -> Self {
        let meta_store = Arc::new(MemStore::default());
        let epoch_generator = Arc::new(MemEpochGenerator::default());
        let env = MetaSrvEnv::<MemStore>::new(meta_store.clone(), epoch_generator.clone()).await;

        let notification_manager = Arc::new(NotificationManager::new());
        let catalog_manager = Arc::new(
            StoredCatalogManager::new(meta_store.clone(), notification_manager.clone())
                .await
                .unwrap(),
        );

        let cluster_manager = Arc::new(
            StoredClusterManager::new(
                env.clone(),
                None,
                notification_manager.clone(),
                Duration::from_secs(3600),
            )
            .await
            .unwrap(),
        );

        Self {
            catalog_srv: CatalogServiceImpl::<MemStore>::new(env, catalog_manager),
            cluster_srv: ClusterServiceImpl::<MemStore>::new(cluster_manager),
            notification_srv: NotificationServiceImpl::new(notification_manager),
        }
    }
}

#[async_trait::async_trait]
impl MetaClientInner for FrontendMockMetaClient {
    async fn list_all_nodes(&self, req: ListAllNodesRequest) -> Result<ListAllNodesResponse> {
        Ok(self
            .cluster_srv
            .list_all_nodes(Request::new(req))
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn add_worker_node(&self, req: AddWorkerNodeRequest) -> Result<AddWorkerNodeResponse> {
        Ok(self
            .cluster_srv
            .add_worker_node(Request::new(req))
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn activate_worker_node(
        &self,
        req: ActivateWorkerNodeRequest,
    ) -> Result<ActivateWorkerNodeResponse> {
        Ok(self
            .cluster_srv
            .activate_worker_node(Request::new(req))
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn delete_worker_node(
        &self,
        req: DeleteWorkerNodeRequest,
    ) -> Result<DeleteWorkerNodeResponse> {
        Ok(self
            .cluster_srv
            .delete_worker_node(Request::new(req))
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn subscribe(&self, req: SubscribeRequest) -> Result<Box<dyn NotificationStream>> {
        Ok(Box::new(
            self.notification_srv
                .subscribe(Request::new(req))
                .await
                .to_rw_result()?
                .into_inner()
                .into_inner(),
        ))
    }
}
