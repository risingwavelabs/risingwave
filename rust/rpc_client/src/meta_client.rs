use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_common::error::ErrorCode::{self, InternalError};
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::try_match_expand;
use risingwave_pb::catalog::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateMaterializedSourceRequest,
    CreateMaterializedSourceResponse, CreateMaterializedViewRequest,
    CreateMaterializedViewResponse, CreateSchemaRequest, CreateSchemaResponse,
    Database as ProstDatabase, Schema as ProstSchema, Table as ProstTable,
};
use risingwave_pb::common::{HostAddress, WorkerNode, WorkerType};
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::hummock::{
    AddTablesRequest, AddTablesResponse, GetNewTableIdRequest, GetNewTableIdResponse,
    PinSnapshotRequest, PinSnapshotResponse, PinVersionRequest, PinVersionResponse,
    ReportCompactionTasksRequest, ReportCompactionTasksResponse, SubscribeCompactTasksRequest,
    SubscribeCompactTasksResponse, UnpinSnapshotRequest, UnpinSnapshotResponse,
    UnpinVersionRequest, UnpinVersionResponse,
};
use risingwave_pb::meta::catalog_service_client::CatalogServiceClient;
use risingwave_pb::meta::cluster_service_client::ClusterServiceClient;
use risingwave_pb::meta::create_request::CatalogBody;
use risingwave_pb::meta::drop_request::CatalogId;
use risingwave_pb::meta::heartbeat_service_client::HeartbeatServiceClient;
use risingwave_pb::meta::notification_service_client::NotificationServiceClient;
use risingwave_pb::meta::{
    ActivateWorkerNodeRequest, ActivateWorkerNodeResponse, AddWorkerNodeRequest,
    AddWorkerNodeResponse, Catalog, CreateRequest, CreateResponse, Database,
    DeleteWorkerNodeRequest, DeleteWorkerNodeResponse, DropRequest, DropResponse,
    GetCatalogRequest, GetCatalogResponse, HeartbeatRequest, HeartbeatResponse,
    ListAllNodesRequest, ListAllNodesResponse, Schema, SubscribeRequest, SubscribeResponse, Table,
};
use risingwave_pb::plan::{DatabaseRefId, SchemaRefId, TableRefId};
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};
use tonic::{Status, Streaming};

type DatabaseId = u32;
type SchemaId = u32;

/// Client to meta server. Cloning the instance is lightweight.
#[derive(Clone)]
pub struct MetaClient {
    worker_id_ref: Option<u32>,
    pub inner: Arc<dyn MetaClientInner>,
}

impl MetaClient {
    /// Connect to the meta server `addr`.
    pub async fn new(meta_addr: &str) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(GrpcMetaClient::new(meta_addr).await?),
            worker_id_ref: None,
        })
    }

    pub fn mock(inner: impl MetaClientInner + 'static) -> Self {
        Self {
            worker_id_ref: None,
            inner: Arc::new(inner),
        }
    }

    pub fn set_worker_id(&mut self, worker_id: u32) {
        self.worker_id_ref = Some(worker_id);
    }

    pub fn worker_id(&self) -> u32 {
        self.worker_id_ref.expect("worker node id is not set.")
    }

    /// Subscribe to notification from meta.
    pub async fn subscribe(
        &self,
        addr: SocketAddr,
        worker_type: WorkerType,
    ) -> Result<Box<dyn NotificationStream>> {
        let host_address = HostAddress {
            host: addr.ip().to_string(),
            port: addr.port() as i32,
        };
        let request = SubscribeRequest {
            worker_type: worker_type as i32,
            host: Some(host_address),
        };
        self.inner.subscribe(request).await
    }

    /// Register the current node to the cluster and set the corresponding worker id.
    pub async fn register(&mut self, addr: SocketAddr, worker_type: WorkerType) -> Result<u32> {
        let host_address = HostAddress {
            host: addr.ip().to_string(),
            port: addr.port() as i32,
        };
        let request = AddWorkerNodeRequest {
            worker_type: worker_type as i32,
            host: Some(host_address),
        };
        let resp = self.inner.add_worker_node(request).await?;
        let worker_node =
            try_match_expand!(resp.node, Some, "AddWorkerNodeResponse::node is empty")?;
        self.set_worker_id(worker_node.id);
        Ok(worker_node.id)
    }

    /// Activate the current node in cluster to confirm it's ready to serve.
    pub async fn activate(&self, addr: SocketAddr) -> Result<()> {
        let host_address = HostAddress {
            host: addr.ip().to_string(),
            port: addr.port() as i32,
        };
        let request = ActivateWorkerNodeRequest {
            host: Some(host_address),
        };
        self.inner.activate_worker_node(request).await?;
        Ok(())
    }

    /// Send heartbeat signal to meta service.
    pub async fn send_heartbeat(&self, node_id: u32) -> Result<()> {
        let request = HeartbeatRequest {
            node_id,
            worker_type: WorkerType::ComputeNode as i32,
        };
        self.inner.heartbeat(request).await?;
        Ok(())
    }

    pub async fn create_database(&self, db: ProstDatabase) -> Result<(DatabaseId, CatalogVersion)> {
        let request = CreateDatabaseRequest { db: Some(db) };
        let resp = self.inner.create_database(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.database_id.into(), resp.version))
    }

    pub async fn create_schema(&self, schema: ProstSchema) -> Result<(SchemaId, CatalogVersion)> {
        let request = CreateSchemaRequest {
            schema: Some(schema),
        };
        let resp = self.inner.create_schema(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.schema_id.into(), resp.version))
    }
    /// Unregister the current node to the cluster.
    pub async fn unregister(&self, addr: SocketAddr) -> Result<()> {
        let host_address = HostAddress {
            host: addr.ip().to_string(),
            port: addr.port() as i32,
        };
        let request = DeleteWorkerNodeRequest {
            host: Some(host_address),
        };
        MetaClientInner::delete_worker_node(self.inner.as_ref(), request).await?;
        Ok(())
    }

    /// Get live nodes with the specified type.
    /// # Arguments
    /// * `worker_type` `WorkerType` of the nodes
    /// * `include_starting_nodes` Whether to include nodes still being created
    pub async fn list_all_nodes(
        &self,
        worker_type: WorkerType,
        include_starting_nodes: bool,
    ) -> Result<Vec<WorkerNode>> {
        let request = ListAllNodesRequest {
            worker_type: worker_type as i32,
            include_starting_nodes,
        };
        let resp = self.inner.list_all_nodes(request).await?;
        Ok(resp.nodes)
    }

    pub fn start_heartbeat_loop(
        meta_client: MetaClient,
        min_interval: Duration,
    ) -> (JoinHandle<()>, UnboundedSender<()>) {
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::unbounded_channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval = tokio::time::interval(min_interval);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval.tick() => {},
                    // Shutdown
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Heartbeat loop is shutting down");
                        return;
                    }
                }
                tracing::debug!("heartbeat");
                if let Err(err) = meta_client.send_heartbeat(meta_client.worker_id()).await {
                    tracing::warn!("Failed to send_heartbeat. {}", err);
                }
            }
        });
        (join_handle, shutdown_tx)
    }
}

/// [`MetaClientInner`] is the low-level api to meta.
/// It can be used for testing and allows implementations to bypass the network
/// and directly call a mocked serivce method.
#[async_trait::async_trait]
pub trait MetaClientInner: Send + Sync {
    async fn subscribe(&self, _req: SubscribeRequest) -> Result<Box<dyn NotificationStream>> {
        unimplemented!()
    }

    async fn add_worker_node(&self, _req: AddWorkerNodeRequest) -> Result<AddWorkerNodeResponse> {
        unimplemented!()
    }

    async fn activate_worker_node(
        &self,
        _req: ActivateWorkerNodeRequest,
    ) -> Result<ActivateWorkerNodeResponse> {
        unimplemented!()
    }

    async fn delete_worker_node(
        &self,
        _req: DeleteWorkerNodeRequest,
    ) -> Result<DeleteWorkerNodeResponse> {
        unimplemented!()
    }

    async fn heartbeat(&self, _req: HeartbeatRequest) -> Result<HeartbeatResponse> {
        Err(ErrorCode::NotImplementedError("heartbeat is not implemented".into()).into())
    }

    // the old catalog interface will be deprecated soon
    async fn create(&self, _req: CreateRequest) -> Result<CreateResponse> {
        unimplemented!()
    }

    // the old catalog interface will be deprecated soon
    async fn drop(&self, _req: DropRequest) -> Result<DropResponse> {
        unimplemented!()
    }

    async fn list_all_nodes(&self, _req: ListAllNodesRequest) -> Result<ListAllNodesResponse> {
        unimplemented!()
    }

    // the old catalog interface will be deprecated soon
    async fn get_catalog(&self, _req: GetCatalogRequest) -> Result<GetCatalogResponse> {
        unimplemented!()
    }

    async fn pin_version(
        &self,
        _req: PinVersionRequest,
    ) -> std::result::Result<PinVersionResponse, tonic::Status> {
        unimplemented!()
    }

    async fn unpin_version(
        &self,
        _req: UnpinVersionRequest,
    ) -> std::result::Result<UnpinVersionResponse, tonic::Status> {
        unimplemented!()
    }

    async fn pin_snapshot(
        &self,
        _req: PinSnapshotRequest,
    ) -> std::result::Result<PinSnapshotResponse, tonic::Status> {
        unimplemented!()
    }

    async fn unpin_snapshot(
        &self,
        _req: UnpinSnapshotRequest,
    ) -> std::result::Result<UnpinSnapshotResponse, tonic::Status> {
        unimplemented!()
    }

    async fn add_tables(
        &self,
        _req: AddTablesRequest,
    ) -> std::result::Result<AddTablesResponse, tonic::Status> {
        unimplemented!()
    }

    async fn report_compaction_tasks(
        &self,
        _req: ReportCompactionTasksRequest,
    ) -> std::result::Result<ReportCompactionTasksResponse, tonic::Status> {
        unimplemented!()
    }

    async fn get_new_table_id(
        &self,
        _req: GetNewTableIdRequest,
    ) -> std::result::Result<GetNewTableIdResponse, tonic::Status> {
        unimplemented!()
    }

    async fn subscribe_compact_tasks(
        &self,
        _req: SubscribeCompactTasksRequest,
    ) -> std::result::Result<Streaming<SubscribeCompactTasksResponse>, tonic::Status> {
        unimplemented!()
    }

    async fn create_database(&self, _req: CreateDatabaseRequest) -> Result<CreateDatabaseResponse> {
        unimplemented!()
    }

    async fn create_schema(&self, _req: CreateSchemaRequest) -> Result<CreateSchemaResponse> {
        unimplemented!()
    }

    async fn create_materialized_source(
        &self,
        _req: CreateMaterializedSourceRequest,
    ) -> Result<CreateMaterializedSourceResponse> {
        unimplemented!()
    }

    async fn create_materialized_view(
        &self,
        _req: CreateMaterializedViewRequest,
    ) -> Result<CreateMaterializedViewResponse> {
        unimplemented!()
    }
}

/// Client to meta server. Cloning the instance is lightweight.
#[derive(Debug, Clone)]
pub struct GrpcMetaClient {
    pub cluster_client: ClusterServiceClient<Channel>,
    pub heartbeat_client: HeartbeatServiceClient<Channel>,
    pub catalog_client: CatalogServiceClient<Channel>,
    pub hummock_client: HummockManagerServiceClient<Channel>,
    pub notification_client: NotificationServiceClient<Channel>,
}

impl GrpcMetaClient {
    /// Connect to the meta server `addr`.
    pub async fn new(addr: &str) -> Result<Self> {
        let channel = Endpoint::from_shared(addr.to_string())
            .map_err(|e| InternalError(format!("{}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .to_rw_result_with(format!("failed to connect to {}", addr))?;
        let cluster_client = ClusterServiceClient::new(channel.clone());
        let heartbeat_client = HeartbeatServiceClient::new(channel.clone());
        let catalog_client = CatalogServiceClient::new(channel.clone());
        let hummock_client = HummockManagerServiceClient::new(channel.clone());
        let notification_client = NotificationServiceClient::new(channel);
        Ok(Self {
            cluster_client,
            heartbeat_client,
            catalog_client,
            hummock_client,
            notification_client,
        })
    }
}

#[async_trait::async_trait]
impl MetaClientInner for GrpcMetaClient {
    // TODO(TaoWu): Use macro to refactor the following methods.

    async fn subscribe(&self, request: SubscribeRequest) -> Result<Box<dyn NotificationStream>> {
        Ok(Box::new(
            self.notification_client
                .to_owned()
                .subscribe(request)
                .await
                .to_rw_result()?
                .into_inner(),
        ))
    }

    async fn add_worker_node(&self, req: AddWorkerNodeRequest) -> Result<AddWorkerNodeResponse> {
        Ok(self
            .cluster_client
            .to_owned()
            .add_worker_node(req)
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn activate_worker_node(
        &self,
        req: ActivateWorkerNodeRequest,
    ) -> Result<ActivateWorkerNodeResponse> {
        Ok(self
            .cluster_client
            .to_owned()
            .activate_worker_node(req)
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn delete_worker_node(
        &self,
        req: DeleteWorkerNodeRequest,
    ) -> Result<DeleteWorkerNodeResponse> {
        Ok(self
            .cluster_client
            .to_owned()
            .delete_worker_node(req)
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn heartbeat(&self, req: HeartbeatRequest) -> Result<HeartbeatResponse> {
        Ok(self
            .heartbeat_client
            .to_owned()
            .heartbeat(req)
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn create(&self, req: CreateRequest) -> Result<CreateResponse> {
        Ok(self
            .catalog_client
            .to_owned()
            .create(req)
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn drop(&self, req: DropRequest) -> Result<DropResponse> {
        Ok(self
            .catalog_client
            .to_owned()
            .drop(req)
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn list_all_nodes(&self, req: ListAllNodesRequest) -> Result<ListAllNodesResponse> {
        Ok(self
            .cluster_client
            .to_owned()
            .list_all_nodes(req)
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn get_catalog(&self, req: GetCatalogRequest) -> Result<GetCatalogResponse> {
        Ok(self
            .catalog_client
            .to_owned()
            .get_catalog(req)
            .await
            .to_rw_result()?
            .into_inner())
    }

    async fn pin_version(
        &self,
        req: PinVersionRequest,
    ) -> std::result::Result<PinVersionResponse, tonic::Status> {
        Ok(self
            .hummock_client
            .to_owned()
            .pin_version(req)
            .await?
            .into_inner())
    }

    async fn unpin_version(
        &self,
        req: UnpinVersionRequest,
    ) -> std::result::Result<UnpinVersionResponse, tonic::Status> {
        Ok(self
            .hummock_client
            .to_owned()
            .unpin_version(req)
            .await?
            .into_inner())
    }

    async fn pin_snapshot(
        &self,
        req: PinSnapshotRequest,
    ) -> std::result::Result<PinSnapshotResponse, tonic::Status> {
        Ok(self
            .hummock_client
            .to_owned()
            .pin_snapshot(req)
            .await?
            .into_inner())
    }

    async fn unpin_snapshot(
        &self,
        req: UnpinSnapshotRequest,
    ) -> std::result::Result<UnpinSnapshotResponse, tonic::Status> {
        Ok(self
            .hummock_client
            .to_owned()
            .unpin_snapshot(req)
            .await?
            .into_inner())
    }

    async fn add_tables(
        &self,
        req: AddTablesRequest,
    ) -> std::result::Result<AddTablesResponse, tonic::Status> {
        Ok(self
            .hummock_client
            .to_owned()
            .add_tables(req)
            .await?
            .into_inner())
    }

    async fn report_compaction_tasks(
        &self,
        req: ReportCompactionTasksRequest,
    ) -> std::result::Result<ReportCompactionTasksResponse, tonic::Status> {
        Ok(self
            .hummock_client
            .to_owned()
            .report_compaction_tasks(req)
            .await?
            .into_inner())
    }

    async fn get_new_table_id(
        &self,
        req: GetNewTableIdRequest,
    ) -> std::result::Result<GetNewTableIdResponse, tonic::Status> {
        Ok(self
            .hummock_client
            .to_owned()
            .get_new_table_id(req)
            .await?
            .into_inner())
    }

    async fn subscribe_compact_tasks(
        &self,
        req: SubscribeCompactTasksRequest,
    ) -> std::result::Result<Streaming<SubscribeCompactTasksResponse>, tonic::Status> {
        Ok(self
            .hummock_client
            .to_owned()
            .subscribe_compact_tasks(req)
            .await?
            .into_inner())
    }
}

#[async_trait::async_trait]
pub trait NotificationStream: Send {
    /// Ok(Some) => receive a `SubscribeResponse`.
    /// Ok(None) => stream terminates.
    /// Err => error happens.
    async fn next(&mut self) -> Result<Option<SubscribeResponse>>;
}

#[async_trait::async_trait]
impl NotificationStream for Streaming<SubscribeResponse> {
    async fn next(&mut self) -> Result<Option<SubscribeResponse>> {
        self.message().await.to_rw_result()
    }
}

#[async_trait::async_trait]
impl NotificationStream for Receiver<std::result::Result<SubscribeResponse, Status>> {
    async fn next(&mut self) -> Result<Option<SubscribeResponse>> {
        match self.recv().await {
            Some(Ok(x)) => Ok(Some(x)),
            Some(Err(e)) => Err(e).to_rw_result(),
            None => Ok(None),
        }
    }
}
