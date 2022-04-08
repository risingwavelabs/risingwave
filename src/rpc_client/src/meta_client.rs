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
use std::fmt::Debug;
use std::time::Duration;

use async_trait::async_trait;
use paste::paste;
use risingwave_common::catalog::{CatalogVersion, TableId};
use risingwave_common::error::ErrorCode::InternalError;
use risingwave_common::error::{Result, ToRwResult};
use risingwave_common::try_match_expand;
use risingwave_common::util::addr::HostAddr;
use risingwave_hummock_sdk::{HummockEpoch, HummockSSTableId, HummockVersionId};
use risingwave_pb::catalog::{
    Database as ProstDatabase, Schema as ProstSchema, Source as ProstSource, Table as ProstTable,
};
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_pb::ddl_service::ddl_service_client::DdlServiceClient;
use risingwave_pb::ddl_service::{
    CreateDatabaseRequest, CreateDatabaseResponse, CreateMaterializedSourceRequest,
    CreateMaterializedSourceResponse, CreateMaterializedViewRequest,
    CreateMaterializedViewResponse, CreateSchemaRequest, CreateSchemaResponse, CreateSourceRequest,
    CreateSourceResponse, DropMaterializedSourceRequest, DropMaterializedSourceResponse,
    DropMaterializedViewRequest, DropMaterializedViewResponse, DropSourceRequest,
    DropSourceResponse,
};
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::hummock::{
    AbortEpochRequest, AbortEpochResponse, AddTablesRequest, AddTablesResponse, CommitEpochRequest,
    CommitEpochResponse, CompactTask, GetNewTableIdRequest, GetNewTableIdResponse, HummockSnapshot,
    HummockVersion, PinSnapshotRequest, PinSnapshotResponse, PinVersionRequest, PinVersionResponse,
    ReportCompactionTasksRequest, ReportCompactionTasksResponse, ReportVacuumTaskRequest,
    ReportVacuumTaskResponse, SstableInfo, SubscribeCompactTasksRequest,
    SubscribeCompactTasksResponse, UnpinSnapshotRequest, UnpinSnapshotResponse,
    UnpinVersionRequest, UnpinVersionResponse, VacuumTask,
};
use risingwave_pb::meta::catalog_service_client::CatalogServiceClient;
use risingwave_pb::meta::cluster_service_client::ClusterServiceClient;
use risingwave_pb::meta::heartbeat_service_client::HeartbeatServiceClient;
use risingwave_pb::meta::notification_service_client::NotificationServiceClient;
use risingwave_pb::meta::stream_manager_service_client::StreamManagerServiceClient;
use risingwave_pb::meta::{
    ActivateWorkerNodeRequest, ActivateWorkerNodeResponse, AddWorkerNodeRequest,
    AddWorkerNodeResponse, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse, FlushRequest,
    FlushResponse, HeartbeatRequest, HeartbeatResponse, ListAllNodesRequest, ListAllNodesResponse,
    SubscribeRequest, SubscribeResponse,
};
use risingwave_pb::stream_plan::StreamNode;
use tokio::sync::mpsc::{Receiver, UnboundedSender};
use tokio::task::JoinHandle;
use tonic::transport::{Channel, Endpoint};
use tonic::{Status, Streaming};

use crate::hummock_meta_client::HummockMetaClient;

type DatabaseId = u32;
type SchemaId = u32;

/// Client to meta server. Cloning the instance is lightweight.
#[derive(Clone)]
pub struct MetaClient {
    worker_id: Option<u32>,
    pub inner: GrpcMetaClient,
}

impl MetaClient {
    /// Connect to the meta server `addr`.
    pub async fn new(meta_addr: &str) -> Result<Self> {
        Ok(Self {
            inner: GrpcMetaClient::new(meta_addr).await?,
            worker_id: None,
        })
    }

    pub fn set_worker_id(&mut self, worker_id: u32) {
        self.worker_id = Some(worker_id);
    }

    pub fn worker_id(&self) -> u32 {
        self.worker_id.expect("worker node id is not set.")
    }

    /// Subscribe to notification from meta.
    pub async fn subscribe(
        &self,
        addr: &HostAddr,
        worker_type: WorkerType,
    ) -> Result<Box<dyn NotificationStream>> {
        let request = SubscribeRequest {
            worker_type: worker_type as i32,
            host: Some(addr.to_protobuf()),
        };
        self.inner.subscribe(request).await
    }

    /// Register the current node to the cluster and set the corresponding worker id.
    pub async fn register(&mut self, addr: &HostAddr, worker_type: WorkerType) -> Result<u32> {
        let request = AddWorkerNodeRequest {
            worker_type: worker_type as i32,
            host: Some(addr.to_protobuf()),
        };
        let resp = self.inner.add_worker_node(request).await?;
        let worker_node =
            try_match_expand!(resp.node, Some, "AddWorkerNodeResponse::node is empty")?;
        self.set_worker_id(worker_node.id);
        Ok(worker_node.id)
    }

    /// Activate the current node in cluster to confirm it's ready to serve.
    pub async fn activate(&self, addr: &HostAddr) -> Result<()> {
        let request = ActivateWorkerNodeRequest {
            host: Some(addr.to_protobuf()),
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
        Ok((resp.database_id, resp.version))
    }

    pub async fn create_schema(&self, schema: ProstSchema) -> Result<(SchemaId, CatalogVersion)> {
        let request = CreateSchemaRequest {
            schema: Some(schema),
        };
        let resp = self.inner.create_schema(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.schema_id, resp.version))
    }

    pub async fn create_materialized_view(
        &self,
        table: ProstTable,
        plan: StreamNode,
    ) -> Result<(TableId, CatalogVersion)> {
        let request = CreateMaterializedViewRequest {
            materialized_view: Some(table),
            stream_node: Some(plan),
        };
        let resp = self.inner.create_materialized_view(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.table_id.into(), resp.version))
    }

    pub async fn drop_materialized_view(&self, table_id: TableId) -> Result<CatalogVersion> {
        let request = DropMaterializedViewRequest {
            table_id: table_id.table_id(),
        };

        let resp = self.inner.drop_materialized_view(request).await?;
        Ok(resp.version)
    }

    pub async fn create_source(&self, source: ProstSource) -> Result<(u32, CatalogVersion)> {
        let request = CreateSourceRequest {
            source: Some(source),
        };

        let resp = self.inner.create_source(request).await?;
        Ok((resp.source_id, resp.version))
    }

    pub async fn create_materialized_source(
        &self,
        source: ProstSource,
        table: ProstTable,
        plan: StreamNode,
    ) -> Result<(TableId, u32, CatalogVersion)> {
        let request = CreateMaterializedSourceRequest {
            materialized_view: Some(table),
            stream_node: Some(plan),
            source: Some(source),
        };
        let resp = self.inner.create_materialized_source(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.table_id.into(), resp.source_id, resp.version))
    }

    pub async fn drop_materialized_source(
        &self,
        source_id: u32,
        table_id: TableId,
    ) -> Result<CatalogVersion> {
        let request = DropMaterializedSourceRequest {
            source_id,
            table_id: table_id.table_id(),
        };

        let resp = self.inner.drop_materialized_source(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_source(&self, source_id: u32) -> Result<CatalogVersion> {
        let request = DropSourceRequest { source_id };
        let resp = self.inner.drop_source(request).await?;
        Ok(resp.version)
    }

    /// Unregister the current node to the cluster.
    pub async fn unregister(&self, addr: HostAddr) -> Result<()> {
        let request = DeleteWorkerNodeRequest {
            host: Some(addr.to_protobuf()),
        };
        self.inner.delete_worker_node(request).await?;
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
            let mut min_interval_ticker = tokio::time::interval(min_interval);
            loop {
                tokio::select! {
                    // Wait for interval
                    _ = min_interval_ticker.tick() => {},
                    // Shutdown
                    _ = shutdown_rx.recv() => {
                        tracing::info!("Heartbeat loop is shutting down");
                        return;
                    }
                }
                tracing::trace!(target: "events::meta::client_heartbeat", "heartbeat");
                match tokio::time::timeout(
                    // TODO: decide better min_interval for timeout
                    min_interval * 3,
                    meta_client.send_heartbeat(meta_client.worker_id()),
                )
                .await
                {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => {
                        tracing::warn!("Failed to send_heartbeat: error {}", err);
                    }
                    Err(err) => {
                        tracing::warn!("Failed to send_heartbeat: timeout {}", err);
                    }
                }
            }
        });
        (join_handle, shutdown_tx)
    }

    pub async fn flush(&self) -> Result<()> {
        let request = FlushRequest::default();
        self.inner.flush(request).await?;
        Ok(())
    }
}

#[async_trait]
impl HummockMetaClient for MetaClient {
    async fn pin_version(&self, last_pinned: HummockVersionId) -> Result<HummockVersion> {
        let req = PinVersionRequest {
            context_id: self.worker_id(),
            last_pinned,
        };
        let resp = self.inner.pin_version(req).await?;
        Ok(resp.pinned_version.unwrap())
    }

    async fn unpin_version(&self, pinned_version_ids: &[HummockVersionId]) -> Result<()> {
        let req = UnpinVersionRequest {
            context_id: self.worker_id(),
            pinned_version_ids: pinned_version_ids.to_owned(),
        };
        self.inner.unpin_version(req).await?;
        Ok(())
    }

    async fn pin_snapshot(&self, last_pinned: HummockEpoch) -> Result<HummockEpoch> {
        let req = PinSnapshotRequest {
            context_id: self.worker_id(),
            last_pinned,
        };
        let resp = self.inner.pin_snapshot(req).await?;
        Ok(resp.snapshot.unwrap().epoch)
    }

    async fn unpin_snapshot(&self, pinned_epochs: &[HummockEpoch]) -> Result<()> {
        let req = UnpinSnapshotRequest {
            context_id: self.worker_id(),
            snapshots: pinned_epochs
                .iter()
                .map(|epoch| HummockSnapshot {
                    epoch: epoch.to_owned(),
                })
                .collect(),
        };
        self.inner.unpin_snapshot(req).await?;
        Ok(())
    }

    async fn get_new_table_id(&self) -> Result<HummockSSTableId> {
        let resp = self.inner.get_new_table_id(GetNewTableIdRequest {}).await?;
        Ok(resp.table_id)
    }

    async fn add_tables(
        &self,
        epoch: HummockEpoch,
        sstables: Vec<SstableInfo>,
    ) -> Result<HummockVersion> {
        let req = AddTablesRequest {
            context_id: self.worker_id(),
            tables: sstables,
            epoch,
        };
        let resp = self.inner.add_tables(req).await?;
        Ok(resp.version.unwrap())
    }

    async fn report_compaction_task(&self, compact_task: CompactTask) -> Result<()> {
        let req = ReportCompactionTasksRequest {
            compact_task: Some(compact_task),
        };
        self.inner.report_compaction_tasks(req).await?;
        Ok(())
    }

    async fn commit_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        let req = CommitEpochRequest { epoch };
        self.inner.commit_epoch(req).await?;
        Ok(())
    }

    async fn abort_epoch(&self, epoch: HummockEpoch) -> Result<()> {
        let req = AbortEpochRequest { epoch };
        self.inner.abort_epoch(req).await?;
        Ok(())
    }

    async fn subscribe_compact_tasks(&self) -> Result<Streaming<SubscribeCompactTasksResponse>> {
        let req = SubscribeCompactTasksRequest {
            context_id: self.worker_id(),
        };
        self.inner.subscribe_compact_tasks(req).await
    }

    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()> {
        let req = ReportVacuumTaskRequest {
            vacuum_task: Some(vacuum_task),
        };
        self.inner.report_vacuum_task(req).await?;
        Ok(())
    }
}

/// Client to meta server. Cloning the instance is lightweight.
#[derive(Debug, Clone)]
pub struct GrpcMetaClient {
    pub cluster_client: ClusterServiceClient<Channel>,
    pub heartbeat_client: HeartbeatServiceClient<Channel>,
    pub catalog_client: CatalogServiceClient<Channel>,
    pub ddl_client: DdlServiceClient<Channel>,
    pub hummock_client: HummockManagerServiceClient<Channel>,
    pub notification_client: NotificationServiceClient<Channel>,
    pub stream_client: StreamManagerServiceClient<Channel>,
}

impl GrpcMetaClient {
    /// Connect to the meta server `addr`.
    pub async fn new(addr: &str) -> Result<Self> {
        let channel = Endpoint::from_shared(addr.to_string())
            .map_err(|e| InternalError(format!("{}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .to_rw_result_with(|| format!("failed to connect to {}", addr))?;
        let cluster_client = ClusterServiceClient::new(channel.clone());
        let heartbeat_client = HeartbeatServiceClient::new(channel.clone());
        let catalog_client = CatalogServiceClient::new(channel.clone());
        let ddl_client = DdlServiceClient::new(channel.clone());
        let hummock_client = HummockManagerServiceClient::new(channel.clone());
        let notification_client = NotificationServiceClient::new(channel.clone());
        let stream_client = StreamManagerServiceClient::new(channel);
        Ok(Self {
            cluster_client,
            heartbeat_client,
            catalog_client,
            ddl_client,
            hummock_client,
            notification_client,
            stream_client,
        })
    }
}

macro_rules! grpc_meta_client_impl {
    ([], $( { $client:ident, $fn_name:ident, $req:ty, $resp:ty }),*) => {
        $(paste! {
            impl GrpcMetaClient {
                pub async fn [<$fn_name>](&self, request: $req) -> Result<$resp> {
                    Ok(self
                        .$client
                        .to_owned()
                        .$fn_name(request)
                        .await
                        .to_rw_result()?
                        .into_inner())
                }
            }
        })*
    }
}

macro_rules! for_all_meta_rpc {
    ($macro:ident $(, $x:tt)*) => {
        $macro! {
            [$($x),*]
            ,{ cluster_client, add_worker_node, AddWorkerNodeRequest, AddWorkerNodeResponse }
            ,{ cluster_client, activate_worker_node, ActivateWorkerNodeRequest, ActivateWorkerNodeResponse }
            ,{ cluster_client, delete_worker_node, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse }
            ,{ cluster_client, list_all_nodes, ListAllNodesRequest, ListAllNodesResponse }
            ,{ heartbeat_client, heartbeat, HeartbeatRequest, HeartbeatResponse }
            ,{ stream_client, flush, FlushRequest, FlushResponse }
            ,{ ddl_client, create_materialized_source, CreateMaterializedSourceRequest, CreateMaterializedSourceResponse }
            ,{ ddl_client, create_materialized_view, CreateMaterializedViewRequest, CreateMaterializedViewResponse }
            ,{ ddl_client, create_source, CreateSourceRequest, CreateSourceResponse }
            ,{ ddl_client, create_schema, CreateSchemaRequest, CreateSchemaResponse }
            ,{ ddl_client, create_database, CreateDatabaseRequest, CreateDatabaseResponse }
            ,{ ddl_client, drop_materialized_source, DropMaterializedSourceRequest, DropMaterializedSourceResponse }
            ,{ ddl_client, drop_materialized_view, DropMaterializedViewRequest, DropMaterializedViewResponse }
            ,{ ddl_client, drop_source, DropSourceRequest, DropSourceResponse }
            ,{ hummock_client, pin_version, PinVersionRequest, PinVersionResponse }
            ,{ hummock_client, unpin_version, UnpinVersionRequest, UnpinVersionResponse }
            ,{ hummock_client, pin_snapshot, PinSnapshotRequest, PinSnapshotResponse }
            ,{ hummock_client, unpin_snapshot, UnpinSnapshotRequest, UnpinSnapshotResponse }
            ,{ hummock_client, add_tables, AddTablesRequest, AddTablesResponse }
            ,{ hummock_client, report_compaction_tasks, ReportCompactionTasksRequest, ReportCompactionTasksResponse }
            ,{ hummock_client, get_new_table_id, GetNewTableIdRequest, GetNewTableIdResponse }
            ,{ hummock_client, subscribe_compact_tasks, SubscribeCompactTasksRequest, Streaming<SubscribeCompactTasksResponse> }
            ,{ hummock_client, report_vacuum_task, ReportVacuumTaskRequest, ReportVacuumTaskResponse }
            ,{ hummock_client, commit_epoch, CommitEpochRequest, CommitEpochResponse }
            ,{ hummock_client, abort_epoch, AbortEpochRequest, AbortEpochResponse }
        }
    };
}

for_all_meta_rpc! { grpc_meta_client_impl }

impl GrpcMetaClient {
    // TODO(TaoWu): Use macro to refactor the following methods.

    pub async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> Result<Box<dyn NotificationStream>> {
        Ok(Box::new(
            self.notification_client
                .to_owned()
                .subscribe(request)
                .await
                .to_rw_result()?
                .into_inner(),
        ))
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
