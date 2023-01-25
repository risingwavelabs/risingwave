// Copyright 2023 Singularity Data
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
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use risingwave_common::catalog::{CatalogVersion, FunctionId, IndexId, TableId};
use risingwave_common::config::MAX_CONNECTION_WINDOW_SIZE;
use risingwave_common::util::addr::HostAddr;
use risingwave_hummock_sdk::compact::CompactorRuntimeConfig;
use risingwave_hummock_sdk::table_stats::to_prost_table_stats_map;
use risingwave_hummock_sdk::{
    CompactionGroupId, HummockEpoch, HummockSstableId, HummockVersionId, LocalSstableInfo,
    SstIdRange,
};
use risingwave_pb::backup_service::backup_service_client::BackupServiceClient;
use risingwave_pb::backup_service::*;
use risingwave_pb::catalog::{
    Database as ProstDatabase, Function as ProstFunction, Index as ProstIndex,
    Schema as ProstSchema, Sink as ProstSink, Source as ProstSource, Table as ProstTable,
    View as ProstView,
};
use risingwave_pb::common::{HostAddress, WorkerType};
use risingwave_pb::ddl_service::ddl_service_client::DdlServiceClient;
use risingwave_pb::ddl_service::drop_table_request::SourceId;
use risingwave_pb::ddl_service::*;
use risingwave_pb::hummock::hummock_manager_service_client::HummockManagerServiceClient;
use risingwave_pb::hummock::rise_ctl_update_compaction_config_request::mutable_config::MutableConfig;
use risingwave_pb::hummock::*;
use risingwave_pb::leader::leader_service_client::LeaderServiceClient;
use risingwave_pb::leader::{LeaderRequest, LeaderResponse};
use risingwave_pb::meta::cluster_service_client::ClusterServiceClient;
use risingwave_pb::meta::heartbeat_request::{extra_info, ExtraInfo};
use risingwave_pb::meta::heartbeat_service_client::HeartbeatServiceClient;
use risingwave_pb::meta::list_table_fragments_response::TableFragmentInfo;
use risingwave_pb::meta::notification_service_client::NotificationServiceClient;
use risingwave_pb::meta::reschedule_request::Reschedule as ProstReschedule;
use risingwave_pb::meta::scale_service_client::ScaleServiceClient;
use risingwave_pb::meta::stream_manager_service_client::StreamManagerServiceClient;
use risingwave_pb::meta::*;
use risingwave_pb::stream_plan::StreamFragmentGraph;
use risingwave_pb::user::update_user_request::UpdateField;
use risingwave_pb::user::user_service_client::UserServiceClient;
use risingwave_pb::user::*;
use tokio::sync::oneshot::Sender;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tonic::transport::{Channel, Endpoint};
use tonic::{Response, Status, Streaming};

use crate::error::Result;
use crate::hummock_meta_client::{CompactTaskItem, HummockMetaClient};
use crate::{meta_rpc_client_method_impl, ExtraInfoSourceRef};

type DatabaseId = u32;
type SchemaId = u32;

/// Client to meta server. Cloning the instance is lightweight.
#[derive(Clone, Debug)]
pub struct MetaClient {
    worker_id: u32,
    worker_type: WorkerType,
    host_addr: HostAddr,
    inner: GrpcMetaClient,
}

impl MetaClient {
    // get request retry strategy
    pub fn get_retry_strategy(&self) -> impl Iterator<Item = Duration> {
        GrpcMetaClient::retry_strategy_for_request()
    }

    pub async fn do_failover_if_needed(&self) -> bool {
        self.inner.do_failover_if_needed().await
    }

    pub fn worker_id(&self) -> u32 {
        self.worker_id
    }

    pub fn host_addr(&self) -> &HostAddr {
        &self.host_addr
    }

    pub fn worker_type(&self) -> WorkerType {
        self.worker_type
    }

    /// Subscribe to notification from meta.
    pub async fn subscribe(
        &self,
        subscribe_type: SubscribeType,
    ) -> Result<Streaming<SubscribeResponse>> {
        let request = SubscribeRequest {
            subscribe_type: subscribe_type as i32,
            host: Some(self.host_addr.to_protobuf()),
            worker_id: self.worker_id(),
        };
        let retry_strategy = GrpcMetaClient::retry_strategy_for_request();
        let mut resp = self.inner.subscribe(request.clone()).await;
        for retry in retry_strategy {
            if resp.is_ok() {
                return resp;
            }
            tokio::time::sleep(retry).await;
            let request = request.clone();
            resp = self.inner.subscribe(request).await;
        }
        resp
    }

    /// Register the current node to the cluster and set the corresponding worker id.
    pub async fn register_new(
        meta_addr: &str,
        worker_type: WorkerType,
        addr: &HostAddr,
        worker_node_parallelism: usize,
    ) -> Result<Self> {
        let grpc_meta_client = GrpcMetaClient::new(meta_addr).await?;
        let request = AddWorkerNodeRequest {
            worker_type: worker_type as i32,
            host: Some(addr.to_protobuf()),
            worker_node_parallelism: worker_node_parallelism as u64,
        };
        let retry_strategy = GrpcMetaClient::retry_strategy_for_request();
        let mut resp = grpc_meta_client.add_worker_node(request.clone()).await;
        for retry in retry_strategy {
            if resp.is_ok() {
                break;
            }
            tokio::time::sleep(retry).await;
            let request = request.clone();
            resp = grpc_meta_client.add_worker_node(request).await;
        }

        let worker_node = resp?.node.expect("AddWorkerNodeResponse::node is empty");
        Ok(Self {
            worker_id: worker_node.id,
            worker_type,
            host_addr: addr.clone(),
            inner: grpc_meta_client,
        })
    }

    /// Activate the current node in cluster to confirm it's ready to serve.
    pub async fn activate(&self, addr: &HostAddr) -> Result<()> {
        let request = ActivateWorkerNodeRequest {
            host: Some(addr.to_protobuf()),
        };
        let retry_strategy = GrpcMetaClient::retry_strategy_for_request();
        let mut resp = self.inner.activate_worker_node(request.clone()).await;
        for retry in retry_strategy {
            if resp.is_ok() {
                break;
            }
            tokio::time::sleep(retry).await;
            let request = request.clone();
            resp = self.inner.activate_worker_node(request).await;
        }
        // return error if all retries failed
        resp?;

        Ok(())
    }

    /// Send heartbeat signal to meta service.
    pub async fn send_heartbeat(&self, node_id: u32, info: Vec<extra_info::Info>) -> Result<()> {
        let request = HeartbeatRequest {
            node_id,
            info: info
                .into_iter()
                .map(|info| ExtraInfo { info: Some(info) })
                .collect(),
        };
        let resp = self.inner.heartbeat(request).await?;
        if let Some(status) = resp.status {
            if status.code() == risingwave_pb::common::status::Code::UnknownWorker {
                tracing::error!("worker expired: {}", status.message);
                std::process::exit(1);
            }
        }
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
        graph: StreamFragmentGraph,
    ) -> Result<(TableId, CatalogVersion)> {
        let request = CreateMaterializedViewRequest {
            materialized_view: Some(table),
            fragment_graph: Some(graph),
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

    pub async fn create_sink(
        &self,
        sink: ProstSink,
        graph: StreamFragmentGraph,
    ) -> Result<(u32, CatalogVersion)> {
        let request = CreateSinkRequest {
            sink: Some(sink),
            fragment_graph: Some(graph),
        };

        let resp = self.inner.create_sink(request).await?;
        Ok((resp.sink_id, resp.version))
    }

    pub async fn create_function(
        &self,
        function: ProstFunction,
    ) -> Result<(FunctionId, CatalogVersion)> {
        let request = CreateFunctionRequest {
            function: Some(function),
        };
        let resp = self.inner.create_function(request).await?;
        Ok((resp.function_id.into(), resp.version))
    }

    pub async fn create_table(
        &self,
        source: Option<ProstSource>,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<(TableId, CatalogVersion)> {
        let request = CreateTableRequest {
            materialized_view: Some(table),
            fragment_graph: Some(graph),
            source,
        };
        let resp = self.inner.create_table(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.table_id.into(), resp.version))
    }

    pub async fn create_view(&self, view: ProstView) -> Result<(u32, CatalogVersion)> {
        let request = CreateViewRequest { view: Some(view) };
        let resp = self.inner.create_view(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.view_id, resp.version))
    }

    pub async fn create_index(
        &self,
        index: ProstIndex,
        table: ProstTable,
        graph: StreamFragmentGraph,
    ) -> Result<(TableId, CatalogVersion)> {
        let request = CreateIndexRequest {
            index: Some(index),
            index_table: Some(table),
            fragment_graph: Some(graph),
        };
        let resp = self.inner.create_index(request).await?;
        // TODO: handle error in `resp.status` here
        Ok((resp.index_id.into(), resp.version))
    }

    pub async fn drop_table(
        &self,
        source_id: Option<u32>,
        table_id: TableId,
    ) -> Result<CatalogVersion> {
        let request = DropTableRequest {
            source_id: source_id.map(SourceId::Id),
            table_id: table_id.table_id(),
        };

        let resp = self.inner.drop_table(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_view(&self, view_id: u32) -> Result<CatalogVersion> {
        let request = DropViewRequest { view_id };
        let resp = self.inner.drop_view(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_source(&self, source_id: u32) -> Result<CatalogVersion> {
        let request = DropSourceRequest { source_id };
        let resp = self.inner.drop_source(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_sink(&self, sink_id: u32) -> Result<CatalogVersion> {
        let request = DropSinkRequest { sink_id };
        let resp = self.inner.drop_sink(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_index(&self, index_id: IndexId) -> Result<CatalogVersion> {
        let request = DropIndexRequest {
            index_id: index_id.index_id,
        };
        let resp = self.inner.drop_index(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_function(&self, function_id: FunctionId) -> Result<CatalogVersion> {
        let request = DropFunctionRequest {
            function_id: function_id.0,
        };
        let resp = self.inner.drop_function(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_database(&self, database_id: u32) -> Result<CatalogVersion> {
        let request = DropDatabaseRequest { database_id };
        let resp = self.inner.drop_database(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_schema(&self, schema_id: u32) -> Result<CatalogVersion> {
        let request = DropSchemaRequest { schema_id };
        let resp = self.inner.drop_schema(request).await?;
        Ok(resp.version)
    }

    // TODO: using UserInfoVersion instead as return type.
    pub async fn create_user(&self, user: UserInfo) -> Result<u64> {
        let request = CreateUserRequest { user: Some(user) };
        let resp = self.inner.create_user(request).await?;
        Ok(resp.version)
    }

    pub async fn drop_user(&self, user_id: u32) -> Result<u64> {
        let request = DropUserRequest { user_id };
        let resp = self.inner.drop_user(request).await?;
        Ok(resp.version)
    }

    pub async fn update_user(
        &self,
        user: UserInfo,
        update_fields: Vec<UpdateField>,
    ) -> Result<u64> {
        let request = UpdateUserRequest {
            user: Some(user),
            update_fields: update_fields
                .into_iter()
                .map(|field| field as i32)
                .collect::<Vec<_>>(),
        };
        let resp = self.inner.update_user(request).await?;
        Ok(resp.version)
    }

    pub async fn grant_privilege(
        &self,
        user_ids: Vec<u32>,
        privileges: Vec<GrantPrivilege>,
        with_grant_option: bool,
        granted_by: u32,
    ) -> Result<u64> {
        let request = GrantPrivilegeRequest {
            user_ids,
            privileges,
            with_grant_option,
            granted_by,
        };
        let resp = self.inner.grant_privilege(request).await?;
        Ok(resp.version)
    }

    pub async fn revoke_privilege(
        &self,
        user_ids: Vec<u32>,
        privileges: Vec<GrantPrivilege>,
        granted_by: Option<u32>,
        revoke_by: u32,
        revoke_grant_option: bool,
        cascade: bool,
    ) -> Result<u64> {
        let granted_by = granted_by.unwrap_or_default();
        let request = RevokePrivilegeRequest {
            user_ids,
            privileges,
            granted_by,
            revoke_by,
            revoke_grant_option,
            cascade,
        };
        let resp = self.inner.revoke_privilege(request).await?;
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

    /// Starts a heartbeat worker.
    ///
    /// When sending heartbeat RPC, it also carries extra info from `extra_info_sources`.
    pub fn start_heartbeat_loop(
        meta_client: MetaClient,
        min_interval: Duration,
        max_interval: Duration,
        extra_info_sources: Vec<ExtraInfoSourceRef>,
    ) -> (JoinHandle<()>, Sender<()>) {
        assert!(min_interval < max_interval);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            let mut min_interval_ticker = tokio::time::interval(min_interval);
            let mut max_interval_ticker = tokio::time::interval(max_interval);
            max_interval_ticker.reset();
            loop {
                tokio::select! {
                    biased;
                    // Shutdown
                    _ = &mut shutdown_rx => {
                        tracing::info!("Heartbeat loop is stopped");
                        return;
                    }
                    // Wait for interval
                    _ = min_interval_ticker.tick() => {},
                    _ = max_interval_ticker.tick() => {
                        // Client has lost connection to the server and reached time limit, it should exit.
                        tracing::error!("Heartbeat timeout, exiting...");
                        std::process::exit(1);
                    },
                }
                let mut extra_info = Vec::with_capacity(extra_info_sources.len());
                for extra_info_source in &extra_info_sources {
                    if let Some(info) = extra_info_source.get_extra_info().await {
                        // None means the info is not available at the moment, and won't be sent to
                        // meta.
                        extra_info.push(info);
                    }
                }
                tracing::trace!(target: "events::meta::client_heartbeat", "heartbeat");
                match tokio::time::timeout(
                    // TODO: decide better min_interval for timeout
                    min_interval * 3,
                    meta_client.send_heartbeat(meta_client.worker_id(), extra_info),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        max_interval_ticker.reset();
                    }
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

    pub async fn risectl_list_state_tables(&self) -> Result<Vec<ProstTable>> {
        let request = RisectlListStateTablesRequest {};
        let resp = self.inner.risectl_list_state_tables(request).await?;
        Ok(resp.tables)
    }

    pub async fn flush(&self, checkpoint: bool) -> Result<HummockSnapshot> {
        let request = FlushRequest { checkpoint };
        let resp = self.inner.flush(request).await?;
        Ok(resp.snapshot.unwrap())
    }

    pub async fn list_table_fragments(
        &self,
        table_ids: &[u32],
    ) -> Result<HashMap<u32, TableFragmentInfo>> {
        let request = ListTableFragmentsRequest {
            table_ids: table_ids.to_vec(),
        };
        let resp = self.inner.list_table_fragments(request).await?;
        Ok(resp.table_fragments)
    }

    pub async fn pause(&self) -> Result<()> {
        let request = PauseRequest {};
        let _resp = self.inner.pause(request).await?;
        Ok(())
    }

    pub async fn resume(&self) -> Result<()> {
        let request = ResumeRequest {};
        let _resp = self.inner.resume(request).await?;
        Ok(())
    }

    pub async fn get_cluster_info(&self) -> Result<GetClusterInfoResponse> {
        let request = GetClusterInfoRequest {};
        let resp = self.inner.get_cluster_info(request).await?;
        Ok(resp)
    }

    pub async fn reschedule(&self, reschedules: HashMap<u32, ProstReschedule>) -> Result<bool> {
        let request = RescheduleRequest { reschedules };
        let resp = self.inner.reschedule(request).await?;
        Ok(resp.success)
    }

    pub async fn risectl_get_pinned_versions_summary(
        &self,
    ) -> Result<RiseCtlGetPinnedVersionsSummaryResponse> {
        let request = RiseCtlGetPinnedVersionsSummaryRequest {};
        self.inner
            .rise_ctl_get_pinned_versions_summary(request)
            .await
    }

    pub async fn risectl_get_pinned_snapshots_summary(
        &self,
    ) -> Result<RiseCtlGetPinnedSnapshotsSummaryResponse> {
        let request = RiseCtlGetPinnedSnapshotsSummaryRequest {};
        self.inner
            .rise_ctl_get_pinned_snapshots_summary(request)
            .await
    }

    pub async fn init_metadata_for_replay(
        &self,
        tables: Vec<ProstTable>,
        compaction_groups: Vec<CompactionGroup>,
    ) -> Result<()> {
        let req = InitMetadataForReplayRequest {
            tables,
            compaction_groups,
        };
        let _resp = self.inner.init_metadata_for_replay(req).await?;
        Ok(())
    }

    pub async fn set_compactor_runtime_config(&self, config: CompactorRuntimeConfig) -> Result<()> {
        let req = SetCompactorRuntimeConfigRequest {
            context_id: self.worker_id,
            config: Some(config.into()),
        };
        let _resp = self.inner.set_compactor_runtime_config(req).await?;
        Ok(())
    }

    pub async fn replay_version_delta(
        &self,
        version_delta: HummockVersionDelta,
    ) -> Result<(HummockVersion, Vec<CompactionGroupId>)> {
        let req = ReplayVersionDeltaRequest {
            version_delta: Some(version_delta),
        };
        let resp = self.inner.replay_version_delta(req).await?;
        Ok((resp.version.unwrap(), resp.modified_compaction_groups))
    }

    pub async fn list_version_deltas(
        &self,
        start_id: u64,
        num_limit: u32,
        committed_epoch_limit: HummockEpoch,
    ) -> Result<HummockVersionDeltas> {
        let req = ListVersionDeltasRequest {
            start_id,
            num_limit,
            committed_epoch_limit,
        };
        Ok(self
            .inner
            .list_version_deltas(req)
            .await?
            .version_deltas
            .unwrap())
    }

    pub async fn trigger_compaction_deterministic(
        &self,
        version_id: HummockVersionId,
        compaction_groups: Vec<CompactionGroupId>,
    ) -> Result<()> {
        let req = TriggerCompactionDeterministicRequest {
            version_id,
            compaction_groups,
        };
        self.inner.trigger_compaction_deterministic(req).await?;
        Ok(())
    }

    pub async fn disable_commit_epoch(&self) -> Result<HummockVersion> {
        let req = DisableCommitEpochRequest {};
        Ok(self
            .inner
            .disable_commit_epoch(req)
            .await?
            .current_version
            .unwrap())
    }

    pub async fn pin_specific_snapshot(&self, epoch: HummockEpoch) -> Result<HummockSnapshot> {
        let req = PinSpecificSnapshotRequest {
            context_id: self.worker_id(),
            epoch,
        };
        let resp = self.inner.pin_specific_snapshot(req).await?;
        Ok(resp.snapshot.unwrap())
    }

    pub async fn get_assigned_compact_task_num(&self) -> Result<usize> {
        let req = GetAssignedCompactTaskNumRequest {};
        let resp = self.inner.get_assigned_compact_task_num(req).await?;
        Ok(resp.num_tasks as usize)
    }

    pub async fn risectl_list_compaction_group(&self) -> Result<Vec<CompactionGroup>> {
        let req = RiseCtlListCompactionGroupRequest {};
        let resp = self.inner.rise_ctl_list_compaction_group(req).await?;
        Ok(resp.compaction_groups)
    }

    pub async fn risectl_update_compaction_config(
        &self,
        compaction_groups: &[CompactionGroupId],
        configs: &[MutableConfig],
    ) -> Result<()> {
        let req = RiseCtlUpdateCompactionConfigRequest {
            compaction_group_ids: compaction_groups.to_vec(),
            configs: configs
                .iter()
                .map(
                    |c| rise_ctl_update_compaction_config_request::MutableConfig {
                        mutable_config: Some(c.clone()),
                    },
                )
                .collect(),
        };
        let _resp = self.inner.rise_ctl_update_compaction_config(req).await?;
        Ok(())
    }

    pub async fn backup_meta(&self) -> Result<u64> {
        let req = BackupMetaRequest {};
        let resp = self.inner.backup_meta(req).await?;
        Ok(resp.job_id)
    }

    pub async fn get_backup_job_status(&self, job_id: u64) -> Result<BackupJobStatus> {
        let req = GetBackupJobStatusRequest { job_id };
        let resp = self.inner.get_backup_job_status(req).await?;
        Ok(resp.job_status())
    }

    pub async fn delete_meta_snapshot(&self, snapshot_ids: &[u64]) -> Result<()> {
        let req = DeleteMetaSnapshotRequest {
            snapshot_ids: snapshot_ids.to_vec(),
        };
        let _resp = self.inner.delete_meta_snapshot(req).await?;
        Ok(())
    }

    pub async fn get_meta_snapshot_manifest(&self) -> Result<MetaSnapshotManifest> {
        let req = GetMetaSnapshotManifestRequest {};
        let resp = self.inner.get_meta_snapshot_manifest(req).await?;
        Ok(resp.manifest.expect("should exist"))
    }
}

#[async_trait]
impl HummockMetaClient for MetaClient {
    async fn unpin_version_before(&self, unpin_version_before: HummockVersionId) -> Result<()> {
        let req = UnpinVersionBeforeRequest {
            context_id: self.worker_id(),
            unpin_version_before,
        };
        self.inner.unpin_version_before(req).await?;
        Ok(())
    }

    async fn get_current_version(&self) -> Result<HummockVersion> {
        let req = GetCurrentVersionRequest::default();
        Ok(self
            .inner
            .get_current_version(req)
            .await?
            .current_version
            .unwrap())
    }

    async fn pin_snapshot(&self) -> Result<HummockSnapshot> {
        let req = PinSnapshotRequest {
            context_id: self.worker_id(),
        };
        let resp = self.inner.pin_snapshot(req).await?;
        Ok(resp.snapshot.unwrap())
    }

    async fn get_epoch(&self) -> Result<HummockSnapshot> {
        let req = GetEpochRequest {};
        let resp = self.inner.get_epoch(req).await?;
        Ok(resp.snapshot.unwrap())
    }

    async fn unpin_snapshot(&self) -> Result<()> {
        let req = UnpinSnapshotRequest {
            context_id: self.worker_id(),
        };
        self.inner.unpin_snapshot(req).await?;
        Ok(())
    }

    async fn unpin_snapshot_before(&self, pinned_epochs: HummockEpoch) -> Result<()> {
        let req = UnpinSnapshotBeforeRequest {
            context_id: self.worker_id(),
            // For unpin_snapshot_before, we do not care about snapshots list but only min epoch.
            min_snapshot: Some(HummockSnapshot {
                committed_epoch: pinned_epochs,
                current_epoch: pinned_epochs,
            }),
        };
        self.inner.unpin_snapshot_before(req).await?;
        Ok(())
    }

    async fn get_new_sst_ids(&self, number: u32) -> Result<SstIdRange> {
        let resp = self
            .inner
            .get_new_sst_ids(GetNewSstIdsRequest { number })
            .await?;
        Ok(SstIdRange::new(resp.start_id, resp.end_id))
    }

    async fn report_compaction_task(
        &self,
        compact_task: CompactTask,
        table_stats_change: HashMap<u32, risingwave_hummock_sdk::table_stats::TableStats>,
    ) -> Result<()> {
        let req = ReportCompactionTasksRequest {
            context_id: self.worker_id(),
            compact_task: Some(compact_task),
            table_stats_change: to_prost_table_stats_map(table_stats_change),
        };
        self.inner.report_compaction_tasks(req).await?;
        Ok(())
    }

    async fn commit_epoch(
        &self,
        _epoch: HummockEpoch,
        _sstables: Vec<LocalSstableInfo>,
    ) -> Result<()> {
        panic!("Only meta service can commit_epoch in production.")
    }

    async fn update_current_epoch(&self, _epoch: HummockEpoch) -> Result<()> {
        panic!("Only meta service can update_current_epoch in production.")
    }

    async fn subscribe_compact_tasks(
        &self,
        max_concurrent_task_number: u64,
    ) -> Result<BoxStream<'static, CompactTaskItem>> {
        let req = SubscribeCompactTasksRequest {
            context_id: self.worker_id(),
            max_concurrent_task_number,
        };
        let stream = self.inner.subscribe_compact_tasks(req).await?;
        Ok(Box::pin(stream))
    }

    async fn report_compaction_task_progress(
        &self,
        progress: Vec<CompactTaskProgress>,
    ) -> Result<()> {
        let req = ReportCompactionTaskProgressRequest {
            context_id: self.worker_id(),
            progress,
        };
        self.inner.report_compaction_task_progress(req).await?;
        Ok(())
    }

    async fn report_vacuum_task(&self, vacuum_task: VacuumTask) -> Result<()> {
        let req = ReportVacuumTaskRequest {
            vacuum_task: Some(vacuum_task),
        };
        self.inner.report_vacuum_task(req).await?;
        Ok(())
    }

    async fn report_full_scan_task(&self, sst_ids: Vec<HummockSstableId>) -> Result<()> {
        let req = ReportFullScanTaskRequest { sst_ids };
        self.inner.report_full_scan_task(req).await?;
        Ok(())
    }

    async fn get_compaction_groups(&self) -> Result<Vec<CompactionGroup>> {
        let req = GetCompactionGroupsRequest {};
        let resp = self.inner.get_compaction_groups(req).await?;
        Ok(resp.compaction_groups)
    }

    async fn trigger_manual_compaction(
        &self,
        compaction_group_id: u64,
        table_id: u32,
        level: u32,
    ) -> Result<()> {
        // TODO: support key_range parameter
        let req = TriggerManualCompactionRequest {
            compaction_group_id,
            table_id, /* if table_id not exist, manual_compaction will include all the sst
                       * without check internal_table_id */
            level,
            ..Default::default()
        };

        self.inner.trigger_manual_compaction(req).await?;
        Ok(())
    }

    async fn trigger_full_gc(&self, sst_retention_time_sec: u64) -> Result<()> {
        self.inner
            .trigger_full_gc(TriggerFullGcRequest {
                sst_retention_time_sec,
            })
            .await?;
        Ok(())
    }
}

/// wrapper for `get_channel`. Does not retry
pub async fn get_channel_no_retry(
    addr: &str,
) -> std::result::Result<Channel, tonic::transport::Error> {
    get_channel(
        addr,
        GRPC_CONN_RETRY_MAX_INTERVAL_MS,
        GRPC_CONN_RETRY_BASE_INTERVAL_MS,
        GRPC_ENDPOINT_KEEP_ALIVE_INTERVAL_SEC,
        GRPC_ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC,
        false,
    )
    .await
}

/// wrapper for `get_channel`
pub async fn get_channel_with_defaults(
    addr: &str,
) -> std::result::Result<Channel, tonic::transport::Error> {
    get_channel(
        addr,
        GRPC_CONN_RETRY_MAX_INTERVAL_MS,
        GRPC_CONN_RETRY_BASE_INTERVAL_MS,
        GRPC_ENDPOINT_KEEP_ALIVE_INTERVAL_SEC,
        GRPC_ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC,
        true,
    )
    .await
}

/// get a channel against server at `addr`
///
/// ## Arguments:
/// addr: Should consist out of protocol, IP and port
/// retry: If true retries using exponential backoff. If false, only tries once
async fn get_channel(
    addr: &str,
    max_retry_ms: u64,
    retry_base_interval: u64,
    keep_alive_interval: u64,
    keep_alive_timeout: u64,
    retry: bool,
) -> std::result::Result<Channel, tonic::transport::Error> {
    let endpoint = Endpoint::from_shared(addr.to_string())?
        .initial_connection_window_size(MAX_CONNECTION_WINDOW_SIZE);
    let do_request = || async {
        let endpoint = endpoint.clone();
        endpoint
            .http2_keep_alive_interval(Duration::from_secs(keep_alive_interval))
            .keep_alive_timeout(Duration::from_secs(keep_alive_timeout))
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await
            .inspect_err(|e| {
                let wait_msg = if retry { ", wait for online" } else { "" };
                tracing::warn!(
                    "Failed to connect to meta server {}{}: {}",
                    addr,
                    wait_msg,
                    e
                );
            })
    };

    if retry {
        let retry_strategy = ExponentialBackoff::from_millis(retry_base_interval)
            .max_delay(Duration::from_millis(max_retry_ms))
            .map(jitter);
        return tokio_retry::Retry::spawn(retry_strategy, do_request).await;
    }
    do_request().await
}

/// Client to meta server. Cloning the instance is lightweight.
///
/// It is a wrapper of tonic client. See [`meta_rpc_client_method_impl`].
#[derive(Debug, Clone)]
struct GrpcMetaClient {
    leader_service_addr: String,
    leader_address: Arc<Mutex<HostAddress>>,
    leader_client: Arc<Mutex<LeaderServiceClient<Channel>>>,
    cluster_client: Arc<Mutex<ClusterServiceClient<Channel>>>,
    heartbeat_client: Arc<Mutex<HeartbeatServiceClient<Channel>>>,
    ddl_client: Arc<Mutex<DdlServiceClient<Channel>>>,
    hummock_client: Arc<Mutex<HummockManagerServiceClient<Channel>>>,
    notification_client: Arc<Mutex<NotificationServiceClient<Channel>>>,
    stream_client: Arc<Mutex<StreamManagerServiceClient<Channel>>>,
    user_client: Arc<Mutex<UserServiceClient<Channel>>>,
    scale_client: Arc<Mutex<ScaleServiceClient<Channel>>>,
    backup_client: Arc<Mutex<BackupServiceClient<Channel>>>,
}

// Retry base interval in ms for connecting to meta server.
const GRPC_CONN_RETRY_BASE_INTERVAL_MS: u64 = 100;
// Max retry interval in ms for connecting to meta server.
const GRPC_CONN_RETRY_MAX_INTERVAL_MS: u64 = 5000;
// See `Endpoint::http2_keep_alive_interval`
const GRPC_ENDPOINT_KEEP_ALIVE_INTERVAL_SEC: u64 = 60;
// See `Endpoint::keep_alive_timeout`
const GRPC_ENDPOINT_KEEP_ALIVE_TIMEOUT_SEC: u64 = 60;
// Max retry times for request to meta server.
const GRPC_REQUEST_RETRY_BASE_INTERVAL_MS: u64 = 50;
// Max retry times for connecting to meta server.
const GRPC_REQUEST_RETRY_MAX_ATTEMPTS: usize = 10;
// Max retry interval in ms for request to meta server.
const GRPC_REQUEST_RETRY_MAX_INTERVAL_MS: u64 = 5000;

impl GrpcMetaClient {
    pub fn get_retry_strategy(&self) -> impl Iterator<Item = Duration> {
        GrpcMetaClient::retry_strategy_for_request()
    }

    /// Retrieve leader from currently connected meta node
    /// Helper function used to scope `MutexGuard`
    async fn make_leader_request(&self) -> std::result::Result<Response<LeaderResponse>, Status> {
        let mut lc = self.leader_client.as_ref().lock().await;
        lc.leader(LeaderRequest {}).await
    }

    // None if leader is presumed failed
    // HostAddress if connected against a working leader or follower node
    // Node may return a stale leader
    async fn try_get_leader_from_connected_node(&self) -> Option<HostAddress> {
        for retry in GrpcMetaClient::retry_strategy_for_request() {
            let response = self.make_leader_request().await;

            // assume leader node crashed if err is unavailable, unimplemented or unknown
            // else repeat request
            if response.is_err() {
                let err_code = response.err().unwrap().code();
                if err_code == tonic::Code::Unavailable
                    || err_code == tonic::Code::Unimplemented
                    || err_code == tonic::Code::Unknown
                {
                    return None;
                }
                tokio::time::sleep(retry).await;
                continue;
            }

            // Meta node is up. May be follower or leader node
            let current_leader = response.unwrap().into_inner().leader_addr;
            if current_leader.is_none() {
                tracing::warn!("Meta node did not know who leader is (current connected node). Trying again...");
                tokio::time::sleep(retry).await;
                continue;
            }
            return Some(current_leader.unwrap());
        }
        None
    }

    // Retrieve the leader from any of the meta nodes via a K8s service / load-balancer
    async fn get_current_leader_from_service(&self) -> Option<HostAddress> {
        for retry in GrpcMetaClient::retry_strategy_for_request() {
            tracing::info!("in get_current_leader_from_service");
            let service_addr = &self.leader_service_addr;
            let service_channel = get_channel_with_defaults(service_addr)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "Unable to establish channel against Meta Service at {}",
                        service_addr
                    );
                });
            let mut lc = LeaderServiceClient::new(service_channel);
            let mut response = lc.leader(LeaderRequest {}).await;
            for retry in self.get_retry_strategy() {
                if response.is_ok() {
                    break;
                }
                tokio::time::sleep(retry).await;
                response = lc.leader(LeaderRequest {}).await;
            }

            let current_leader = response.unwrap().into_inner().leader_addr;
            if current_leader.is_none() {
                tracing::warn!("Meta node did not know who leader is (service). Trying again...");
                tokio::time::sleep(retry).await;
                continue;
            }
            let current_leader = current_leader.unwrap();

            return Some(current_leader);
        }
        None
    }

    async fn do_failover(&self, mut current_leader_init: Option<HostAddress>) {
        for retry in self.get_retry_strategy() {
            tracing::info!("in do_failover"); // TODO: Remove line

            let current_leader = if current_leader_init.is_some() {
                current_leader_init.clone()
            } else {
                self.get_current_leader_from_service().await
            };
            if current_leader.is_none() {
                // May happen if all meta nodes are down OR if there is a problem with Nginx routing
                // the request to the meta nodes
                tracing::warn!("Unable to retrieve leader address via service. Retrying...");
                tokio::time::sleep(retry).await;
                continue;
            }
            let current_leader = current_leader.expect("Should know current leader");

            // always use service for retries
            current_leader_init = None;

            // TODO: How do we know that it always is http?
            let addr = format!(
                "http://{}:{}",
                current_leader.get_host(),
                current_leader.get_port()
            );
            tracing::info!("Failing over to new meta leader {}", addr);
            let leader_channel = match get_channel_no_retry(addr.as_str()).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::warn!("Failed to establish connection to leader at {}. Err was {}. Assuming stale leader info. Retrying...", addr, e);
                    tokio::time::sleep(retry).await;
                    continue;
                }
            };

            // Hold locks on all sub-clients, to update atomically
            {
                let mut leader_c = self.leader_client.as_ref().lock().await;
                let mut cluster_c = self.cluster_client.as_ref().lock().await;
                let mut heartbeat_c = self.heartbeat_client.as_ref().lock().await;
                let mut ddl_c = self.ddl_client.as_ref().lock().await;
                let mut hummock_c = self.hummock_client.as_ref().lock().await;
                let mut notification_c = self.notification_client.as_ref().lock().await;
                let mut stream_c = self.stream_client.as_ref().lock().await;
                let mut user_c = self.user_client.as_ref().lock().await;
                let mut scale_c = self.scale_client.as_ref().lock().await;
                let mut backup_c = self.backup_client.as_ref().lock().await;

                *leader_c = LeaderServiceClient::new(leader_channel.clone());
                *cluster_c = ClusterServiceClient::new(leader_channel.clone());
                *heartbeat_c = HeartbeatServiceClient::new(leader_channel.clone());
                *ddl_c = DdlServiceClient::new(leader_channel.clone());
                *hummock_c = HummockManagerServiceClient::new(leader_channel.clone());
                *notification_c = NotificationServiceClient::new(leader_channel.clone());
                *stream_c = StreamManagerServiceClient::new(leader_channel.clone());
                *user_c = UserServiceClient::new(leader_channel.clone());
                *scale_c = ScaleServiceClient::new(leader_channel.clone());
                *backup_c = BackupServiceClient::new(leader_channel);
            } // release MutexGuards on all clients

            tracing::info!("Successfully failed over meta_client to {}", addr);
        }
    }

    /// Execute the failover if it is needed. If no failover is needed do nothing
    /// Returns true if failover was needed, else false
    pub async fn do_failover_if_needed(&self) -> bool {
        let current_leader = self.try_get_leader_from_connected_node().await;
        let current_leader_clone = current_leader.clone();
        let connected_node = self.leader_address.as_ref().lock().await.clone();
        if current_leader.is_some() && current_leader.unwrap() == connected_node {
            return false;
        }
        tracing::warn!(
            "failing over meta_client. Old meta node was {:?}",
            connected_node,
        );
        self.do_failover(current_leader_clone).await;
        true
    }

    /// Connect to the meta server `addr`.
    pub async fn new(addr: &str) -> Result<Self> {
        tracing::info!("GrpcMetaClient originally connected against {}", addr);
        let channel = get_channel_with_defaults(addr).await?;

        // Dummy address forces a client failover
        let dummy_address = HostAddress {
            host: "dummy".to_owned(),
            port: -1,
        };

        let client = GrpcMetaClient {
            leader_service_addr: addr.to_owned(),
            leader_address: Arc::new(Mutex::new(dummy_address)),
            leader_client: Arc::new(Mutex::new(LeaderServiceClient::new(channel.clone()))),
            cluster_client: Arc::new(Mutex::new(ClusterServiceClient::new(channel.clone()))),
            heartbeat_client: Arc::new(Mutex::new(HeartbeatServiceClient::new(channel.clone()))),
            ddl_client: Arc::new(Mutex::new(DdlServiceClient::new(channel.clone()))),
            hummock_client: Arc::new(Mutex::new(HummockManagerServiceClient::new(
                channel.clone(),
            ))),
            notification_client: Arc::new(Mutex::new(NotificationServiceClient::new(
                channel.clone(),
            ))),
            stream_client: Arc::new(Mutex::new(StreamManagerServiceClient::new(channel.clone()))),
            user_client: Arc::new(Mutex::new(UserServiceClient::new(channel.clone()))),
            scale_client: Arc::new(Mutex::new(ScaleServiceClient::new(channel.clone()))),
            backup_client: Arc::new(Mutex::new(BackupServiceClient::new(channel))),
        };

        // Original connection may be against follower meta node OR against a K8s service/Nginx LB
        client.do_failover_if_needed().await;

        Ok(client)
    }

    /// Return retry strategy for retrying meta requests.
    pub fn retry_strategy_for_request() -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(GRPC_REQUEST_RETRY_BASE_INTERVAL_MS)
            .max_delay(Duration::from_millis(GRPC_REQUEST_RETRY_MAX_INTERVAL_MS))
            .map(jitter)
            .take(GRPC_REQUEST_RETRY_MAX_ATTEMPTS)
    }
}

macro_rules! for_all_meta_rpc {
    ($macro:ident) => {
        $macro! {
             { cluster_client, add_worker_node, AddWorkerNodeRequest, AddWorkerNodeResponse }
            ,{ cluster_client, activate_worker_node, ActivateWorkerNodeRequest, ActivateWorkerNodeResponse }
            ,{ cluster_client, delete_worker_node, DeleteWorkerNodeRequest, DeleteWorkerNodeResponse }
            //(not used) ,{ cluster_client, list_all_nodes, ListAllNodesRequest, ListAllNodesResponse }
            ,{ heartbeat_client, heartbeat, HeartbeatRequest, HeartbeatResponse }
            ,{ stream_client, flush, FlushRequest, FlushResponse }
            ,{ stream_client, list_table_fragments, ListTableFragmentsRequest, ListTableFragmentsResponse }
            ,{ ddl_client, create_table, CreateTableRequest, CreateTableResponse }
            ,{ ddl_client, create_materialized_view, CreateMaterializedViewRequest, CreateMaterializedViewResponse }
            ,{ ddl_client, create_view, CreateViewRequest, CreateViewResponse }
            ,{ ddl_client, create_source, CreateSourceRequest, CreateSourceResponse }
            ,{ ddl_client, create_sink, CreateSinkRequest, CreateSinkResponse }
            ,{ ddl_client, create_schema, CreateSchemaRequest, CreateSchemaResponse }
            ,{ ddl_client, create_database, CreateDatabaseRequest, CreateDatabaseResponse }
            ,{ ddl_client, create_index, CreateIndexRequest, CreateIndexResponse }
            ,{ ddl_client, create_function, CreateFunctionRequest, CreateFunctionResponse }
            ,{ ddl_client, drop_table, DropTableRequest, DropTableResponse }
            ,{ ddl_client, drop_materialized_view, DropMaterializedViewRequest, DropMaterializedViewResponse }
            ,{ ddl_client, drop_view, DropViewRequest, DropViewResponse }
            ,{ ddl_client, drop_source, DropSourceRequest, DropSourceResponse }
            ,{ ddl_client, drop_sink, DropSinkRequest, DropSinkResponse }
            ,{ ddl_client, drop_database, DropDatabaseRequest, DropDatabaseResponse }
            ,{ ddl_client, drop_schema, DropSchemaRequest, DropSchemaResponse }
            ,{ ddl_client, drop_index, DropIndexRequest, DropIndexResponse }
            ,{ ddl_client, drop_function, DropFunctionRequest, DropFunctionResponse }
            ,{ ddl_client, risectl_list_state_tables, RisectlListStateTablesRequest, RisectlListStateTablesResponse }
            ,{ hummock_client, unpin_version_before, UnpinVersionBeforeRequest, UnpinVersionBeforeResponse }
            ,{ hummock_client, get_current_version, GetCurrentVersionRequest, GetCurrentVersionResponse }
            ,{ hummock_client, replay_version_delta, ReplayVersionDeltaRequest, ReplayVersionDeltaResponse }
            ,{ hummock_client, list_version_deltas, ListVersionDeltasRequest, ListVersionDeltasResponse }
            ,{ hummock_client, get_assigned_compact_task_num, GetAssignedCompactTaskNumRequest, GetAssignedCompactTaskNumResponse }
            ,{ hummock_client, trigger_compaction_deterministic, TriggerCompactionDeterministicRequest, TriggerCompactionDeterministicResponse }
            ,{ hummock_client, disable_commit_epoch, DisableCommitEpochRequest, DisableCommitEpochResponse }
            ,{ hummock_client, pin_snapshot, PinSnapshotRequest, PinSnapshotResponse }
            ,{ hummock_client, pin_specific_snapshot, PinSpecificSnapshotRequest, PinSnapshotResponse }
            ,{ hummock_client, get_epoch, GetEpochRequest, GetEpochResponse }
            ,{ hummock_client, unpin_snapshot, UnpinSnapshotRequest, UnpinSnapshotResponse }
            ,{ hummock_client, unpin_snapshot_before, UnpinSnapshotBeforeRequest, UnpinSnapshotBeforeResponse }
            ,{ hummock_client, report_compaction_tasks, ReportCompactionTasksRequest, ReportCompactionTasksResponse }
            ,{ hummock_client, get_new_sst_ids, GetNewSstIdsRequest, GetNewSstIdsResponse }
            ,{ hummock_client, subscribe_compact_tasks, SubscribeCompactTasksRequest, Streaming<SubscribeCompactTasksResponse> }
            ,{ hummock_client, report_compaction_task_progress, ReportCompactionTaskProgressRequest, ReportCompactionTaskProgressResponse }
            ,{ hummock_client, report_vacuum_task, ReportVacuumTaskRequest, ReportVacuumTaskResponse }
            ,{ hummock_client, get_compaction_groups, GetCompactionGroupsRequest, GetCompactionGroupsResponse }
            ,{ hummock_client, trigger_manual_compaction, TriggerManualCompactionRequest, TriggerManualCompactionResponse }
            ,{ hummock_client, report_full_scan_task, ReportFullScanTaskRequest, ReportFullScanTaskResponse }
            ,{ hummock_client, trigger_full_gc, TriggerFullGcRequest, TriggerFullGcResponse }
            ,{ hummock_client, rise_ctl_get_pinned_versions_summary, RiseCtlGetPinnedVersionsSummaryRequest, RiseCtlGetPinnedVersionsSummaryResponse }
            ,{ hummock_client, rise_ctl_get_pinned_snapshots_summary, RiseCtlGetPinnedSnapshotsSummaryRequest, RiseCtlGetPinnedSnapshotsSummaryResponse }
            ,{ hummock_client, rise_ctl_list_compaction_group, RiseCtlListCompactionGroupRequest, RiseCtlListCompactionGroupResponse }
            ,{ hummock_client, rise_ctl_update_compaction_config, RiseCtlUpdateCompactionConfigRequest, RiseCtlUpdateCompactionConfigResponse }
            ,{ hummock_client, init_metadata_for_replay, InitMetadataForReplayRequest, InitMetadataForReplayResponse }
            ,{ hummock_client, set_compactor_runtime_config, SetCompactorRuntimeConfigRequest, SetCompactorRuntimeConfigResponse }
            ,{ user_client, create_user, CreateUserRequest, CreateUserResponse }
            ,{ user_client, update_user, UpdateUserRequest, UpdateUserResponse }
            ,{ user_client, drop_user, DropUserRequest, DropUserResponse }
            ,{ user_client, grant_privilege, GrantPrivilegeRequest, GrantPrivilegeResponse }
            ,{ user_client, revoke_privilege, RevokePrivilegeRequest, RevokePrivilegeResponse }
            ,{ scale_client, pause, PauseRequest, PauseResponse }
            ,{ scale_client, resume, ResumeRequest, ResumeResponse }
            ,{ scale_client, get_cluster_info, GetClusterInfoRequest, GetClusterInfoResponse }
            ,{ scale_client, reschedule, RescheduleRequest, RescheduleResponse }
            ,{ notification_client, subscribe, SubscribeRequest, Streaming<SubscribeResponse> }
            ,{ backup_client, backup_meta, BackupMetaRequest, BackupMetaResponse }
            ,{ backup_client, get_backup_job_status, GetBackupJobStatusRequest, GetBackupJobStatusResponse }
            ,{ backup_client, delete_meta_snapshot, DeleteMetaSnapshotRequest, DeleteMetaSnapshotResponse}
            ,{ backup_client, get_meta_snapshot_manifest, GetMetaSnapshotManifestRequest, GetMetaSnapshotManifestResponse}
        }
    };
}

impl GrpcMetaClient {
    for_all_meta_rpc! { meta_rpc_client_method_impl }
}
