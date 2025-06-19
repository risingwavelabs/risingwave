// Copyright 2025 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use risingwave_common::session_config::SessionConfig;
use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_common::util::cluster_limit::ClusterLimit;
use risingwave_hummock_sdk::HummockVersionId;
use risingwave_hummock_sdk::version::{HummockVersion, HummockVersionDelta};
use risingwave_pb::backup_service::MetaSnapshotMetadata;
use risingwave_pb::catalog::Table;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::write_limits::WriteLimit;
use risingwave_pb::hummock::{
    BranchedObject, CompactTaskAssignment, CompactTaskProgress, CompactionGroupInfo,
};
use risingwave_pb::meta::cancel_creating_jobs_request::PbJobs;
use risingwave_pb::meta::list_actor_splits_response::ActorSplit;
use risingwave_pb::meta::list_actor_states_response::ActorState;
use risingwave_pb::meta::list_iceberg_tables_response::IcebergTable;
use risingwave_pb::meta::list_object_dependencies_response::PbObjectDependencies;
use risingwave_pb::meta::list_rate_limits_response::RateLimitInfo;
use risingwave_pb::meta::list_streaming_job_states_response::StreamingJobState;
use risingwave_pb::meta::list_table_fragments_response::TableFragmentInfo;
use risingwave_pb::meta::{EventLog, FragmentDistribution, PbThrottleTarget, RecoveryStatus};
use risingwave_pb::secret::PbSecretRef;
use risingwave_rpc_client::error::Result;
use risingwave_rpc_client::{HummockMetaClient, MetaClient};

use crate::catalog::DatabaseId;

/// A wrapper around the `MetaClient` that only provides a minor set of meta rpc.
/// Most of the rpc to meta are delegated by other separate structs like `CatalogWriter`,
/// `WorkerNodeManager`, etc. So frontend rarely needs to call `MetaClient` directly.
/// Hence instead of to mock all rpc of `MetaClient` in tests, we aggregate those "direct" rpc
/// in this trait so that the mocking can be simplified.
#[async_trait::async_trait]
pub trait FrontendMetaClient: Send + Sync {
    async fn try_unregister(&self);

    async fn flush(&self, database_id: DatabaseId) -> Result<HummockVersionId>;

    async fn wait(&self) -> Result<()>;

    async fn recover(&self) -> Result<()>;

    async fn cancel_creating_jobs(&self, jobs: PbJobs) -> Result<Vec<u32>>;

    async fn list_table_fragments(
        &self,
        table_ids: &[u32],
    ) -> Result<HashMap<u32, TableFragmentInfo>>;

    async fn list_streaming_job_states(&self) -> Result<Vec<StreamingJobState>>;

    async fn list_fragment_distribution(&self) -> Result<Vec<FragmentDistribution>>;

    async fn list_creating_fragment_distribution(&self) -> Result<Vec<FragmentDistribution>>;

    async fn list_actor_states(&self) -> Result<Vec<ActorState>>;

    async fn list_actor_splits(&self) -> Result<Vec<ActorSplit>>;

    async fn list_object_dependencies(&self) -> Result<Vec<PbObjectDependencies>>;

    async fn list_meta_snapshots(&self) -> Result<Vec<MetaSnapshotMetadata>>;

    async fn set_system_param(
        &self,
        param: String,
        value: Option<String>,
    ) -> Result<Option<SystemParamsReader>>;

    async fn get_session_params(&self) -> Result<SessionConfig>;

    async fn set_session_param(&self, param: String, value: Option<String>) -> Result<String>;

    async fn get_ddl_progress(&self) -> Result<Vec<DdlProgress>>;

    async fn get_tables(
        &self,
        table_ids: &[u32],
        include_dropped_table: bool,
    ) -> Result<HashMap<u32, Table>>;

    /// Returns vector of (worker_id, min_pinned_version_id)
    async fn list_hummock_pinned_versions(&self) -> Result<Vec<(u32, u64)>>;

    async fn get_hummock_current_version(&self) -> Result<HummockVersion>;

    async fn get_hummock_checkpoint_version(&self) -> Result<HummockVersion>;

    async fn list_version_deltas(&self) -> Result<Vec<HummockVersionDelta>>;

    async fn list_branched_objects(&self) -> Result<Vec<BranchedObject>>;

    async fn list_hummock_compaction_group_configs(&self) -> Result<Vec<CompactionGroupInfo>>;

    async fn list_hummock_active_write_limits(&self) -> Result<HashMap<u64, WriteLimit>>;

    async fn list_hummock_meta_configs(&self) -> Result<HashMap<String, String>>;

    async fn list_event_log(&self) -> Result<Vec<EventLog>>;
    async fn list_compact_task_assignment(&self) -> Result<Vec<CompactTaskAssignment>>;

    async fn list_all_nodes(&self) -> Result<Vec<WorkerNode>>;

    async fn list_compact_task_progress(&self) -> Result<Vec<CompactTaskProgress>>;

    async fn apply_throttle(
        &self,
        kind: PbThrottleTarget,
        id: u32,
        rate_limit: Option<u32>,
    ) -> Result<()>;

    async fn get_cluster_recovery_status(&self) -> Result<RecoveryStatus>;

    async fn get_cluster_limits(&self) -> Result<Vec<ClusterLimit>>;

    async fn list_rate_limits(&self) -> Result<Vec<RateLimitInfo>>;

    async fn get_meta_store_endpoint(&self) -> Result<String>;

    async fn alter_sink_props(
        &self,
        sink_id: u32,
        changed_props: BTreeMap<String, String>,
        changed_secret_refs: BTreeMap<String, PbSecretRef>,
        connector_conn_ref: Option<u32>,
    ) -> Result<()>;

    async fn list_hosted_iceberg_tables(&self) -> Result<Vec<IcebergTable>>;

    async fn get_fragment_by_id(&self, fragment_id: u32) -> Result<Option<FragmentDistribution>>;

    fn worker_id(&self) -> u32;
}

pub struct FrontendMetaClientImpl(pub MetaClient);

#[async_trait::async_trait]
impl FrontendMetaClient for FrontendMetaClientImpl {
    async fn try_unregister(&self) {
        self.0.try_unregister().await;
    }

    async fn flush(&self, database_id: DatabaseId) -> Result<HummockVersionId> {
        self.0.flush(database_id).await
    }

    async fn wait(&self) -> Result<()> {
        self.0.wait().await
    }

    async fn recover(&self) -> Result<()> {
        self.0.recover().await
    }

    async fn cancel_creating_jobs(&self, infos: PbJobs) -> Result<Vec<u32>> {
        self.0.cancel_creating_jobs(infos).await
    }

    async fn list_table_fragments(
        &self,
        table_ids: &[u32],
    ) -> Result<HashMap<u32, TableFragmentInfo>> {
        self.0.list_table_fragments(table_ids).await
    }

    async fn list_streaming_job_states(&self) -> Result<Vec<StreamingJobState>> {
        self.0.list_streaming_job_states().await
    }

    async fn list_fragment_distribution(&self) -> Result<Vec<FragmentDistribution>> {
        self.0.list_fragment_distributions().await
    }

    async fn list_creating_fragment_distribution(&self) -> Result<Vec<FragmentDistribution>> {
        self.0.list_creating_fragment_distribution().await
    }

    async fn list_actor_states(&self) -> Result<Vec<ActorState>> {
        self.0.list_actor_states().await
    }

    async fn list_actor_splits(&self) -> Result<Vec<ActorSplit>> {
        self.0.list_actor_splits().await
    }

    async fn list_object_dependencies(&self) -> Result<Vec<PbObjectDependencies>> {
        self.0.list_object_dependencies().await
    }

    async fn list_meta_snapshots(&self) -> Result<Vec<MetaSnapshotMetadata>> {
        let manifest = self.0.get_meta_snapshot_manifest().await?;
        Ok(manifest.snapshot_metadata)
    }

    async fn set_system_param(
        &self,
        param: String,
        value: Option<String>,
    ) -> Result<Option<SystemParamsReader>> {
        self.0.set_system_param(param, value).await
    }

    async fn get_session_params(&self) -> Result<SessionConfig> {
        let session_config: SessionConfig =
            serde_json::from_str(&self.0.get_session_params().await?)
                .context("failed to parse session config")?;
        Ok(session_config)
    }

    async fn set_session_param(&self, param: String, value: Option<String>) -> Result<String> {
        self.0.set_session_param(param, value).await
    }

    async fn get_ddl_progress(&self) -> Result<Vec<DdlProgress>> {
        let ddl_progress = self.0.get_ddl_progress().await?;
        Ok(ddl_progress)
    }

    async fn get_tables(
        &self,
        table_ids: &[u32],
        include_dropped_tables: bool,
    ) -> Result<HashMap<u32, Table>> {
        let tables = self.0.get_tables(table_ids, include_dropped_tables).await?;
        Ok(tables)
    }

    async fn list_hummock_pinned_versions(&self) -> Result<Vec<(u32, u64)>> {
        let pinned_versions = self
            .0
            .risectl_get_pinned_versions_summary()
            .await?
            .summary
            .unwrap()
            .pinned_versions;
        let ret = pinned_versions
            .into_iter()
            .map(|v| (v.context_id, v.min_pinned_id))
            .collect();
        Ok(ret)
    }

    async fn get_hummock_current_version(&self) -> Result<HummockVersion> {
        self.0.get_current_version().await
    }

    async fn get_hummock_checkpoint_version(&self) -> Result<HummockVersion> {
        self.0
            .risectl_get_checkpoint_hummock_version()
            .await
            .map(|v| HummockVersion::from_rpc_protobuf(&v.checkpoint_version.unwrap()))
    }

    async fn list_version_deltas(&self) -> Result<Vec<HummockVersionDelta>> {
        // FIXME #8612: there can be lots of version deltas, so better to fetch them by pages and refactor `SysRowSeqScanExecutor` to yield multiple chunks.
        self.0
            .list_version_deltas(HummockVersionId::new(0), u32::MAX, u64::MAX)
            .await
    }

    async fn list_branched_objects(&self) -> Result<Vec<BranchedObject>> {
        self.0.list_branched_object().await
    }

    async fn list_hummock_compaction_group_configs(&self) -> Result<Vec<CompactionGroupInfo>> {
        self.0.risectl_list_compaction_group().await
    }

    async fn list_hummock_active_write_limits(&self) -> Result<HashMap<u64, WriteLimit>> {
        self.0.list_active_write_limit().await
    }

    async fn list_hummock_meta_configs(&self) -> Result<HashMap<String, String>> {
        self.0.list_hummock_meta_config().await
    }

    async fn list_event_log(&self) -> Result<Vec<EventLog>> {
        self.0.list_event_log().await
    }

    async fn list_compact_task_assignment(&self) -> Result<Vec<CompactTaskAssignment>> {
        self.0.list_compact_task_assignment().await
    }

    async fn list_all_nodes(&self) -> Result<Vec<WorkerNode>> {
        self.0.list_worker_nodes(None).await
    }

    async fn list_compact_task_progress(&self) -> Result<Vec<CompactTaskProgress>> {
        self.0.list_compact_task_progress().await
    }

    async fn apply_throttle(
        &self,
        kind: PbThrottleTarget,
        id: u32,
        rate_limit: Option<u32>,
    ) -> Result<()> {
        self.0
            .apply_throttle(kind, id, rate_limit)
            .await
            .map(|_| ())
    }

    async fn get_cluster_recovery_status(&self) -> Result<RecoveryStatus> {
        self.0.get_cluster_recovery_status().await
    }

    async fn get_cluster_limits(&self) -> Result<Vec<ClusterLimit>> {
        self.0.get_cluster_limits().await
    }

    async fn list_rate_limits(&self) -> Result<Vec<RateLimitInfo>> {
        self.0.list_rate_limits().await
    }

    async fn get_meta_store_endpoint(&self) -> Result<String> {
        self.0.get_meta_store_endpoint().await
    }

    async fn alter_sink_props(
        &self,
        sink_id: u32,
        changed_props: BTreeMap<String, String>,
        changed_secret_refs: BTreeMap<String, PbSecretRef>,
        connector_conn_ref: Option<u32>,
    ) -> Result<()> {
        self.0
            .alter_sink_props(
                sink_id,
                changed_props,
                changed_secret_refs,
                connector_conn_ref,
            )
            .await
    }

    async fn list_hosted_iceberg_tables(&self) -> Result<Vec<IcebergTable>> {
        self.0.list_hosted_iceberg_tables().await
    }

    async fn get_fragment_by_id(&self, fragment_id: u32) -> Result<Option<FragmentDistribution>> {
        self.0.get_fragment_by_id(fragment_id).await
    }

    fn worker_id(&self) -> u32 {
        self.0.worker_id()
    }
}
