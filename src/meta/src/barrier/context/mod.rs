// Copyright 2024 RisingWave Labs
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

mod context_impl;
mod recovery;

use std::future::Future;
use std::sync::Arc;

use arc_swap::ArcSwap;
use risingwave_common::catalog::DatabaseId;
use risingwave_common::id::JobId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::barrier_complete_response::{
    PbListFinishedSource, PbLoadFinishedSource,
};
use risingwave_pb::stream_service::streaming_control_stream_request::PbInitRequest;
use risingwave_rpc_client::StreamingControlHandle;

use crate::MetaResult;
use crate::barrier::command::PostCollectCommand;
use crate::barrier::progress::TrackingJob;
use crate::barrier::schedule::{MarkReadyOptions, ScheduledBarriers};
use crate::barrier::{
    BarrierManagerStatus, BarrierScheduler, BarrierWorkerRuntimeInfoSnapshot,
    CreateStreamingJobCommandInfo, CreateStreamingJobType, DatabaseRuntimeInfoSnapshot,
    RecoveryReason, Scheduled, SnapshotBackfillInfo,
};
use crate::hummock::{CommitEpochInfo, HummockManagerRef};
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::stream::{GlobalRefreshManagerRef, ScaleControllerRef, SourceManagerRef};

#[derive(Debug)]
pub(super) struct CreateSnapshotBackfillJobCommandInfo {
    pub info: CreateStreamingJobCommandInfo,
    pub snapshot_backfill_info: SnapshotBackfillInfo,
    pub cross_db_snapshot_backfill_info: SnapshotBackfillInfo,
}

impl CreateSnapshotBackfillJobCommandInfo {
    pub(super) fn into_post_collect(self) -> PostCollectCommand {
        PostCollectCommand::CreateStreamingJob {
            info: self.info,
            job_type: CreateStreamingJobType::SnapshotBackfill(self.snapshot_backfill_info),
            cross_db_snapshot_backfill_info: self.cross_db_snapshot_backfill_info,
        }
    }
}

pub(super) trait GlobalBarrierWorkerContext: Send + Sync + 'static {
    fn commit_epoch(
        &self,
        commit_info: CommitEpochInfo,
    ) -> impl Future<Output = MetaResult<HummockVersionStats>> + Send + '_;

    async fn next_scheduled(&self) -> Scheduled;
    fn abort_and_mark_blocked(
        &self,
        database_id: Option<DatabaseId>,
        recovery_reason: RecoveryReason,
    );
    fn mark_ready(&self, options: MarkReadyOptions);

    fn post_collect_command(
        &self,
        command: PostCollectCommand,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    async fn notify_creating_job_failed(&self, database_id: Option<DatabaseId>, err: String);

    fn finish_creating_job(
        &self,
        job: TrackingJob,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    fn finish_cdc_table_backfill(
        &self,
        job_id: JobId,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    fn new_control_stream<'a>(
        &'a self,
        node: &'a WorkerNode,
        init_request: &'a PbInitRequest,
    ) -> impl Future<Output = MetaResult<StreamingControlHandle>> + Send + 'a;

    async fn reload_runtime_info(&self) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot>;

    async fn reload_database_runtime_info(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<Option<DatabaseRuntimeInfoSnapshot>>;

    fn handle_list_finished_source_ids(
        &self,
        list_finished_source_ids: Vec<PbListFinishedSource>,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    fn handle_load_finished_source_ids(
        &self,
        load_finished_source_ids: Vec<PbLoadFinishedSource>,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    fn handle_refresh_finished_table_ids(
        &self,
        refresh_finished_table_job_ids: Vec<JobId>,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;
}

pub(super) struct GlobalBarrierWorkerContextImpl {
    scheduled_barriers: ScheduledBarriers,

    status: Arc<ArcSwap<BarrierManagerStatus>>,

    pub(super) metadata_manager: MetadataManager,

    hummock_manager: HummockManagerRef,

    source_manager: SourceManagerRef,

    _scale_controller: ScaleControllerRef,

    pub(super) env: MetaSrvEnv,

    /// Barrier scheduler for scheduling load finish commands
    barrier_scheduler: BarrierScheduler,

    pub(super) refresh_manager: GlobalRefreshManagerRef,

    sink_manager: SinkCoordinatorManager,
}

impl GlobalBarrierWorkerContextImpl {
    pub(super) fn new(
        scheduled_barriers: ScheduledBarriers,
        status: Arc<ArcSwap<BarrierManagerStatus>>,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        scale_controller: ScaleControllerRef,
        env: MetaSrvEnv,
        barrier_scheduler: BarrierScheduler,
        refresh_manager: GlobalRefreshManagerRef,
        sink_manager: SinkCoordinatorManager,
    ) -> Self {
        Self {
            scheduled_barriers,
            status,
            metadata_manager,
            hummock_manager,
            source_manager,
            _scale_controller: scale_controller,
            env,
            barrier_scheduler,
            refresh_manager,
            sink_manager,
        }
    }

    pub(super) fn status(&self) -> Arc<ArcSwap<BarrierManagerStatus>> {
        self.status.clone()
    }
}
