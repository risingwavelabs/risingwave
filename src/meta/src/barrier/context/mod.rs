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

mod context_impl;
mod recovery;

use std::future::Future;
use std::sync::Arc;

use arc_swap::ArcSwap;
use risingwave_common::catalog::DatabaseId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_service::streaming_control_stream_request::PbInitRequest;
use risingwave_rpc_client::StreamingControlHandle;

use crate::MetaResult;
use crate::barrier::command::CommandContext;
use crate::barrier::progress::TrackingJob;
use crate::barrier::schedule::{MarkReadyOptions, ScheduledBarriers};
use crate::barrier::{
    BarrierManagerStatus, BarrierWorkerRuntimeInfoSnapshot, DatabaseRuntimeInfoSnapshot,
    RecoveryReason, Scheduled,
};
use crate::hummock::{CommitEpochInfo, HummockManagerRef};
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::stream::{ScaleControllerRef, SourceManagerRef};

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

    fn post_collect_command<'a>(
        &'a self,
        command: &'a CommandContext,
    ) -> impl Future<Output = MetaResult<()>> + Send + 'a;

    async fn notify_creating_job_failed(&self, database_id: Option<DatabaseId>, err: String);

    fn finish_creating_job(
        &self,
        job: TrackingJob,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    async fn new_control_stream(
        &self,
        node: &WorkerNode,
        init_request: &PbInitRequest,
    ) -> MetaResult<StreamingControlHandle>;

    async fn reload_runtime_info(&self) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot>;

    async fn reload_database_runtime_info(
        &self,
        database_id: DatabaseId,
    ) -> MetaResult<Option<DatabaseRuntimeInfoSnapshot>>;
}

pub(super) struct GlobalBarrierWorkerContextImpl {
    scheduled_barriers: ScheduledBarriers,

    status: Arc<ArcSwap<BarrierManagerStatus>>,

    pub(super) metadata_manager: MetadataManager,

    hummock_manager: HummockManagerRef,

    source_manager: SourceManagerRef,

    scale_controller: ScaleControllerRef,

    pub(super) env: MetaSrvEnv,
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
    ) -> Self {
        Self {
            scheduled_barriers,
            status,
            metadata_manager,
            hummock_manager,
            source_manager,
            scale_controller,
            env,
        }
    }

    pub(super) fn status(&self) -> Arc<ArcSwap<BarrierManagerStatus>> {
        self.status.clone()
    }
}
