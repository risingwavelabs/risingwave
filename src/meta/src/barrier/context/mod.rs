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
use risingwave_pb::common::WorkerNode;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::stream_plan::SubscriptionUpstreamInfo;
use risingwave_rpc_client::StreamingControlHandle;

use crate::barrier::command::CommandContext;
use crate::barrier::progress::TrackingJob;
use crate::barrier::schedule::ScheduledBarriers;
use crate::barrier::{
    BarrierManagerStatus, BarrierWorkerRuntimeInfoSnapshot, RecoveryReason, Scheduled,
};
use crate::hummock::{CommitEpochInfo, HummockManagerRef};
use crate::manager::{MetaSrvEnv, MetadataManager};
use crate::stream::{ScaleControllerRef, SourceManagerRef};
use crate::{MetaError, MetaResult};

pub(super) trait GlobalBarrierWorkerContext: Send + Sync + 'static {
    fn commit_epoch(
        &self,
        commit_info: CommitEpochInfo,
    ) -> impl Future<Output = MetaResult<HummockVersionStats>> + Send + '_;

    async fn next_scheduled(&self) -> Scheduled;
    fn abort_and_mark_blocked(&self, recovery_reason: RecoveryReason);
    fn mark_ready(&self);

    fn post_collect_command<'a>(
        &'a self,
        command: &'a CommandContext,
    ) -> impl Future<Output = MetaResult<()>> + Send + 'a;

    async fn notify_creating_job_failed(&self, err: &MetaError);

    fn finish_creating_job(
        &self,
        job: TrackingJob,
    ) -> impl Future<Output = MetaResult<()>> + Send + '_;

    async fn new_control_stream(
        &self,
        node: &WorkerNode,
        subscriptions: impl Iterator<Item = SubscriptionUpstreamInfo>,
    ) -> MetaResult<StreamingControlHandle>;

    async fn reload_runtime_info(&self) -> MetaResult<BarrierWorkerRuntimeInfoSnapshot>;
}

pub(crate) struct GlobalBarrierWorkerContextImpl {
    pub(crate) scheduled_barriers: ScheduledBarriers,

    pub(crate) status: Arc<ArcSwap<BarrierManagerStatus>>,

    pub(crate) metadata_manager: MetadataManager,

    pub(crate) hummock_manager: HummockManagerRef,

    pub(crate) source_manager: SourceManagerRef,

    pub(crate) scale_controller: ScaleControllerRef,

    pub(crate) env: MetaSrvEnv,
}
