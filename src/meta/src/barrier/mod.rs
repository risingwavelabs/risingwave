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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::{pending, Future};
use std::mem::{replace, take};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use arc_swap::ArcSwap;
use fail::fail_point;
use futures::future::try_join_all;
use itertools::Itertools;
use prometheus::HistogramTimer;
use risingwave_common::catalog::TableId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::system_param::PAUSE_ON_NEXT_BOOTSTRAP_KEY;
use risingwave_common::{bail, must_match};
use risingwave_connector::source::SplitImpl;
use risingwave_hummock_sdk::change_log::build_table_change_log_delta;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::from_prost_table_stats_map;
use risingwave_hummock_sdk::table_watermark::{
    merge_multiple_new_table_watermarks, TableWatermarks,
};
use risingwave_hummock_sdk::{HummockSstableObjectId, HummockVersionId, LocalSstableInfo};
use risingwave_meta_model::WorkerId;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::{PausedReason, PbRecoveryStatus};
use risingwave_pb::stream_plan::StreamActor;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use risingwave_rpc_client::StreamingControlHandle;
use thiserror_ext::AsReport;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn, Instrument};

use self::command::CommandContext;
use self::notifier::Notifier;
use crate::barrier::creating_job::{CompleteJobType, CreatingStreamingJobControl};
use crate::barrier::info::{BarrierInfo, InflightGraphInfo};
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingCommand, TrackingJob};
use crate::barrier::rpc::{merge_node_rpc_errors, ControlStreamManager};
use crate::barrier::schedule::{PeriodicBarriers, ScheduledBarriers};
use crate::barrier::state::BarrierWorkerState;
use crate::error::MetaErrorInner;
use crate::hummock::{CommitEpochInfo, HummockManagerRef, NewTableFragmentInfo};
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{
    ActiveStreamingWorkerChange, ActiveStreamingWorkerNodes, LocalNotification, MetaSrvEnv,
    MetadataManager,
};
use crate::model::{ActorId, TableFragments};
use crate::rpc::metrics::GLOBAL_META_METRICS;
use crate::stream::{ScaleControllerRef, SourceManagerRef};
use crate::{MetaError, MetaResult};

mod command;
mod creating_job;
mod info;
mod notifier;
mod progress;
mod recovery;
mod rpc;
mod schedule;
mod state;
mod trace;

pub use self::command::{
    BarrierKind, Command, CreateStreamingJobCommandInfo, CreateStreamingJobType, ReplaceTablePlan,
    Reschedule, SnapshotBackfillInfo,
};
pub use self::info::InflightSubscriptionInfo;
pub use self::schedule::BarrierScheduler;
pub use self::trace::TracedEpoch;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct TableMap<T> {
    inner: HashMap<TableId, T>,
}

impl<T> From<HashMap<TableId, T>> for TableMap<T> {
    fn from(inner: HashMap<TableId, T>) -> Self {
        Self { inner }
    }
}

impl<T> From<TableMap<T>> for HashMap<TableId, T> {
    fn from(table_map: TableMap<T>) -> Self {
        table_map.inner
    }
}

/// The reason why the cluster is recovering.
enum RecoveryReason {
    /// After bootstrap.
    Bootstrap,
    /// After failure.
    Failover(MetaError),
    /// Manually triggered
    Adhoc,
}

/// Status of barrier manager.
enum BarrierManagerStatus {
    /// Barrier manager is starting.
    Starting,
    /// Barrier manager is under recovery.
    Recovering(RecoveryReason),
    /// Barrier manager is running.
    Running,
}

/// Scheduled command with its notifiers.
struct Scheduled {
    command: Command,
    notifiers: Vec<Notifier>,
    span: tracing::Span,
    /// Choose a different barrier(checkpoint == true) according to it
    checkpoint: bool,
}

impl From<&BarrierManagerStatus> for PbRecoveryStatus {
    fn from(status: &BarrierManagerStatus) -> Self {
        match status {
            BarrierManagerStatus::Starting => Self::StatusStarting,
            BarrierManagerStatus::Recovering(reason) => match reason {
                RecoveryReason::Bootstrap => Self::StatusStarting,
                RecoveryReason::Failover(_) | RecoveryReason::Adhoc => Self::StatusRecovering,
            },
            BarrierManagerStatus::Running => Self::StatusRunning,
        }
    }
}

pub(crate) enum BarrierManagerRequest {
    GetDdlProgress(Sender<HashMap<u32, DdlProgress>>),
}

struct GlobalBarrierWorkerContextImpl {
    scheduled_barriers: ScheduledBarriers,

    status: Arc<ArcSwap<BarrierManagerStatus>>,

    metadata_manager: MetadataManager,

    hummock_manager: HummockManagerRef,

    source_manager: SourceManagerRef,

    scale_controller: ScaleControllerRef,

    sink_manager: SinkCoordinatorManager,

    env: MetaSrvEnv,
}

impl GlobalBarrierManager {
    pub async fn start(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        sink_manager: SinkCoordinatorManager,
        scale_controller: ScaleControllerRef,
    ) -> (Arc<Self>, JoinHandle<()>, oneshot::Sender<()>) {
        let (request_tx, request_rx) = unbounded_channel();
        let barrier_worker = GlobalBarrierWorker::new(
            scheduled_barriers,
            env,
            metadata_manager,
            hummock_manager,
            source_manager,
            sink_manager,
            scale_controller,
            request_rx,
        )
        .await;
        let manager = Self {
            status: barrier_worker.context.status.clone(),
            hummock_manager: barrier_worker.context.hummock_manager.clone(),
            request_tx,
            metadata_manager: barrier_worker.context.metadata_manager.clone(),
        };
        let (join_handle, shutdown_tx) = barrier_worker.start();
        (Arc::new(manager), join_handle, shutdown_tx)
    }
}

trait GlobalBarrierWorkerContext: Send + Sync + 'static {
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
        mv_depended_subscriptions: &HashMap<TableId, HashMap<u32, u64>>,
    ) -> MetaResult<StreamingControlHandle>;

    async fn reload_runtime_info(
        &self,
    ) -> MetaResult<(
        ActiveStreamingWorkerNodes,
        InflightGraphInfo,
        InflightSubscriptionInfo,
        Option<TracedEpoch>,
        HashMap<WorkerId, Vec<StreamActor>>,
        HashMap<ActorId, Vec<SplitImpl>>,
        HashMap<TableId, (String, TableFragments)>,
        HummockVersionStats,
    )>;
}

impl GlobalBarrierWorkerContext for GlobalBarrierWorkerContextImpl {
    async fn commit_epoch(&self, commit_info: CommitEpochInfo) -> MetaResult<HummockVersionStats> {
        self.hummock_manager.commit_epoch(commit_info).await?;
        Ok(self.hummock_manager.get_version_stats().await)
    }

    async fn next_scheduled(&self) -> Scheduled {
        self.scheduled_barriers.next_scheduled().await
    }

    fn abort_and_mark_blocked(&self, recovery_reason: RecoveryReason) {
        self.set_status(BarrierManagerStatus::Recovering(recovery_reason));

        // Mark blocked and abort buffered schedules, they might be dirty already.
        self.scheduled_barriers
            .abort_and_mark_blocked("cluster is under recovering");
    }

    fn mark_ready(&self) {
        self.scheduled_barriers.mark_ready();
        self.set_status(BarrierManagerStatus::Running);
    }

    async fn post_collect_command<'a>(&'a self, command: &'a CommandContext) -> MetaResult<()> {
        command.post_collect(self).await
    }

    async fn notify_creating_job_failed(&self, err: &MetaError) {
        self.metadata_manager.notify_finish_failed(err).await
    }

    async fn finish_creating_job(&self, job: TrackingJob) -> MetaResult<()> {
        job.finish(&self.metadata_manager).await
    }

    async fn new_control_stream(
        &self,
        node: &WorkerNode,
        mv_depended_subscriptions: &HashMap<TableId, HashMap<u32, u64>>,
    ) -> MetaResult<StreamingControlHandle> {
        self.new_control_stream_impl(node, mv_depended_subscriptions)
            .await
    }

    async fn reload_runtime_info(
        &self,
    ) -> MetaResult<(
        ActiveStreamingWorkerNodes,
        InflightGraphInfo,
        InflightSubscriptionInfo,
        Option<TracedEpoch>,
        HashMap<WorkerId, Vec<StreamActor>>,
        HashMap<ActorId, Vec<SplitImpl>>,
        HashMap<TableId, (String, TableFragments)>,
        HummockVersionStats,
    )> {
        self.reload_runtime_info_impl().await
    }
}

/// [`crate::barrier::GlobalBarrierWorker`] sends barriers to all registered compute nodes and
/// collect them, with monotonic increasing epoch numbers. On compute nodes, `LocalBarrierManager`
/// in `risingwave_stream` crate will serve these requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`crate::barrier::GlobalBarrierWorker`] provides a set of interfaces like a state machine,
/// accepting [`Command`] that carries info to build `Mutation`. To keep the consistency between
/// barrier manager and meta store, some actions like "drop materialized view" or "create mv on mv"
/// must be done in barrier manager transactional using [`Command`].
struct GlobalBarrierWorker<C> {
    /// Enable recovery or not when failover.
    enable_recovery: bool,

    /// The queue of scheduled barriers.
    periodic_barriers: PeriodicBarriers,

    /// The max barrier nums in flight
    in_flight_barrier_nums: usize,

    context: Arc<C>,

    env: MetaSrvEnv,

    checkpoint_control: CheckpointControl,

    /// Command that has been collected but is still completing.
    /// The join handle of the completing future is stored.
    completing_task: CompletingTask,

    request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,

    active_streaming_nodes: ActiveStreamingWorkerNodes,

    control_stream_manager: ControlStreamManager,
}

/// Controls the concurrent execution of commands.
struct CheckpointControl {
    state: BarrierWorkerState,

    /// Save the state and message of barrier in order.
    /// Key is the `prev_epoch`.
    command_ctx_queue: BTreeMap<u64, EpochNode>,
    /// The barrier that are completing.
    /// Some((`prev_epoch`, `should_pause_inject_barrier`))
    completing_barrier: Option<(u64, bool)>,

    creating_streaming_job_controls: HashMap<TableId, CreatingStreamingJobControl>,

    hummock_version_stats: HummockVersionStats,

    create_mview_tracker: CreateMviewProgressTracker,
}

impl CheckpointControl {
    fn new(
        create_mview_tracker: CreateMviewProgressTracker,
        state: BarrierWorkerState,
        hummock_version_stats: HummockVersionStats,
    ) -> Self {
        Self {
            state,
            command_ctx_queue: Default::default(),
            completing_barrier: None,
            creating_streaming_job_controls: Default::default(),
            hummock_version_stats,
            create_mview_tracker,
        }
    }

    fn total_command_num(&self) -> usize {
        self.command_ctx_queue.len()
            + match &self.completing_barrier {
                Some(_) => 1,
                None => 0,
            }
    }

    /// Update the metrics of barrier nums.
    fn update_barrier_nums_metrics(&self) {
        GLOBAL_META_METRICS.in_flight_barrier_nums.set(
            self.command_ctx_queue
                .values()
                .filter(|x| x.state.is_inflight())
                .count() as i64,
        );
        GLOBAL_META_METRICS
            .all_barrier_nums
            .set(self.total_command_num() as i64);
    }

    fn jobs_to_merge(&self) -> Option<HashMap<TableId, (SnapshotBackfillInfo, InflightGraphInfo)>> {
        let mut table_ids_to_merge = HashMap::new();

        for (table_id, creating_streaming_job) in &self.creating_streaming_job_controls {
            if let Some(graph_info) = creating_streaming_job.should_merge_to_upstream() {
                table_ids_to_merge.insert(
                    *table_id,
                    (
                        creating_streaming_job.snapshot_backfill_info.clone(),
                        graph_info,
                    ),
                );
            }
        }
        if table_ids_to_merge.is_empty() {
            None
        } else {
            Some(table_ids_to_merge)
        }
    }

    /// Enqueue a barrier command
    fn enqueue_command(
        &mut self,
        command_ctx: CommandContext,
        notifiers: Vec<Notifier>,
        node_to_collect: HashSet<WorkerId>,
        creating_jobs_to_wait: HashSet<TableId>,
    ) {
        let timer = GLOBAL_META_METRICS.barrier_latency.start_timer();

        if let Some((_, node)) = self.command_ctx_queue.last_key_value() {
            assert_eq!(
                command_ctx.barrier_info.prev_epoch.value(),
                node.command_ctx.barrier_info.curr_epoch.value()
            );
        }

        tracing::trace!(
            prev_epoch = command_ctx.barrier_info.prev_epoch(),
            ?creating_jobs_to_wait,
            "enqueue command"
        );
        self.command_ctx_queue.insert(
            command_ctx.barrier_info.prev_epoch(),
            EpochNode {
                enqueue_time: timer,
                state: BarrierEpochState {
                    node_to_collect,
                    resps: vec![],
                    creating_jobs_to_wait,
                    finished_jobs: HashMap::new(),
                },
                command_ctx,
                notifiers,
            },
        );
    }

    /// Change the state of this `prev_epoch` to `Completed`. Return continuous nodes
    /// with `Completed` starting from first node [`Completed`..`InFlight`) and remove them.
    fn barrier_collected(
        &mut self,
        resp: BarrierCompleteResponse,
        control_stream_manager: &mut ControlStreamManager,
    ) -> MetaResult<()> {
        let worker_id = resp.worker_id;
        let prev_epoch = resp.epoch;
        tracing::trace!(
            worker_id,
            prev_epoch,
            partial_graph_id = resp.partial_graph_id,
            "barrier collected"
        );
        if resp.partial_graph_id == u32::MAX {
            if let Some(node) = self.command_ctx_queue.get_mut(&prev_epoch) {
                assert!(node.state.node_to_collect.remove(&(worker_id as _)));
                node.state.resps.push(resp);
            } else {
                panic!(
                    "collect barrier on non-existing barrier: {}, {}",
                    prev_epoch, worker_id
                );
            }
        } else {
            let creating_table_id = TableId::new(resp.partial_graph_id);
            self.creating_streaming_job_controls
                .get_mut(&creating_table_id)
                .expect("should exist")
                .collect(prev_epoch, worker_id as _, resp, control_stream_manager)?;
        }
        Ok(())
    }

    /// Pause inject barrier until True.
    fn can_inject_barrier(&self, in_flight_barrier_nums: usize) -> bool {
        let in_flight_not_full = self
            .command_ctx_queue
            .values()
            .filter(|x| x.state.is_inflight())
            .count()
            < in_flight_barrier_nums;

        // Whether some command requires pausing concurrent barrier. If so, it must be the last one.
        let should_pause = self
            .command_ctx_queue
            .last_key_value()
            .map(|(_, x)| x.command_ctx.command.should_pause_inject_barrier())
            .or(self
                .completing_barrier
                .map(|(_, should_pause)| should_pause))
            .unwrap_or(false);
        debug_assert_eq!(
            self.command_ctx_queue
                .values()
                .map(|node| node.command_ctx.command.should_pause_inject_barrier())
                .chain(
                    self.completing_barrier
                        .map(|(_, should_pause)| should_pause)
                        .into_iter()
                )
                .any(|should_pause| should_pause),
            should_pause
        );

        in_flight_not_full && !should_pause
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// We need to make sure there are no changes when doing recovery
    pub async fn clear_on_err(&mut self, err: &MetaError) {
        // join spawned completing command to finish no matter it succeeds or not.
        let is_err = match replace(&mut self.completing_task, CompletingTask::None) {
            CompletingTask::None => false,
            CompletingTask::Completing {
                join_handle,
                command_prev_epoch,
                creating_job_epochs,
                ..
            } => {
                info!("waiting for completing command to finish in recovery");
                match join_handle.await {
                    Err(e) => {
                        warn!(err = ?e.as_report(), "failed to join completing task");
                        true
                    }
                    Ok(Err(e)) => {
                        warn!(err = ?e.as_report(), "failed to complete barrier during clear");
                        true
                    }
                    Ok(Ok(hummock_version_stats)) => {
                        self.checkpoint_control
                            .ack_completed(BarrierCompleteOutput {
                                command_prev_epoch,
                                creating_job_epochs,
                                hummock_version_stats,
                            });
                        false
                    }
                }
            }
            CompletingTask::Err(_) => true,
        };
        if !is_err {
            // continue to finish the pending collected barrier.
            while let Some(task) = self.checkpoint_control.next_complete_barrier_task(None) {
                let (command_prev_epoch, creating_job_epochs) = (
                    task.command_context
                        .as_ref()
                        .map(|(command, _)| command.barrier_info.prev_epoch()),
                    task.creating_job_epochs.clone(),
                );
                match task
                    .complete_barrier(&*self.context, self.env.clone())
                    .await
                {
                    Ok(hummock_version_stats) => {
                        self.checkpoint_control
                            .ack_completed(BarrierCompleteOutput {
                                command_prev_epoch,
                                creating_job_epochs,
                                hummock_version_stats,
                            });
                    }
                    Err(e) => {
                        error!(
                            err = ?e.as_report(),
                            "failed to complete barrier during recovery"
                        );
                        break;
                    }
                }
            }
        }
        for (_, node) in take(&mut self.checkpoint_control.command_ctx_queue) {
            for notifier in node.notifiers {
                notifier.notify_failed(err.clone());
            }
            node.enqueue_time.observe_duration();
        }
        self.checkpoint_control.create_mview_tracker.abort_all();
    }
}

impl CheckpointControl {
    /// Return the earliest command waiting on the `worker_id`.
    fn barrier_wait_collect_from_worker(&self, worker_id: WorkerId) -> Option<&BarrierInfo> {
        for epoch_node in self.command_ctx_queue.values() {
            if epoch_node.state.node_to_collect.contains(&worker_id) {
                return Some(&epoch_node.command_ctx.barrier_info);
            }
        }
        // TODO: include barrier in creating jobs
        None
    }
}

/// The state and message of this barrier, a node for concurrent checkpoint.
struct EpochNode {
    /// Timer for recording barrier latency, taken after `complete_barriers`.
    enqueue_time: HistogramTimer,

    /// Whether this barrier is in-flight or completed.
    state: BarrierEpochState,

    /// Context of this command to generate barrier and do some post jobs.
    command_ctx: CommandContext,
    /// Notifiers of this barrier.
    notifiers: Vec<Notifier>,
}

#[derive(Debug)]
/// The state of barrier.
struct BarrierEpochState {
    node_to_collect: HashSet<WorkerId>,

    resps: Vec<BarrierCompleteResponse>,

    creating_jobs_to_wait: HashSet<TableId>,

    finished_jobs: HashMap<TableId, (CreateStreamingJobCommandInfo, Vec<BarrierCompleteResponse>)>,
}

impl BarrierEpochState {
    fn is_inflight(&self) -> bool {
        !self.node_to_collect.is_empty() || !self.creating_jobs_to_wait.is_empty()
    }
}

enum CompletingTask {
    None,
    Completing {
        command_prev_epoch: Option<u64>,
        creating_job_epochs: Vec<(TableId, u64)>,

        // The join handle of a spawned task that completes the barrier.
        // The return value indicate whether there is some create streaming job command
        // that has finished but not checkpointed. If there is any, we will force checkpoint on the next barrier
        join_handle: JoinHandle<MetaResult<HummockVersionStats>>,
    },
    #[expect(dead_code)]
    Err(MetaError),
}

impl GlobalBarrierWorker<GlobalBarrierWorkerContextImpl> {
    /// Create a new [`crate::barrier::GlobalBarrierWorker`].
    pub async fn new(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        sink_manager: SinkCoordinatorManager,
        scale_controller: ScaleControllerRef,
        request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;
        let in_flight_barrier_nums = env.opts.in_flight_barrier_nums;

        let initial_invalid_state = BarrierWorkerState::new(
            None,
            InflightGraphInfo::default(),
            InflightSubscriptionInfo::default(),
            None,
        );

        let active_streaming_nodes = ActiveStreamingWorkerNodes::uninitialized();

        let tracker = CreateMviewProgressTracker::default();

        let status = Arc::new(ArcSwap::new(Arc::new(BarrierManagerStatus::Starting)));

        let context = Arc::new(GlobalBarrierWorkerContextImpl {
            scheduled_barriers,
            status,
            metadata_manager,
            hummock_manager,
            source_manager,
            scale_controller,
            sink_manager,
            env: env.clone(),
        });

        let control_stream_manager = ControlStreamManager::new(env.clone());
        let checkpoint_control = CheckpointControl::new(
            tracker,
            initial_invalid_state,
            context.hummock_manager.get_version_stats().await,
        );

        let checkpoint_frequency = env.system_params_reader().await.checkpoint_frequency() as _;
        let interval =
            Duration::from_millis(env.system_params_reader().await.barrier_interval_ms() as u64);
        let periodic_barriers = PeriodicBarriers::new(interval, checkpoint_frequency);
        tracing::info!(
            "Starting barrier scheduler with: checkpoint_frequency={:?}",
            checkpoint_frequency,
        );

        Self {
            enable_recovery,
            periodic_barriers,
            in_flight_barrier_nums,
            context,
            env,
            checkpoint_control,
            completing_task: CompletingTask::None,
            request_rx,
            active_streaming_nodes,
            control_stream_manager,
        }
    }

    pub fn start(self) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            self.run(shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }

    /// Check whether we should pause on bootstrap from the system parameter and reset it.
    async fn take_pause_on_bootstrap(&mut self) -> MetaResult<bool> {
        let paused = self
            .env
            .system_params_reader()
            .await
            .pause_on_next_bootstrap();
        if paused {
            warn!(
                "The cluster will bootstrap with all data sources paused as specified by the system parameter `{}`. \
                 It will now be reset to `false`. \
                 To resume the data sources, either restart the cluster again or use `risectl meta resume`.",
                PAUSE_ON_NEXT_BOOTSTRAP_KEY
            );
            self.env
                .system_params_manager_impl_ref()
                .set_param(PAUSE_ON_NEXT_BOOTSTRAP_KEY, Some("false".to_owned()))
                .await?;
        }
        Ok(paused)
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    async fn run(mut self, shutdown_rx: Receiver<()>) {
        tracing::info!(
            "Starting barrier manager with: enable_recovery={}, in_flight_barrier_nums={}",
            self.enable_recovery,
            self.in_flight_barrier_nums,
        );

        if !self.enable_recovery {
            let job_exist = self
                .context
                .metadata_manager
                .catalog_controller
                .has_any_streaming_jobs()
                .await
                .unwrap();
            if job_exist {
                panic!(
                    "Some streaming jobs already exist in meta, please start with recovery enabled \
                or clean up the metadata using `./risedev clean-data`"
                );
            }
        }

        {
            // Bootstrap recovery. Here we simply trigger a recovery process to achieve the
            // consistency.
            // Even if there's no actor to recover, we still go through the recovery process to
            // inject the first `Initial` barrier.
            let span = tracing::info_span!("bootstrap_recovery");
            crate::telemetry::report_event(
                risingwave_pb::telemetry::TelemetryEventStage::Recovery,
                "normal_recovery",
                0,
                None,
                None,
                None,
            );

            let paused = self.take_pause_on_bootstrap().await.unwrap_or(false);
            let paused_reason = paused.then_some(PausedReason::Manual);

            self.recovery(paused_reason, None, RecoveryReason::Bootstrap)
                .instrument(span)
                .await;
        }

        self.run_inner(shutdown_rx).await
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    async fn run_inner(mut self, mut shutdown_rx: Receiver<()>) {
        let (local_notification_tx, mut local_notification_rx) =
            tokio::sync::mpsc::unbounded_channel();
        self.env
            .notification_manager()
            .insert_local_sender(local_notification_tx)
            .await;

        // Start the event loop.
        loop {
            tokio::select! {
                biased;

                // Shutdown
                _ = &mut shutdown_rx => {
                    tracing::info!("Barrier manager is stopped");
                    break;
                }

                request = self.request_rx.recv() => {
                    if let Some(request) = request {
                        match request {
                            BarrierManagerRequest::GetDdlProgress(result_tx) => {
                                // Progress of normal backfill
                                let mut progress = self.checkpoint_control.create_mview_tracker.gen_ddl_progress();
                                // Progress of snapshot backfill
                                for creating_job in self.checkpoint_control.creating_streaming_job_controls.values() {
                                    progress.extend([(creating_job.info.table_fragments.table_id().table_id, creating_job.gen_ddl_progress())]);
                                }
                                if result_tx.send(progress).is_err() {
                                    error!("failed to send get ddl progress");
                                }
                            }
                        }
                    } else {
                        tracing::info!("end of request stream. meta node may be shutting down. Stop global barrier manager");
                        return;
                    }
                }

                changed_worker = self.active_streaming_nodes.changed() => {
                    if cfg!(debug_assertions)
                        && let Some(context) = (&self.context as &dyn std::any::Any).downcast_ref::<GlobalBarrierWorkerContextImpl>() {
                        info!("downcast to GlobalBarrierWorkerContext success");
                        use risingwave_pb::common::WorkerNode;
                        match context
                            .metadata_manager
                            .list_active_streaming_compute_nodes()
                            .await
                        {
                            Ok(worker_nodes) => {
                                let ignore_irrelevant_info = |node: &WorkerNode| {
                                    (
                                        node.id,
                                        WorkerNode {
                                            id: node.id,
                                            r#type: node.r#type,
                                            host: node.host.clone(),
                                            parallelism: node.parallelism,
                                            property: node.property.clone(),
                                            resource: node.resource.clone(),
                                            ..Default::default()
                                        },
                                    )
                                };
                                let worker_nodes: HashMap<_, _> =
                                    worker_nodes.iter().map(ignore_irrelevant_info).collect();
                                let curr_worker_nodes: HashMap<_, _> = self
                                    .active_streaming_nodes
                                    .current()
                                    .values()
                                    .map(ignore_irrelevant_info)
                                    .collect();
                                if worker_nodes != curr_worker_nodes {
                                    warn!(
                                        ?worker_nodes,
                                        ?curr_worker_nodes,
                                        "different to global snapshot"
                                    );
                                }
                            }
                            Err(e) => {
                                warn!(e = ?e.as_report(), "fail to list_active_streaming_compute_nodes to compare with local snapshot");
                            }
                        }
                    }

                    info!(?changed_worker, "worker changed");

                    self.checkpoint_control.state.inflight_graph_info
                        .on_new_worker_node_map(self.active_streaming_nodes.current());
                    self.checkpoint_control.creating_streaming_job_controls.values().for_each(|job| job.on_new_worker_node_map(self.active_streaming_nodes.current()));
                    if let ActiveStreamingWorkerChange::Add(node) | ActiveStreamingWorkerChange::Update(node) = changed_worker {
                        self.control_stream_manager.add_worker(node, &self.checkpoint_control.state.inflight_subscription_info, &*self.context).await;
                    }
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.unwrap();
                    match notification {
                        // Handle barrier interval and checkpoint frequency changes.
                        LocalNotification::SystemParamsChange(p) => {
                            self.periodic_barriers.set_min_interval(Duration::from_millis(p.barrier_interval_ms() as u64));
                            self.periodic_barriers
                                .set_checkpoint_frequency(p.checkpoint_frequency() as usize)
                        },
                        // Handle adhoc recovery triggered by user.
                        LocalNotification::AdhocRecovery => {
                            self.adhoc_recovery().await;
                        }
                        _ => {}
                    }
                }
                complete_result = self
                    .completing_task
                    .next_completed_barrier(
                        &mut self.periodic_barriers,
                        &mut self.checkpoint_control,
                        &mut self.control_stream_manager,
                        &self.context,
                        &self.env,
                ) => {
                    match complete_result {
                        Ok(output) => {
                            self.checkpoint_control.ack_completed(output);
                        }
                        Err(e) => {
                            self.failure_recovery(e).await;
                        }
                    }
                },
                (worker_id, resp_result) = self.control_stream_manager.next_collect_barrier_response() => {
                    if let  Err(e) = resp_result.and_then(|resp| self.checkpoint_control.barrier_collected(resp, &mut self.control_stream_manager)) {
                        {
                            let failed_barrier = self.checkpoint_control.barrier_wait_collect_from_worker(worker_id as _);
                            if failed_barrier.is_some()
                                || self.checkpoint_control.state.inflight_graph_info.contains_worker(worker_id as _)
                                || self.checkpoint_control.creating_streaming_job_controls.values().any(|job| job.is_wait_on_worker(worker_id)) {
                                let errors = self.control_stream_manager.collect_errors(worker_id, e).await;
                                let err = merge_node_rpc_errors("get error from control stream", errors);
                                if let Some(failed_barrier) = failed_barrier {
                                    self.report_collect_failure(failed_barrier, &err);
                                }
                                self.failure_recovery(err).await;
                            } else {
                                warn!(e = %e.as_report(), worker_id, "no barrier to collect from worker, ignore err");
                            }
                        }
                    }
                }
                scheduled = self.periodic_barriers.next_barrier(&*self.context),
                    if self
                        .checkpoint_control
                        .can_inject_barrier(self.in_flight_barrier_nums) => {
                    if let Err(e) = self.checkpoint_control.handle_new_barrier(scheduled, &mut self.control_stream_manager, &self.active_streaming_nodes) {
                        self.failure_recovery(e).await;
                    }
                }
            }
            self.checkpoint_control.update_barrier_nums_metrics();
        }
    }
}

impl CheckpointControl {
    /// Handle the new barrier from the scheduled queue and inject it.
    fn handle_new_barrier(
        &mut self,
        scheduled: Scheduled,
        control_stream_manager: &mut ControlStreamManager,
        active_streaming_nodes: &ActiveStreamingWorkerNodes,
    ) -> MetaResult<()> {
        let Scheduled {
            mut command,
            mut notifiers,
            checkpoint,
            span,
        } = scheduled;

        if let Some(table_to_cancel) = command.table_to_cancel()
            && self
                .creating_streaming_job_controls
                .contains_key(&table_to_cancel)
        {
            warn!(
                table_id = table_to_cancel.table_id,
                "ignore cancel command on creating streaming job"
            );
            for notifier in notifiers {
                notifier
                    .notify_start_failed(anyhow!("cannot cancel creating streaming job, the job will continue creating until created or recovery").into());
            }
            return Ok(());
        }

        if let Command::RescheduleFragment { .. } = &command {
            if !self.creating_streaming_job_controls.is_empty() {
                warn!("ignore reschedule when creating streaming job with snapshot backfill");
                for notifier in notifiers {
                    notifier.notify_start_failed(
                        anyhow!(
                            "cannot reschedule when creating streaming job with snapshot backfill",
                        )
                        .into(),
                    );
                }
                return Ok(());
            }
        }

        let Some(barrier_info) = self.state.next_barrier_info(&command, checkpoint) else {
            // skip the command when there is nothing to do with the barrier
            for mut notifier in notifiers {
                notifier.notify_started();
                notifier.notify_collected();
            }
            return Ok(());
        };

        // Insert newly added creating job
        if let Command::CreateStreamingJob {
            job_type: CreateStreamingJobType::SnapshotBackfill(snapshot_backfill_info),
            info,
        } = &command
        {
            if self.state.paused_reason().is_some() {
                warn!("cannot create streaming job with snapshot backfill when paused");
                for notifier in notifiers {
                    notifier.notify_start_failed(
                        anyhow!("cannot create streaming job with snapshot backfill when paused",)
                            .into(),
                    );
                }
                return Ok(());
            }
            let mutation = command
                .to_mutation(None)
                .expect("should have some mutation in `CreateStreamingJob` command");
            self.creating_streaming_job_controls.insert(
                info.table_fragments.table_id(),
                CreatingStreamingJobControl::new(
                    info.clone(),
                    snapshot_backfill_info.clone(),
                    barrier_info.prev_epoch(),
                    &self.hummock_version_stats,
                    mutation,
                ),
            );
        }

        // Collect the jobs to finish
        if let (BarrierKind::Checkpoint(_), Command::Plain(None)) = (&barrier_info.kind, &command)
            && let Some(jobs_to_merge) = self.jobs_to_merge()
        {
            command = Command::MergeSnapshotBackfillStreamingJobs(jobs_to_merge);
        }

        let command = command;

        let (
            pre_applied_graph_info,
            pre_applied_subscription_info,
            table_ids_to_commit,
            jobs_to_wait,
            prev_paused_reason,
        ) = self.state.apply_command(&command);

        // Tracing related stuff
        barrier_info.prev_epoch.span().in_scope(|| {
            tracing::info!(target: "rw_tracing", epoch = barrier_info.curr_epoch.value().0, "new barrier enqueued");
        });
        span.record("epoch", barrier_info.curr_epoch.value().0);

        for creating_job in &mut self.creating_streaming_job_controls.values_mut() {
            creating_job.on_new_command(control_stream_manager, &command, &barrier_info)?;
        }

        let node_to_collect = match control_stream_manager.inject_command_ctx_barrier(
            &command,
            &barrier_info,
            prev_paused_reason,
            &pre_applied_graph_info,
            Some(&self.state.inflight_graph_info),
        ) {
            Ok(node_to_collect) => node_to_collect,
            Err(err) => {
                for notifier in notifiers {
                    notifier.notify_failed(err.clone());
                }
                fail_point!("inject_barrier_err_success");
                return Err(err);
            }
        };

        // Notify about the injection.
        notifiers.iter_mut().for_each(|n| n.notify_started());

        let command_ctx = CommandContext::new(
            active_streaming_nodes.current().clone(),
            barrier_info,
            pre_applied_subscription_info,
            table_ids_to_commit.clone(),
            command,
            span,
        );

        // Record the in-flight barrier.
        self.enqueue_command(command_ctx, notifiers, node_to_collect, jobs_to_wait);

        Ok(())
    }
}

impl<C: GlobalBarrierWorkerContext> GlobalBarrierWorker<C> {
    /// Set barrier manager status.

    async fn failure_recovery(&mut self, err: MetaError) {
        self.clear_on_err(&err).await;

        if self.enable_recovery {
            let span = tracing::info_span!(
                "failure_recovery",
                error = %err.as_report(),
            );

            crate::telemetry::report_event(
                risingwave_pb::telemetry::TelemetryEventStage::Recovery,
                "failure_recovery",
                0,
                None,
                None,
                None,
            );

            let reason = RecoveryReason::Failover(err.clone());

            // No need to clean dirty tables for barrier recovery,
            // The foreground stream job should cleanup their own tables.
            self.recovery(None, Some(err), reason)
                .instrument(span)
                .await;
        } else {
            panic!("failed to execute barrier: {}", err.as_report());
        }
    }

    async fn adhoc_recovery(&mut self) {
        let err = MetaErrorInner::AdhocRecovery.into();
        self.clear_on_err(&err).await;

        let span = tracing::info_span!(
            "adhoc_recovery",
            error = %err.as_report(),
        );

        crate::telemetry::report_event(
            risingwave_pb::telemetry::TelemetryEventStage::Recovery,
            "adhoc_recovery",
            0,
            None,
            None,
            None,
        );

        // No need to clean dirty tables for barrier recovery,
        // The foreground stream job should cleanup their own tables.
        self.recovery(None, Some(err), RecoveryReason::Adhoc)
            .instrument(span)
            .await;
    }
}

#[derive(Default)]
pub struct CompleteBarrierTask {
    commit_info: CommitEpochInfo,
    finished_jobs: Vec<TrackingJob>,
    notifiers: Vec<Notifier>,
    /// Some((`command_ctx`, `enqueue_time`))
    command_context: Option<(CommandContext, HistogramTimer)>,
    creating_job_epochs: Vec<(TableId, u64)>,
}

impl GlobalBarrierWorkerContextImpl {
    fn set_status(&self, new_status: BarrierManagerStatus) {
        self.status.store(Arc::new(new_status));
    }

    fn collect_creating_job_commit_epoch_info(
        commit_info: &mut CommitEpochInfo,
        epoch: u64,
        resps: Vec<BarrierCompleteResponse>,
        tables_to_commit: impl Iterator<Item = TableId>,
        is_first_time: bool,
    ) {
        let (sst_to_context, sstables, new_table_watermarks, old_value_sst) =
            collect_resp_info(resps);
        assert!(old_value_sst.is_empty());
        commit_info.sst_to_context.extend(sst_to_context);
        commit_info.sstables.extend(sstables);
        commit_info
            .new_table_watermarks
            .extend(new_table_watermarks);
        let tables_to_commit: HashSet<_> = tables_to_commit.collect();
        tables_to_commit.iter().for_each(|table_id| {
            commit_info
                .tables_to_commit
                .try_insert(*table_id, epoch)
                .expect("non duplicate");
        });
        if is_first_time {
            commit_info
                .new_table_fragment_infos
                .push(NewTableFragmentInfo::NewCompactionGroup {
                    table_ids: tables_to_commit,
                });
        };
    }
}

impl CompleteBarrierTask {
    async fn complete_barrier(
        self,
        context: &impl GlobalBarrierWorkerContext,
        env: MetaSrvEnv,
    ) -> MetaResult<HummockVersionStats> {
        let result: MetaResult<HummockVersionStats> = try {
            let wait_commit_timer = GLOBAL_META_METRICS
                .barrier_wait_commit_latency
                .start_timer();
            let version_stats = context.commit_epoch(self.commit_info).await?;
            if let Some((command_ctx, _)) = &self.command_context {
                context.post_collect_command(command_ctx).await?;
            }

            wait_commit_timer.observe_duration();
            version_stats
        };

        let version_stats = {
            let version_stats = match result {
                Ok(version_stats) => version_stats,
                Err(e) => {
                    for notifier in self.notifiers {
                        notifier.notify_collection_failed(e.clone());
                    }
                    return Err(e);
                }
            };
            self.notifiers.into_iter().for_each(|notifier| {
                notifier.notify_collected();
            });
            try_join_all(
                self.finished_jobs
                    .into_iter()
                    .map(|finished_job| context.finish_creating_job(finished_job)),
            )
            .await?;
            if let Some((command_ctx, enqueue_time)) = self.command_context {
                let duration_sec = enqueue_time.stop_and_record();
                Self::report_complete_event(env, duration_sec, &command_ctx);
                GLOBAL_META_METRICS
                    .last_committed_barrier_time
                    .set(command_ctx.barrier_info.curr_epoch.value().as_unix_secs() as i64);
            }
            version_stats
        };

        Ok(version_stats)
    }
}

impl CreateMviewProgressTracker {
    fn update_tracking_jobs<'a>(
        &mut self,
        info: Option<(&CreateStreamingJobCommandInfo, Option<&ReplaceTablePlan>)>,
        create_mview_progress: impl IntoIterator<Item = &'a CreateMviewProgress>,
        version_stats: &HummockVersionStats,
    ) {
        {
            {
                // Save `finished_commands` for Create MVs.
                let finished_commands = {
                    let mut commands = vec![];
                    // Add the command to tracker.
                    if let Some((create_job_info, replace_table)) = info
                        && let Some(command) =
                            self.add(create_job_info, replace_table, version_stats)
                    {
                        // Those with no actors to track can be finished immediately.
                        commands.push(command);
                    }
                    // Update the progress of all commands.
                    for progress in create_mview_progress {
                        // Those with actors complete can be finished immediately.
                        if let Some(command) = self.update(progress, version_stats) {
                            tracing::trace!(?progress, "finish progress");
                            commands.push(command);
                        } else {
                            tracing::trace!(?progress, "update progress");
                        }
                    }
                    commands
                };

                for command in finished_commands {
                    self.stash_command_to_finish(command);
                }
            }
        }
    }
}

impl CompleteBarrierTask {
    fn report_complete_event(env: MetaSrvEnv, duration_sec: f64, command_ctx: &CommandContext) {
        // Record barrier latency in event log.
        use risingwave_pb::meta::event_log;
        let event = event_log::EventBarrierComplete {
            prev_epoch: command_ctx.barrier_info.prev_epoch(),
            cur_epoch: command_ctx.barrier_info.curr_epoch.value().0,
            duration_sec,
            command: command_ctx.command.to_string(),
            barrier_kind: command_ctx.barrier_info.kind.as_str_name().to_string(),
        };
        env.event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::BarrierComplete(event)]);
    }
}

struct BarrierCompleteOutput {
    command_prev_epoch: Option<u64>,
    creating_job_epochs: Vec<(TableId, u64)>,
    hummock_version_stats: HummockVersionStats,
}

impl CheckpointControl {
    /// return creating job table fragment id -> (backfill progress epoch , {`upstream_mv_table_id`})
    fn collect_backfill_pinned_upstream_log_epoch(
        &self,
    ) -> HashMap<TableId, (u64, HashSet<TableId>)> {
        self.creating_streaming_job_controls
            .iter()
            .filter_map(|(table_id, creating_job)| {
                creating_job
                    .pinned_upstream_log_epoch()
                    .map(|progress_epoch| {
                        (
                            *table_id,
                            (
                                progress_epoch,
                                creating_job
                                    .snapshot_backfill_info
                                    .upstream_mv_table_ids
                                    .clone(),
                            ),
                        )
                    })
            })
            .collect()
    }

    fn next_complete_barrier_task(
        &mut self,
        mut context: Option<(&mut PeriodicBarriers, &mut ControlStreamManager)>,
    ) -> Option<CompleteBarrierTask> {
        // `Vec::new` is a const fn, and do not have memory allocation, and therefore is lightweight enough
        let mut creating_jobs_task = vec![];
        {
            // `Vec::new` is a const fn, and do not have memory allocation, and therefore is lightweight enough
            let mut finished_jobs = Vec::new();
            let min_upstream_inflight_barrier = self
                .command_ctx_queue
                .first_key_value()
                .map(|(epoch, _)| *epoch);
            for (table_id, job) in &mut self.creating_streaming_job_controls {
                if let Some((epoch, resps, status)) =
                    job.start_completing(min_upstream_inflight_barrier)
                {
                    let is_first_time = match status {
                        CompleteJobType::First => true,
                        CompleteJobType::Normal => false,
                        CompleteJobType::Finished => {
                            finished_jobs.push((*table_id, epoch, resps));
                            continue;
                        }
                    };
                    creating_jobs_task.push((*table_id, epoch, resps, is_first_time));
                }
            }
            if !finished_jobs.is_empty()
                && let Some((_, control_stream_manager)) = &mut context
            {
                control_stream_manager.remove_partial_graph(
                    finished_jobs
                        .iter()
                        .map(|(table_id, _, _)| table_id.table_id)
                        .collect(),
                );
            }
            for (table_id, epoch, resps) in finished_jobs {
                let epoch_state = &mut self
                    .command_ctx_queue
                    .get_mut(&epoch)
                    .expect("should exist")
                    .state;
                assert!(epoch_state.creating_jobs_to_wait.remove(&table_id));
                debug!(epoch, ?table_id, "finish creating job");
                // It's safe to remove the creating job, because on CompleteJobType::Finished,
                // all previous barriers have been collected and completed.
                let creating_streaming_job = self
                    .creating_streaming_job_controls
                    .remove(&table_id)
                    .expect("should exist");
                assert!(creating_streaming_job.is_finished());
                assert!(epoch_state
                    .finished_jobs
                    .insert(table_id, (creating_streaming_job.info, resps))
                    .is_none());
            }
        }
        let mut task = None;
        assert!(self.completing_barrier.is_none());
        while let Some((_, EpochNode { state, .. })) = self.command_ctx_queue.first_key_value()
            && !state.is_inflight()
        {
            {
                let (_, mut node) = self.command_ctx_queue.pop_first().expect("non-empty");
                assert!(node.state.creating_jobs_to_wait.is_empty());
                assert!(node.state.node_to_collect.is_empty());
                let mut finished_jobs = self
                    .create_mview_tracker
                    .apply_collected_command(&node, &self.hummock_version_stats);
                if !node.command_ctx.barrier_info.kind.is_checkpoint() {
                    assert!(finished_jobs.is_empty());
                    node.notifiers.into_iter().for_each(|notifier| {
                        notifier.notify_collected();
                    });
                    if let Some((scheduled_barriers, _)) = &mut context
                        && self.create_mview_tracker.has_pending_finished_jobs()
                        && self
                            .command_ctx_queue
                            .values()
                            .all(|node| !node.command_ctx.barrier_info.kind.is_checkpoint())
                    {
                        scheduled_barriers.force_checkpoint_in_next_barrier();
                    }
                    continue;
                }
                node.state
                    .finished_jobs
                    .drain()
                    .for_each(|(_, (info, resps))| {
                        node.state.resps.extend(resps);
                        finished_jobs.push(TrackingJob::New(TrackingCommand {
                            info,
                            replace_table_info: None,
                        }));
                    });
                let commit_info = collect_commit_epoch_info(
                    take(&mut node.state.resps),
                    &node.command_ctx,
                    self.collect_backfill_pinned_upstream_log_epoch(),
                );
                self.completing_barrier = Some((
                    node.command_ctx.barrier_info.prev_epoch(),
                    node.command_ctx.command.should_pause_inject_barrier(),
                ));
                task = Some(CompleteBarrierTask {
                    commit_info,
                    finished_jobs,
                    notifiers: node.notifiers,
                    command_context: Some((node.command_ctx, node.enqueue_time)),
                    creating_job_epochs: vec![],
                });
                break;
            }
        }
        if !creating_jobs_task.is_empty() {
            let task = task.get_or_insert_default();
            for (table_id, epoch, resps, is_first_time) in creating_jobs_task {
                GlobalBarrierWorkerContextImpl::collect_creating_job_commit_epoch_info(
                    &mut task.commit_info,
                    epoch,
                    resps,
                    self.creating_streaming_job_controls[&table_id]
                        .info
                        .table_fragments
                        .all_table_ids()
                        .map(TableId::new),
                    is_first_time,
                );
                task.creating_job_epochs.push((table_id, epoch));
            }
        }
        task
    }
}

impl CompletingTask {
    pub(super) fn next_completed_barrier<'a>(
        &'a mut self,
        scheduled_barriers: &mut PeriodicBarriers,
        checkpoint_control: &mut CheckpointControl,
        control_stream_manager: &mut ControlStreamManager,
        context: &Arc<impl GlobalBarrierWorkerContext>,
        env: &MetaSrvEnv,
    ) -> impl Future<Output = MetaResult<BarrierCompleteOutput>> + 'a {
        // If there is no completing barrier, try to start completing the earliest barrier if
        // it has been collected.
        if let CompletingTask::None = self {
            if let Some(task) = checkpoint_control
                .next_complete_barrier_task(Some((scheduled_barriers, control_stream_manager)))
            {
                {
                    let creating_job_epochs = task.creating_job_epochs.clone();
                    let command_prev_epoch = task
                        .command_context
                        .as_ref()
                        .map(|(command, _)| command.barrier_info.prev_epoch());
                    let context = context.clone();
                    let env = env.clone();
                    let join_handle =
                        tokio::spawn(async move { task.complete_barrier(&*context, env).await });
                    *self = CompletingTask::Completing {
                        command_prev_epoch,
                        join_handle,
                        creating_job_epochs,
                    };
                }
            }
        }

        self.next_completed_barrier_inner()
    }

    async fn next_completed_barrier_inner(&mut self) -> MetaResult<BarrierCompleteOutput> {
        let CompletingTask::Completing { join_handle, .. } = self else {
            return pending().await;
        };

        {
            {
                let join_result: MetaResult<_> = try {
                    join_handle
                        .await
                        .context("failed to join completing command")??
                };
                // It's important to reset the completing_command after await no matter the result is err
                // or not, and otherwise the join handle will be polled again after ready.
                let next_completing_command_status = if let Err(e) = &join_result {
                    CompletingTask::Err(e.clone())
                } else {
                    CompletingTask::None
                };
                let completed_command = replace(self, next_completing_command_status);
                let hummock_version_stats = join_result?;

                must_match!(completed_command, CompletingTask::Completing {
                    creating_job_epochs,
                    command_prev_epoch,
                    ..
                } => {
                    Ok(BarrierCompleteOutput {
                        command_prev_epoch,
                        creating_job_epochs,
                        hummock_version_stats,
                    })
                })
            }
        }
    }
}

impl CheckpointControl {
    fn ack_completed(&mut self, output: BarrierCompleteOutput) {
        {
            self.hummock_version_stats = output.hummock_version_stats;
            assert_eq!(
                self.completing_barrier
                    .take()
                    .map(|(prev_epoch, _)| prev_epoch),
                output.command_prev_epoch
            );
            for (table_id, epoch) in output.creating_job_epochs {
                self.creating_streaming_job_controls
                    .get_mut(&table_id)
                    .expect("should exist")
                    .ack_completed(epoch)
            }
        }
    }
}

impl GlobalBarrierManager {
    /// Check the status of barrier manager, return error if it is not `Running`.
    pub fn check_status_running(&self) -> MetaResult<()> {
        let status = self.status.load();
        match &**status {
            BarrierManagerStatus::Starting
            | BarrierManagerStatus::Recovering(RecoveryReason::Bootstrap) => {
                bail!("The cluster is bootstrapping")
            }
            BarrierManagerStatus::Recovering(RecoveryReason::Failover(e)) => {
                Err(anyhow::anyhow!(e.clone()).context("The cluster is recovering"))?
            }
            BarrierManagerStatus::Recovering(RecoveryReason::Adhoc) => {
                bail!("The cluster is recovering-adhoc")
            }
            BarrierManagerStatus::Running => Ok(()),
        }
    }

    pub fn get_recovery_status(&self) -> PbRecoveryStatus {
        (&**self.status.load()).into()
    }
}

impl GlobalBarrierWorkerContextImpl {
    /// Resolve actor information from cluster, fragment manager and `ChangedTableId`.
    /// We use `changed_table_id` to modify the actors to be sent or collected. Because these actor
    /// will create or drop before this barrier flow through them.
    async fn resolve_graph_info(&self) -> MetaResult<InflightGraphInfo> {
        let all_actor_infos = self
            .metadata_manager
            .catalog_controller
            .load_all_actors()
            .await?;

        Ok(InflightGraphInfo::new(
            all_actor_infos
                .fragment_infos
                .into_iter()
                .map(|(id, info)| (id as _, info))
                .collect(),
        ))
    }
}

pub struct GlobalBarrierManager {
    status: Arc<ArcSwap<BarrierManagerStatus>>,
    hummock_manager: HummockManagerRef,
    request_tx: mpsc::UnboundedSender<BarrierManagerRequest>,
    metadata_manager: MetadataManager,
}

pub type BarrierManagerRef = Arc<GlobalBarrierManager>;

impl GlobalBarrierManager {
    /// Serving `SHOW JOBS / SELECT * FROM rw_ddl_progress`
    pub async fn get_ddl_progress(&self) -> MetaResult<Vec<DdlProgress>> {
        let mut ddl_progress = {
            let (tx, rx) = oneshot::channel();
            self.request_tx
                .send(BarrierManagerRequest::GetDdlProgress(tx))
                .context("failed to send get ddl progress request")?;
            rx.await.context("failed to receive get ddl progress")?
        };
        // If not in tracker, means the first barrier not collected yet.
        // In that case just return progress 0.
        let mviews = self
            .metadata_manager
            .catalog_controller
            .list_background_creating_mviews(true)
            .await
            .unwrap();
        for mview in mviews {
            if let Entry::Vacant(e) = ddl_progress.entry(mview.table_id as _) {
                e.insert(DdlProgress {
                    id: mview.table_id as u64,
                    statement: mview.definition,
                    progress: "0.0%".into(),
                });
            }
        }

        Ok(ddl_progress.into_values().collect())
    }

    pub async fn get_hummock_version_id(&self) -> HummockVersionId {
        self.hummock_manager.get_version_id().await
    }
}

#[expect(clippy::type_complexity)]
fn collect_resp_info(
    resps: Vec<BarrierCompleteResponse>,
) -> (
    HashMap<HummockSstableObjectId, u32>,
    Vec<LocalSstableInfo>,
    HashMap<TableId, TableWatermarks>,
    Vec<SstableInfo>,
) {
    let mut sst_to_worker: HashMap<HummockSstableObjectId, u32> = HashMap::new();
    let mut synced_ssts: Vec<LocalSstableInfo> = vec![];
    let mut table_watermarks = Vec::with_capacity(resps.len());
    let mut old_value_ssts = Vec::with_capacity(resps.len());

    for resp in resps {
        let ssts_iter = resp.synced_sstables.into_iter().map(|local_sst| {
            let sst_info = local_sst.sst.expect("field not None");
            sst_to_worker.insert(sst_info.object_id, resp.worker_id);
            LocalSstableInfo::new(
                sst_info.into(),
                from_prost_table_stats_map(local_sst.table_stats_map),
                local_sst.created_at,
            )
        });
        synced_ssts.extend(ssts_iter);
        table_watermarks.push(resp.table_watermarks);
        old_value_ssts.extend(resp.old_value_sstables.into_iter().map(|s| s.into()));
    }

    (
        sst_to_worker,
        synced_ssts,
        merge_multiple_new_table_watermarks(
            table_watermarks
                .into_iter()
                .map(|watermarks| {
                    watermarks
                        .into_iter()
                        .map(|(table_id, watermarks)| {
                            (TableId::new(table_id), TableWatermarks::from(&watermarks))
                        })
                        .collect()
                })
                .collect_vec(),
        ),
        old_value_ssts,
    )
}

fn collect_commit_epoch_info(
    resps: Vec<BarrierCompleteResponse>,
    command_ctx: &CommandContext,
    backfill_pinned_log_epoch: HashMap<TableId, (u64, HashSet<TableId>)>,
) -> CommitEpochInfo {
    let (sst_to_context, synced_ssts, new_table_watermarks, old_value_ssts) =
        collect_resp_info(resps);

    let new_table_fragment_infos = if let Command::CreateStreamingJob { info, job_type } =
        &command_ctx.command
        && !matches!(job_type, CreateStreamingJobType::SnapshotBackfill(_))
    {
        let table_fragments = &info.table_fragments;
        vec![NewTableFragmentInfo::Normal {
            mv_table_id: table_fragments.mv_table_id().map(TableId::new),
            internal_table_ids: table_fragments
                .internal_table_ids()
                .into_iter()
                .map(TableId::new)
                .collect(),
        }]
    } else {
        vec![]
    };

    let mut mv_log_store_truncate_epoch = HashMap::new();
    let mut update_truncate_epoch =
        |table_id: TableId, truncate_epoch| match mv_log_store_truncate_epoch
            .entry(table_id.table_id)
        {
            Entry::Occupied(mut entry) => {
                let prev_truncate_epoch = entry.get_mut();
                if truncate_epoch < *prev_truncate_epoch {
                    *prev_truncate_epoch = truncate_epoch;
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(truncate_epoch);
            }
        };
    for (mv_table_id, subscriptions) in &command_ctx.subscription_info.mv_depended_subscriptions {
        if let Some(truncate_epoch) = subscriptions
            .values()
            .max()
            .map(|max_retention| command_ctx.get_truncate_epoch(*max_retention).0)
        {
            update_truncate_epoch(*mv_table_id, truncate_epoch);
        }
    }
    for (_, (backfill_epoch, upstream_mv_table_ids)) in backfill_pinned_log_epoch {
        for mv_table_id in upstream_mv_table_ids {
            update_truncate_epoch(mv_table_id, backfill_epoch);
        }
    }

    let table_new_change_log = build_table_change_log_delta(
        old_value_ssts.into_iter(),
        synced_ssts.iter().map(|sst| &sst.sst_info),
        must_match!(&command_ctx.barrier_info.kind, BarrierKind::Checkpoint(epochs) => epochs),
        mv_log_store_truncate_epoch.into_iter(),
    );

    let epoch = command_ctx.barrier_info.prev_epoch();
    let tables_to_commit = command_ctx
        .table_ids_to_commit
        .iter()
        .map(|table_id| (*table_id, epoch))
        .collect();

    CommitEpochInfo {
        sstables: synced_ssts,
        new_table_watermarks,
        sst_to_context,
        new_table_fragment_infos,
        change_log_delta: table_new_change_log,
        tables_to_commit,
    }
}
