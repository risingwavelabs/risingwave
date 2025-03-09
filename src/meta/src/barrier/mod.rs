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
use risingwave_hummock_sdk::change_log::build_table_change_log_delta;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::table_stats::from_prost_table_stats_map;
use risingwave_hummock_sdk::table_watermark::{
    merge_multiple_new_table_watermarks, TableWatermarks,
};
use risingwave_hummock_sdk::{HummockSstableObjectId, LocalSstableInfo};
use risingwave_meta_model::WorkerId;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::hummock::HummockVersionStats;
use risingwave_pb::meta::{PausedReason, PbRecoveryStatus};
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn, Instrument};
use uuid::Uuid;

use self::command::CommandContext;
use self::notifier::Notifier;
use crate::barrier::creating_job::{CompleteJobType, CreatingStreamingJobControl};
use crate::barrier::info::InflightGraphInfo;
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingCommand, TrackingJob};
use crate::barrier::rpc::{merge_node_rpc_errors, ControlStreamManager};
use crate::barrier::schedule::ScheduledBarriers;
use crate::barrier::state::BarrierManagerState;
use crate::error::MetaErrorInner;
use crate::hummock::{CommitEpochInfo, HummockManagerRef, NewTableFragmentInfo};
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{
    ActiveStreamingWorkerChange, ActiveStreamingWorkerNodes, LocalNotification, MetaSrvEnv,
    MetadataManager,
};
use crate::rpc::metrics::MetaMetrics;
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
    send_latency_timer: HistogramTimer,
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

pub enum BarrierManagerRequest {
    GetDdlProgress(Sender<HashMap<u32, DdlProgress>>),
}

#[derive(Clone)]
pub struct GlobalBarrierManagerContext {
    status: Arc<ArcSwap<BarrierManagerStatus>>,

    request_tx: mpsc::UnboundedSender<BarrierManagerRequest>,

    metadata_manager: MetadataManager,

    hummock_manager: HummockManagerRef,

    source_manager: SourceManagerRef,

    scale_controller: ScaleControllerRef,

    sink_manager: SinkCoordinatorManager,

    pub(super) metrics: Arc<MetaMetrics>,

    env: MetaSrvEnv,
}

/// [`crate::barrier::GlobalBarrierManager`] sends barriers to all registered compute nodes and
/// collect them, with monotonic increasing epoch numbers. On compute nodes, `LocalBarrierManager`
/// in `risingwave_stream` crate will serve these requests and dispatch them to source actors.
///
/// Configuration change in our system is achieved by the mutation in the barrier. Thus,
/// [`crate::barrier::GlobalBarrierManager`] provides a set of interfaces like a state machine,
/// accepting [`Command`] that carries info to build `Mutation`. To keep the consistency between
/// barrier manager and meta store, some actions like "drop materialized view" or "create mv on mv"
/// must be done in barrier manager transactional using [`Command`].
pub struct GlobalBarrierManager {
    /// Enable recovery or not when failover.
    enable_recovery: bool,

    /// The queue of scheduled barriers.
    scheduled_barriers: schedule::ScheduledBarriers,

    /// The max barrier nums in flight
    in_flight_barrier_nums: usize,

    context: GlobalBarrierManagerContext,

    env: MetaSrvEnv,

    state: BarrierManagerState,

    checkpoint_control: CheckpointControl,

    request_rx: mpsc::UnboundedReceiver<BarrierManagerRequest>,

    /// The `prev_epoch` of pending non checkpoint barriers
    pending_non_checkpoint_barriers: Vec<u64>,

    active_streaming_nodes: ActiveStreamingWorkerNodes,

    control_stream_manager: ControlStreamManager,

    term_id: String,
}

/// Controls the concurrent execution of commands.
struct CheckpointControl {
    /// Save the state and message of barrier in order.
    /// Key is the `prev_epoch`.
    command_ctx_queue: BTreeMap<u64, EpochNode>,

    creating_streaming_job_controls: HashMap<TableId, CreatingStreamingJobControl>,

    /// Command that has been collected but is still completing.
    /// The join handle of the completing future is stored.
    completing_task: CompletingTask,

    hummock_version_stats: HummockVersionStats,

    create_mview_tracker: CreateMviewProgressTracker,

    context: GlobalBarrierManagerContext,
}

impl CheckpointControl {
    async fn new(
        context: GlobalBarrierManagerContext,
        create_mview_tracker: CreateMviewProgressTracker,
    ) -> Self {
        Self {
            command_ctx_queue: Default::default(),
            creating_streaming_job_controls: Default::default(),
            completing_task: CompletingTask::None,
            hummock_version_stats: context.hummock_manager.get_version_stats().await,
            create_mview_tracker,
            context,
        }
    }

    fn total_command_num(&self) -> usize {
        self.command_ctx_queue.len()
            + match &self.completing_task {
                CompletingTask::Completing {
                    command_ctx: Some(_),
                    ..
                } => 1,
                _ => 0,
            }
    }

    /// Update the metrics of barrier nums.
    fn update_barrier_nums_metrics(&self) {
        self.context.metrics.in_flight_barrier_nums.set(
            self.command_ctx_queue
                .values()
                .filter(|x| x.state.is_inflight())
                .count() as i64,
        );
        self.context
            .metrics
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
        command_ctx: Arc<CommandContext>,
        notifiers: Vec<Notifier>,
        node_to_collect: HashSet<WorkerId>,
        creating_jobs_to_wait: HashSet<TableId>,
    ) {
        let timer = self.context.metrics.barrier_latency.start_timer();

        if let Some((_, node)) = self.command_ctx_queue.last_key_value() {
            assert_eq!(
                command_ctx.prev_epoch.value(),
                node.command_ctx.curr_epoch.value()
            );
        }

        tracing::trace!(
            prev_epoch = command_ctx.prev_epoch.value().0,
            ?creating_jobs_to_wait,
            "enqueue command"
        );
        self.command_ctx_queue.insert(
            command_ctx.prev_epoch.value().0,
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
            .map(|(_, x)| &x.command_ctx)
            .or(match &self.completing_task {
                CompletingTask::None | CompletingTask::Err(_) => None,
                CompletingTask::Completing { command_ctx, .. } => command_ctx.as_ref(),
            })
            .map(|command_ctx| command_ctx.command.should_pause_inject_barrier())
            .unwrap_or(false);
        debug_assert_eq!(
            self.command_ctx_queue
                .values()
                .map(|node| &node.command_ctx)
                .chain(
                    match &self.completing_task {
                        CompletingTask::None | CompletingTask::Err(_) => None,
                        CompletingTask::Completing { command_ctx, .. } => command_ctx.as_ref(),
                    }
                    .into_iter()
                )
                .any(|command_ctx| command_ctx.command.should_pause_inject_barrier()),
            should_pause
        );

        in_flight_not_full && !should_pause
    }

    /// We need to make sure there are no changes when doing recovery
    pub async fn clear_on_err(&mut self, err: &MetaError) {
        // join spawned completing command to finish no matter it succeeds or not.
        let is_err = match replace(&mut self.completing_task, CompletingTask::None) {
            CompletingTask::None => false,
            CompletingTask::Completing { join_handle, .. } => {
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
                    Ok(Ok(_)) => false,
                }
            }
            CompletingTask::Err(_) => true,
        };
        if !is_err {
            // continue to finish the pending collected barrier.
            while let Some(task) = self.next_complete_barrier_task(None) {
                if let Err(e) = self.context.clone().complete_barrier(task).await {
                    error!(
                        err = ?e.as_report(),
                        "failed to complete barrier during recovery"
                    );
                    break;
                } else {
                    info!("succeed to complete barrier during recovery")
                }
            }
        }
        for (_, node) in take(&mut self.command_ctx_queue) {
            for notifier in node.notifiers {
                notifier.notify_failed(err.clone());
            }
            node.enqueue_time.observe_duration();
        }
        self.create_mview_tracker.abort_all();
    }

    /// Return the earliest command waiting on the `worker_id`.
    fn command_wait_collect_from_worker(&self, worker_id: WorkerId) -> Option<&CommandContext> {
        for epoch_node in self.command_ctx_queue.values() {
            if epoch_node.state.node_to_collect.contains(&worker_id) {
                return Some(&epoch_node.command_ctx);
            }
        }
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
    command_ctx: Arc<CommandContext>,
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
        command_ctx: Option<Arc<CommandContext>>,
        table_ids_to_finish: HashSet<TableId>,
        creating_job_epochs: Vec<(TableId, u64)>,

        // The join handle of a spawned task that completes the barrier.
        // The return value indicate whether there is some create streaming job command
        // that has finished but not checkpointed. If there is any, we will force checkpoint on the next barrier
        join_handle: JoinHandle<MetaResult<HummockVersionStats>>,
    },
    #[expect(dead_code)]
    Err(MetaError),
}

impl GlobalBarrierManager {
    /// Create a new [`crate::barrier::GlobalBarrierManager`].
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        sink_manager: SinkCoordinatorManager,
        metrics: Arc<MetaMetrics>,
        scale_controller: ScaleControllerRef,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;
        let in_flight_barrier_nums = env.opts.in_flight_barrier_nums;

        let initial_invalid_state = BarrierManagerState::new(
            None,
            InflightGraphInfo::default(),
            InflightSubscriptionInfo::default(),
            None,
        );

        let active_streaming_nodes = ActiveStreamingWorkerNodes::uninitialized();

        let tracker = CreateMviewProgressTracker::default();

        let (request_tx, request_rx) = mpsc::unbounded_channel();

        let context = GlobalBarrierManagerContext {
            status: Arc::new(ArcSwap::new(Arc::new(BarrierManagerStatus::Starting))),
            request_tx,
            metadata_manager,
            hummock_manager,
            source_manager,
            scale_controller,
            sink_manager,
            metrics,
            env: env.clone(),
        };

        let control_stream_manager = ControlStreamManager::new(context.clone());
        let checkpoint_control = CheckpointControl::new(context.clone(), tracker).await;

        Self {
            enable_recovery,
            scheduled_barriers,
            in_flight_barrier_nums,
            context,
            env,
            state: initial_invalid_state,
            checkpoint_control,
            request_rx,
            pending_non_checkpoint_barriers: Vec::new(),
            active_streaming_nodes,
            control_stream_manager,
            term_id: GlobalBarrierManagerContext::next_term_id(),
        }
    }

    pub fn context(&self) -> &GlobalBarrierManagerContext {
        &self.context
    }

    pub fn start(barrier_manager: GlobalBarrierManager) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            barrier_manager.run(shutdown_rx).await;
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
    async fn run(mut self, mut shutdown_rx: Receiver<()>) {
        // Initialize the barrier manager.
        let interval = Duration::from_millis(
            self.env.system_params_reader().await.barrier_interval_ms() as u64,
        );
        self.scheduled_barriers.set_min_interval(interval);
        tracing::info!(
            "Starting barrier manager with: interval={:?}, enable_recovery={}, in_flight_barrier_nums={}",
            interval,
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
            self.context
                .set_status(BarrierManagerStatus::Recovering(RecoveryReason::Bootstrap));
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

            self.recovery(paused_reason, None).instrument(span).await;
        }

        self.context.set_status(BarrierManagerStatus::Running);

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
                    #[cfg(debug_assertions)]
                    {
                        use risingwave_pb::common::WorkerNode;
                        match self
                            .context
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

                    self.state.inflight_graph_info
                        .on_new_worker_node_map(self.active_streaming_nodes.current());
                    self.checkpoint_control.creating_streaming_job_controls.values().for_each(|job| job.on_new_worker_node_map(self.active_streaming_nodes.current()));
                    if let ActiveStreamingWorkerChange::Add(node) | ActiveStreamingWorkerChange::Update(node) = changed_worker {
                        self.control_stream_manager.add_worker(node, &self.state.inflight_subscription_info, self.term_id.clone()).await;
                    }
                }

                notification = local_notification_rx.recv() => {
                    let notification = notification.unwrap();
                    match notification {
                        // Handle barrier interval and checkpoint frequency changes.
                        LocalNotification::SystemParamsChange(p) => {
                            self.scheduled_barriers.set_min_interval(Duration::from_millis(p.barrier_interval_ms() as u64));
                            self.scheduled_barriers
                                .set_checkpoint_frequency(p.checkpoint_frequency() as usize)
                        },
                        // Handle adhoc recovery triggered by user.
                        LocalNotification::AdhocRecovery => {
                            self.adhoc_recovery().await;
                        }
                        _ => {}
                    }
                }
                (worker_id, resp_result) = self.control_stream_manager.next_complete_barrier_response() => {
                    if let  Err(e) = resp_result.and_then(|resp| self.checkpoint_control.barrier_collected(resp, &mut self.control_stream_manager)) {
                        {
                            let failed_command = self.checkpoint_control.command_wait_collect_from_worker(worker_id as _);
                            if failed_command.is_some()
                                || self.state.inflight_graph_info.contains_worker(worker_id as _)
                                || self.checkpoint_control.creating_streaming_job_controls.values().any(|job| job.is_wait_on_worker(worker_id as _)) {
                                let errors = self.control_stream_manager.collect_errors(worker_id, e).await;
                                let err = merge_node_rpc_errors("get error from control stream", errors);
                                if let Some(failed_command) = failed_command {
                                    self.context.report_collect_failure(failed_command, &err);
                                }
                                self.failure_recovery(err).await;
                            } else {
                                warn!(e = %e.as_report(), worker_id, "no barrier to collect from worker, ignore err");
                            }
                        }
                    }
                }
                complete_result = self.checkpoint_control.next_completed_barrier(&mut self.scheduled_barriers) => {
                    match complete_result {
                        Ok(output) => {
                            if !output.table_ids_to_finish.is_empty() {
                                self.control_stream_manager.remove_partial_graph(
                                    output.table_ids_to_finish.iter().map(|table_id| table_id.table_id).collect()
                                );
                            }
                        }
                        Err(e) => {
                            self.failure_recovery(e).await;
                        }
                    }
                },
                scheduled = self.scheduled_barriers.next_barrier(),
                    if self
                        .checkpoint_control
                        .can_inject_barrier(self.in_flight_barrier_nums) => {
                    if let Err(e) = self.handle_new_barrier(scheduled) {
                        self.failure_recovery(e).await;
                    }
                }
            }
            self.checkpoint_control.update_barrier_nums_metrics();
        }
    }

    /// Handle the new barrier from the scheduled queue and inject it.
    fn handle_new_barrier(&mut self, scheduled: Scheduled) -> MetaResult<()> {
        let Scheduled {
            mut command,
            mut notifiers,
            send_latency_timer,
            checkpoint,
            span,
        } = scheduled;

        if let Some(table_to_cancel) = command.table_to_cancel()
            && self
                .checkpoint_control
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
            if !self
                .checkpoint_control
                .creating_streaming_job_controls
                .is_empty()
            {
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

        let Some((prev_epoch, curr_epoch)) = self.state.next_epoch_pair(&command) else {
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
            self.checkpoint_control
                .creating_streaming_job_controls
                .insert(
                    info.table_fragments.table_id(),
                    CreatingStreamingJobControl::new(
                        info.clone(),
                        snapshot_backfill_info.clone(),
                        prev_epoch.value().0,
                        &self.checkpoint_control.hummock_version_stats,
                        &self.context.metrics,
                        mutation,
                    ),
                );
        }

        self.pending_non_checkpoint_barriers
            .push(prev_epoch.value().0);
        let kind = if checkpoint {
            let epochs = take(&mut self.pending_non_checkpoint_barriers);
            BarrierKind::Checkpoint(epochs)
        } else {
            BarrierKind::Barrier
        };

        tracing::trace!(prev_epoch = prev_epoch.value().0, "inject barrier");

        // Collect the jobs to finish
        if let (BarrierKind::Checkpoint(_), Command::Plain(None)) = (&kind, &command)
            && let Some(jobs_to_merge) = self.checkpoint_control.jobs_to_merge()
        {
            command = Command::MergeSnapshotBackfillStreamingJobs(jobs_to_merge);
        }

        let command = command;

        let (
            pre_applied_graph_info,
            pre_applied_subscription_info,
            table_ids_to_commit,
            jobs_to_wait,
        ) = self.state.apply_command(&command);

        // Tracing related stuff
        prev_epoch.span().in_scope(|| {
            tracing::info!(target: "rw_tracing", epoch = curr_epoch.value().0, "new barrier enqueued");
        });
        span.record("epoch", curr_epoch.value().0);

        let command_ctx = Arc::new(CommandContext::new(
            self.active_streaming_nodes.current().clone(),
            pre_applied_subscription_info,
            prev_epoch.clone(),
            curr_epoch.clone(),
            table_ids_to_commit,
            self.state.paused_reason(),
            command,
            kind,
            self.context.clone(),
            span,
        ));

        send_latency_timer.observe_duration();

        for creating_job in &mut self
            .checkpoint_control
            .creating_streaming_job_controls
            .values_mut()
        {
            creating_job.on_new_command(&mut self.control_stream_manager, &command_ctx)?;
        }

        let node_to_collect = match self.control_stream_manager.inject_command_ctx_barrier(
            &command_ctx,
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
        let curr_paused_reason = command_ctx.next_paused_reason();

        notifiers.iter_mut().for_each(|n| n.notify_started());

        // Update the paused state after the barrier is injected.
        self.state.set_paused_reason(curr_paused_reason);
        // Record the in-flight barrier.
        self.checkpoint_control.enqueue_command(
            command_ctx,
            notifiers,
            node_to_collect,
            jobs_to_wait,
        );

        Ok(())
    }

    async fn failure_recovery(&mut self, err: MetaError) {
        self.checkpoint_control.clear_on_err(&err).await;
        self.pending_non_checkpoint_barriers.clear();

        if self.enable_recovery {
            self.context
                .set_status(BarrierManagerStatus::Recovering(RecoveryReason::Failover(
                    err.clone(),
                )));
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

            // No need to clean dirty tables for barrier recovery,
            // The foreground stream job should cleanup their own tables.
            self.recovery(None, Some(err)).instrument(span).await;
            self.context.set_status(BarrierManagerStatus::Running);
        } else {
            panic!("failed to execute barrier: {}", err.as_report());
        }
    }

    async fn adhoc_recovery(&mut self) {
        let err = MetaErrorInner::AdhocRecovery.into();
        self.checkpoint_control.clear_on_err(&err).await;

        self.context
            .set_status(BarrierManagerStatus::Recovering(RecoveryReason::Adhoc));
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
        self.recovery(None, Some(err)).instrument(span).await;
        self.context.set_status(BarrierManagerStatus::Running);
    }
}

#[derive(Default)]
struct CompleteBarrierTask {
    commit_info: CommitEpochInfo,
    finished_jobs: Vec<TrackingJob>,
    notifiers: Vec<Notifier>,
    /// Some((`command_ctx`, `enqueue_time`))
    command_context: Option<(Arc<CommandContext>, HistogramTimer)>,
    table_ids_to_finish: HashSet<TableId>,
    creating_job_epochs: Vec<(TableId, u64)>,
}

impl GlobalBarrierManagerContext {
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

    async fn complete_barrier(self, task: CompleteBarrierTask) -> MetaResult<HummockVersionStats> {
        let result: MetaResult<()> = try {
            let wait_commit_timer = self.metrics.barrier_wait_commit_latency.start_timer();
            self.hummock_manager.commit_epoch(task.commit_info).await?;
            if let Some((command_ctx, _)) = &task.command_context {
                command_ctx.post_collect().await?;
            }

            wait_commit_timer.observe_duration();
        };

        {
            if let Err(e) = result {
                for notifier in task.notifiers {
                    notifier.notify_collection_failed(e.clone());
                }
                return Err(e);
            }
            task.notifiers.into_iter().for_each(|notifier| {
                notifier.notify_collected();
            });
            try_join_all(
                task.finished_jobs
                    .into_iter()
                    .map(|finished_job| finished_job.finish(&self.metadata_manager)),
            )
            .await?;
            if let Some((command_ctx, enqueue_time)) = task.command_context {
                let duration_sec = enqueue_time.stop_and_record();
                self.report_complete_event(duration_sec, &command_ctx);
                self.metrics
                    .last_committed_barrier_time
                    .set(command_ctx.curr_epoch.value().as_unix_secs() as i64);
            }
        }

        Ok(self.hummock_manager.get_version_stats().await)
    }

    pub fn hummock_manager(&self) -> &HummockManagerRef {
        &self.hummock_manager
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

impl GlobalBarrierManagerContext {
    fn report_complete_event(&self, duration_sec: f64, command_ctx: &CommandContext) {
        // Record barrier latency in event log.
        use risingwave_pb::meta::event_log;
        let event = event_log::EventBarrierComplete {
            prev_epoch: command_ctx.prev_epoch.value().0,
            cur_epoch: command_ctx.curr_epoch.value().0,
            duration_sec,
            command: command_ctx.command.to_string(),
            barrier_kind: command_ctx.kind.as_str_name().to_string(),
        };
        self.env
            .event_log_manager_ref()
            .add_event_logs(vec![event_log::Event::BarrierComplete(event)]);
    }
}

struct BarrierCompleteOutput {
    table_ids_to_finish: HashSet<TableId>,
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
        mut scheduled_barriers: Option<&mut ScheduledBarriers>,
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
                if !node.command_ctx.kind.is_checkpoint() {
                    assert!(finished_jobs.is_empty());
                    node.notifiers.into_iter().for_each(|notifier| {
                        notifier.notify_collected();
                    });
                    if let Some(scheduled_barriers) = &mut scheduled_barriers
                        && self.create_mview_tracker.has_pending_finished_jobs()
                        && self
                            .command_ctx_queue
                            .values()
                            .all(|node| !node.command_ctx.kind.is_checkpoint())
                    {
                        scheduled_barriers.force_checkpoint_in_next_barrier();
                    }
                    continue;
                }
                let table_ids_to_finish = node
                    .state
                    .finished_jobs
                    .drain()
                    .map(|(table_id, (info, resps))| {
                        node.state.resps.extend(resps);
                        finished_jobs.push(TrackingJob::New(TrackingCommand {
                            info,
                            replace_table_info: None,
                        }));
                        table_id
                    })
                    .collect();
                let commit_info = collect_commit_epoch_info(
                    take(&mut node.state.resps),
                    &node.command_ctx,
                    self.collect_backfill_pinned_upstream_log_epoch(),
                );
                task = Some(CompleteBarrierTask {
                    commit_info,
                    finished_jobs,
                    notifiers: node.notifiers,
                    command_context: Some((node.command_ctx, node.enqueue_time)),
                    table_ids_to_finish,
                    creating_job_epochs: vec![],
                });
                break;
            }
        }
        if !creating_jobs_task.is_empty() {
            let task = task.get_or_insert_default();
            for (table_id, epoch, resps, is_first_time) in creating_jobs_task {
                GlobalBarrierManagerContext::collect_creating_job_commit_epoch_info(
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

    pub(super) fn next_completed_barrier<'a>(
        &'a mut self,
        scheduled_barriers: &mut ScheduledBarriers,
    ) -> impl Future<Output = MetaResult<BarrierCompleteOutput>> + 'a {
        // If there is no completing barrier, try to start completing the earliest barrier if
        // it has been collected.
        if let CompletingTask::None = &self.completing_task {
            if let Some(task) = self.next_complete_barrier_task(Some(scheduled_barriers)) {
                {
                    let command_ctx = task
                        .command_context
                        .as_ref()
                        .map(|(command_ctx, _)| command_ctx.clone());
                    let table_ids_to_finish = task.table_ids_to_finish.clone();
                    let creating_job_epochs = task.creating_job_epochs.clone();
                    let join_handle = tokio::spawn(self.context.clone().complete_barrier(task));
                    self.completing_task = CompletingTask::Completing {
                        command_ctx,
                        join_handle,
                        table_ids_to_finish,
                        creating_job_epochs,
                    };
                }
            }
        }

        self.next_completed_barrier_inner()
    }

    async fn next_completed_barrier_inner(&mut self) -> MetaResult<BarrierCompleteOutput> {
        let CompletingTask::Completing { join_handle, .. } = &mut self.completing_task else {
            return pending().await;
        };

        let (table_ids_to_finish, creating_job_epochs) = {
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
                let completed_command =
                    replace(&mut self.completing_task, next_completing_command_status);
                self.hummock_version_stats = join_result?;

                must_match!(completed_command, CompletingTask::Completing {
                    table_ids_to_finish,
                    creating_job_epochs,
                    ..
                } => (table_ids_to_finish, creating_job_epochs))
            }
        };

        {
            for (table_id, epoch) in creating_job_epochs {
                self.creating_streaming_job_controls
                    .get_mut(&table_id)
                    .expect("should exist")
                    .ack_completed(epoch)
            }

            Ok(BarrierCompleteOutput {
                table_ids_to_finish,
            })
        }
    }
}

impl GlobalBarrierManagerContext {
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

    /// Set barrier manager status.
    fn set_status(&self, new_status: BarrierManagerStatus) {
        self.status.store(Arc::new(new_status));
    }

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

    fn next_term_id() -> String {
        Uuid::new_v4().to_string()
    }
}

pub type BarrierManagerRef = GlobalBarrierManagerContext;

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
        must_match!(&command_ctx.kind, BarrierKind::Checkpoint(epochs) => epochs),
        mv_log_store_truncate_epoch.into_iter(),
    );

    let epoch = command_ctx.prev_epoch.value().0;
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
