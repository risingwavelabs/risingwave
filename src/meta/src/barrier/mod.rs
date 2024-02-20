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

use std::assert_matches::assert_matches;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::{poll_fn, Future};
use std::sync::Arc;
use std::task::{ready, Poll};
use std::time::Duration;

use anyhow::anyhow;
use arc_swap::ArcSwap;
use fail::fail_point;
use futures::FutureExt;
use itertools::Itertools;
use prometheus::HistogramTimer;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::system_param::PAUSE_ON_NEXT_BOOTSTRAP_KEY;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::table_watermark::{
    merge_multiple_new_table_watermarks, TableWatermarks,
};
use risingwave_hummock_sdk::{ExtendedSstableInfo, HummockSstableObjectId};
use risingwave_pb::catalog::table::TableType;
use risingwave_pb::common::WorkerNode;
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::PausedReason;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, warn, Instrument};

use self::command::CommandContext;
use self::notifier::Notifier;
use self::progress::TrackingCommand;
use crate::barrier::info::InflightActorInfo;
use crate::barrier::notifier::BarrierInfo;
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingJob};
use crate::barrier::rpc::BarrierCollectFuture;
use crate::barrier::state::BarrierManagerState;
use crate::hummock::{CommitEpochInfo, HummockManagerRef};
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{
    ActiveStreamingWorkerNodes, LocalNotification, MetaSrvEnv, MetadataManager, WorkerId,
};
use crate::model::{ActorId, TableFragments};
use crate::rpc::metrics::MetaMetrics;
use crate::stream::{ScaleControllerRef, SourceManagerRef};
use crate::{MetaError, MetaResult};

mod command;
mod info;
mod notifier;
mod progress;
mod recovery;
mod rpc;
mod schedule;
mod state;
mod trace;

pub use self::command::{Command, ReplaceTablePlan, Reschedule};
pub use self::rpc::StreamRpcManager;
pub use self::schedule::BarrierScheduler;
pub use self::trace::TracedEpoch;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct TableMap<T> {
    inner: HashMap<TableId, T>,
}

impl<T> TableMap<T> {
    pub fn remove(&mut self, table_id: &TableId) -> Option<T> {
        self.inner.remove(table_id)
    }
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

pub(crate) type TableActorMap = TableMap<HashSet<ActorId>>;
pub(crate) type TableUpstreamMvCountMap = TableMap<HashMap<TableId, usize>>;
pub(crate) type TableDefinitionMap = TableMap<String>;
pub(crate) type TableNotifierMap = TableMap<Notifier>;
pub(crate) type TableFragmentMap = TableMap<TableFragments>;

/// The reason why the cluster is recovering.
enum RecoveryReason {
    /// After bootstrap.
    Bootstrap,
    /// After failure.
    Failover(MetaError),
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

#[derive(Clone)]
pub struct GlobalBarrierManagerContext {
    status: Arc<ArcSwap<BarrierManagerStatus>>,

    tracker: Arc<Mutex<CreateMviewProgressTracker>>,

    metadata_manager: MetadataManager,

    hummock_manager: HummockManagerRef,

    source_manager: SourceManagerRef,

    scale_controller: ScaleControllerRef,

    sink_manager: SinkCoordinatorManager,

    pub(super) metrics: Arc<MetaMetrics>,

    stream_rpc_manager: StreamRpcManager,

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

    active_streaming_nodes: ActiveStreamingWorkerNodes,

    prev_injecting_barrier: Option<Receiver<()>>,
}

/// Controls the concurrent execution of commands.
struct CheckpointControl {
    /// Save the state and message of barrier in order.
    inflight_command_ctx_queue: VecDeque<InflightCommand>,

    /// Command that has been collected but is still completing.
    /// The join handle of the completing future is stored.
    completing_command: Option<CompletingCommand>,

    context: GlobalBarrierManagerContext,
}

impl CheckpointControl {
    fn new(context: GlobalBarrierManagerContext) -> Self {
        Self {
            inflight_command_ctx_queue: Default::default(),
            completing_command: None,
            context,
        }
    }
}

impl CreateMviewProgressTracker {
    /// Stash a command to finish later.
    fn stash_command_to_finish(&mut self, finished_job: TrackingJob) {
        self.finished_jobs.push(finished_job);
    }

    /// Finish stashed jobs.
    /// If checkpoint, means all jobs can be finished.
    /// If not checkpoint, jobs which do not require checkpoint can be finished.
    ///
    /// Returns whether there are still remaining stashed jobs to finish.
    async fn finish_jobs(&mut self, checkpoint: bool) -> MetaResult<bool> {
        for job in self
            .finished_jobs
            .extract_if(|job| checkpoint || !job.is_checkpoint_required())
        {
            // The command is ready to finish. We can now call `pre_finish`.
            job.pre_finish().await?;
            job.notify_finished();
        }
        Ok(!self.finished_jobs.is_empty())
    }
}

impl CheckpointControl {
    fn cancel_command(&mut self, cancelled_job: TrackingJob) {
        if let TrackingJob::New(cancelled_command) = cancelled_job {
            if let Some(index) = self.inflight_command_ctx_queue.iter().position(|command| {
                command.command_ctx.prev_epoch.value()
                    == cancelled_command.context.prev_epoch.value()
            }) {
                self.inflight_command_ctx_queue.remove(index);
            }
        } else {
            // Recovered jobs do not need to be cancelled since only `RUNNING` actors will get recovered.
        }
    }
}

impl CreateMviewProgressTracker {
    fn cancel_stashed_command(&mut self, id: TableId) {
        self.finished_jobs
            .retain(|x| x.table_to_create() != Some(id));
    }
}

impl CheckpointControl {
    /// Update the metrics of barrier nums.
    fn update_barrier_nums_metrics(&self) {
        self.context
            .metrics
            .in_flight_barrier_nums
            .set(self.inflight_command_ctx_queue.len() as i64);
        self.context
            .metrics
            .all_barrier_nums
            .set(self.inflight_command_ctx_queue.len() as i64);
    }

    /// Enqueue a barrier command, and init its state to `InFlight`.
    fn enqueue_inflight_command(
        &mut self,
        command_ctx: Arc<CommandContext>,
        notifiers: Vec<Notifier>,
        barrier_collect_future: BarrierCollectFuture,
    ) {
        let enqueue_time = self.context.metrics.barrier_latency.start_timer();

        if let Some(command) = self.inflight_command_ctx_queue.back() {
            assert_eq!(
                command.command_ctx.curr_epoch.value(),
                command_ctx.prev_epoch.value()
            );
        }

        self.inflight_command_ctx_queue.push_back(InflightCommand {
            command_ctx,
            barrier_collect_future,
            enqueue_time,
            notifiers,
        });
    }

    /// Pause inject barrier until True.
    fn can_inject_barrier(&self, in_flight_barrier_nums: usize) -> bool {
        let in_flight_not_full = self.inflight_command_ctx_queue.len() < in_flight_barrier_nums;

        // Whether some command requires pausing concurrent barrier. If so, it must be the last one.
        let should_pause = self
            .inflight_command_ctx_queue
            .back()
            .map(|command| command.command_ctx.command.should_pause_inject_barrier())
            .unwrap_or(false);
        debug_assert_eq!(
            self.inflight_command_ctx_queue
                .iter()
                .any(|command| command.command_ctx.command.should_pause_inject_barrier()),
            should_pause
        );

        in_flight_not_full && !should_pause
    }

    /// We need to make sure there are no changes when doing recovery
    pub async fn clear_and_fail_all_nodes(&mut self, err: &MetaError) {
        // join spawned completing command to finish no matter it succeeds or not.
        if let Some(command) = self.completing_command.take() {
            info!(
                prev_epoch = ?command.command_ctx.prev_epoch,
                curr_epoch = ?command.command_ctx.curr_epoch,
                "waiting for completing command to finish in recovery"
            );
            match command.join_handle.await {
                Err(e) => {
                    warn!(err = ?e.as_report(), "failed to join completing task");
                }
                Ok(Err(e)) => {
                    warn!(err = ?e.as_report(), "failed to complete barrier during clear");
                }
                Ok(Ok(_)) => {}
            };
        }
        for command in self.inflight_command_ctx_queue.drain(..) {
            for notifier in command.notifiers {
                notifier.notify_collection_failed(err.clone());
            }
            command.enqueue_time.observe_duration();
        }
    }
}

struct InflightCommand {
    command_ctx: Arc<CommandContext>,

    barrier_collect_future: BarrierCollectFuture,

    enqueue_time: HistogramTimer,

    notifiers: Vec<Notifier>,
}

struct CompleteBarrierOutput {
    cancelled_job: Option<TrackingJob>,
    has_remaining_job: bool,
}

struct CompletingCommand {
    command_ctx: Arc<CommandContext>,

    join_handle: JoinHandle<MetaResult<CompleteBarrierOutput>>,
}

/// The result of barrier collect.
#[derive(Debug)]
struct BarrierCollectResult {
    prev_epoch: u64,
    result: MetaResult<Vec<BarrierCompleteResponse>>,
}

impl GlobalBarrierManager {
    /// Create a new [`crate::barrier::GlobalBarrierManager`].
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv,
        metadata_manager: MetadataManager,
        hummock_manager: HummockManagerRef,
        source_manager: SourceManagerRef,
        sink_manager: SinkCoordinatorManager,
        metrics: Arc<MetaMetrics>,
        stream_rpc_manager: StreamRpcManager,
        scale_controller: ScaleControllerRef,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;
        let in_flight_barrier_nums = env.opts.in_flight_barrier_nums;

        let initial_invalid_state = BarrierManagerState::new(
            TracedEpoch::new(Epoch(INVALID_EPOCH)),
            InflightActorInfo::default(),
            None,
        );

        let active_streaming_nodes = ActiveStreamingWorkerNodes::uninitialized();

        let tracker = CreateMviewProgressTracker::new();

        let context = GlobalBarrierManagerContext {
            status: Arc::new(ArcSwap::new(Arc::new(BarrierManagerStatus::Starting))),
            metadata_manager,
            hummock_manager,
            source_manager,
            scale_controller,
            sink_manager,
            metrics,
            tracker: Arc::new(Mutex::new(tracker)),
            stream_rpc_manager,
            env: env.clone(),
        };

        let checkpoint_control = CheckpointControl::new(context.clone());

        Self {
            enable_recovery,
            scheduled_barriers,
            in_flight_barrier_nums,
            context,
            env,
            state: initial_invalid_state,
            checkpoint_control,
            active_streaming_nodes,
            prev_injecting_barrier: None,
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
    async fn take_pause_on_bootstrap(&self) -> MetaResult<bool> {
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
            if let Some(system_ctl) = self.env.system_params_controller() {
                system_ctl
                    .set_param(PAUSE_ON_NEXT_BOOTSTRAP_KEY, Some("false".to_owned()))
                    .await?;
            } else {
                self.env
                    .system_params_manager()
                    .unwrap()
                    .set_param(PAUSE_ON_NEXT_BOOTSTRAP_KEY, Some("false".to_owned()))
                    .await?;
            }
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
            let job_exist = match &self.context.metadata_manager {
                MetadataManager::V1(mgr) => mgr.fragment_manager.has_any_table_fragments().await,
                MetadataManager::V2(mgr) => mgr
                    .catalog_controller
                    .has_any_streaming_jobs()
                    .await
                    .unwrap(),
            };
            if job_exist {
                panic!(
                    "Some streaming jobs already exist in meta, please start with recovery enabled \
                or clean up the metadata using `./risedev clean-data`"
                );
            }
        }

        {
            let latest_snapshot = self.context.hummock_manager.latest_snapshot();
            assert_eq!(
                latest_snapshot.committed_epoch, latest_snapshot.current_epoch,
                "persisted snapshot must be from a checkpoint barrier"
            );
            let prev_epoch = TracedEpoch::new(latest_snapshot.committed_epoch.into());

            // Bootstrap recovery. Here we simply trigger a recovery process to achieve the
            // consistency.
            // Even if there's no actor to recover, we still go through the recovery process to
            // inject the first `Initial` barrier.
            self.context
                .set_status(BarrierManagerStatus::Recovering(RecoveryReason::Bootstrap));
            let span = tracing::info_span!("bootstrap_recovery", prev_epoch = prev_epoch.value().0);

            let paused = self.take_pause_on_bootstrap().await.unwrap_or(false);
            let paused_reason = paused.then_some(PausedReason::Manual);

            self.recovery(paused_reason).instrument(span).await;
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

                changed_worker = self.active_streaming_nodes.changed() => {
                    #[cfg(debug_assertions)]
                    {
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
                                            parallel_units: node.parallel_units.clone(),
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

                    self.state
                        .resolve_worker_nodes(self.active_streaming_nodes.current().values().cloned());
                }

                // Checkpoint frequency changes.
                notification = local_notification_rx.recv() => {
                    let notification = notification.unwrap();
                    // Handle barrier interval and checkpoint frequency changes
                    if let LocalNotification::SystemParamsChange(p) = &notification {
                        self.scheduled_barriers.set_min_interval(Duration::from_millis(p.barrier_interval_ms() as u64));
                        self.scheduled_barriers
                            .set_checkpoint_frequency(p.checkpoint_frequency() as usize)
                    }
                }
                complete_result = self.checkpoint_control.next_completed_barrier() => {
                    match complete_result {
                        Ok((command_context, remaining)) => {
                            // If there are remaining commands (that requires checkpoint to finish), we force
                            // the next barrier to be a checkpoint.
                            if remaining {
                                assert_matches!(command_context.kind, BarrierKind::Barrier);
                                self.scheduled_barriers.force_checkpoint_in_next_barrier();
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
                    self.handle_new_barrier(scheduled);
                }
            }
            self.checkpoint_control.update_barrier_nums_metrics();
        }
    }

    /// Handle the new barrier from the scheduled queue and inject it.
    fn handle_new_barrier(&mut self, scheduled: Scheduled) {
        let Scheduled {
            command,
            mut notifiers,
            send_latency_timer,
            checkpoint,
            span,
        } = scheduled;

        let info = self.state.apply_command(&command);

        let (prev_epoch, curr_epoch) = self.state.next_epoch_pair();
        let kind = if checkpoint {
            BarrierKind::Checkpoint
        } else {
            BarrierKind::Barrier
        };

        // Tracing related stuff
        tracing::info!(target: "rw_tracing", parent: prev_epoch.span(), epoch = curr_epoch.value().0, "new barrier enqueued");
        span.record("epoch", curr_epoch.value().0);

        let command_ctx = Arc::new(CommandContext::new(
            info,
            prev_epoch.clone(),
            curr_epoch.clone(),
            self.state.paused_reason(),
            command,
            kind,
            self.context.clone(),
            span,
        ));

        send_latency_timer.observe_duration();

        // this is to notify that the barrier has been injected so that the next
        // barrier can be injected to avoid out of order barrier injection.
        // TODO: can be removed when bidi-stream control in implemented.
        let (inject_tx, inject_rx) = oneshot::channel();
        let prev_inject_rx = self.prev_injecting_barrier.replace(inject_rx);
        let await_collect_future =
            self.context
                .inject_barrier(command_ctx.clone(), Some(inject_tx), prev_inject_rx);

        // Notify about the injection.
        let prev_paused_reason = self.state.paused_reason();
        let curr_paused_reason = command_ctx.next_paused_reason();

        let info = BarrierInfo {
            prev_epoch: prev_epoch.value(),
            curr_epoch: curr_epoch.value(),
            prev_paused_reason,
            curr_paused_reason,
        };
        notifiers.iter_mut().for_each(|n| n.notify_started(info));

        // Update the paused state after the barrier is injected.
        self.state.set_paused_reason(curr_paused_reason);
        // Record the in-flight barrier.
        self.checkpoint_control.enqueue_inflight_command(
            command_ctx.clone(),
            notifiers,
            await_collect_future,
        );
    }

    async fn failure_recovery(&mut self, err: MetaError) {
        self.prev_injecting_barrier = None;
        self.checkpoint_control.clear_and_fail_all_nodes(&err).await;

        if self.enable_recovery {
            self.context
                .set_status(BarrierManagerStatus::Recovering(RecoveryReason::Failover(
                    err.clone(),
                )));
            let latest_snapshot = self.context.hummock_manager.latest_snapshot();
            let prev_epoch = TracedEpoch::new(latest_snapshot.committed_epoch.into()); // we can only recovery from the committed epoch
            let span = tracing::info_span!(
                "failure_recovery",
                error = %err.as_report(),
                prev_epoch = prev_epoch.value().0
            );

            // No need to clean dirty tables for barrier recovery,
            // The foreground stream job should cleanup their own tables.
            self.recovery(None).instrument(span).await;
            self.context.set_status(BarrierManagerStatus::Running);
        } else {
            panic!("failed to execute barrier: {}", err.as_report());
        }
    }
}

impl GlobalBarrierManagerContext {
    /// Try to commit this node. If err, returns
    async fn complete_barrier(
        self,
        command_ctx: Arc<CommandContext>,
        resps: Vec<BarrierCompleteResponse>,
        mut notifiers: Vec<Notifier>,
        enqueue_time: HistogramTimer,
    ) -> MetaResult<CompleteBarrierOutput> {
        let wait_commit_timer = self.metrics.barrier_wait_commit_latency.start_timer();
        let (commit_info, create_mview_progress) = collect_commit_epoch_info(resps);
        if let Err(e) = self.update_snapshot(&command_ctx, commit_info).await {
            for notifier in notifiers {
                notifier.notify_collection_failed(e.clone());
            }
            return Err(e);
        };
        notifiers.iter_mut().for_each(|notifier| {
            notifier.notify_collected();
        });
        let result = self
            .update_tracking_jobs(notifiers, command_ctx.clone(), create_mview_progress)
            .await;
        let duration_sec = enqueue_time.stop_and_record();
        self.report_complete_event(duration_sec, &command_ctx);
        wait_commit_timer.observe_duration();
        result
    }

    async fn update_snapshot(
        &self,
        command_ctx: &CommandContext,
        commit_info: CommitEpochInfo,
    ) -> MetaResult<()> {
        {
            {
                let prev_epoch = command_ctx.prev_epoch.value().0;
                // We must ensure all epochs are committed in ascending order,
                // because the storage engine will query from new to old in the order in which
                // the L0 layer files are generated.
                // See https://github.com/risingwave-labs/risingwave/issues/1251
                let kind = command_ctx.kind;
                // hummock_manager commit epoch.
                let mut new_snapshot = None;

                match kind {
                    BarrierKind::Unspecified => unreachable!(),
                    BarrierKind::Initial => assert!(
                        commit_info.sstables.is_empty(),
                        "no sstables should be produced in the first epoch"
                    ),
                    BarrierKind::Checkpoint => {
                        new_snapshot = self
                            .hummock_manager
                            .commit_epoch(command_ctx.prev_epoch.value().0, commit_info)
                            .await?;
                    }
                    BarrierKind::Barrier => {
                        new_snapshot = Some(self.hummock_manager.update_current_epoch(prev_epoch));
                        // if we collect a barrier(checkpoint = false),
                        // we need to ensure that command is Plain and the notifier's checkpoint is
                        // false
                        assert!(!command_ctx.command.need_checkpoint());
                    }
                }

                command_ctx.post_collect().await?;
                // Notify new snapshot after fragment_mapping changes have been notified in
                // `post_collect`.
                if let Some(snapshot) = new_snapshot {
                    self.env
                        .notification_manager()
                        .notify_frontend_without_version(
                            Operation::Update, // Frontends don't care about operation.
                            Info::HummockSnapshot(snapshot),
                        );
                }
                Ok(())
            }
        }
    }

    async fn update_tracking_jobs(
        &self,
        notifiers: Vec<Notifier>,
        command_ctx: Arc<CommandContext>,
        create_mview_progress: Vec<CreateMviewProgress>,
    ) -> MetaResult<CompleteBarrierOutput> {
        {
            {
                // Notify about collected.
                let version_stats = self.hummock_manager.get_version_stats().await;

                let mut tracker = self.tracker.lock().await;

                // Save `cancelled_command` for Create MVs.
                let actors_to_cancel = command_ctx.actors_to_cancel();
                let cancelled_job = if !actors_to_cancel.is_empty() {
                    tracker.find_cancelled_command(actors_to_cancel)
                } else {
                    None
                };

                // Save `finished_commands` for Create MVs.
                let finished_commands = {
                    let mut commands = vec![];
                    // Add the command to tracker.
                    if let Some(command) = tracker.add(
                        TrackingCommand {
                            context: command_ctx.clone(),
                            notifiers,
                        },
                        &version_stats,
                    ) {
                        // Those with no actors to track can be finished immediately.
                        commands.push(command);
                    }
                    // Update the progress of all commands.
                    for progress in create_mview_progress {
                        // Those with actors complete can be finished immediately.
                        if let Some(command) = tracker.update(&progress, &version_stats) {
                            tracing::trace!(?progress, "finish progress");
                            commands.push(command);
                        } else {
                            tracing::trace!(?progress, "update progress");
                        }
                    }
                    commands
                };

                for command in finished_commands {
                    tracker.stash_command_to_finish(command);
                }

                if let Some(table_id) = command_ctx.table_to_cancel() {
                    // the cancelled command is possibly stashed in `finished_commands` and waiting
                    // for checkpoint, we should also clear it.
                    tracker.cancel_stashed_command(table_id);
                }

                let has_remaining_job = tracker
                    .finish_jobs(command_ctx.kind.is_checkpoint())
                    .await?;

                Ok(CompleteBarrierOutput {
                    cancelled_job,
                    has_remaining_job,
                })
            }
        }
    }

    fn report_complete_event(&self, duration_sec: f64, command_ctx: &CommandContext) {
        {
            {
                {
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
        }
    }
}

impl CheckpointControl {
    pub(super) fn next_completed_barrier(
        &mut self,
    ) -> impl Future<Output = MetaResult<(Arc<CommandContext>, bool)>> + '_ {
        poll_fn(|cx| {
            let completing_command = match &mut self.completing_command {
                Some(command) => command,
                None => {
                    // If there is no completing barrier, try to start completing the earliest barrier if
                    // it has been collected.
                    if let Some(inflight_command) = self.inflight_command_ctx_queue.front_mut() {
                        // If the earliest barrier has been collected (i.e. return Poll::Ready), start completing
                        // the barrier is the collection is ok. When it is still pending, the current poll cannot
                        // make any progress and will return Poll::Pending.
                        let BarrierCollectResult { prev_epoch, result } =
                            ready!(inflight_command.barrier_collect_future.poll_unpin(cx));
                        let resps = match result {
                            Ok(resps) => resps,
                            Err(err) => {
                                // FIXME: If it is a connector source error occurred in the init barrier, we should pass
                                // back to frontend
                                fail_point!("inject_barrier_err_success");
                                warn!(%prev_epoch, error = %err.as_report(), "Failed to collect epoch");
                                return Poll::Ready(Err(err));
                            }
                        };
                        // We only pop the command on successful collect. On error, no need to pop the node out,
                        // because the error will trigger an error immediately after return, which clears the failed node.
                        let earliest_inflight_command =
                            self.inflight_command_ctx_queue.pop_front().unwrap();
                        let join_handle = tokio::spawn(self.context.clone().complete_barrier(
                            earliest_inflight_command.command_ctx.clone(),
                            resps,
                            earliest_inflight_command.notifiers,
                            earliest_inflight_command.enqueue_time,
                        ));
                        self.completing_command.insert(CompletingCommand {
                            command_ctx: earliest_inflight_command.command_ctx,
                            join_handle,
                        })
                    } else {
                        return Poll::Pending;
                    }
                }
            };
            let join_result = ready!(completing_command.join_handle.poll_unpin(cx))
                .map_err(|e| {
                    anyhow!("failed to join completing command: {:?}", e.as_report()).into()
                })
                .and_then(|result| result);
            let completed_command = self.completing_command.take().expect("non-empty");
            let result = join_result.map(|output| {
                let command_ctx = completed_command.command_ctx.clone();
                if let Some(job) = output.cancelled_job {
                    self.cancel_command(job);
                }
                (command_ctx, output.has_remaining_job)
            });

            Poll::Ready(result)
        })
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
            BarrierManagerStatus::Running => Ok(()),
        }
    }

    /// Set barrier manager status.
    fn set_status(&self, new_status: BarrierManagerStatus) {
        self.status.store(Arc::new(new_status));
    }

    /// Resolve actor information from cluster, fragment manager and `ChangedTableId`.
    /// We use `changed_table_id` to modify the actors to be sent or collected. Because these actor
    /// will create or drop before this barrier flow through them.
    async fn resolve_actor_info(
        &self,
        all_nodes: Vec<WorkerNode>,
    ) -> MetaResult<InflightActorInfo> {
        let info = match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let all_actor_infos = mgr.fragment_manager.load_all_actors().await;

                InflightActorInfo::resolve(all_nodes, all_actor_infos)
            }
            MetadataManager::V2(mgr) => {
                let all_actor_infos = mgr.catalog_controller.load_all_actors().await?;

                InflightActorInfo::resolve(all_nodes, all_actor_infos)
            }
        };

        Ok(info)
    }

    pub async fn get_ddl_progress(&self) -> Vec<DdlProgress> {
        let mut ddl_progress = self.tracker.lock().await.gen_ddl_progress();
        // If not in tracker, means the first barrier not collected yet.
        // In that case just return progress 0.
        match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                for table in mgr.catalog_manager.list_persisted_creating_tables().await {
                    if table.table_type != TableType::MaterializedView as i32 {
                        continue;
                    }
                    if let Entry::Vacant(e) = ddl_progress.entry(table.id) {
                        e.insert(DdlProgress {
                            id: table.id as u64,
                            statement: table.definition,
                            progress: "0.0%".into(),
                        });
                    }
                }
            }
            MetadataManager::V2(mgr) => {
                let mviews = mgr
                    .catalog_controller
                    .list_background_creating_mviews()
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
            }
        }

        ddl_progress.into_values().collect()
    }
}

pub type BarrierManagerRef = GlobalBarrierManagerContext;

fn collect_commit_epoch_info(
    resps: Vec<BarrierCompleteResponse>,
) -> (CommitEpochInfo, Vec<CreateMviewProgress>) {
    let mut sst_to_worker: HashMap<HummockSstableObjectId, WorkerId> = HashMap::new();
    let mut synced_ssts: Vec<ExtendedSstableInfo> = vec![];
    let mut table_watermarks = Vec::with_capacity(resps.len());
    let mut progresses = Vec::new();
    for resp in resps {
        let ssts_iter = resp.synced_sstables.into_iter().map(|grouped| {
            let sst_info = grouped.sst.expect("field not None");
            sst_to_worker.insert(sst_info.get_object_id(), resp.worker_id);
            ExtendedSstableInfo::new(
                grouped.compaction_group_id,
                sst_info,
                grouped.table_stats_map,
            )
        });
        synced_ssts.extend(ssts_iter);
        table_watermarks.push(resp.table_watermarks);
        progresses.extend(resp.create_mview_progress);
    }
    let info = CommitEpochInfo::new(
        synced_ssts,
        merge_multiple_new_table_watermarks(
            table_watermarks
                .into_iter()
                .map(|watermarks| {
                    watermarks
                        .into_iter()
                        .map(|(table_id, watermarks)| {
                            (
                                TableId::new(table_id),
                                TableWatermarks::from_protobuf(&watermarks),
                            )
                        })
                        .collect()
                })
                .collect_vec(),
        ),
        sst_to_worker,
    );
    (info, progresses)
}
