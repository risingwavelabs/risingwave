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
use std::mem::take;
use std::sync::Arc;
use std::time::Duration;

use fail::fail_point;
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
use risingwave_pb::ddl_service::DdlProgress;
use risingwave_pb::meta::subscribe_response::{Info, Operation};
use risingwave_pb::meta::PausedReason;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use thiserror_ext::AsReport;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::Instrument;

use self::command::CommandContext;
use self::notifier::Notifier;
use self::progress::TrackingCommand;
use crate::barrier::info::InflightActorInfo;
use crate::barrier::notifier::BarrierInfo;
use crate::barrier::progress::{CreateMviewProgressTracker, TrackingJob};
use crate::barrier::rpc::BarrierRpcManager;
use crate::barrier::state::BarrierManagerState;
use crate::barrier::BarrierEpochState::{Completed, InFlight};
use crate::hummock::{CommitEpochInfo, HummockManagerRef};
use crate::manager::sink_coordination::SinkCoordinatorManager;
use crate::manager::{LocalNotification, MetaSrvEnv, MetadataManager, WorkerId};
use crate::model::{ActorId, TableFragments};
use crate::rpc::metrics::MetaMetrics;
use crate::stream::{ScaleController, ScaleControllerRef, SourceManagerRef};
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
    status: Arc<Mutex<BarrierManagerStatus>>,

    tracker: Arc<Mutex<CreateMviewProgressTracker>>,

    metadata_manager: MetadataManager,

    hummock_manager: HummockManagerRef,

    source_manager: SourceManagerRef,

    scale_controller: ScaleControllerRef,

    sink_manager: SinkCoordinatorManager,

    metrics: Arc<MetaMetrics>,

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

    rpc_manager: BarrierRpcManager,
}

/// Controls the concurrent execution of commands.
struct CheckpointControl {
    /// Save the state and message of barrier in order.
    command_ctx_queue: VecDeque<EpochNode>,

    metrics: Arc<MetaMetrics>,

    /// Get notified when we finished Create MV and collect a barrier(checkpoint = true)
    finished_jobs: Vec<TrackingJob>,
}

impl CheckpointControl {
    fn new(metrics: Arc<MetaMetrics>) -> Self {
        Self {
            command_ctx_queue: Default::default(),
            metrics,
            finished_jobs: Default::default(),
        }
    }

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

    fn cancel_command(&mut self, cancelled_job: TrackingJob) {
        if let TrackingJob::New(cancelled_command) = cancelled_job {
            if let Some(index) = self.command_ctx_queue.iter().position(|x| {
                x.command_ctx.prev_epoch.value() == cancelled_command.context.prev_epoch.value()
            }) {
                self.command_ctx_queue.remove(index);
            }
        } else {
            // Recovered jobs do not need to be cancelled since only `RUNNING` actors will get recovered.
        }
    }

    fn cancel_stashed_command(&mut self, id: TableId) {
        self.finished_jobs
            .retain(|x| x.table_to_create() != Some(id));
    }

    /// Update the metrics of barrier nums.
    fn update_barrier_nums_metrics(&self) {
        self.metrics.in_flight_barrier_nums.set(
            self.command_ctx_queue
                .iter()
                .filter(|x| matches!(x.state, InFlight))
                .count() as i64,
        );
        self.metrics
            .all_barrier_nums
            .set(self.command_ctx_queue.len() as i64);
    }

    /// Enqueue a barrier command, and init its state to `InFlight`.
    fn enqueue_command(&mut self, command_ctx: Arc<CommandContext>, notifiers: Vec<Notifier>) {
        let timer = self.metrics.barrier_latency.start_timer();

        self.command_ctx_queue.push_back(EpochNode {
            timer: Some(timer),
            wait_commit_timer: None,

            state: InFlight,
            command_ctx,
            notifiers,
        });
    }

    /// Change the state of this `prev_epoch` to `Completed`. Return continuous nodes
    /// with `Completed` starting from first node [`Completed`..`InFlight`) and remove them.
    fn barrier_completed(
        &mut self,
        prev_epoch: u64,
        result: Vec<BarrierCompleteResponse>,
    ) -> Vec<EpochNode> {
        // change state to complete, and wait for nodes with the smaller epoch to commit
        let wait_commit_timer = self.metrics.barrier_wait_commit_latency.start_timer();
        if let Some(node) = self
            .command_ctx_queue
            .iter_mut()
            .find(|x| x.command_ctx.prev_epoch.value().0 == prev_epoch)
        {
            assert!(matches!(node.state, InFlight));
            node.wait_commit_timer = Some(wait_commit_timer);
            node.state = Completed(result);
        };
        // Find all continuous nodes with 'Complete' starting from first node
        let index = self
            .command_ctx_queue
            .iter()
            .position(|x| !matches!(x.state, Completed(_)))
            .unwrap_or(self.command_ctx_queue.len());
        let complete_nodes = self.command_ctx_queue.drain(..index).collect_vec();
        complete_nodes
    }

    /// Remove all nodes from queue and return them.
    fn barrier_failed(&mut self) -> Vec<EpochNode> {
        self.command_ctx_queue.drain(..).collect_vec()
    }

    /// Pause inject barrier until True.
    fn can_inject_barrier(&self, in_flight_barrier_nums: usize) -> bool {
        let in_flight_not_full = self
            .command_ctx_queue
            .iter()
            .filter(|x| matches!(x.state, InFlight))
            .count()
            < in_flight_barrier_nums;

        // Whether some command requires pausing concurrent barrier. If so, it must be the last one.
        let should_pause = self
            .command_ctx_queue
            .back()
            .map(|x| x.command_ctx.command.should_pause_inject_barrier())
            .unwrap_or(false);
        debug_assert_eq!(
            self.command_ctx_queue
                .iter()
                .any(|x| x.command_ctx.command.should_pause_inject_barrier()),
            should_pause
        );

        in_flight_not_full && !should_pause
    }

    /// Check whether the target epoch is managed by `CheckpointControl`.
    pub fn contains_epoch(&self, epoch: u64) -> bool {
        self.command_ctx_queue
            .iter()
            .any(|x| x.command_ctx.prev_epoch.value().0 == epoch)
    }

    /// We need to make sure there are no changes when doing recovery
    pub fn clear_changes(&mut self) {
        self.finished_jobs.clear();
    }
}

/// The state and message of this barrier, a node for concurrent checkpoint.
pub struct EpochNode {
    /// Timer for recording barrier latency, taken after `complete_barriers`.
    timer: Option<HistogramTimer>,
    /// The timer of `barrier_wait_commit_latency`
    wait_commit_timer: Option<HistogramTimer>,

    /// Whether this barrier is in-flight or completed.
    state: BarrierEpochState,
    /// Context of this command to generate barrier and do some post jobs.
    command_ctx: Arc<CommandContext>,
    /// Notifiers of this barrier.
    notifiers: Vec<Notifier>,
}

/// The state of barrier.
enum BarrierEpochState {
    /// This barrier is current in-flight on the stream graph of compute nodes.
    InFlight,

    /// This barrier is completed or failed.
    Completed(Vec<BarrierCompleteResponse>),
}

/// The result of barrier completion.
#[derive(Debug)]
struct BarrierCompletion {
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
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;
        let in_flight_barrier_nums = env.opts.in_flight_barrier_nums;

        let initial_invalid_state = BarrierManagerState::new(
            TracedEpoch::new(Epoch(INVALID_EPOCH)),
            InflightActorInfo::default(),
            None,
        );
        let checkpoint_control = CheckpointControl::new(metrics.clone());

        let tracker = CreateMviewProgressTracker::new();

        let scale_controller = Arc::new(ScaleController::new(
            &metadata_manager,
            source_manager.clone(),
            env.clone(),
        ));

        let context = GlobalBarrierManagerContext {
            status: Arc::new(Mutex::new(BarrierManagerStatus::Starting)),
            metadata_manager,
            hummock_manager,
            source_manager,
            scale_controller,
            sink_manager,
            metrics,
            tracker: Arc::new(Mutex::new(tracker)),
            env: env.clone(),
        };

        let rpc_manager = BarrierRpcManager::new(context.clone());

        Self {
            enable_recovery,
            scheduled_barriers,
            in_flight_barrier_nums,
            context,
            env,
            state: initial_invalid_state,
            checkpoint_control,
            rpc_manager,
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
            tracing::warn!(
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

        self.state = {
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
                .set_status(BarrierManagerStatus::Recovering(RecoveryReason::Bootstrap))
                .await;
            let span = tracing::info_span!("bootstrap_recovery", prev_epoch = prev_epoch.value().0);

            let paused = self.take_pause_on_bootstrap().await.unwrap_or(false);
            let paused_reason = paused.then_some(PausedReason::Manual);

            self.context
                .recovery(prev_epoch, paused_reason, &self.scheduled_barriers)
                .instrument(span)
                .await
        };

        self.context.set_status(BarrierManagerStatus::Running).await;

        let mut min_interval = tokio::time::interval(interval);
        min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
                // Checkpoint frequency changes.
                notification = local_notification_rx.recv() => {
                    let notification = notification.unwrap();
                    // Handle barrier interval and checkpoint frequency changes
                    if let LocalNotification::SystemParamsChange(p) = &notification {
                        let new_interval = Duration::from_millis(p.barrier_interval_ms() as u64);
                        if new_interval != min_interval.period() {
                            min_interval = tokio::time::interval(new_interval);
                            min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                        }
                        self.scheduled_barriers
                            .set_checkpoint_frequency(p.checkpoint_frequency() as usize)
                    }
                }
                // Barrier completes.
                completion = self.rpc_manager.next_complete_barrier() => {
                    self.handle_barrier_complete(
                        completion,
                    )
                    .await;
                }

                // There's barrier scheduled.
                _ = self.scheduled_barriers.wait_one(), if self.checkpoint_control.can_inject_barrier(self.in_flight_barrier_nums) => {
                    min_interval.reset(); // Reset the interval as we have a new barrier.
                    self.handle_new_barrier().await;
                }
                // Minimum interval reached.
                _ = min_interval.tick(), if self.checkpoint_control.can_inject_barrier(self.in_flight_barrier_nums) => {
                    self.handle_new_barrier().await;
                }
            }
            self.checkpoint_control.update_barrier_nums_metrics();
        }
    }

    /// Handle the new barrier from the scheduled queue and inject it.
    async fn handle_new_barrier(&mut self) {
        assert!(self
            .checkpoint_control
            .can_inject_barrier(self.in_flight_barrier_nums));

        let Scheduled {
            command,
            mut notifiers,
            send_latency_timer,
            checkpoint,
            span,
        } = self.scheduled_barriers.pop_or_default().await;

        let all_nodes = self
            .context
            .metadata_manager
            .list_active_streaming_compute_nodes()
            .await
            .unwrap();
        self.state.resolve_worker_nodes(all_nodes);
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
            span.clone(),
        ));

        send_latency_timer.observe_duration();

        self.rpc_manager
            .inject_barrier(command_ctx.clone())
            .instrument(span)
            .await;

        // Notify about the injection.
        let prev_paused_reason = self.state.paused_reason();
        let curr_paused_reason = command_ctx.next_paused_reason();

        let info = BarrierInfo {
            prev_epoch: prev_epoch.value(),
            curr_epoch: curr_epoch.value(),
            prev_paused_reason,
            curr_paused_reason,
        };
        notifiers.iter_mut().for_each(|n| n.notify_injected(info));

        // Update the paused state after the barrier is injected.
        self.state.set_paused_reason(curr_paused_reason);
        // Record the in-flight barrier.
        self.checkpoint_control
            .enqueue_command(command_ctx.clone(), notifiers);
    }

    /// Changes the state to `Complete`, and try to commit all epoch that state is `Complete` in
    /// order. If commit is err, all nodes will be handled.
    async fn handle_barrier_complete(&mut self, completion: BarrierCompletion) {
        let BarrierCompletion { prev_epoch, result } = completion;

        assert!(
            self.checkpoint_control.contains_epoch(prev_epoch),
            "received barrier complete response for an unknown epoch: {}",
            prev_epoch
        );

        if let Err(err) = result {
            // FIXME: If it is a connector source error occurred in the init barrier, we should pass
            // back to frontend
            fail_point!("inject_barrier_err_success");
            let fail_node = self.checkpoint_control.barrier_failed();
            tracing::warn!(%prev_epoch, error = %err.as_report(), "Failed to complete epoch");
            self.failure_recovery(err, fail_node).await;
            return;
        }
        // change the state to Complete
        let mut complete_nodes = self
            .checkpoint_control
            .barrier_completed(prev_epoch, result.unwrap());
        // try commit complete nodes
        let (mut index, mut err_msg) = (0, None);
        for (i, node) in complete_nodes.iter_mut().enumerate() {
            assert!(matches!(node.state, Completed(_)));
            let span = node.command_ctx.span.clone();
            if let Err(err) = self.complete_barrier(node).instrument(span).await {
                index = i;
                err_msg = Some(err);
                break;
            }
        }
        // Handle the error node and the nodes after it
        if let Some(err) = err_msg {
            let fail_nodes = complete_nodes
                .drain(index..)
                .chain(self.checkpoint_control.barrier_failed().into_iter())
                .collect_vec();
            tracing::warn!(%prev_epoch, error = %err.as_report(), "Failed to commit epoch");
            self.failure_recovery(err, fail_nodes).await;
        }
    }

    async fn failure_recovery(
        &mut self,
        err: MetaError,
        fail_nodes: impl IntoIterator<Item = EpochNode>,
    ) {
        self.checkpoint_control.clear_changes();
        self.rpc_manager.clear();

        for node in fail_nodes {
            if let Some(timer) = node.timer {
                timer.observe_duration();
            }
            if let Some(wait_commit_timer) = node.wait_commit_timer {
                wait_commit_timer.observe_duration();
            }
            node.notifiers
                .into_iter()
                .for_each(|notifier| notifier.notify_collection_failed(err.clone()));
        }

        if self.enable_recovery {
            self.context
                .set_status(BarrierManagerStatus::Recovering(RecoveryReason::Failover(
                    err.clone(),
                )))
                .await;
            let latest_snapshot = self.context.hummock_manager.latest_snapshot();
            let prev_epoch = TracedEpoch::new(latest_snapshot.committed_epoch.into()); // we can only recovery from the committed epoch
            let span = tracing::info_span!(
                "failure_recovery",
                error = %err.as_report(),
                prev_epoch = prev_epoch.value().0
            );

            // No need to clean dirty tables for barrier recovery,
            // The foreground stream job should cleanup their own tables.
            self.state = self
                .context
                .recovery(prev_epoch, None, &self.scheduled_barriers)
                .instrument(span)
                .await;
            self.context.set_status(BarrierManagerStatus::Running).await;
        } else {
            panic!("failed to execute barrier: {}", err.as_report());
        }
    }

    /// Try to commit this node. If err, returns
    async fn complete_barrier(&mut self, node: &mut EpochNode) -> MetaResult<()> {
        let prev_epoch = node.command_ctx.prev_epoch.value().0;
        match &mut node.state {
            Completed(resps) => {
                // We must ensure all epochs are committed in ascending order,
                // because the storage engine will query from new to old in the order in which
                // the L0 layer files are generated.
                // See https://github.com/risingwave-labs/risingwave/issues/1251
                let kind = node.command_ctx.kind;
                let commit_info = collect_commit_epoch_info(resps);
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
                            .context
                            .hummock_manager
                            .commit_epoch(node.command_ctx.prev_epoch.value().0, commit_info)
                            .await?;
                    }
                    BarrierKind::Barrier => {
                        new_snapshot = Some(
                            self.context
                                .hummock_manager
                                .update_current_epoch(prev_epoch),
                        );
                        // if we collect a barrier(checkpoint = false),
                        // we need to ensure that command is Plain and the notifier's checkpoint is
                        // false
                        assert!(!node.command_ctx.command.need_checkpoint());
                    }
                }

                node.command_ctx.post_collect().await?;
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

                // Notify about collected.
                let mut notifiers = take(&mut node.notifiers);
                notifiers.iter_mut().for_each(|notifier| {
                    notifier.notify_collected();
                });

                // Save `cancelled_command` for Create MVs.
                let actors_to_cancel = node.command_ctx.actors_to_cancel();
                let cancelled_command = if !actors_to_cancel.is_empty() {
                    let mut tracker = self.context.tracker.lock().await;
                    tracker.find_cancelled_command(actors_to_cancel)
                } else {
                    None
                };

                // Save `finished_commands` for Create MVs.
                let finished_commands = {
                    let mut commands = vec![];
                    let version_stats = self.context.hummock_manager.get_version_stats().await;
                    let mut tracker = self.context.tracker.lock().await;
                    // Add the command to tracker.
                    if let Some(command) = tracker.add(
                        TrackingCommand {
                            context: node.command_ctx.clone(),
                            notifiers,
                        },
                        &version_stats,
                    ) {
                        // Those with no actors to track can be finished immediately.
                        commands.push(command);
                    }
                    // Update the progress of all commands.
                    for progress in resps.iter().flat_map(|r| &r.create_mview_progress) {
                        // Those with actors complete can be finished immediately.
                        if let Some(command) = tracker.update(progress, &version_stats) {
                            tracing::trace!(?progress, "finish progress");
                            commands.push(command);
                        } else {
                            tracing::trace!(?progress, "update progress");
                        }
                    }
                    commands
                };

                for command in finished_commands {
                    self.checkpoint_control.stash_command_to_finish(command);
                }

                if let Some(command) = cancelled_command {
                    self.checkpoint_control.cancel_command(command);
                } else if let Some(table_id) = node.command_ctx.table_to_cancel() {
                    // the cancelled command is possibly stashed in `finished_commands` and waiting
                    // for checkpoint, we should also clear it.
                    self.checkpoint_control.cancel_stashed_command(table_id);
                }

                let remaining = self
                    .checkpoint_control
                    .finish_jobs(kind.is_checkpoint())
                    .await?;
                // If there are remaining commands (that requires checkpoint to finish), we force
                // the next barrier to be a checkpoint.
                if remaining {
                    assert_matches!(kind, BarrierKind::Barrier);
                    self.scheduled_barriers.force_checkpoint_in_next_barrier();
                }

                let duration_sec = node.timer.take().unwrap().stop_and_record();
                node.wait_commit_timer.take().unwrap().observe_duration();

                {
                    // Record barrier latency in event log.
                    use risingwave_pb::meta::event_log;
                    let event = event_log::EventBarrierComplete {
                        prev_epoch: node.command_ctx.prev_epoch.value().0,
                        cur_epoch: node.command_ctx.curr_epoch.value().0,
                        duration_sec,
                        command: node.command_ctx.command.to_string(),
                        barrier_kind: node.command_ctx.kind.as_str_name().to_string(),
                    };
                    self.env
                        .event_log_manager_ref()
                        .add_event_logs(vec![event_log::Event::BarrierComplete(event)]);
                }

                Ok(())
            }
            InFlight => unreachable!(),
        }
    }
}

impl GlobalBarrierManagerContext {
    /// Check the status of barrier manager, return error if it is not `Running`.
    pub async fn check_status_running(&self) -> MetaResult<()> {
        let status = self.status.lock().await;
        match &*status {
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
    async fn set_status(&self, new_status: BarrierManagerStatus) {
        let mut status = self.status.lock().await;
        *status = new_status;
    }

    /// Resolve actor information from cluster, fragment manager and `ChangedTableId`.
    /// We use `changed_table_id` to modify the actors to be sent or collected. Because these actor
    /// will create or drop before this barrier flow through them.
    async fn resolve_actor_info(&self) -> InflightActorInfo {
        let info = match &self.metadata_manager {
            MetadataManager::V1(mgr) => {
                let all_nodes = mgr
                    .cluster_manager
                    .list_active_streaming_compute_nodes()
                    .await;
                let all_actor_infos = mgr.fragment_manager.load_all_actors().await;

                InflightActorInfo::resolve(all_nodes, all_actor_infos)
            }
            MetadataManager::V2(mgr) => {
                let all_nodes = mgr
                    .cluster_controller
                    .list_active_streaming_workers()
                    .await
                    .unwrap();
                let pu_mappings = all_nodes
                    .iter()
                    .flat_map(|node| node.parallel_units.iter().map(|pu| (pu.id, pu.clone())))
                    .collect();
                let all_actor_infos = mgr
                    .catalog_controller
                    .load_all_actors(&pu_mappings)
                    .await
                    .unwrap();

                InflightActorInfo::resolve(all_nodes, all_actor_infos)
            }
        };

        info
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

fn collect_commit_epoch_info(resps: &mut [BarrierCompleteResponse]) -> CommitEpochInfo {
    let mut sst_to_worker: HashMap<HummockSstableObjectId, WorkerId> = HashMap::new();
    let mut synced_ssts: Vec<ExtendedSstableInfo> = vec![];
    for resp in &mut *resps {
        let mut t: Vec<ExtendedSstableInfo> = resp
            .synced_sstables
            .iter_mut()
            .map(|grouped| {
                let sst_info = std::mem::take(&mut grouped.sst).expect("field not None");
                sst_to_worker.insert(sst_info.get_object_id(), resp.worker_id);
                ExtendedSstableInfo::new(
                    grouped.compaction_group_id,
                    sst_info,
                    std::mem::take(&mut grouped.table_stats_map),
                )
            })
            .collect_vec();
        synced_ssts.append(&mut t);
    }
    CommitEpochInfo::new(
        synced_ssts,
        merge_multiple_new_table_watermarks(
            resps
                .iter()
                .map(|resp| {
                    resp.table_watermarks
                        .iter()
                        .map(|(table_id, watermarks)| {
                            (
                                TableId::new(*table_id),
                                TableWatermarks::from_protobuf(watermarks),
                            )
                        })
                        .collect()
                })
                .collect_vec(),
        ),
        sst_to_worker,
    )
}
