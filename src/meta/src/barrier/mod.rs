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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::pending;
use std::mem::{replace, take};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use arc_swap::ArcSwap;
use fail::fail_point;
use itertools::Itertools;
use prometheus::HistogramTimer;
use risingwave_common::catalog::TableId;
use risingwave_common::system_param::reader::SystemParamsRead;
use risingwave_common::system_param::PAUSE_ON_NEXT_BOOTSTRAP_KEY;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_common::{bail, must_match};
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
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{error, info, warn, Instrument};

use self::command::CommandContext;
use self::notifier::Notifier;
use self::progress::TrackingCommand;
use crate::barrier::info::InflightActorInfo;
use crate::barrier::notifier::BarrierInfo;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::rpc::BarrierRpcManager;
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

    rpc_manager: BarrierRpcManager,

    active_streaming_nodes: ActiveStreamingWorkerNodes,
}

/// Controls the concurrent execution of commands.
struct CheckpointControl {
    /// Save the state and message of barrier in order.
    /// Key is the prev_epoch.
    command_ctx_queue: BTreeMap<u64, EpochNode>,

    /// Command that has been collected but is still completing.
    /// The join handle of the completing future is stored.
    completing_command: CompletingCommand,

    context: GlobalBarrierManagerContext,
}

impl CheckpointControl {
    fn new(context: GlobalBarrierManagerContext) -> Self {
        Self {
            command_ctx_queue: Default::default(),
            completing_command: CompletingCommand::None,
            context,
        }
    }

    fn total_command_num(&self) -> usize {
        self.command_ctx_queue.len()
            + match &self.completing_command {
                CompletingCommand::Completing { .. } => 1,
                _ => 0,
            }
    }

    /// Update the metrics of barrier nums.
    fn update_barrier_nums_metrics(&self) {
        self.context.metrics.in_flight_barrier_nums.set(
            self.command_ctx_queue
                .values()
                .filter(|x| matches!(x.state, BarrierEpochState::InFlight))
                .count() as i64,
        );
        self.context
            .metrics
            .all_barrier_nums
            .set(self.total_command_num() as i64);
    }

    /// Enqueue a barrier command, and init its state to `InFlight`.
    fn enqueue_command(&mut self, command_ctx: Arc<CommandContext>, notifiers: Vec<Notifier>) {
        let timer = self.context.metrics.barrier_latency.start_timer();

        if let Some((_, node)) = self.command_ctx_queue.last_key_value() {
            assert_eq!(
                command_ctx.prev_epoch.value(),
                node.command_ctx.curr_epoch.value()
            );
        }
        self.command_ctx_queue.insert(
            command_ctx.prev_epoch.value().0,
            EpochNode {
                enqueue_time: timer,
                state: BarrierEpochState::InFlight,
                command_ctx,
                notifiers,
            },
        );
    }

    /// Change the state of this `prev_epoch` to `Completed`. Return continuous nodes
    /// with `Completed` starting from first node [`Completed`..`InFlight`) and remove them.
    fn barrier_collected(&mut self, prev_epoch: u64, result: Vec<BarrierCompleteResponse>) {
        if let Some(node) = self.command_ctx_queue.get_mut(&prev_epoch) {
            assert!(matches!(node.state, BarrierEpochState::InFlight));
            node.state = BarrierEpochState::Collected(result);
        } else {
            panic!(
                "received barrier complete response for an unknown epoch: {}",
                prev_epoch
            );
        }
    }

    /// Pause inject barrier until True.
    fn can_inject_barrier(&self, in_flight_barrier_nums: usize) -> bool {
        let in_flight_not_full = self
            .command_ctx_queue
            .values()
            .filter(|x| matches!(x.state, BarrierEpochState::InFlight))
            .count()
            < in_flight_barrier_nums;

        // Whether some command requires pausing concurrent barrier. If so, it must be the last one.
        let should_pause = self
            .command_ctx_queue
            .last_key_value()
            .map(|(_, x)| &x.command_ctx)
            .or(match &self.completing_command {
                CompletingCommand::None | CompletingCommand::Err(_) => None,
                CompletingCommand::Completing { command_ctx, .. } => Some(command_ctx),
            })
            .map(|command_ctx| command_ctx.command.should_pause_inject_barrier())
            .unwrap_or(false);
        debug_assert_eq!(
            self.command_ctx_queue
                .values()
                .map(|node| &node.command_ctx)
                .chain(
                    match &self.completing_command {
                        CompletingCommand::None | CompletingCommand::Err(_) => None,
                        CompletingCommand::Completing { command_ctx, .. } => Some(command_ctx),
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
        let is_err = match replace(&mut self.completing_command, CompletingCommand::None) {
            CompletingCommand::None => false,
            CompletingCommand::Completing {
                command_ctx,
                join_handle,
            } => {
                info!(
                    prev_epoch = ?command_ctx.prev_epoch,
                    curr_epoch = ?command_ctx.curr_epoch,
                    "waiting for completing command to finish in recovery"
                );
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
            CompletingCommand::Err(_) => true,
        };
        if !is_err {
            // continue to finish the pending collected barrier.
            while let Some((
                _,
                EpochNode {
                    state: BarrierEpochState::Collected(_),
                    ..
                },
            )) = self.command_ctx_queue.first_key_value()
            {
                let (_, node) = self.command_ctx_queue.pop_first().expect("non-empty");
                let command_ctx = node.command_ctx.clone();
                if let Err(e) = self.context.clone().complete_barrier(node).await {
                    error!(
                        prev_epoch = ?command_ctx.prev_epoch,
                        curr_epoch = ?command_ctx.curr_epoch,
                        err = ?e.as_report(),
                        "failed to complete barrier during recovery"
                    );
                    break;
                } else {
                    info!(
                        prev_epoch = ?command_ctx.prev_epoch,
                        curr_epoch = ?command_ctx.curr_epoch,
                        "succeed to complete barrier during recovery"
                    )
                }
            }
        }
        for (_, node) in take(&mut self.command_ctx_queue) {
            for notifier in node.notifiers {
                notifier.notify_failed(err.clone());
            }
            node.enqueue_time.observe_duration();
        }
    }
}

/// The state and message of this barrier, a node for concurrent checkpoint.
pub struct EpochNode {
    /// Timer for recording barrier latency, taken after `complete_barriers`.
    enqueue_time: HistogramTimer,

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

    /// This barrier is collected.
    Collected(Vec<BarrierCompleteResponse>),
}

enum CompletingCommand {
    None,
    Completing {
        command_ctx: Arc<CommandContext>,

        // The join handle of a spawned task that completes the barrier.
        // The return value indicate whether there is some create streaming job command
        // that has finished but not checkpointed. If there is any, we will force checkpoint on the next barrier
        join_handle: JoinHandle<MetaResult<BarrierCompleteOutput>>,
    },
    Err(MetaError),
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
            active_streaming_nodes,
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
                // Barrier completes.
                collect_result = self.rpc_manager.next_collected_barrier() => {
                    match collect_result.result {
                        Ok(resps) => {
                            self.checkpoint_control.barrier_collected(collect_result.prev_epoch, resps);
                        },
                        Err(e) => {
                            fail_point!("inject_barrier_err_success");
                            self.failure_recovery(e).await;
                        }
                    }
                }
                complete_result = self.checkpoint_control.next_completed_barrier() => {
                    match complete_result {
                        Ok(output) => {
                            // If there are remaining commands (that requires checkpoint to finish), we force
                            // the next barrier to be a checkpoint.
                            if output.require_next_checkpoint {
                                assert_matches!(output.command_ctx.kind, BarrierKind::Barrier);
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
        prev_epoch.span().in_scope(|| {
            tracing::info!(target: "rw_tracing", epoch = curr_epoch.value().0, "new barrier enqueued");
        });
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

        self.rpc_manager.inject_barrier(command_ctx.clone());

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
        self.checkpoint_control
            .enqueue_command(command_ctx.clone(), notifiers);
    }

    async fn failure_recovery(&mut self, err: MetaError) {
        self.context.tracker.lock().await.abort_all(&err);
        self.rpc_manager.clear();
        self.checkpoint_control.clear_on_err(&err).await;

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
    async fn complete_barrier(self, node: EpochNode) -> MetaResult<BarrierCompleteOutput> {
        let EpochNode {
            command_ctx,
            mut notifiers,
            enqueue_time,
            state,
            ..
        } = node;
        let resps = must_match!(state, BarrierEpochState::Collected(resps) => resps);
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
        let has_remaining = self
            .update_tracking_jobs(notifiers, command_ctx.clone(), create_mview_progress)
            .await?;
        let duration_sec = enqueue_time.stop_and_record();
        self.report_complete_event(duration_sec, &command_ctx);
        wait_commit_timer.observe_duration();
        Ok(BarrierCompleteOutput {
            command_ctx,
            require_next_checkpoint: has_remaining,
        })
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
    ) -> MetaResult<bool> {
        {
            {
                // Notify about collected.
                let version_stats = self.hummock_manager.get_version_stats().await;
                let mut tracker = self.tracker.lock().await;

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
                    tracker.cancel_command(table_id);
                }

                let has_remaining_job = tracker
                    .finish_jobs(command_ctx.kind.is_checkpoint())
                    .await?;

                Ok(has_remaining_job)
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

struct BarrierCompleteOutput {
    command_ctx: Arc<CommandContext>,
    require_next_checkpoint: bool,
}

impl CheckpointControl {
    pub(super) async fn next_completed_barrier(&mut self) -> MetaResult<BarrierCompleteOutput> {
        if matches!(&self.completing_command, CompletingCommand::None) {
            // If there is no completing barrier, try to start completing the earliest barrier if
            // it has been collected.
            if let Some((
                _,
                EpochNode {
                    state: BarrierEpochState::Collected(_),
                    ..
                },
            )) = self.command_ctx_queue.first_key_value()
            {
                let (_, node) = self.command_ctx_queue.pop_first().expect("non-empty");
                let command_ctx = node.command_ctx.clone();
                let join_handle = tokio::spawn(self.context.clone().complete_barrier(node));
                self.completing_command = CompletingCommand::Completing {
                    command_ctx,
                    join_handle,
                };
            }
        }

        if let CompletingCommand::Completing { join_handle, .. } = &mut self.completing_command {
            let join_result = join_handle
                .await
                .map_err(|e| {
                    anyhow!("failed to join completing command: {:?}", e.as_report()).into()
                })
                .and_then(|result| result);
            // It's important to reset the completing_command after await no matter the result is err
            // or not, and otherwise the join handle will be polled again after ready.
            if let Err(e) = &join_result {
                self.completing_command = CompletingCommand::Err(e.clone());
            } else {
                self.completing_command = CompletingCommand::None;
            }
            join_result
        } else {
            pending().await
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
