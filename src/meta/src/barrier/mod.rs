// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::{HashMap, HashSet, VecDeque};
use std::mem::take;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use fail::fail_point;
use futures::future::try_join_all;
use itertools::Itertools;
use prometheus::HistogramTimer;
use risingwave_common::bail;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::{Epoch, INVALID_EPOCH};
use risingwave_hummock_sdk::{HummockSstableId, LocalSstableInfo};
use risingwave_pb::common::worker_node::State::Running;
use risingwave_pb::common::WorkerType;
use risingwave_pb::meta::table_fragments::ActorState;
use risingwave_pb::stream_plan::Barrier;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_pb::stream_service::{
    BarrierCompleteRequest, BarrierCompleteResponse, InjectBarrierRequest,
};
use risingwave_rpc_client::StreamClientPoolRef;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::task::JoinHandle;
use uuid::Uuid;

use self::command::CommandContext;
use self::info::BarrierActorInfo;
use self::notifier::Notifier;
use crate::barrier::progress::CreateMviewProgressTracker;
use crate::barrier::BarrierEpochState::{Completed, InFlight};
use crate::hummock::HummockManagerRef;
use crate::manager::{
    CatalogManagerRef, ClusterManagerRef, FragmentManagerRef, MetaSrvEnv, WorkerId,
};
use crate::model::{ActorId, BarrierManagerState};
use crate::rpc::metrics::MetaMetrics;
use crate::storage::meta_store::MetaStore;
use crate::{MetaError, MetaResult};

mod command;
mod info;
mod notifier;
mod progress;
mod recovery;
mod schedule;

pub use self::command::{Command, Reschedule};
pub use self::schedule::BarrierScheduler;

/// Scheduled command with its notifiers.
type Scheduled = (Command, Vec<Notifier>);

/// Post-processing information for barriers.
type CheckpointPost<S> = (
    Arc<CommandContext<S>>,
    Vec<Notifier>,
    Vec<CreateMviewProgress>,
);

/// Changes to the actors to be sent or collected after this command is committed.
///
/// Since the checkpoints might be concurrent, the meta store of table fragments is only updated
/// after the command is committed. When resolving the actor info for those commands after this
/// command, this command might be in-flight and the changes are not yet committed, so we need to
/// record these uncommitted changes and assume they will be eventually successful.
///
/// See also [`CheckpointControl::can_actor_send_or_collect`].
#[derive(Debug, Clone)]
pub enum CommandChanges {
    /// This table will be dropped.
    DropTable(TableId),
    /// This table will be created.
    CreateTable(TableId),
    /// Some actors will be added or removed.
    Actor {
        to_add: HashSet<ActorId>,
        to_remove: HashSet<ActorId>,
    },
    /// No changes.
    None,
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
pub struct GlobalBarrierManager<S: MetaStore> {
    /// The maximal interval for sending a barrier.
    interval: Duration,

    /// Enable recovery or not when failover.
    enable_recovery: bool,

    /// The queue of scheduled barriers.
    scheduled_barriers: schedule::ScheduledBarriers,

    /// The max barrier nums in flight
    in_flight_barrier_nums: usize,

    cluster_manager: ClusterManagerRef<S>,

    pub(crate) catalog_manager: CatalogManagerRef<S>,

    fragment_manager: FragmentManagerRef<S>,

    hummock_manager: HummockManagerRef<S>,

    metrics: Arc<MetaMetrics>,

    pub(crate) env: MetaSrvEnv<S>,
}

/// Post-processing information for barriers and previously uncommitted ssts
struct UncommittedMessages<S: MetaStore> {
    uncommitted_checkpoint_post: VecDeque<CheckpointPost<S>>,
    /// Ssts that need to commit with next checkpoint. We need to save data in reverse order of
    /// epoch, because we will save `uncommitted_ssts` in version, and traverse them in the
    /// forward direction and return the key when we find it
    uncommitted_ssts: VecDeque<LocalSstableInfo>,
    /// Work_ids that need to commit with next checkpoint.
    uncommitted_work_ids: HashMap<HummockSstableId, WorkerId>,
}

impl<S> Default for UncommittedMessages<S>
where
    S: MetaStore,
{
    fn default() -> Self {
        Self {
            uncommitted_checkpoint_post: Default::default(),
            uncommitted_ssts: Default::default(),
            uncommitted_work_ids: Default::default(),
        }
    }
}

/// Controls the concurrent execution of commands.
struct CheckpointControl<S: MetaStore> {
    /// Save the state and message of barrier in order.
    command_ctx_queue: VecDeque<EpochNode<S>>,

    // Below for uncommitted changes for the inflight barriers.
    /// In addition to the actors with status `Running`. The barrier needs to send or collect the
    /// actors of these tables.
    creating_tables: HashSet<TableId>,
    /// The barrier does not send or collect the actors of these tables, even if they are
    /// `Running`.
    dropping_tables: HashSet<TableId>,
    /// In addition to the actors with status `Running`. The barrier needs to send or collect these
    /// actors.
    adding_actors: HashSet<ActorId>,
    /// The barrier does not send or collect these actors, even if they are `Running`.
    removing_actors: HashSet<ActorId>,

    metrics: Arc<MetaMetrics>,

    /// Messages that needs to be completed or processed with checkpoints
    uncommitted_messages: UncommittedMessages<S>,
}

impl<S> CheckpointControl<S>
where
    S: MetaStore,
{
    fn new(metrics: Arc<MetaMetrics>) -> Self {
        Self {
            command_ctx_queue: Default::default(),
            creating_tables: Default::default(),
            dropping_tables: Default::default(),
            adding_actors: Default::default(),
            removing_actors: Default::default(),
            metrics,
            uncommitted_messages: Default::default(),
        }
    }

    fn add_uncommitted_messages(
        &mut self,
        resps: &Vec<BarrierCompleteResponse>,
        checkpoint_post: CheckpointPost<S>,
    ) {
        for resp in resps {
            resp.synced_sstables.iter().cloned().for_each(|grouped| {
                let sst = grouped.sst.expect("field not None");
                self.uncommitted_messages
                    .uncommitted_work_ids
                    .insert(sst.id, resp.worker_id);
                self.uncommitted_messages
                    .uncommitted_ssts
                    .push_front((grouped.compaction_group_id, sst));
            });
        }
        self.uncommitted_messages
            .uncommitted_checkpoint_post
            .push_front(checkpoint_post);
    }

    fn get_uncommitted_states(&mut self) -> UncommittedMessages<S> {
        take(&mut self.uncommitted_messages)
    }

    /// Before resolving the actors to be sent or collected, we should first record the newly
    /// created table and added actors into checkpoint control, so that `can_actor_send_or_collect`
    /// will return `true`.
    fn pre_resolve(&mut self, command: &Command) {
        match command.changes() {
            CommandChanges::CreateTable(table) => {
                assert!(
                    !self.dropping_tables.contains(&table),
                    "conflict table in concurrent checkpoint"
                );
                assert!(
                    self.creating_tables.insert(table),
                    "duplicated table in concurrent checkpoint"
                );
            }

            CommandChanges::Actor { to_add, .. } => {
                assert!(
                    self.adding_actors.is_disjoint(&to_add),
                    "duplicated actor in concurrent checkpoint"
                );
                self.adding_actors.extend(to_add);
            }

            _ => {}
        }
    }

    /// After resolving the actors to be sent or collected, we should remove the dropped table and
    /// removed actors from checkpoint control, so that `can_actor_send_or_collect` will return
    /// `false`.
    fn post_resolve(&mut self, command: &Command) {
        match command.changes() {
            CommandChanges::DropTable(table) => {
                assert!(
                    !self.creating_tables.contains(&table),
                    "conflict table in concurrent checkpoint"
                );
                assert!(
                    self.dropping_tables.insert(table),
                    "duplicated table in concurrent checkpoint"
                );
            }

            CommandChanges::Actor { to_remove, .. } => {
                assert!(
                    self.removing_actors.is_disjoint(&to_remove),
                    "duplicated actor in concurrent checkpoint"
                );
                self.removing_actors.extend(to_remove);
            }

            _ => {}
        }
    }

    /// Barrier can be sent to and collected from an actor if:
    /// 1. The actor is Running and not being dropped or removed in rescheduling.
    /// 2. The actor is Inactive and belongs to a creating MV or adding in rescheduling.
    fn can_actor_send_or_collect(
        &self,
        s: ActorState,
        table_id: TableId,
        actor_id: ActorId,
    ) -> bool {
        let removing =
            self.dropping_tables.contains(&table_id) || self.removing_actors.contains(&actor_id);
        let adding =
            self.creating_tables.contains(&table_id) || self.adding_actors.contains(&actor_id);

        match s {
            ActorState::Inactive => adding,
            ActorState::Running => !removing,
            ActorState::Unspecified => unreachable!(),
        }
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
    fn enqueue_command(&mut self, command_ctx: Arc<CommandContext<S>>, notifiers: Vec<Notifier>) {
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
    ) -> Vec<EpochNode<S>> {
        // change state to complete, and wait for nodes with the smaller epoch to commit
        let wait_commit_timer = self.metrics.barrier_wait_commit_latency.start_timer();
        if let Some(node) = self
            .command_ctx_queue
            .iter_mut()
            .find(|x| x.command_ctx.prev_epoch.0 == prev_epoch)
        {
            let checkpoint = result.iter().all(|node| node.checkpoint);
            assert!(matches!(node.state, InFlight));
            node.wait_commit_timer = Some(wait_commit_timer);
            node.state = Completed((result, checkpoint));
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
    fn barrier_failed(&mut self) -> Vec<EpochNode<S>> {
        let complete_nodes = self.command_ctx_queue.drain(..).collect_vec();
        complete_nodes
            .iter()
            .for_each(|node| self.remove_changes(node.command_ctx.command.changes()));
        complete_nodes
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

    /// After some command is committed, the changes will be applied to the meta store so we can
    /// remove the changes from checkpoint control.
    pub fn remove_changes(&mut self, changes: CommandChanges) {
        match changes {
            CommandChanges::CreateTable(table_id) => {
                assert!(self.creating_tables.remove(&table_id));
            }
            CommandChanges::DropTable(table_id) => {
                assert!(self.dropping_tables.remove(&table_id));
            }
            CommandChanges::Actor { to_add, to_remove } => {
                assert!(self.adding_actors.is_superset(&to_add));
                assert!(self.removing_actors.is_superset(&to_remove));

                self.adding_actors.retain(|a| !to_add.contains(a));
                self.removing_actors.retain(|a| !to_remove.contains(a));
            }
            CommandChanges::None => {}
        }
    }
}

/// The state and message of this barrier, a node for concurrent checkpoint.
pub struct EpochNode<S: MetaStore> {
    /// Timer for recording barrier latency, taken after `complete_barriers`.
    timer: Option<HistogramTimer>,
    /// The timer of `barrier_wait_commit_latency`
    wait_commit_timer: Option<HistogramTimer>,
    /// Whether this barrier is in-flight or completed.
    state: BarrierEpochState,
    /// Context of this command to generate barrier and do some post jobs.
    command_ctx: Arc<CommandContext<S>>,
    /// Notifiers of this barrier.
    notifiers: Vec<Notifier>,
}

/// The state of barrier.
enum BarrierEpochState {
    /// This barrier is current in-flight on the stream graph of compute nodes.
    InFlight,

    /// This barrier is completed or failed. We use a bool to mark if this barrier needs to do
    /// checkpoint, If it is false, we will just use `update_current_epoch` instead of
    /// `commit_epoch`
    Completed((Vec<BarrierCompleteResponse>, bool)),
}

impl<S> GlobalBarrierManager<S>
where
    S: MetaStore,
{
    /// Create a new [`crate::barrier::GlobalBarrierManager`].
    pub fn new(
        scheduled_barriers: schedule::ScheduledBarriers,
        env: MetaSrvEnv<S>,
        cluster_manager: ClusterManagerRef<S>,
        catalog_manager: CatalogManagerRef<S>,
        fragment_manager: FragmentManagerRef<S>,
        hummock_manager: HummockManagerRef<S>,
        metrics: Arc<MetaMetrics>,
    ) -> Self {
        let enable_recovery = env.opts.enable_recovery;
        let interval = env.opts.checkpoint_interval;
        let in_flight_barrier_nums = env.opts.in_flight_barrier_nums;
        tracing::info!(
            "Starting barrier manager with: interval={:?}, enable_recovery={} , in_flight_barrier_nums={}",
            interval,
            enable_recovery,
            in_flight_barrier_nums,
        );

        Self {
            interval,
            enable_recovery,
            scheduled_barriers,
            in_flight_barrier_nums,
            cluster_manager,
            catalog_manager,
            fragment_manager,
            hummock_manager,
            metrics,
            env,
        }
    }

    pub async fn start(barrier_manager: BarrierManagerRef<S>) -> (JoinHandle<()>, Sender<()>) {
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let join_handle = tokio::spawn(async move {
            barrier_manager.run(shutdown_rx).await;
        });

        (join_handle, shutdown_tx)
    }

    /// Start an infinite loop to take scheduled barriers and send them.
    async fn run(&self, mut shutdown_rx: Receiver<()>) {
        let mut tracker = CreateMviewProgressTracker::default();
        let mut state = BarrierManagerState::create(self.env.meta_store()).await;
        if self.enable_recovery {
            // handle init, here we simply trigger a recovery process to achieve the consistency. We
            // may need to avoid this when we have more state persisted in meta store.
            let new_epoch = state.in_flight_prev_epoch.next();
            assert!(new_epoch > state.in_flight_prev_epoch);
            state.in_flight_prev_epoch = new_epoch;

            let (new_epoch, actors_to_track, create_mview_progress) =
                self.recovery(state.in_flight_prev_epoch).await;
            tracker.add(new_epoch, actors_to_track, vec![]);
            for progress in &create_mview_progress {
                tracker.update(progress);
            }
            state.in_flight_prev_epoch = new_epoch;
            state
                .update_inflight_prev_epoch(self.env.meta_store())
                .await
                .unwrap();
        }
        let mut min_interval = tokio::time::interval(self.interval);
        min_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut barrier_timer: Option<HistogramTimer> = None;
        let (barrier_complete_tx, mut barrier_complete_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut checkpoint_control = CheckpointControl::new(self.metrics.clone());
        loop {
            tokio::select! {
                biased;
                // Shutdown
                _ = &mut shutdown_rx => {
                    tracing::info!("Barrier manager is stopped");
                    return;
                }
                result = barrier_complete_rx.recv() => {
                    checkpoint_control.update_barrier_nums_metrics();

                    let (prev_epoch, result) = result.unwrap();
                    self.barrier_complete_and_commit(
                        prev_epoch,
                        result,
                        &mut state,
                        &mut tracker,
                        &mut checkpoint_control,
                    )
                    .await;
                    continue;
                }
                // there's barrier scheduled.
                _ = self.scheduled_barriers.wait_one(), if checkpoint_control.can_inject_barrier(self.in_flight_barrier_nums) => {}
                // Wait for the minimal interval,
                _ = min_interval.tick(), if checkpoint_control.can_inject_barrier(self.in_flight_barrier_nums) => {}
            }

            if let Some(barrier_timer) = barrier_timer {
                barrier_timer.observe_duration();
            }
            barrier_timer = Some(self.metrics.barrier_send_latency.start_timer());
            let (command, notifiers) = self.scheduled_barriers.pop_or_default().await;
            let info = self
                .resolve_actor_info(&mut checkpoint_control, &command)
                .await;
            // When there's no actors exist in the cluster, we don't need to send the barrier. This
            // is an advance optimization. Besides if another barrier comes immediately,
            // it may send a same epoch and fail the epoch check.
            if info.nothing_to_do() {
                let mut notifiers = notifiers;
                notifiers.iter_mut().for_each(Notifier::notify_to_send);
                notifiers.iter_mut().for_each(Notifier::notify_collected);
                continue;
            }
            let prev_epoch = state.in_flight_prev_epoch;
            let new_epoch = prev_epoch.next();
            state.in_flight_prev_epoch = new_epoch;
            assert!(
                new_epoch > prev_epoch,
                "new{:?},prev{:?}",
                new_epoch,
                prev_epoch
            );
            state
                .update_inflight_prev_epoch(self.env.meta_store())
                .await
                .unwrap();

            let command_ctx = Arc::new(CommandContext::new(
                self.fragment_manager.clone(),
                self.env.stream_client_pool_ref(),
                info,
                prev_epoch,
                new_epoch,
                command,
            ));
            let mut notifiers = notifiers;
            notifiers.iter_mut().for_each(Notifier::notify_to_send);

            checkpoint_control.enqueue_command(command_ctx.clone(), notifiers);
            self.inject_barrier(command_ctx, barrier_complete_tx.clone())
                .await;
        }
    }

    /// Inject a barrier to all CNs and spawn a task to collect it
    async fn inject_barrier(
        &self,
        command_context: Arc<CommandContext<S>>,
        barrier_complete_tx: UnboundedSender<(u64, MetaResult<Vec<BarrierCompleteResponse>>)>,
    ) {
        let prev_epoch = command_context.prev_epoch.0;
        let result = self.inject_barrier_inner(command_context.clone()).await;
        match result {
            Ok(node_need_collect) => {
                let _ = tokio::spawn(Self::collect_barrier(
                    node_need_collect,
                    self.env.stream_client_pool_ref(),
                    command_context,
                    barrier_complete_tx,
                ));
            }
            Err(e) => barrier_complete_tx.send((prev_epoch, Err(e))).unwrap(),
        }
    }

    /// Send inject-barrier-rpc to stream service and wait for its response before returns.
    async fn inject_barrier_inner(
        &self,
        command_context: Arc<CommandContext<S>>,
    ) -> MetaResult<HashMap<WorkerId, bool>> {
        fail_point!("inject_barrier_err", |_| bail!("inject_barrier_err"));
        let mutation = command_context.to_mutation().await?;
        let info = command_context.info.clone();
        let mut node_need_collect = HashMap::new();
        let inject_futures = info.node_map.iter().filter_map(|(node_id, node)| {
            let actor_ids_to_send = info.actor_ids_to_send(node_id).collect_vec();
            let actor_ids_to_collect = info.actor_ids_to_collect(node_id).collect_vec();
            if actor_ids_to_collect.is_empty() {
                // No need to send or collect barrier for this node.
                assert!(actor_ids_to_send.is_empty());
                node_need_collect.insert(*node_id, false);
                None
            } else {
                node_need_collect.insert(*node_id, true);
                let mutation = mutation.clone();
                let request_id = Uuid::new_v4().to_string();
                let barrier = Barrier {
                    epoch: Some(risingwave_pb::data::Epoch {
                        curr: command_context.curr_epoch.0,
                        prev: command_context.prev_epoch.0,
                    }),
                    mutation,
                    // TODO(chi): add distributed tracing
                    span: vec![],
                    checkpoint: true,
                    passed_actors: vec![],
                };
                async move {
                    let client = self.env.stream_client_pool().get(node).await?;

                    let request = InjectBarrierRequest {
                        request_id,
                        barrier: Some(barrier),
                        actor_ids_to_send,
                        actor_ids_to_collect,
                    };
                    tracing::trace!(
                        target: "events::meta::barrier::inject_barrier",
                        "inject barrier request: {:?}", request
                    );

                    // This RPC returns only if this worker node has injected this barrier.
                    client.inject_barrier(request).await
                }
                .into()
            }
        });
        try_join_all(inject_futures).await?;
        Ok(node_need_collect)
    }

    /// Send barrier-complete-rpc and wait for responses from all CNs
    async fn collect_barrier(
        node_need_collect: HashMap<WorkerId, bool>,
        client_pool_ref: StreamClientPoolRef,
        command_context: Arc<CommandContext<S>>,
        barrier_complete_tx: UnboundedSender<(u64, MetaResult<Vec<BarrierCompleteResponse>>)>,
    ) {
        let prev_epoch = command_context.prev_epoch.0;
        let info = command_context.info.clone();
        let client_pool = client_pool_ref.deref();
        let collect_futures = info.node_map.iter().filter_map(|(node_id, node)| {
            if !*node_need_collect.get(node_id).unwrap() {
                // No need to send or collect barrier for this node.
                None
            } else {
                let request_id = Uuid::new_v4().to_string();
                async move {
                    let client = client_pool.get(node).await?;
                    let request = BarrierCompleteRequest {
                        request_id,
                        prev_epoch,
                    };
                    tracing::trace!(
                        target: "events::meta::barrier::barrier_complete",
                        "barrier complete request: {:?}", request
                    );

                    // This RPC returns only if this worker node has collected this barrier.
                    client.barrier_complete(request).await
                }
                .into()
            }
        });

        let result = try_join_all(collect_futures).await;
        barrier_complete_tx
            .send((prev_epoch, result.map_err(Into::into)))
            .unwrap();
    }

    /// Changes the state is `Complete`, and try commit all epoch that state is `Complete` in
    /// order. If commit is err, all nodes will be handled.
    async fn barrier_complete_and_commit(
        &self,
        prev_epoch: u64,
        result: MetaResult<Vec<BarrierCompleteResponse>>,
        state: &mut BarrierManagerState,
        tracker: &mut CreateMviewProgressTracker,
        checkpoint_control: &mut CheckpointControl<S>,
    ) {
        if let Err(err) = result {
            fail_point!("inject_barrier_err_success");
            let fail_node = checkpoint_control.barrier_failed();
            tracing::warn!("Failed to commit epoch {}: {:?}", prev_epoch, err);
            self.do_recovery(err, fail_node.into_iter(), state, tracker)
                .await;
            return;
        }
        // change the state is Complete
        let mut complete_nodes = checkpoint_control.barrier_completed(prev_epoch, result.unwrap());
        // try commit complete nodes
        let (mut index, mut err_msg) = (0, None);
        for (i, node) in complete_nodes.iter_mut().enumerate() {
            assert!(matches!(node.state, Completed(_)));
            if let Err(err) = self
                .complete_barrier(node, tracker, checkpoint_control)
                .await
            {
                index = i;
                err_msg = Some(err);
                break;
            }
        }
        // Handle the error node and the nodes after it
        if let Some(err) = err_msg {
            let fail_nodes = complete_nodes
                .drain(index..)
                .chain(checkpoint_control.barrier_failed().into_iter());
            self.do_recovery(err, fail_nodes, state, tracker).await;
        }
    }

    async fn do_recovery(
        &self,
        err: MetaError,
        fail_nodes: impl IntoIterator<Item = EpochNode<S>>,
        state: &mut BarrierManagerState,
        tracker: &mut CreateMviewProgressTracker,
    ) {
        let mut new_epoch = Epoch::from(INVALID_EPOCH);
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
            new_epoch = node.command_ctx.prev_epoch;
        }
        if self.enable_recovery {
            // If failed, enter recovery mode.
            let (new_epoch, actors_to_track, create_mview_progress) =
                self.recovery(new_epoch).await;
            *tracker = CreateMviewProgressTracker::default();
            tracker.add(new_epoch, actors_to_track, vec![]);
            for progress in &create_mview_progress {
                tracker.update(progress);
            }
            state.in_flight_prev_epoch = new_epoch;
            state
                .update_inflight_prev_epoch(self.env.meta_store())
                .await
                .unwrap();
        } else {
            panic!("failed to execute barrier: {:?}", err);
        }
    }

    /// Try to commit this node. If err, returns
    async fn complete_barrier(
        &self,
        node: &mut EpochNode<S>,
        tracker: &mut CreateMviewProgressTracker,
        checkpoint_control: &mut CheckpointControl<S>,
    ) -> MetaResult<()> {
        let prev_epoch = node.command_ctx.prev_epoch.0;
        match &node.state {
            Completed((resps, checkpoint)) => {
                // We must ensure all epochs are committed in ascending order,
                // because the storage engine will query from new to old in the order in which
                // the L0 layer files are generated.
                // See https://github.com/risingwavelabs/risingwave/issues/1251

                let notifiers = take(&mut node.notifiers);
                let command_ctx = node.command_ctx.clone();
                let create_mv_progress = resps
                    .iter()
                    .flat_map(|r| r.create_mview_progress.clone())
                    .collect_vec();
                checkpoint_control
                    .add_uncommitted_messages(resps, (command_ctx, notifiers, create_mv_progress));

                // If no checkpoint, we can't notify collection completion
                if *checkpoint {
                    let mut uncommitted_states = checkpoint_control.get_uncommitted_states();
                    if prev_epoch != INVALID_EPOCH {
                        self.hummock_manager
                            .commit_epoch(
                                prev_epoch,
                                uncommitted_states
                                    .uncommitted_ssts
                                    .into_iter()
                                    .collect_vec(),
                                uncommitted_states.uncommitted_work_ids,
                            )
                            .await?;
                    }
                    while let Some((command_ctx, mut notifiers, create_mv_progress)) =
                        uncommitted_states.uncommitted_checkpoint_post.pop_back()
                    {
                        checkpoint_control.remove_changes(command_ctx.command.changes());
                        command_ctx.post_collect().await?;

                        // Notify about collected first.
                        notifiers.iter_mut().for_each(Notifier::notify_collected);

                        // Then try to finish the barrier for Create MVs.
                        let actors_to_finish = command_ctx.actors_to_track();
                        tracker.add(command_ctx.curr_epoch, actors_to_finish, notifiers);
                        for progress in create_mv_progress {
                            tracker.update(&progress);
                        }
                    }
                } else if prev_epoch != INVALID_EPOCH {
                    self.hummock_manager
                        .update_current_epoch(prev_epoch)
                        .await?;
                }
                node.timer.take().unwrap().observe_duration();
                node.wait_commit_timer.take().unwrap().observe_duration();
                Ok(())
            }

            InFlight => unreachable!(),
        }
    }

    /// Resolve actor information from cluster, fragment manager and `ChangedTableId`.
    /// We use `changed_table_id` to modify the actors to be sent or collected. Because these actor
    /// will create or drop before this barrier flow through them.
    async fn resolve_actor_info(
        &self,
        checkpoint_control: &mut CheckpointControl<S>,
        command: &Command,
    ) -> BarrierActorInfo {
        checkpoint_control.pre_resolve(command);

        let check_state = |s: ActorState, table_id: TableId, actor_id: ActorId| {
            checkpoint_control.can_actor_send_or_collect(s, table_id, actor_id)
        };
        let all_nodes = self
            .cluster_manager
            .list_worker_node(WorkerType::ComputeNode, Some(Running))
            .await;
        let all_actor_infos = self.fragment_manager.load_all_actors(check_state).await;

        let info = BarrierActorInfo::resolve(all_nodes, all_actor_infos);

        checkpoint_control.post_resolve(command);

        info
    }
}

pub type BarrierManagerRef<S> = Arc<GlobalBarrierManager<S>>;
