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

use std::cell::LazyCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::{pending, poll_fn, Future};
use std::sync::Arc;
use std::task::Poll;

use await_tree::InstrumentAwait;
use futures::future::BoxFuture;
use futures::{FutureExt, TryFutureExt};
use prometheus::HistogramTimer;
use risingwave_common::catalog::TableId;
use risingwave_common::util::epoch::EpochPair;
use risingwave_hummock_sdk::SyncResult;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use thiserror_ext::AsReport;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use super::progress::BackfillState;
use super::LocalBarrierWorker;
use crate::error::{StreamError, StreamResult};
use crate::executor::monitor::StreamingMetrics;
use crate::executor::Barrier;
use crate::task::{ActorId, PartialGraphId, SharedContext, StreamActorManager};

struct IssuedState {
    /// Actor ids remaining to be collected.
    pub remaining_actors: BTreeSet<ActorId>,

    pub barrier_inflight_latency: HistogramTimer,

    /// Only be `Some(_)` when `kind` is `Checkpoint`
    pub table_ids: Option<HashSet<TableId>>,
}

impl Debug for IssuedState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuedState")
            .field("remaining_actors", &self.remaining_actors)
            .field("table_ids", &self.table_ids)
            .finish()
    }
}

#[derive(Debug)]
pub(super) struct BarrierState {
    barrier: Barrier,
    inner: IssuedState,
}

mod await_epoch_completed_future {
    use std::future::Future;

    use futures::future::BoxFuture;
    use futures::FutureExt;
    use risingwave_hummock_sdk::SyncResult;
    use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;

    use crate::error::StreamResult;
    use crate::executor::Barrier;
    use crate::task::{await_tree_key, BarrierCompleteResult, PartialGraphId};

    pub(crate) type AwaitEpochCompletedFuture = impl Future<Output = (PartialGraphId, Barrier, StreamResult<BarrierCompleteResult>)>
        + 'static;

    pub(super) fn instrument_complete_barrier_future(
        partial_graph_id: PartialGraphId,
        complete_barrier_future: Option<BoxFuture<'static, StreamResult<SyncResult>>>,
        barrier: Barrier,
        barrier_await_tree_reg: Option<&await_tree::Registry>,
        create_mview_progress: Vec<PbCreateMviewProgress>,
    ) -> AwaitEpochCompletedFuture {
        let prev_epoch = barrier.epoch.prev;
        let future = async move {
            if let Some(future) = complete_barrier_future {
                let result = future.await;
                result.map(Some)
            } else {
                Ok(None)
            }
        }
        .map(move |result| {
            (
                partial_graph_id,
                barrier,
                result.map(|sync_result| BarrierCompleteResult {
                    sync_result,
                    create_mview_progress,
                }),
            )
        });
        if let Some(reg) = barrier_await_tree_reg {
            reg.register(
                await_tree_key::BarrierAwait { prev_epoch },
                format!("SyncEpoch({})", prev_epoch),
            )
            .instrument(future)
            .left_future()
        } else {
            future.right_future()
        }
    }
}

pub(crate) use await_epoch_completed_future::*;
use risingwave_pb::stream_plan::SubscriptionUpstreamInfo;
use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;
use risingwave_pb::stream_service::streaming_control_stream_request::InitialPartialGraph;
use risingwave_pb::stream_service::InjectBarrierRequest;

fn sync_epoch<S: StateStore>(
    state_store: &S,
    streaming_metrics: &StreamingMetrics,
    prev_epoch: u64,
    table_ids: HashSet<TableId>,
) -> BoxFuture<'static, StreamResult<SyncResult>> {
    let timer = streaming_metrics.barrier_sync_latency.start_timer();
    let future = state_store.sync(prev_epoch, table_ids);
    future
        .instrument_await(format!("sync_epoch (epoch {})", prev_epoch))
        .inspect_ok(move |_| {
            timer.observe_duration();
        })
        .map_err(move |e| {
            tracing::error!(
                prev_epoch,
                error = %e.as_report(),
                "Failed to sync state store",
            );
            e.into()
        })
        .boxed()
}

pub(super) struct ManagedBarrierStateDebugInfo<'a> {
    graph_states: &'a HashMap<PartialGraphId, PartialGraphManagedBarrierState>,
}

impl Display for ManagedBarrierStateDebugInfo<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for (partial_graph_id, graph_states) in self.graph_states {
            writeln!(f, "--- Partial Group {}", partial_graph_id.0)?;
            write!(f, "{}", graph_states)?;
        }
        Ok(())
    }
}

impl Display for &'_ PartialGraphManagedBarrierState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut prev_epoch = 0u64;
        for (epoch, barrier_state) in &self.epoch_barrier_state_map {
            write!(f, "> Epoch {}: ", epoch)?;
            {
                {
                    let state = &barrier_state.inner;
                    write!(
                        f,
                        "Issued [{:?}]. Remaining actors: [",
                        barrier_state.barrier.kind
                    )?;
                    let mut is_prev_epoch_issued = false;
                    if prev_epoch != 0 {
                        let bs = &self.epoch_barrier_state_map[&prev_epoch];
                        let remaining_actors_prev = &bs.inner.remaining_actors;
                        {
                            // Only show the actors that are not in the previous epoch.
                            is_prev_epoch_issued = true;
                            let mut duplicates = 0usize;
                            for actor_id in &state.remaining_actors {
                                if !remaining_actors_prev.contains(actor_id) {
                                    write!(f, "{}, ", actor_id)?;
                                } else {
                                    duplicates += 1;
                                }
                            }
                            if duplicates > 0 {
                                write!(f, "...and {} actors in prev epoch", duplicates)?;
                            }
                        }
                    }
                    if !is_prev_epoch_issued {
                        for actor_id in &state.remaining_actors {
                            write!(f, "{}, ", actor_id)?;
                        }
                    }
                    write!(f, "]")?;
                }
            }
            prev_epoch = *epoch;
            writeln!(f)?;
        }

        if !self.create_mview_progress.is_empty() {
            writeln!(f, "Create MView Progress:")?;
            for (epoch, progress) in &self.create_mview_progress {
                write!(f, "> Epoch {}:", epoch)?;
                for (actor_id, state) in progress {
                    write!(f, ">> Actor {}: {}, ", actor_id, state)?;
                }
            }
        }

        Ok(())
    }
}

enum InflightActorStatus {
    /// The actor has been issued some barriers, but has not collected the first barrier
    IssuedFirst(Vec<Barrier>),
    /// The actor has been issued some barriers, and has collected the first barrier
    Running(u64),
}

impl InflightActorStatus {
    fn max_issued_epoch(&self) -> u64 {
        match self {
            InflightActorStatus::Running(epoch) => *epoch,
            InflightActorStatus::IssuedFirst(issued_barriers) => {
                issued_barriers.last().expect("non-empty").epoch.prev
            }
        }
    }
}

pub(crate) struct InflightActorState {
    actor_id: ActorId,
    barrier_senders: Vec<mpsc::UnboundedSender<Barrier>>,
    /// `prev_epoch` -> partial graph id
    pub(super) inflight_barriers: BTreeMap<u64, PartialGraphId>,
    status: InflightActorStatus,
    /// Whether the actor has been issued a stop barrier
    is_stopping: bool,

    join_handle: JoinHandle<()>,
    monitor_task_handle: Option<JoinHandle<()>>,
}

impl InflightActorState {
    pub(super) fn start(
        actor_id: ActorId,
        initial_partial_graph_id: PartialGraphId,
        initial_barrier: &Barrier,
        join_handle: JoinHandle<()>,
        monitor_task_handle: Option<JoinHandle<()>>,
    ) -> Self {
        Self {
            actor_id,
            barrier_senders: vec![],
            inflight_barriers: BTreeMap::from_iter([(
                initial_barrier.epoch.prev,
                initial_partial_graph_id,
            )]),
            status: InflightActorStatus::IssuedFirst(vec![initial_barrier.clone()]),
            is_stopping: false,
            join_handle,
            monitor_task_handle,
        }
    }

    pub(super) fn issue_barrier(
        &mut self,
        partial_graph_id: PartialGraphId,
        barrier: &Barrier,
        is_stop: bool,
    ) -> StreamResult<()> {
        assert!(barrier.epoch.prev > self.status.max_issued_epoch());

        for barrier_sender in &self.barrier_senders {
            barrier_sender.send(barrier.clone()).map_err(|_| {
                StreamError::barrier_send(
                    barrier.clone(),
                    self.actor_id,
                    "failed to send to registered sender",
                )
            })?;
        }

        assert!(self
            .inflight_barriers
            .insert(barrier.epoch.prev, partial_graph_id)
            .is_none());

        match &mut self.status {
            InflightActorStatus::IssuedFirst(pending_barriers) => {
                pending_barriers.push(barrier.clone());
            }
            InflightActorStatus::Running(prev_epoch) => {
                *prev_epoch = barrier.epoch.prev;
            }
        };

        if is_stop {
            assert!(!self.is_stopping, "stopped actor should not issue barrier");
            self.is_stopping = true;
        }
        Ok(())
    }

    pub(super) fn collect(&mut self, epoch: EpochPair) -> (PartialGraphId, bool) {
        let (prev_epoch, prev_partial_graph_id) =
            self.inflight_barriers.pop_first().expect("should exist");
        assert_eq!(prev_epoch, epoch.prev);
        match &self.status {
            InflightActorStatus::IssuedFirst(pending_barriers) => {
                assert_eq!(
                    prev_epoch,
                    pending_barriers.first().expect("non-empty").epoch.prev
                );
                self.status = InflightActorStatus::Running(
                    pending_barriers.last().expect("non-empty").epoch.prev,
                );
            }
            InflightActorStatus::Running(_) => {}
        }
        (
            prev_partial_graph_id,
            self.inflight_barriers.is_empty() && self.is_stopping,
        )
    }

    pub(super) fn is_running(&self) -> bool {
        matches!(&self.status, InflightActorStatus::Running(_))
    }
}

pub(super) struct PartialGraphManagedBarrierState {
    /// Record barrier state for each epoch of concurrent checkpoints.
    ///
    /// The key is `prev_epoch`, and the first value is `curr_epoch`
    epoch_barrier_state_map: BTreeMap<u64, BarrierState>,

    prev_barrier_table_ids: Option<(EpochPair, HashSet<TableId>)>,

    mv_depended_subscriptions: HashMap<TableId, HashSet<u32>>,

    /// Record the progress updates of creating mviews for each epoch of concurrent checkpoints.
    ///
    /// This is updated by [`super::CreateMviewProgressReporter::update`] and will be reported to meta
    /// in [`crate::task::barrier_manager::BarrierCompleteResult`].
    pub(super) create_mview_progress: HashMap<u64, HashMap<ActorId, BackfillState>>,

    pub(super) state_store: StateStoreImpl,

    pub(super) streaming_metrics: Arc<StreamingMetrics>,
}

impl PartialGraphManagedBarrierState {
    pub(super) fn new(actor_manager: &StreamActorManager) -> Self {
        Self::new_inner(
            actor_manager.env.state_store(),
            actor_manager.streaming_metrics.clone(),
        )
    }

    fn new_inner(state_store: StateStoreImpl, streaming_metrics: Arc<StreamingMetrics>) -> Self {
        Self {
            epoch_barrier_state_map: Default::default(),
            prev_barrier_table_ids: None,
            mv_depended_subscriptions: Default::default(),
            create_mview_progress: Default::default(),
            state_store,
            streaming_metrics,
        }
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new_inner(
            StateStoreImpl::for_test(),
            Arc::new(StreamingMetrics::unused()),
        )
    }

    pub(super) fn is_empty(&self) -> bool {
        self.epoch_barrier_state_map.is_empty()
    }
}

pub(crate) struct ManagedBarrierState {
    pub(super) actor_states: HashMap<ActorId, InflightActorState>,

    pub(super) graph_states: HashMap<PartialGraphId, PartialGraphManagedBarrierState>,

    actor_manager: Arc<StreamActorManager>,

    current_shared_context: Arc<SharedContext>,
}

impl ManagedBarrierState {
    /// Create a barrier manager state. This will be called only once.
    pub(super) fn new(
        actor_manager: Arc<StreamActorManager>,
        current_shared_context: Arc<SharedContext>,
        initial_partial_graphs: Vec<InitialPartialGraph>,
    ) -> Self {
        Self {
            actor_states: Default::default(),
            graph_states: initial_partial_graphs
                .into_iter()
                .map(|graph| {
                    let mut state = PartialGraphManagedBarrierState::new(&actor_manager);
                    state.add_subscriptions(graph.subscriptions);
                    (PartialGraphId::new(graph.partial_graph_id), state)
                })
                .collect(),
            actor_manager,
            current_shared_context,
        }
    }

    pub(super) fn to_debug_info(&self) -> ManagedBarrierStateDebugInfo<'_> {
        ManagedBarrierStateDebugInfo {
            graph_states: &self.graph_states,
        }
    }

    pub(crate) async fn abort_actors(&mut self) {
        for (actor_id, state) in &self.actor_states {
            tracing::debug!("force stopping actor {}", actor_id);
            state.join_handle.abort();
            if let Some(monitor_task_handle) = &state.monitor_task_handle {
                monitor_task_handle.abort();
            }
        }
        for (actor_id, state) in self.actor_states.drain() {
            tracing::debug!("join actor {}", actor_id);
            let result = state.join_handle.await;
            assert!(result.is_ok() || result.unwrap_err().is_cancelled());
        }
    }
}

impl InflightActorState {
    pub(super) fn register_barrier_sender(
        &mut self,
        tx: mpsc::UnboundedSender<Barrier>,
    ) -> StreamResult<()> {
        match &self.status {
            InflightActorStatus::IssuedFirst(pending_barriers) => {
                for barrier in pending_barriers {
                    tx.send(barrier.clone()).map_err(|_| {
                        StreamError::barrier_send(
                            barrier.clone(),
                            self.actor_id,
                            "failed to send pending barriers to newly registered sender",
                        )
                    })?;
                }
                self.barrier_senders.push(tx);
            }
            InflightActorStatus::Running(_) => {
                unreachable!("should not register barrier sender when entering Running status")
            }
        }
        Ok(())
    }
}

impl ManagedBarrierState {
    pub(super) fn register_barrier_sender(
        &mut self,
        actor_id: ActorId,
        tx: mpsc::UnboundedSender<Barrier>,
    ) -> StreamResult<()> {
        self.actor_states
            .get_mut(&actor_id)
            .expect("should exist")
            .register_barrier_sender(tx)
    }
}

impl PartialGraphManagedBarrierState {
    pub(super) fn add_subscriptions(&mut self, subscriptions: Vec<SubscriptionUpstreamInfo>) {
        for subscription_to_add in subscriptions {
            if !self
                .mv_depended_subscriptions
                .entry(TableId::new(subscription_to_add.upstream_mv_table_id))
                .or_default()
                .insert(subscription_to_add.subscriber_id)
            {
                if cfg!(debug_assertions) {
                    panic!("add an existing subscription: {:?}", subscription_to_add);
                }
                warn!(?subscription_to_add, "add an existing subscription");
            }
        }
    }

    pub(super) fn remove_subscriptions(&mut self, subscriptions: Vec<SubscriptionUpstreamInfo>) {
        for subscription_to_remove in subscriptions {
            let upstream_table_id = TableId::new(subscription_to_remove.upstream_mv_table_id);
            let Some(subscribers) = self.mv_depended_subscriptions.get_mut(&upstream_table_id)
            else {
                if cfg!(debug_assertions) {
                    panic!(
                        "unable to find upstream mv table to remove: {:?}",
                        subscription_to_remove
                    );
                }
                warn!(
                    ?subscription_to_remove,
                    "unable to find upstream mv table to remove"
                );
                continue;
            };
            if !subscribers.remove(&subscription_to_remove.subscriber_id) {
                if cfg!(debug_assertions) {
                    panic!(
                        "unable to find subscriber to remove: {:?}",
                        subscription_to_remove
                    );
                }
                warn!(
                    ?subscription_to_remove,
                    "unable to find subscriber to remove"
                );
            }
            if subscribers.is_empty() {
                self.mv_depended_subscriptions.remove(&upstream_table_id);
            }
        }
    }
}

impl ManagedBarrierState {
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        request: InjectBarrierRequest,
    ) -> StreamResult<()> {
        let partial_graph_id = PartialGraphId::new(request.partial_graph_id);
        let actor_to_stop = barrier.all_stop_actors();
        let is_stop_actor = |actor_id| {
            actor_to_stop
                .map(|actors| actors.contains(&actor_id))
                .unwrap_or(false)
        };
        let graph_state = self
            .graph_states
            .get_mut(&partial_graph_id)
            .expect("should exist");

        graph_state.add_subscriptions(request.subscriptions_to_add);
        graph_state.remove_subscriptions(request.subscriptions_to_remove);

        graph_state.transform_to_issued(
            barrier,
            request.actor_ids_to_collect.iter().cloned(),
            HashSet::from_iter(request.table_ids_to_sync.iter().cloned().map(TableId::new)),
        );

        let mut new_actors = HashSet::new();
        let subscriptions =
            LazyCell::new(|| Arc::new(graph_state.mv_depended_subscriptions.clone()));
        for actor in request.actors_to_build {
            let actor_id = actor.actor_id;
            assert!(!is_stop_actor(actor_id));
            assert!(new_actors.insert(actor_id));
            assert!(request.actor_ids_to_collect.contains(&actor_id));
            let (join_handle, monitor_join_handle) = self.actor_manager.spawn_actor(
                actor,
                (*subscriptions).clone(),
                self.current_shared_context.clone(),
            );
            assert!(self
                .actor_states
                .try_insert(
                    actor_id,
                    InflightActorState::start(
                        actor_id,
                        partial_graph_id,
                        barrier,
                        join_handle,
                        monitor_join_handle
                    )
                )
                .is_ok());
        }

        // Spawn a trivial join handle to be compatible with the unit test
        if cfg!(test) {
            for actor_id in &request.actor_ids_to_collect {
                if !self.actor_states.contains_key(actor_id) {
                    let join_handle = self.actor_manager.runtime.spawn(async { pending().await });
                    assert!(self
                        .actor_states
                        .try_insert(
                            *actor_id,
                            InflightActorState::start(
                                *actor_id,
                                partial_graph_id,
                                barrier,
                                join_handle,
                                None,
                            )
                        )
                        .is_ok());
                    new_actors.insert(*actor_id);
                }
            }
        }

        // Note: it's important to issue barrier to actor after issuing to graph to ensure that
        // we call `start_epoch` on the graph before the actors receive the barrier
        for actor_id in &request.actor_ids_to_collect {
            if new_actors.contains(actor_id) {
                continue;
            }
            self.actor_states
                .get_mut(actor_id)
                .unwrap_or_else(|| {
                    panic!(
                        "should exist: {} {:?}",
                        actor_id, request.actor_ids_to_collect
                    );
                })
                .issue_barrier(partial_graph_id, barrier, is_stop_actor(*actor_id))?;
        }

        Ok(())
    }

    pub(super) fn next_collected_epoch(
        &mut self,
    ) -> impl Future<
        Output = (
            PartialGraphId,
            Barrier,
            Vec<PbCreateMviewProgress>,
            Option<HashSet<TableId>>,
        ),
    > {
        let mut output = None;
        for (partial_graph_id, graph_state) in &mut self.graph_states {
            if let Some((barrier, create_mview_progress, table_ids)) =
                graph_state.may_have_collected_all()
            {
                if let Some(actors_to_stop) = barrier.all_stop_actors() {
                    self.current_shared_context.drop_actors(actors_to_stop);
                }
                output = Some((*partial_graph_id, barrier, create_mview_progress, table_ids));
                break;
            }
        }
        poll_fn(move |_| output.take().map(Poll::Ready).unwrap_or(Poll::Pending))
    }
}

impl ManagedBarrierState {
    pub(super) fn collect(&mut self, actor_id: ActorId, epoch: EpochPair) {
        let (prev_partial_graph_id, is_finished) = self
            .actor_states
            .get_mut(&actor_id)
            .expect("should exist")
            .collect(epoch);
        if is_finished {
            let state = self.actor_states.remove(&actor_id).expect("should exist");
            if let Some(monitor_task_handle) = state.monitor_task_handle {
                monitor_task_handle.abort();
            }
        }
        let prev_graph_state = self
            .graph_states
            .get_mut(&prev_partial_graph_id)
            .expect("should exist");
        prev_graph_state.collect(actor_id, epoch);
    }
}

impl PartialGraphManagedBarrierState {
    /// This method is called when barrier state is modified in either `Issued` or `Stashed`
    /// to transform the state to `AllCollected` and start state store `sync` when the barrier
    /// has been collected from all actors for an `Issued` barrier.
    fn may_have_collected_all(
        &mut self,
    ) -> Option<(
        Barrier,
        Vec<PbCreateMviewProgress>,
        Option<HashSet<TableId>>,
    )> {
        if let Some((_, barrier_state)) = self.epoch_barrier_state_map.first_key_value()
            && barrier_state.inner.remaining_actors.is_empty()
        {
            self.streaming_metrics.barrier_manager_progress.inc();

            let (_, barrier_state) = self.epoch_barrier_state_map.pop_first().expect("non-empty");

            let table_ids = {
                let IssuedState {
                    barrier_inflight_latency: timer,
                    table_ids,
                    ..
                } = barrier_state.inner;
                timer.observe_duration();
                table_ids
            };

            let create_mview_progress = self
                .create_mview_progress
                .remove(&barrier_state.barrier.epoch.curr)
                .unwrap_or_default()
                .into_iter()
                .map(|(actor, state)| state.to_pb(actor))
                .collect();
            Some((barrier_state.barrier, create_mview_progress, table_ids))
        } else {
            None
        }
    }
}

impl LocalBarrierWorker {
    pub(super) fn complete_barrier(
        &mut self,
        partial_graph_id: PartialGraphId,
        barrier: Barrier,
        create_mview_progress: Vec<PbCreateMviewProgress>,
        table_ids: Option<HashSet<TableId>>,
    ) {
        {
            let prev_epoch = barrier.epoch.prev;
            let complete_barrier_future = match &barrier.kind {
                BarrierKind::Unspecified => unreachable!(),
                BarrierKind::Initial => {
                    tracing::info!(
                        epoch = prev_epoch,
                        "ignore sealing data for the first barrier"
                    );
                    tracing::info!(?prev_epoch, "ignored syncing data for the first barrier");
                    None
                }
                BarrierKind::Barrier => None,
                BarrierKind::Checkpoint => {
                    dispatch_state_store!(&self.actor_manager.env.state_store(), state_store, {
                        Some(sync_epoch(
                            state_store,
                            &self.actor_manager.streaming_metrics,
                            prev_epoch,
                            table_ids.expect("should be Some on BarrierKind::Checkpoint"),
                        ))
                    })
                }
            };

            self.await_epoch_completed_futures.push_back({
                instrument_complete_barrier_future(
                    partial_graph_id,
                    complete_barrier_future,
                    barrier,
                    self.actor_manager.await_tree_reg.as_ref(),
                    create_mview_progress,
                )
            });
        }
    }
}

impl PartialGraphManagedBarrierState {
    /// Collect a `barrier` from the actor with `actor_id`.
    pub(super) fn collect(&mut self, actor_id: ActorId, epoch: EpochPair) {
        tracing::debug!(
            target: "events::stream::barrier::manager::collect",
            ?epoch, actor_id, state = ?self.epoch_barrier_state_map,
            "collect_barrier",
        );

        match self.epoch_barrier_state_map.get_mut(&epoch.prev) {
            None => {
                // If the barrier's state is stashed, this occurs exclusively in scenarios where the barrier has not been
                // injected by the barrier manager, or the barrier message is blocked at the `RemoteInput` side waiting for injection.
                // Given these conditions, it's inconceivable for an actor to attempt collect at this point.
                panic!(
                    "cannot collect new actor barrier {:?} at current state: None",
                    epoch,
                )
            }
            Some(&mut BarrierState {
                ref barrier,
                inner:
                    IssuedState {
                        ref mut remaining_actors,
                        ..
                    },
                ..
            }) => {
                let exist = remaining_actors.remove(&actor_id);
                assert!(
                    exist,
                    "the actor doesn't exist. actor_id: {:?}, curr_epoch: {:?}",
                    actor_id, epoch.curr
                );
                assert_eq!(barrier.epoch.curr, epoch.curr);
            }
        }
    }

    /// When the meta service issues a `send_barrier` request, call this function to transform to
    /// `Issued` and start to collect or to notify.
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
        table_ids: HashSet<TableId>,
    ) {
        let timer = self
            .streaming_metrics
            .barrier_inflight_latency
            .start_timer();

        if let Some(hummock) = self.state_store.as_hummock() {
            hummock.start_epoch(barrier.epoch.curr, table_ids.clone());
        }

        let table_ids = match barrier.kind {
            BarrierKind::Unspecified => {
                unreachable!()
            }
            BarrierKind::Initial => {
                assert!(
                    self.prev_barrier_table_ids.is_none(),
                    "non empty table_ids at initial barrier: {:?}",
                    self.prev_barrier_table_ids
                );
                info!(epoch = ?barrier.epoch, "initialize at Initial barrier");
                self.prev_barrier_table_ids = Some((barrier.epoch, table_ids));
                None
            }
            BarrierKind::Barrier => {
                if let Some((prev_epoch, prev_table_ids)) = self.prev_barrier_table_ids.as_mut() {
                    assert_eq!(prev_epoch.curr, barrier.epoch.prev);
                    assert_eq!(prev_table_ids, &table_ids);
                    *prev_epoch = barrier.epoch;
                } else {
                    info!(epoch = ?barrier.epoch, "initialize at non-checkpoint barrier");
                    self.prev_barrier_table_ids = Some((barrier.epoch, table_ids));
                }
                None
            }
            BarrierKind::Checkpoint => Some(
                if let Some((prev_epoch, prev_table_ids)) = self
                    .prev_barrier_table_ids
                    .replace((barrier.epoch, table_ids))
                    && prev_epoch.curr == barrier.epoch.prev
                {
                    prev_table_ids
                } else {
                    debug!(epoch = ?barrier.epoch, "reinitialize at Checkpoint barrier");
                    HashSet::new()
                },
            ),
        };

        if let Some(BarrierState { ref inner, .. }) =
            self.epoch_barrier_state_map.get_mut(&barrier.epoch.prev)
        {
            {
                panic!(
                    "barrier epochs{:?} state has already been `Issued`. Current state: {:?}",
                    barrier.epoch, inner
                );
            }
        };

        self.epoch_barrier_state_map.insert(
            barrier.epoch.prev,
            BarrierState {
                barrier: barrier.clone(),
                inner: IssuedState {
                    remaining_actors: BTreeSet::from_iter(actor_ids_to_collect),
                    barrier_inflight_latency: timer,
                    table_ids,
                },
            },
        );
    }

    #[cfg(test)]
    async fn pop_next_completed_epoch(&mut self) -> u64 {
        if let Some((barrier, _, _)) = self.may_have_collected_all() {
            return barrier.epoch.prev;
        }
        pending().await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_common::util::epoch::test_epoch;

    use crate::executor::Barrier;
    use crate::task::barrier_manager::managed_state::PartialGraphManagedBarrierState;

    #[tokio::test]
    async fn test_managed_state_add_actor() {
        let mut managed_barrier_state = PartialGraphManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        let barrier3 = Barrier::new_test_barrier(test_epoch(3));
        let actor_ids_to_collect1 = HashSet::from([1, 2]);
        let actor_ids_to_collect2 = HashSet::from([1, 2]);
        let actor_ids_to_collect3 = HashSet::from([1, 2, 3]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, HashSet::new());
        managed_barrier_state.collect(1, barrier1.epoch);
        managed_barrier_state.collect(2, barrier1.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(0)
        );
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &test_epoch(1)
        );
        managed_barrier_state.collect(1, barrier2.epoch);
        managed_barrier_state.collect(1, barrier3.epoch);
        managed_barrier_state.collect(2, barrier2.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(1)
        );
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            { &test_epoch(2) }
        );
        managed_barrier_state.collect(2, barrier3.epoch);
        managed_barrier_state.collect(3, barrier3.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(2)
        );
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_stop_actor() {
        let mut managed_barrier_state = PartialGraphManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        let barrier3 = Barrier::new_test_barrier(test_epoch(3));
        let actor_ids_to_collect1 = HashSet::from([1, 2, 3, 4]);
        let actor_ids_to_collect2 = HashSet::from([1, 2, 3]);
        let actor_ids_to_collect3 = HashSet::from([1, 2]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, HashSet::new());

        managed_barrier_state.collect(1, barrier1.epoch);
        managed_barrier_state.collect(1, barrier2.epoch);
        managed_barrier_state.collect(1, barrier3.epoch);
        managed_barrier_state.collect(2, barrier1.epoch);
        managed_barrier_state.collect(2, barrier2.epoch);
        managed_barrier_state.collect(2, barrier3.epoch);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &0
        );
        managed_barrier_state.collect(3, barrier1.epoch);
        managed_barrier_state.collect(3, barrier2.epoch);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &0
        );
        managed_barrier_state.collect(4, barrier1.epoch);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(0)
        );
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(1)
        );
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(2)
        );
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }
}
