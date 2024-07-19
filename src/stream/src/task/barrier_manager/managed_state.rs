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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{Debug, Display, Formatter};
use std::future::{poll_fn, Future};
use std::mem::replace;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use futures::{FutureExt, StreamExt, TryFutureExt};
use prometheus::HistogramTimer;
use risingwave_common::catalog::TableId;
use risingwave_common::must_match;
use risingwave_common::util::epoch::EpochPair;
use risingwave_hummock_sdk::SyncResult;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_pb::stream_service::PartialGraphInfo;
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use thiserror_ext::AsReport;
use tokio::sync::mpsc;

use super::progress::BackfillState;
use super::{BarrierCompleteResult, SubscribeMutationItem};
use crate::error::StreamResult;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::{Barrier, Mutation};
use crate::task::{await_tree_key, ActorId, PartialGraphId};

struct IssuedState {
    pub mutation: Option<Arc<Mutation>>,
    /// Actor ids remaining to be collected.
    pub remaining_actors: BTreeSet<ActorId>,

    pub barrier_inflight_latency: HistogramTimer,

    /// Only be `Some(_)` when `kind` is `Checkpoint`
    pub table_ids: Option<HashSet<TableId>>,

    pub kind: BarrierKind,
}

impl Debug for IssuedState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IssuedState")
            .field("mutation", &self.mutation)
            .field("remaining_actors", &self.remaining_actors)
            .field("table_ids", &self.table_ids)
            .field("kind", &self.kind)
            .finish()
    }
}

/// The state machine of local barrier manager.
#[derive(Debug)]
enum ManagedBarrierStateInner {
    /// Meta service has issued a `send_barrier` request. We're collecting barriers now.
    Issued(IssuedState),

    /// The barrier has been collected by all remaining actors
    AllCollected,

    /// The barrier has been completed, which means the barrier has been collected by all actors and
    /// synced in state store
    Completed(StreamResult<BarrierCompleteResult>),
}

#[derive(Debug)]
pub(super) struct BarrierState {
    curr_epoch: u64,
    inner: ManagedBarrierStateInner,
}

type AwaitEpochCompletedFuture =
    impl Future<Output = (u64, StreamResult<BarrierCompleteResult>)> + 'static;

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
            match &barrier_state.inner {
                ManagedBarrierStateInner::Issued(state) => {
                    write!(f, "Issued [{:?}]. Remaining actors: [", state.kind)?;
                    let mut is_prev_epoch_issued = false;
                    if prev_epoch != 0 {
                        let bs = &self.epoch_barrier_state_map[&prev_epoch];
                        if let ManagedBarrierStateInner::Issued(IssuedState {
                            remaining_actors: remaining_actors_prev,
                            ..
                        }) = &bs.inner
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
                ManagedBarrierStateInner::AllCollected => {
                    write!(f, "AllCollected")?;
                }
                ManagedBarrierStateInner::Completed(_) => {
                    write!(f, "Completed")?;
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

#[derive(Default)]
pub(super) struct InflightActorState {
    pending_subscribers: BTreeMap<u64, Vec<mpsc::UnboundedSender<SubscribeMutationItem>>>,
    started_subscribers: Vec<mpsc::UnboundedSender<SubscribeMutationItem>>,
    // epoch -> partial graph id
    pub(super) inflight_barriers: BTreeMap<u64, PartialGraphId>,
    is_stopped: bool,
}

pub(super) struct PartialGraphManagedBarrierState {
    /// This is a temporary workaround for the need to still calling `seal_epoch` for storage.
    /// Can be removed after `seal_epoch` is deprecated in storage.
    need_seal_epoch: bool,
    /// Record barrier state for each epoch of concurrent checkpoints.
    ///
    /// The key is `prev_epoch`, and the first value is `curr_epoch`
    epoch_barrier_state_map: BTreeMap<u64, BarrierState>,

    prev_barrier_table_ids: Option<(EpochPair, HashSet<TableId>)>,

    /// Record the progress updates of creating mviews for each epoch of concurrent checkpoints.
    pub(super) create_mview_progress: HashMap<u64, HashMap<ActorId, BackfillState>>,

    /// Futures will be finished in the order of epoch in ascending order.
    await_epoch_completed_futures: FuturesOrdered<AwaitEpochCompletedFuture>,

    pub(super) state_store: StateStoreImpl,

    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    /// Manages the await-trees of all barriers.
    barrier_await_tree_reg: Option<await_tree::Registry>,
}

impl PartialGraphManagedBarrierState {
    fn new(
        need_seal_epoch: bool,
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        barrier_await_tree_reg: Option<await_tree::Registry>,
    ) -> Self {
        Self {
            need_seal_epoch,
            epoch_barrier_state_map: Default::default(),
            prev_barrier_table_ids: None,
            create_mview_progress: Default::default(),
            await_epoch_completed_futures: Default::default(),
            state_store,
            streaming_metrics,
            barrier_await_tree_reg,
        }
    }
}

pub(super) struct ManagedBarrierState {
    pub(super) actor_states: HashMap<ActorId, InflightActorState>,

    pub(super) graph_states: HashMap<PartialGraphId, PartialGraphManagedBarrierState>,

    pub(super) state_store: StateStoreImpl,

    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    /// Manages the await-trees of all barriers.
    barrier_await_tree_reg: Option<await_tree::Registry>,
}

impl ManagedBarrierState {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(
            StateStoreImpl::for_test(),
            Arc::new(StreamingMetrics::unused()),
            None,
        )
    }

    /// Create a barrier manager state. This will be called only once.
    pub(super) fn new(
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
        barrier_await_tree_reg: Option<await_tree::Registry>,
    ) -> Self {
        Self {
            actor_states: Default::default(),
            graph_states: Default::default(),
            state_store,
            streaming_metrics,
            barrier_await_tree_reg,
        }
    }

    pub(super) fn to_debug_info(&self) -> ManagedBarrierStateDebugInfo<'_> {
        ManagedBarrierStateDebugInfo {
            graph_states: &self.graph_states,
        }
    }

    pub(super) fn subscribe_actor_mutation(
        &mut self,
        actor_id: ActorId,
        start_prev_epoch: u64,
        tx: mpsc::UnboundedSender<SubscribeMutationItem>,
    ) {
        let actor_state = self.actor_states.entry(actor_id).or_default();
        if let Some(start_partial_graph_id) = actor_state.inflight_barriers.get(&start_prev_epoch) {
            let start_graph_state = self
                .graph_states
                .get(start_partial_graph_id)
                .expect("should exist")
                .epoch_barrier_state_map
                .get(&start_prev_epoch)
                .expect("should exist");
            match &start_graph_state.inner {
                ManagedBarrierStateInner::Issued(issued_state) => {
                    assert!(issued_state.remaining_actors.contains(&actor_id));
                    for (prev_epoch, partial_graph_id) in
                        actor_state.inflight_barriers.range(start_prev_epoch..)
                    {
                        let graph_state = self
                            .graph_states
                            .get(partial_graph_id)
                            .expect("should exist")
                            .epoch_barrier_state_map
                            .get(prev_epoch)
                            .expect("should exist");
                        match &graph_state.inner {
                            ManagedBarrierStateInner::Issued(issued_state) => {
                                if issued_state.remaining_actors.contains(&actor_id) {
                                    if tx
                                        .send((*prev_epoch, issued_state.mutation.clone()))
                                        .is_err()
                                    {
                                        // No more subscribe on the mutation. Simply return.
                                        return;
                                    }
                                } else {
                                    // The barrier no more collect from such actor. End subscribe on mutation.
                                    return;
                                }
                            }
                            state @ ManagedBarrierStateInner::AllCollected
                            | state @ ManagedBarrierStateInner::Completed(_) => {
                                unreachable!(
                                    "should be Issued when having new subscriber, but current state: {:?}",
                                    state
                                )
                            }
                        }
                    }
                    actor_state.started_subscribers.push(tx);
                }
                state @ ManagedBarrierStateInner::AllCollected
                | state @ ManagedBarrierStateInner::Completed(_) => {
                    unreachable!(
                        "should be Issued when having new subscriber, but current state: {:?}",
                        state
                    )
                }
            }
        } else {
            // Barrier has not issued yet. Store the pending tx
            if let Some((last_epoch, _)) = actor_state.inflight_barriers.last_key_value() {
                assert!(
                    *last_epoch < start_prev_epoch,
                    "later barrier {} has been issued, but skip the start epoch {:?}",
                    last_epoch,
                    start_prev_epoch
                );
            }
            actor_state
                .pending_subscribers
                .entry(start_prev_epoch)
                .or_default()
                .push(tx);
        }
    }

    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        graph_infos: &HashMap<u32, PartialGraphInfo>,
    ) {
        let actor_to_stop = barrier.all_stop_actors();
        for (partial_graph_id, graph_info) in graph_infos {
            let partial_graph_id = PartialGraphId::new(*partial_graph_id);
            for actor_id in &graph_info.actor_ids_to_collect {
                let actor_state = self.actor_states.entry(*actor_id).or_default();
                if let Some((first_epoch, _)) = actor_state.pending_subscribers.first_key_value() {
                    assert!(
                        *first_epoch >= barrier.epoch.prev,
                        "barrier epoch {:?} skip subscribed epoch {}",
                        barrier.epoch,
                        first_epoch
                    );
                    if *first_epoch == barrier.epoch.prev {
                        actor_state.started_subscribers.extend(
                            actor_state
                                .pending_subscribers
                                .pop_first()
                                .expect("should exist")
                                .1,
                        );
                    }
                }
                actor_state.started_subscribers.retain(|tx| {
                    tx.send((barrier.epoch.prev, barrier.mutation.clone()))
                        .is_ok()
                });
                if let Some((prev_epoch, _)) = actor_state.inflight_barriers.first_key_value() {
                    assert!(*prev_epoch < barrier.epoch.prev);
                }
                actor_state
                    .inflight_barriers
                    .insert(barrier.epoch.prev, partial_graph_id);
                if let Some(actor_to_stop) = actor_to_stop
                    && actor_to_stop.contains(actor_id)
                {
                    actor_state.is_stopped = true;
                }
            }

            let graph_state = self
                .graph_states
                .entry(partial_graph_id)
                .or_insert_with(|| {
                    PartialGraphManagedBarrierState::new(
                        partial_graph_id.0 == u32::MAX,
                        self.state_store.clone(),
                        self.streaming_metrics.clone(),
                        self.barrier_await_tree_reg.clone(),
                    )
                });
            graph_state.transform_to_issued(
                barrier,
                graph_info.actor_ids_to_collect.iter().cloned().collect(),
                graph_info
                    .table_ids_to_sync
                    .iter()
                    .map(|table_id| TableId::new(*table_id))
                    .collect(),
            );
        }
    }

    pub(super) fn next_completed_epoch(
        &mut self,
    ) -> impl Future<Output = (PartialGraphId, u64)> + '_ {
        poll_fn(|cx| {
            for (partial_graph_id, graph_state) in &mut self.graph_states {
                let poll = graph_state.poll_next_completed_epoch(cx);
                if poll.is_ready() {
                    return poll.map(|epoch| (*partial_graph_id, epoch));
                }
            }
            Poll::Pending
        })
    }

    pub(super) fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        let actor_states = self.actor_states.get_mut(&actor_id).expect("should exist");
        let (prev_epoch, partial_graph_id) = actor_states
            .inflight_barriers
            .pop_first()
            .expect("should not be empty");
        assert_eq!(prev_epoch, barrier.epoch.prev);
        if actor_states.is_stopped && actor_states.inflight_barriers.is_empty() {
            self.actor_states.remove(&actor_id);
        }
        self.graph_states
            .get_mut(&partial_graph_id)
            .expect("should exist")
            .collect(actor_id, barrier);
    }
}

impl PartialGraphManagedBarrierState {
    /// This method is called when barrier state is modified in either `Issued` or `Stashed`
    /// to transform the state to `AllCollected` and start state store `sync` when the barrier
    /// has been collected from all actors for an `Issued` barrier.
    fn may_have_collected_all(&mut self, prev_epoch: u64) {
        // Report if there's progress on the earliest in-flight barrier.
        if self.epoch_barrier_state_map.keys().next() == Some(&prev_epoch) {
            self.streaming_metrics.barrier_manager_progress.inc();
        }

        for (prev_epoch, barrier_state) in &mut self.epoch_barrier_state_map {
            let prev_epoch = *prev_epoch;
            match &barrier_state.inner {
                ManagedBarrierStateInner::Issued(IssuedState {
                    remaining_actors, ..
                }) if remaining_actors.is_empty() => {}
                ManagedBarrierStateInner::AllCollected | ManagedBarrierStateInner::Completed(_) => {
                    continue;
                }
                ManagedBarrierStateInner::Issued(_) => {
                    break;
                }
            }

            let prev_state = replace(
                &mut barrier_state.inner,
                ManagedBarrierStateInner::AllCollected,
            );

            let (kind, table_ids) = must_match!(prev_state, ManagedBarrierStateInner::Issued(IssuedState {
                barrier_inflight_latency: timer,
                kind,
                table_ids,
                ..
            }) => {
                timer.observe_duration();
                (kind, table_ids)
            });

            let create_mview_progress = self
                .create_mview_progress
                .remove(&barrier_state.curr_epoch)
                .unwrap_or_default()
                .into_iter()
                .map(|(actor, state)| CreateMviewProgress {
                    backfill_actor_id: actor,
                    done: matches!(state, BackfillState::Done(_)),
                    consumed_epoch: match state {
                        BackfillState::ConsumingUpstream(consumed_epoch, _) => consumed_epoch,
                        BackfillState::Done(_) => barrier_state.curr_epoch,
                    },
                    consumed_rows: match state {
                        BackfillState::ConsumingUpstream(_, consumed_rows) => consumed_rows,
                        BackfillState::Done(consumed_rows) => consumed_rows,
                    },
                })
                .collect();

            let complete_barrier_future = match kind {
                BarrierKind::Unspecified => unreachable!(),
                BarrierKind::Initial => {
                    tracing::info!(
                        epoch = prev_epoch,
                        "ignore sealing data for the first barrier"
                    );
                    if let Some(hummock) = self.state_store.as_hummock() {
                        let mce = hummock.get_pinned_version().max_committed_epoch();
                        assert_eq!(
                            mce, prev_epoch,
                            "first epoch should match with the current version",
                        );
                    }
                    tracing::info!(?prev_epoch, "ignored syncing data for the first barrier");
                    None
                }
                BarrierKind::Barrier => {
                    if self.need_seal_epoch {
                        dispatch_state_store!(&self.state_store, state_store, {
                            state_store.seal_epoch(prev_epoch, kind.is_checkpoint());
                        });
                    }
                    None
                }
                BarrierKind::Checkpoint => {
                    dispatch_state_store!(&self.state_store, state_store, {
                        if self.need_seal_epoch {
                            state_store.seal_epoch(prev_epoch, kind.is_checkpoint());
                        }
                        Some(sync_epoch(
                            state_store,
                            &self.streaming_metrics,
                            prev_epoch,
                            table_ids.expect("should be Some on BarrierKind::Checkpoint"),
                        ))
                    })
                }
            };

            self.await_epoch_completed_futures.push_back({
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
                        prev_epoch,
                        result.map(|sync_result| BarrierCompleteResult {
                            sync_result,
                            create_mview_progress,
                        }),
                    )
                });
                if let Some(reg) = &self.barrier_await_tree_reg {
                    reg.register(
                        await_tree_key::BarrierAwait { prev_epoch },
                        format!("SyncEpoch({})", prev_epoch),
                    )
                    .instrument(future)
                    .left_future()
                } else {
                    future.right_future()
                }
            });
        }
    }

    /// Collect a `barrier` from the actor with `actor_id`.
    pub(super) fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        tracing::debug!(
            target: "events::stream::barrier::manager::collect",
            epoch = ?barrier.epoch, actor_id, state = ?self.epoch_barrier_state_map,
            "collect_barrier",
        );

        match self.epoch_barrier_state_map.get_mut(&barrier.epoch.prev) {
            None => {
                // If the barrier's state is stashed, this occurs exclusively in scenarios where the barrier has not been
                // injected by the barrier manager, or the barrier message is blocked at the `RemoteInput` side waiting for injection.
                // Given these conditions, it's inconceivable for an actor to attempt collect at this point.
                panic!(
                    "cannot collect new actor barrier {:?} at current state: None",
                    barrier.epoch,
                )
            }
            Some(&mut BarrierState {
                curr_epoch,
                inner:
                    ManagedBarrierStateInner::Issued(IssuedState {
                        ref mut remaining_actors,
                        ..
                    }),
                ..
            }) => {
                let exist = remaining_actors.remove(&actor_id);
                assert!(
                    exist,
                    "the actor doesn't exist. actor_id: {:?}, curr_epoch: {:?}",
                    actor_id, barrier.epoch.curr
                );
                assert_eq!(curr_epoch, barrier.epoch.curr);
                self.may_have_collected_all(barrier.epoch.prev);
            }
            Some(BarrierState { inner, .. }) => {
                panic!(
                    "cannot collect new actor barrier {:?} at current state: {:?}",
                    barrier.epoch, inner
                )
            }
        }
    }

    /// When the meta service issues a `send_barrier` request, call this function to transform to
    /// `Issued` and start to collect or to notify.
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: HashSet<ActorId>,
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
                {
                    assert_eq!(prev_epoch.curr, barrier.epoch.prev);
                    prev_table_ids
                } else {
                    info!(epoch = ?barrier.epoch, "initialize at Checkpoint barrier");
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
                curr_epoch: barrier.epoch.curr,
                inner: ManagedBarrierStateInner::Issued(IssuedState {
                    remaining_actors: BTreeSet::from_iter(actor_ids_to_collect),
                    mutation: barrier.mutation.clone(),
                    barrier_inflight_latency: timer,
                    kind: barrier.kind,
                    table_ids,
                }),
            },
        );
        self.may_have_collected_all(barrier.epoch.prev);
    }

    /// Return a future that yields the next completed epoch. The future is cancellation safe.
    pub(crate) fn poll_next_completed_epoch(&mut self, cx: &mut Context<'_>) -> Poll<u64> {
        ready!(self.await_epoch_completed_futures.next().poll_unpin(cx))
            .map(|(prev_epoch, result)| {
                let state = self
                    .epoch_barrier_state_map
                    .get_mut(&prev_epoch)
                    .expect("should exist");
                // sanity check on barrier state
                assert_matches!(&state.inner, ManagedBarrierStateInner::AllCollected);
                state.inner = ManagedBarrierStateInner::Completed(result);
                prev_epoch
            })
            .map(Poll::Ready)
            .unwrap_or(Poll::Pending)
    }

    /// Pop the completion result of an completed epoch.
    /// Return:
    /// - `Err(_)` `prev_epoch` is not an epoch to be collected.
    /// - `Ok(None)` when `prev_epoch` exists but has not completed.
    /// - `Ok(Some(_))` when `prev_epoch` has completed but not been reclaimed yet.
    ///    The `BarrierCompleteResult` will be popped out.
    pub(crate) fn pop_completed_epoch(
        &mut self,
        prev_epoch: u64,
    ) -> StreamResult<Option<StreamResult<BarrierCompleteResult>>> {
        let state = self
            .epoch_barrier_state_map
            .get(&prev_epoch)
            .ok_or_else(|| {
                // It's still possible that `collect_complete_receiver` does not contain the target epoch
                // when receiving collect_barrier request. Because `collect_complete_receiver` could
                // be cleared when CN is under recovering. We should return error rather than panic.
                anyhow!(
                    "barrier collect complete receiver for prev epoch {} not exists",
                    prev_epoch
                )
            })?;
        match &state.inner {
            ManagedBarrierStateInner::Completed(_) => {
                match self
                    .epoch_barrier_state_map
                    .remove(&prev_epoch)
                    .expect("should exists")
                    .inner
                {
                    ManagedBarrierStateInner::Completed(result) => Ok(Some(result)),
                    _ => unreachable!(),
                }
            }
            _ => Ok(None),
        }
    }

    #[cfg(test)]
    async fn pop_next_completed_epoch(&mut self) -> u64 {
        let epoch = self.next_completed_epoch().await;
        let _ = self.pop_completed_epoch(epoch).unwrap().unwrap();
        epoch
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_common::util::epoch::test_epoch;

    use crate::executor::Barrier;
    use crate::task::barrier_manager::managed_state::ManagedBarrierState;

    #[tokio::test]
    async fn test_managed_state_add_actor() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        let barrier3 = Barrier::new_test_barrier(test_epoch(3));
        let actor_ids_to_collect1 = HashSet::from([1, 2]);
        let actor_ids_to_collect2 = HashSet::from([1, 2]);
        let actor_ids_to_collect3 = HashSet::from([1, 2, 3]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, HashSet::new());
        managed_barrier_state.collect(1, &barrier1);
        managed_barrier_state.collect(2, &barrier1);
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
        managed_barrier_state.collect(1, &barrier2);
        managed_barrier_state.collect(1, &barrier3);
        managed_barrier_state.collect(2, &barrier2);
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
        managed_barrier_state.collect(2, &barrier3);
        managed_barrier_state.collect(3, &barrier3);
        assert_eq!(
            managed_barrier_state.pop_next_completed_epoch().await,
            test_epoch(2)
        );
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_stop_actor() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(test_epoch(1));
        let barrier2 = Barrier::new_test_barrier(test_epoch(2));
        let barrier3 = Barrier::new_test_barrier(test_epoch(3));
        let actor_ids_to_collect1 = HashSet::from([1, 2, 3, 4]);
        let actor_ids_to_collect2 = HashSet::from([1, 2, 3]);
        let actor_ids_to_collect3 = HashSet::from([1, 2]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, HashSet::new());
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, HashSet::new());

        managed_barrier_state.collect(1, &barrier1);
        managed_barrier_state.collect(1, &barrier2);
        managed_barrier_state.collect(1, &barrier3);
        managed_barrier_state.collect(2, &barrier1);
        managed_barrier_state.collect(2, &barrier2);
        managed_barrier_state.collect(2, &barrier3);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &0
        );
        managed_barrier_state.collect(3, &barrier1);
        managed_barrier_state.collect(3, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &0
        );
        managed_barrier_state.collect(4, &barrier1);
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
