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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::iter::once;
use std::mem::replace;
use std::ops::Sub;
use std::sync::Arc;

use anyhow::anyhow;
use await_tree::InstrumentAwait;
use futures::stream::FuturesOrdered;
use futures::{FutureExt, StreamExt};
use prometheus::HistogramTimer;
use risingwave_common::must_match;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_storage::store::SyncResult;
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use rw_futures_util::pending_on_none;
use thiserror_ext::AsReport;

use super::progress::BackfillState;
use super::BarrierCompleteResult;
use crate::error::StreamResult;
use crate::executor::monitor::StreamingMetrics;
use crate::executor::Barrier;
use crate::task::ActorId;

/// The state machine of local barrier manager.
#[derive(Debug)]
enum ManagedBarrierStateInner {
    /// Barriers from some actors have been collected and stashed, however no `send_barrier`
    /// request from the meta service is issued.
    Stashed {
        /// Actor ids we've collected and stashed.
        collected_actors: HashSet<ActorId>,
    },

    /// Meta service has issued a `send_barrier` request. We're collecting barriers now.
    Issued {
        /// Actor ids remaining to be collected.
        remaining_actors: HashSet<ActorId>,

        barrier_inflight_latency: HistogramTimer,
    },

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
    kind: BarrierKind,
}

type AwaitEpochCompletedFuture =
    impl Future<Output = (u64, StreamResult<BarrierCompleteResult>)> + 'static;

fn sync_epoch(
    state_store: &StateStoreImpl,
    streaming_metrics: &StreamingMetrics,
    prev_epoch: u64,
    kind: BarrierKind,
) -> impl Future<Output = StreamResult<Option<SyncResult>>> + 'static {
    let barrier_sync_latency = streaming_metrics.barrier_sync_latency.clone();
    let state_store = state_store.clone();

    async move {
        let sync_result = match kind {
            BarrierKind::Unspecified => unreachable!(),
            BarrierKind::Initial => {
                if let Some(hummock) = state_store.as_hummock() {
                    let mce = hummock.get_pinned_version().max_committed_epoch();
                    assert_eq!(
                        mce, prev_epoch,
                        "first epoch should match with the current version",
                    );
                }
                tracing::info!(?prev_epoch, "ignored syncing data for the first barrier");
                None
            }
            BarrierKind::Barrier => None,
            BarrierKind::Checkpoint => {
                let timer = barrier_sync_latency.start_timer();
                let sync_result = dispatch_state_store!(state_store, store, {
                    store
                        .sync(prev_epoch)
                        .instrument_await(format!("sync_epoch (epoch {})", prev_epoch))
                        .await
                        .inspect_err(|e| {
                            tracing::error!(
                                prev_epoch,
                                error = %e.as_report(),
                                "Failed to sync state store",
                            );
                        })
                })?;
                timer.observe_duration();
                Some(sync_result)
            }
        };
        Ok(sync_result)
    }
}

pub(super) struct ManagedBarrierState {
    /// Record barrier state for each epoch of concurrent checkpoints.
    ///
    /// The key is prev_epoch, and the first value is curr_epoch
    epoch_barrier_state_map: BTreeMap<u64, BarrierState>,

    /// Record the progress updates of creating mviews for each epoch of concurrent checkpoints.
    pub(super) create_mview_progress: HashMap<u64, HashMap<ActorId, BackfillState>>,

    pub(super) state_store: StateStoreImpl,

    pub(super) streaming_metrics: Arc<StreamingMetrics>,

    /// Futures will be finished in the order of epoch in ascending order.
    await_epoch_completed_futures: FuturesOrdered<AwaitEpochCompletedFuture>,
}

impl ManagedBarrierState {
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(
            StateStoreImpl::for_test(),
            Arc::new(StreamingMetrics::unused()),
        )
    }

    /// Create a barrier manager state. This will be called only once.
    pub(super) fn new(
        state_store: StateStoreImpl,
        streaming_metrics: Arc<StreamingMetrics>,
    ) -> Self {
        Self {
            epoch_barrier_state_map: BTreeMap::default(),
            create_mview_progress: Default::default(),
            state_store,
            streaming_metrics,
            await_epoch_completed_futures: FuturesOrdered::new(),
        }
    }

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
                ManagedBarrierStateInner::Issued {
                    remaining_actors, ..
                } if remaining_actors.is_empty() => {}
                ManagedBarrierStateInner::AllCollected | ManagedBarrierStateInner::Completed(_) => {
                    continue;
                }
                ManagedBarrierStateInner::Stashed { .. }
                | ManagedBarrierStateInner::Issued { .. } => {
                    break;
                }
            }

            let prev_state = replace(
                &mut barrier_state.inner,
                ManagedBarrierStateInner::AllCollected,
            );

            must_match!(prev_state, ManagedBarrierStateInner::Issued {
                barrier_inflight_latency: timer,
                ..
            } => {
                timer.observe_duration();
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

            let kind = barrier_state.kind;
            match kind {
                BarrierKind::Unspecified => unreachable!(),
                BarrierKind::Initial => tracing::info!(
                    epoch = prev_epoch,
                    "ignore sealing data for the first barrier"
                ),
                BarrierKind::Barrier | BarrierKind::Checkpoint => {
                    dispatch_state_store!(&self.state_store, state_store, {
                        state_store.seal_epoch(prev_epoch, kind.is_checkpoint());
                    });
                }
            }

            self.await_epoch_completed_futures.push_back(
                sync_epoch(&self.state_store, &self.streaming_metrics, prev_epoch, kind).map(
                    move |result| {
                        (
                            prev_epoch,
                            result.map(move |sync_result| BarrierCompleteResult {
                                sync_result,
                                create_mview_progress,
                            }),
                        )
                    },
                ),
            );
        }
    }

    /// Returns an iterator on epochs that is awaiting on `actor_id`.
    /// This is used on notifying actor failure. On actor failure, the
    /// barrier manager can call this method to iterate on epochs that
    /// waits on the failed actor and then notify failure on the result
    /// sender of the epoch.
    pub(crate) fn epochs_await_on_actor(
        &self,
        actor_id: ActorId,
    ) -> impl Iterator<Item = u64> + '_ {
        self.epoch_barrier_state_map
            .iter()
            .filter_map(move |(prev_epoch, barrier_state)| {
                #[allow(clippy::single_match)]
                match barrier_state.inner {
                    ManagedBarrierStateInner::Issued {
                        ref remaining_actors,
                        ..
                    } => {
                        if remaining_actors.contains(&actor_id) {
                            Some(*prev_epoch)
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            })
    }

    /// Collect a `barrier` from the actor with `actor_id`.
    pub(super) fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        tracing::debug!(
            target: "events::stream::barrier::manager::collect",
            epoch = ?barrier.epoch, actor_id, state = ?self.epoch_barrier_state_map,
            "collect_barrier",
        );

        match self.epoch_barrier_state_map.get_mut(&barrier.epoch.prev) {
            Some(&mut BarrierState {
                curr_epoch,
                inner:
                    ManagedBarrierStateInner::Stashed {
                        ref mut collected_actors,
                    },
                ..
            }) => {
                let new = collected_actors.insert(actor_id);
                assert!(new);
                assert_eq!(curr_epoch, barrier.epoch.curr);
            }
            Some(&mut BarrierState {
                curr_epoch,
                inner:
                    ManagedBarrierStateInner::Issued {
                        ref mut remaining_actors,
                        ..
                    },
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
            None => {
                self.epoch_barrier_state_map.insert(
                    barrier.epoch.prev,
                    BarrierState {
                        curr_epoch: barrier.epoch.curr,
                        inner: ManagedBarrierStateInner::Stashed {
                            collected_actors: once(actor_id).collect(),
                        },
                        kind: barrier.kind,
                    },
                );
            }
        }
    }

    /// When the meta service issues a `send_barrier` request, call this function to transform to
    /// `Issued` and start to collect or to notify.
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: HashSet<ActorId>,
    ) {
        let timer = self
            .streaming_metrics
            .barrier_inflight_latency
            .start_timer();
        let inner = match self.epoch_barrier_state_map.get_mut(&barrier.epoch.prev) {
            Some(&mut BarrierState {
                inner:
                    ManagedBarrierStateInner::Stashed {
                        ref mut collected_actors,
                    },
                ..
            }) => {
                assert!(
                    actor_ids_to_collect.is_superset(collected_actors),
                    "to_collect: {:?}, collected: {:?}",
                    actor_ids_to_collect,
                    collected_actors
                );
                let remaining_actors: HashSet<ActorId> = actor_ids_to_collect.sub(collected_actors);
                ManagedBarrierStateInner::Issued {
                    remaining_actors,
                    barrier_inflight_latency: timer,
                }
            }
            Some(BarrierState { ref inner, .. }) => {
                panic!(
                    "barrier epochs{:?} state has already been `Issued`. Current state: {:?}",
                    barrier.epoch, inner
                );
            }
            None => ManagedBarrierStateInner::Issued {
                remaining_actors: actor_ids_to_collect,
                barrier_inflight_latency: timer,
            },
        };
        self.epoch_barrier_state_map.insert(
            barrier.epoch.prev,
            BarrierState {
                curr_epoch: barrier.epoch.curr,
                inner,
                kind: barrier.kind,
            },
        );
        self.may_have_collected_all(barrier.epoch.prev);
    }

    /// Return a future that yields the next completed epoch. The future is cancellation safe.
    pub(crate) fn next_completed_epoch(&mut self) -> impl Future<Output = u64> + '_ {
        pending_on_none(self.await_epoch_completed_futures.next()).map(|(prev_epoch, result)| {
            let state = self
                .epoch_barrier_state_map
                .get_mut(&prev_epoch)
                .expect("should exist");
            // sanity check on barrier state
            assert_matches!(&state.inner, ManagedBarrierStateInner::AllCollected);
            state.inner = ManagedBarrierStateInner::Completed(result);
            prev_epoch
        })
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

    use crate::executor::Barrier;
    use crate::task::barrier_manager::managed_state::ManagedBarrierState;

    #[tokio::test]
    async fn test_managed_state_add_actor() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(1);
        let barrier2 = Barrier::new_test_barrier(2);
        let barrier3 = Barrier::new_test_barrier(3);
        let actor_ids_to_collect1 = HashSet::from([1, 2]);
        let actor_ids_to_collect2 = HashSet::from([1, 2]);
        let actor_ids_to_collect3 = HashSet::from([1, 2, 3]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1);
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2);
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3);
        managed_barrier_state.collect(1, &barrier1);
        managed_barrier_state.collect(2, &barrier1);
        assert_eq!(managed_barrier_state.pop_next_completed_epoch().await, 0);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &1
        );
        managed_barrier_state.collect(1, &barrier2);
        managed_barrier_state.collect(1, &barrier3);
        managed_barrier_state.collect(2, &barrier2);
        assert_eq!(managed_barrier_state.pop_next_completed_epoch().await, 1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
        );
        managed_barrier_state.collect(2, &barrier3);
        managed_barrier_state.collect(3, &barrier3);
        assert_eq!(managed_barrier_state.pop_next_completed_epoch().await, 2);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_stop_actor() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(1);
        let barrier2 = Barrier::new_test_barrier(2);
        let barrier3 = Barrier::new_test_barrier(3);
        let actor_ids_to_collect1 = HashSet::from([1, 2, 3, 4]);
        let actor_ids_to_collect2 = HashSet::from([1, 2, 3]);
        let actor_ids_to_collect3 = HashSet::from([1, 2]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1);
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2);
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3);

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
        assert_eq!(managed_barrier_state.pop_next_completed_epoch().await, 0);
        assert_eq!(managed_barrier_state.pop_next_completed_epoch().await, 1);
        assert_eq!(managed_barrier_state.pop_next_completed_epoch().await, 2);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_issued_after_collect() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(1);
        let barrier2 = Barrier::new_test_barrier(2);
        let barrier3 = Barrier::new_test_barrier(3);
        let actor_ids_to_collect1 = HashSet::from([1, 2]);
        let actor_ids_to_collect2 = HashSet::from([1, 2, 3]);
        let actor_ids_to_collect3 = HashSet::from([1, 2, 3]);

        managed_barrier_state.collect(1, &barrier3);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
        );
        managed_barrier_state.collect(1, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &1
        );
        managed_barrier_state.collect(1, &barrier1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &0
        );
        managed_barrier_state.collect(2, &barrier1);
        managed_barrier_state.collect(2, &barrier2);
        managed_barrier_state.collect(2, &barrier3);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1);
        assert_eq!(managed_barrier_state.pop_next_completed_epoch().await, 0);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &1
        );
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2);
        managed_barrier_state.collect(3, &barrier2);
        assert_eq!(managed_barrier_state.pop_next_completed_epoch().await, 1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
        );
        managed_barrier_state.collect(3, &barrier3);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
        );
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3);
        assert_eq!(managed_barrier_state.pop_next_completed_epoch().await, 2);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }
}
