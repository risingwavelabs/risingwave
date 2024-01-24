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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::iter::once;
use std::ops::Sub;
use std::sync::Arc;

use prometheus::HistogramTimer;
use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use tokio::sync::oneshot;

use super::progress::BackfillState;
use super::CollectResult;
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

        /// Notify that the collection is finished.
        collect_notifier: Option<oneshot::Sender<StreamResult<CollectResult>>>,

        barrier_inflight_latency: HistogramTimer,
    },
}

#[derive(Debug)]
pub(super) struct BarrierState {
    curr_epoch: u64,
    inner: ManagedBarrierStateInner,
    kind: BarrierKind,
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
        }
    }

    /// Notify if we have collected barriers from all actor ids. The state must be `Issued`.
    fn may_notify(&mut self, prev_epoch: u64) {
        // Report if there's progress on the earliest in-flight barrier.
        if self.epoch_barrier_state_map.keys().next() == Some(&prev_epoch) {
            self.streaming_metrics.barrier_manager_progress.inc();
        }

        while let Some(entry) = self.epoch_barrier_state_map.first_entry() {
            let to_notify = matches!(
                &entry.get().inner,
                ManagedBarrierStateInner::Issued {
                    remaining_actors, ..
                } if remaining_actors.is_empty(),
            );

            if !to_notify {
                break;
            }

            let (prev_epoch, barrier_state) = entry.remove_entry();

            let collect_notifier = match barrier_state.inner {
                ManagedBarrierStateInner::Issued {
                    collect_notifier,
                    barrier_inflight_latency: timer,
                    ..
                } => {
                    timer.observe_duration();
                    collect_notifier
                }
                _ => unreachable!(),
            };

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

            if let Some(notifier) = collect_notifier {
                // Notify about barrier finishing.
                let result = CollectResult {
                    create_mview_progress,
                    kind,
                };

                if notifier.send(Ok(result)).is_err() {
                    warn!(
                        "failed to notify barrier collection with epoch {}",
                        prev_epoch
                    )
                }
            }
        }
    }

    /// Returns an iterator on the notifiers of epochs that is awaiting on `actor_id`.
    /// This is used on notifying actor failure. On actor failure, the
    /// barrier manager can call this method to iterate on notifiers of epochs that
    /// waits on the failed actor and then notify failure on the result
    /// sender of the epoch.
    pub(crate) fn notifiers_await_on_actor(
        &mut self,
        actor_id: ActorId,
    ) -> impl Iterator<Item = (u64, oneshot::Sender<StreamResult<CollectResult>>)> + '_ {
        self.epoch_barrier_state_map
            .iter_mut()
            .filter_map(move |(prev_epoch, barrier_state)| {
                #[allow(clippy::single_match)]
                match &mut barrier_state.inner {
                    ManagedBarrierStateInner::Issued {
                        ref remaining_actors,
                        ref mut collect_notifier,
                        ..
                    } => {
                        if remaining_actors.contains(&actor_id) {
                            collect_notifier
                                .take()
                                .map(|notifier| (*prev_epoch, notifier))
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
                self.may_notify(barrier.epoch.prev);
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
        collect_notifier: oneshot::Sender<StreamResult<CollectResult>>,
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
                    collect_notifier: Some(collect_notifier),
                }
            }
            Some(&mut BarrierState {
                inner: ManagedBarrierStateInner::Issued { .. },
                ..
            }) => {
                panic!(
                    "barrier epochs{:?} state has already been `Issued`",
                    barrier.epoch
                );
            }
            None => ManagedBarrierStateInner::Issued {
                remaining_actors: actor_ids_to_collect,
                barrier_inflight_latency: timer,
                collect_notifier: Some(collect_notifier),
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
        self.may_notify(barrier.epoch.prev);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_common::util::epoch::TestEpoch;
    use tokio::sync::oneshot;

    use crate::executor::Barrier;
    use crate::task::barrier_manager::managed_state::ManagedBarrierState;

    #[tokio::test]
    async fn test_managed_state_add_actor() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(65536);
        let barrier2 = Barrier::new_test_barrier(TestEpoch::new_without_offset(2).as_u64());
        let barrier3 = Barrier::new_test_barrier(TestEpoch::new_without_offset(3).as_u64());
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();
        let actor_ids_to_collect1 = HashSet::from([1, 2]);
        let actor_ids_to_collect2 = HashSet::from([1, 2]);
        let actor_ids_to_collect3 = HashSet::from([1, 2, 3]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, tx1);
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, tx2);
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, tx3);
        managed_barrier_state.collect(1, &barrier1);
        managed_barrier_state.collect(2, &barrier1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &65536
        );
        managed_barrier_state.collect(1, &barrier2);
        managed_barrier_state.collect(1, &barrier3);
        managed_barrier_state.collect(2, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &(TestEpoch::new_without_offset(2).as_u64() as u64)
        );
        managed_barrier_state.collect(2, &barrier3);
        managed_barrier_state.collect(3, &barrier3);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_stop_actor() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(65536);
        let barrier2 = Barrier::new_test_barrier(TestEpoch::new_without_offset(2).as_u64());
        let barrier3 = Barrier::new_test_barrier(TestEpoch::new_without_offset(3).as_u64());
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();
        let actor_ids_to_collect1 = HashSet::from([1, 2, 3, 4]);
        let actor_ids_to_collect2 = HashSet::from([1, 2, 3]);
        let actor_ids_to_collect3 = HashSet::from([1, 2]);
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, tx1);
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, tx2);
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, tx3);

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
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_issued_after_collect() {
        let mut managed_barrier_state = ManagedBarrierState::for_test();
        let barrier1 = Barrier::new_test_barrier(65536);
        let barrier2 = Barrier::new_test_barrier(TestEpoch::new_without_offset(2).as_u64());
        let barrier3 = Barrier::new_test_barrier(TestEpoch::new_without_offset(3).as_u64());
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();
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
            &(TestEpoch::new_without_offset(2).as_u64() as u64)
        );
        managed_barrier_state.collect(1, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &(TestEpoch::new_without_offset(1).as_u64() as u64)
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
        managed_barrier_state.transform_to_issued(&barrier1, actor_ids_to_collect1, tx1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &(TestEpoch::new_without_offset(1).as_u64() as u64)
        );
        managed_barrier_state.transform_to_issued(&barrier2, actor_ids_to_collect2, tx2);
        managed_barrier_state.collect(3, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &(TestEpoch::new_without_offset(2).as_u64() as u64)
        );
        managed_barrier_state.collect(3, &barrier3);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &(TestEpoch::new_without_offset(2).as_u64() as u64)
        );
        managed_barrier_state.transform_to_issued(&barrier3, actor_ids_to_collect3, tx3);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }
}
