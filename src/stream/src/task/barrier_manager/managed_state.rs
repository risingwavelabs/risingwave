// Copyright 2023 RisingWave Labs
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

use risingwave_pb::stream_plan::barrier::BarrierKind;
use risingwave_pb::stream_service::barrier_complete_response::CreateMviewProgress;
use risingwave_storage::{dispatch_state_store, StateStore, StateStoreImpl};
use thiserror_ext::AsReport;
use tokio::sync::oneshot;

use super::progress::BackfillState;
use super::CollectResult;
use crate::error::{IntoUnexpectedExit, StreamError, StreamResult};
use crate::executor::monitor::GLOBAL_STREAMING_METRICS;
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
    },
}

#[derive(Debug)]
pub(super) struct BarrierState {
    prev_epoch: u64,
    inner: ManagedBarrierStateInner,
    kind: BarrierKind,
}

#[derive(Debug)]
pub(super) struct ManagedBarrierState {
    /// Record barrier state for each epoch of concurrent checkpoints.
    ///
    /// The key is curr_epoch, and the first value is prev_epoch
    epoch_barrier_state_map: BTreeMap<u64, BarrierState>,

    /// Record the progress updates of creating mviews for each epoch of concurrent checkpoints.
    pub(super) create_mview_progress: HashMap<u64, HashMap<ActorId, BackfillState>>,

    /// Record all unexpected exited actors.
    failure_actors: HashMap<ActorId, StreamError>,

    state_store: StateStoreImpl,
}

impl ManagedBarrierState {
    /// Create a barrier manager state. This will be called only once.
    pub(super) fn new(state_store: StateStoreImpl) -> Self {
        Self {
            epoch_barrier_state_map: BTreeMap::default(),
            create_mview_progress: Default::default(),
            failure_actors: Default::default(),
            state_store,
        }
    }

    /// Notify if we have collected barriers from all actor ids. The state must be `Issued`.
    fn may_notify(&mut self, curr_epoch: u64) {
        // Report if there's progress on the earliest in-flight barrier.
        if self.epoch_barrier_state_map.keys().next() == Some(&curr_epoch) {
            if let Some(metrics) = GLOBAL_STREAMING_METRICS.get() {
                metrics.barrier_manager_progress.inc();
            }
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

            let (epoch, barrier_state) = entry.remove_entry();
            let create_mview_progress = self
                .create_mview_progress
                .remove(&epoch)
                .unwrap_or_default()
                .into_iter()
                .map(|(actor, state)| CreateMviewProgress {
                    backfill_actor_id: actor,
                    done: matches!(state, BackfillState::Done(_)),
                    consumed_epoch: match state {
                        BackfillState::ConsumingUpstream(consumed_epoch, _) => consumed_epoch,
                        BackfillState::Done(_) => epoch,
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
                    epoch = barrier_state.prev_epoch,
                    "ignore sealing data for the first barrier"
                ),
                BarrierKind::Barrier | BarrierKind::Checkpoint => {
                    dispatch_state_store!(&self.state_store, state_store, {
                        state_store.seal_epoch(barrier_state.prev_epoch, kind.is_checkpoint());
                    });
                }
            }

            match barrier_state.inner {
                ManagedBarrierStateInner::Issued {
                    collect_notifier, ..
                } => {
                    // Notify about barrier finishing.
                    let result = CollectResult {
                        create_mview_progress,
                        kind,
                    };
                    if collect_notifier.unwrap().send(Ok(result)).is_err() {
                        warn!("failed to notify barrier collection with epoch {}", epoch)
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    /// Clear and reset all states.
    pub(crate) fn clear_all_states(&mut self) {
        tracing::debug!("clear all states in local barrier manager");

        *self = Self::new(self.state_store.clone());
    }

    /// Notify unexpected actor exit with given `actor_id`.
    pub(crate) fn notify_failure(&mut self, actor_id: ActorId, err: StreamError) {
        // Attach the actor id to the error.
        let err = err.into_unexpected_exit(actor_id);

        for barrier_state in self.epoch_barrier_state_map.values_mut() {
            #[allow(clippy::single_match)]
            match barrier_state.inner {
                ManagedBarrierStateInner::Issued {
                    ref remaining_actors,
                    ref mut collect_notifier,
                } => {
                    if remaining_actors.contains(&actor_id)
                        && let Some(collect_notifier) = collect_notifier.take()
                        && collect_notifier
                            .send(Err(err.clone()))
                            .is_err()
                    {
                        warn!(error = %err.as_report(), actor_id, "failed to notify actor exiting");
                    }
                }
                _ => {}
            }
        }
        self.failure_actors.insert(actor_id, err);
    }

    /// Collect a `barrier` from the actor with `actor_id`.
    pub(super) fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        tracing::debug!(
            target: "events::stream::barrier::manager::collect",
            epoch = barrier.epoch.curr, actor_id, state = ?self,
            "collect_barrier",
        );

        match self.epoch_barrier_state_map.get_mut(&barrier.epoch.curr) {
            Some(&mut BarrierState {
                prev_epoch,
                inner:
                    ManagedBarrierStateInner::Stashed {
                        ref mut collected_actors,
                    },
                ..
            }) => {
                let new = collected_actors.insert(actor_id);
                assert!(new);
                assert_eq!(prev_epoch, barrier.epoch.prev);
            }
            Some(&mut BarrierState {
                prev_epoch,
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
                assert_eq!(prev_epoch, barrier.epoch.prev);
                self.may_notify(barrier.epoch.curr);
            }
            None => {
                self.epoch_barrier_state_map.insert(
                    barrier.epoch.curr,
                    BarrierState {
                        prev_epoch: barrier.epoch.prev,
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
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
        collect_notifier: oneshot::Sender<StreamResult<CollectResult>>,
    ) -> StreamResult<()> {
        let inner = match self.epoch_barrier_state_map.get_mut(&barrier.epoch.curr) {
            Some(&mut BarrierState {
                inner:
                    ManagedBarrierStateInner::Stashed {
                        ref mut collected_actors,
                    },
                ..
            }) => {
                let remaining_actors: HashSet<ActorId> = actor_ids_to_collect
                    .into_iter()
                    .filter(|a| !collected_actors.remove(a))
                    .collect();
                for (actor_id, err) in &self.failure_actors {
                    if remaining_actors.contains(actor_id) {
                        return Err(err.clone());
                    }
                }
                assert!(collected_actors.is_empty());
                ManagedBarrierStateInner::Issued {
                    remaining_actors,
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
            None => {
                let remaining_actors: HashSet<ActorId> = actor_ids_to_collect.into_iter().collect();
                // The failure actors could exit before the barrier is issued, while their
                // up-downstream actors could be stuck somehow. Return error directly to trigger the
                // recovery.
                for (actor_id, err) in &self.failure_actors {
                    if remaining_actors.contains(actor_id) {
                        return Err(err.clone());
                    }
                }
                ManagedBarrierStateInner::Issued {
                    remaining_actors,
                    collect_notifier: Some(collect_notifier),
                }
            }
        };
        self.epoch_barrier_state_map.insert(
            barrier.epoch.curr,
            BarrierState {
                prev_epoch: barrier.epoch.prev,
                inner,
                kind: barrier.kind,
            },
        );
        self.may_notify(barrier.epoch.curr);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use risingwave_storage::StateStoreImpl;
    use tokio::sync::oneshot;

    use crate::executor::Barrier;
    use crate::task::barrier_manager::managed_state::ManagedBarrierState;

    #[tokio::test]
    async fn test_managed_state_add_actor() {
        let mut managed_barrier_state = ManagedBarrierState::new(StateStoreImpl::for_test());
        let barrier1 = Barrier::new_test_barrier(1);
        let barrier2 = Barrier::new_test_barrier(2);
        let barrier3 = Barrier::new_test_barrier(3);
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();
        let actor_ids_to_collect1 = HashSet::from([1, 2]);
        let actor_ids_to_collect2 = HashSet::from([1, 2]);
        let actor_ids_to_collect3 = HashSet::from([1, 2, 3]);
        managed_barrier_state
            .transform_to_issued(&barrier1, actor_ids_to_collect1, tx1)
            .unwrap();
        managed_barrier_state
            .transform_to_issued(&barrier2, actor_ids_to_collect2, tx2)
            .unwrap();
        managed_barrier_state
            .transform_to_issued(&barrier3, actor_ids_to_collect3, tx3)
            .unwrap();
        managed_barrier_state.collect(1, &barrier1);
        managed_barrier_state.collect(2, &barrier1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
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
            &3
        );
        managed_barrier_state.collect(2, &barrier3);
        managed_barrier_state.collect(3, &barrier3);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_stop_actor() {
        let mut managed_barrier_state = ManagedBarrierState::new(StateStoreImpl::for_test());
        let barrier1 = Barrier::new_test_barrier(1);
        let barrier2 = Barrier::new_test_barrier(2);
        let barrier3 = Barrier::new_test_barrier(3);
        let (tx1, _rx1) = oneshot::channel();
        let (tx2, _rx2) = oneshot::channel();
        let (tx3, _rx3) = oneshot::channel();
        let actor_ids_to_collect1 = HashSet::from([1, 2, 3, 4]);
        let actor_ids_to_collect2 = HashSet::from([1, 2, 3]);
        let actor_ids_to_collect3 = HashSet::from([1, 2]);
        managed_barrier_state
            .transform_to_issued(&barrier1, actor_ids_to_collect1, tx1)
            .unwrap();
        managed_barrier_state
            .transform_to_issued(&barrier2, actor_ids_to_collect2, tx2)
            .unwrap();
        managed_barrier_state
            .transform_to_issued(&barrier3, actor_ids_to_collect3, tx3)
            .unwrap();

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
            &1
        );
        managed_barrier_state.collect(3, &barrier1);
        managed_barrier_state.collect(3, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &1
        );
        managed_barrier_state.collect(4, &barrier1);
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }

    #[tokio::test]
    async fn test_managed_state_issued_after_collect() {
        let mut managed_barrier_state = ManagedBarrierState::new(StateStoreImpl::for_test());
        let barrier1 = Barrier::new_test_barrier(1);
        let barrier2 = Barrier::new_test_barrier(2);
        let barrier3 = Barrier::new_test_barrier(3);
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
            &3
        );
        managed_barrier_state.collect(1, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
        );
        managed_barrier_state.collect(1, &barrier1);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &1
        );
        managed_barrier_state.collect(2, &barrier1);
        managed_barrier_state.collect(2, &barrier2);
        managed_barrier_state.collect(2, &barrier3);
        managed_barrier_state
            .transform_to_issued(&barrier1, actor_ids_to_collect1, tx1)
            .unwrap();
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &2
        );
        managed_barrier_state
            .transform_to_issued(&barrier2, actor_ids_to_collect2, tx2)
            .unwrap();
        managed_barrier_state.collect(3, &barrier2);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &3
        );
        managed_barrier_state.collect(3, &barrier3);
        assert_eq!(
            managed_barrier_state
                .epoch_barrier_state_map
                .first_key_value()
                .unwrap()
                .0,
            &3
        );
        managed_barrier_state
            .transform_to_issued(&barrier3, actor_ids_to_collect3, tx3)
            .unwrap();
        assert!(managed_barrier_state.epoch_barrier_state_map.is_empty());
    }
}
