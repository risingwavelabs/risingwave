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

use std::collections::{HashMap, HashSet};
use std::iter::once;

use tokio::sync::oneshot;

use super::{CollectResult, FinishedCreateMview};
use crate::executor::Barrier;
use crate::task::ActorId;

/// The state machine of local barrier manager.
#[derive(Debug)]
enum ManagedBarrierStateInner {
    /// Currently no barrier on the flight.

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
        collect_notifier: oneshot::Sender<CollectResult>,
    },
}

#[derive(Debug)]
pub(super) struct ManagedBarrierState {
    map: HashMap<u64, ManagedBarrierStateInner>,

    pub finished_create_mviews: Vec<FinishedCreateMview>,
}

impl ManagedBarrierState {
    /// Create a barrier manager state. This will be called only once.
    pub(super) fn new() -> Self {
        Self {
            // inner: ManagedBarrierStateInner::Pending {
            //     // TODO: specify last epoch
            //     last_epoch: None,
            // },
            // finished_create_mviews: Default::default(),
            map: HashMap::new(),
            finished_create_mviews: Default::default(),
        }
    }

    // fn inner_mut(&mut self) -> &mut ManagedBarrierStateInner {
    //     &mut self.inner
    // }

    /// Notify if we have collected barriers from all actor ids. The state must be `Issued`.
    fn may_notify(&mut self, curr_epoch: u64) {
        let to_notify = match self.map.get(&curr_epoch) {
            Some(ManagedBarrierStateInner::Issued {
                remaining_actors, ..
            }) => (remaining_actors.is_empty()),
            _ => unreachable!(),
        };

        if to_notify {
            let inner = self.map.remove(&curr_epoch);

            let finished_create_mviews = std::mem::take(&mut self.finished_create_mviews);

            // debug!("finished{:?}",finished_create_mviews);
            match inner {
                Some(ManagedBarrierStateInner::Issued {
                    collect_notifier, ..
                }) => {
                    // Notify about barrier finishing.
                    let result = CollectResult {
                        finished_create_mviews,
                    };
                    if collect_notifier.send(result).is_err() {
                        warn!(
                            "failed to notify barrier collection with epoch {}",
                            curr_epoch
                        )
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    /// Collect a `barrier` from the actor with `actor_id`.
    pub(super) fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        tracing::trace!(
            target: "events::stream::barrier::collect_barrier",
            "collect_barrier: epoch = {}, actor_id = {}, state = {:#?}",
            barrier.epoch.curr,
            actor_id,
            self
        );

        match self.map.get_mut(&barrier.epoch.curr) {
            Some(ManagedBarrierStateInner::Stashed { collected_actors }) => {
                let new = collected_actors.insert(actor_id);
                assert!(new);
            }
            Some(ManagedBarrierStateInner::Issued {
                remaining_actors, ..
            }) => {
                // debug!("actor_id{:?},barrier{:?},set{:?}",actor_id,barrier,remaining_actors);
                let exist = remaining_actors.remove(&actor_id);
                assert!(exist);
                self.may_notify(barrier.epoch.curr);
            }
            None => {
                self.map.insert(
                    barrier.epoch.curr,
                    ManagedBarrierStateInner::Stashed {
                        collected_actors: once(actor_id).collect(),
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
        collect_notifier: oneshot::Sender<CollectResult>,
    ) {
        match self.map.get(&barrier.epoch.curr) {
            Some(ManagedBarrierStateInner::Stashed { collected_actors }) => {
                // debug!("stash_barrier{:?},set{:?}",barrier,collected_actors);
                let remaining_actors = actor_ids_to_collect
                    .into_iter()
                    .filter(|a| !collected_actors.contains(a))
                    .collect();
                // debug!("stash_barrier{:?},set{:?}",barrier,remaining_actors);
                self.map.insert(
                    barrier.epoch.curr,
                    ManagedBarrierStateInner::Issued {
                        remaining_actors,
                        collect_notifier,
                    },
                );
                self.may_notify(barrier.epoch.curr);
            }
            Some(ManagedBarrierStateInner::Issued { .. }) => {
                panic!(
                    "barrier epochs{:?} state has already been `Issued`",
                    barrier.epoch
                )
            }
            None => {
                let remaining_actors = actor_ids_to_collect.into_iter().collect();
                // debug!("none_barrier{:?},set{:?}",barrier,remaining_actors);
                self.map.insert(
                    barrier.epoch.curr,
                    ManagedBarrierStateInner::Issued {
                        remaining_actors,
                        collect_notifier,
                    },
                );
                self.may_notify(barrier.epoch.curr);
            }
        }
    }
}
