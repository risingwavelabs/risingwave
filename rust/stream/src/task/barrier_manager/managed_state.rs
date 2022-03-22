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
//
use std::collections::HashSet;
use std::iter::once;

use super::{BarrierCollectResult, BarrierCollectTx};
use crate::executor::Barrier;
use crate::task::ActorId;

#[derive(Debug)]
pub(super) enum ManagedBarrierState {
    /// Currently no barrier on the flight.
    Pending {
        /// Last epoch of barriers.
        // TODO: an initial value should be specified
        last_epoch: Option<u64>,
    },

    /// Barriers from some actors have been collected and stashed, however no `send_barrier`
    /// request from the meta service is issued.
    Stashed {
        epoch: u64,

        /// Actor ids we've collected and stashed.
        collected_actors: HashSet<ActorId>,
    },

    /// Meta service has issued a `send_barrier` request. We're collecting barriers now.
    Issued {
        epoch: u64,

        /// Actor ids remaining to be collected.
        remaining_actors: HashSet<ActorId>,

        /// Notify that the collection is finished.
        collect_notifier: BarrierCollectTx,
    },
}

impl ManagedBarrierState {
    /// Take the notifier back if we have collected barriers from all actor ids.
    /// The state must be `Issued`.
    #[must_use]
    fn may_finish_collecting(&mut self) -> Option<BarrierCollectTx> {
        let (epoch, to_notify) = match self {
            ManagedBarrierState::Issued {
                epoch,
                remaining_actors,
                ..
            } => (*epoch, remaining_actors.is_empty()),

            _ => unreachable!(),
        };

        if to_notify {
            let state = std::mem::replace(
                self,
                ManagedBarrierState::Pending {
                    last_epoch: Some(epoch),
                },
            );

            match state {
                ManagedBarrierState::Issued {
                    collect_notifier, ..
                } => Some(collect_notifier),

                _ => unreachable!(),
            }
        } else {
            None
        }
    }

    /// Collect a `barrier` from the actor with `actor_id`.
    ///
    /// Returns the notifier if we've finished collecting.
    #[must_use]
    pub(super) fn collect(
        &mut self,
        actor_id: ActorId,
        barrier: &Barrier,
    ) -> Option<BarrierCollectTx> {
        tracing::trace!(
            target: "events::stream::barrier::collect_barrier",
            "collect_barrier: epoch = {}, actor_id = {}, state = {:#?}",
            barrier.current_epoch(),
            actor_id,
            self
        );

        match self {
            ManagedBarrierState::Pending { last_epoch } => {
                if let Some(last_epoch) = *last_epoch {
                    assert_eq!(barrier.epoch.prev, last_epoch)
                }
                *self = Self::Stashed {
                    epoch: barrier.current_epoch(),
                    collected_actors: once(actor_id).collect(),
                };

                None
            }

            ManagedBarrierState::Stashed {
                epoch,
                collected_actors,
            } => {
                assert_eq!(barrier.current_epoch(), *epoch);
                let new = collected_actors.insert(actor_id);
                assert!(new);

                None
            }

            ManagedBarrierState::Issued {
                epoch,
                remaining_actors,
                ..
            } => {
                assert_eq!(barrier.current_epoch(), *epoch);
                let exist = remaining_actors.remove(&actor_id);
                assert!(exist);

                self.may_finish_collecting()
            }
        }
    }

    /// When the meta service issues a `send_barrier` request, call this function to transform to
    /// `Issued` and start to collect or to notify.
    ///
    /// Returns the notifier if we've finished collecting.
    #[must_use]
    pub(super) fn transform_to_issued(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
        collect_notifier: BarrierCollectTx,
    ) -> Option<BarrierCollectTx> {
        match self {
            ManagedBarrierState::Pending { .. } => {
                let remaining_actors = actor_ids_to_collect.into_iter().collect();

                *self = Self::Issued {
                    epoch: barrier.current_epoch(),
                    remaining_actors,
                    collect_notifier,
                };

                self.may_finish_collecting()
            }

            ManagedBarrierState::Stashed {
                epoch,
                collected_actors,
            } => {
                assert_eq!(barrier.current_epoch(), *epoch);

                let remaining_actors = actor_ids_to_collect
                    .into_iter()
                    .filter(|a| !collected_actors.contains(a))
                    .collect();

                *self = Self::Issued {
                    epoch: barrier.current_epoch(),
                    remaining_actors,
                    collect_notifier,
                };

                self.may_finish_collecting()
            }

            ManagedBarrierState::Issued { .. } => panic!("barrier state has already been `Issued`"),
        }
    }
}
