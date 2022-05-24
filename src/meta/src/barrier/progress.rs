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

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use itertools::Itertools;

use super::notifier::Notifier;
use crate::model::ActorId;

type Epoch = u64;
type CreateMviewEpoch = Epoch;

enum ActorState {
    ConsumingSnapshot,
    ConsumingUpstream(Epoch),
    Done,
}

struct Progress {
    states: HashMap<ActorId, ActorState>,

    done_count: usize,
}

impl Progress {
    fn new(actors: impl IntoIterator<Item = ActorId>) -> Self {
        let states = actors
            .into_iter()
            .map(|a| (a, ActorState::ConsumingSnapshot))
            .collect::<HashMap<_, _>>();
        assert!(!states.is_empty());

        Self {
            states,
            done_count: 0,
        }
    }

    fn update(&mut self, actor: ActorId, consumed_epoch: Epoch, current_epoch: Epoch) {
        match self.states.get_mut(&actor).unwrap() {
            state @ (ActorState::ConsumingSnapshot | ActorState::ConsumingUpstream(_)) => {
                if consumed_epoch == current_epoch {
                    *state = ActorState::Done;
                    self.done_count += 1;
                } else {
                    *state = ActorState::ConsumingUpstream(consumed_epoch);
                }
            }
            ActorState::Done => panic!("should not report progress after done"),
        }
    }

    fn is_done(&self) -> bool {
        self.done_count == self.states.len()
    }

    fn actors(&self) -> impl Iterator<Item = ActorId> + '_ {
        self.states.keys().cloned()
    }
}

/// Stores the notifiers for commands that are not finished yet. Essentially for
/// `CreateMaterializedView`.
#[derive(Default)]
pub(super) struct CreateMviewProgressTracker {
    progress_map: HashMap<CreateMviewEpoch, (Progress, Vec<Notifier>)>,

    actor_map: HashMap<ActorId, CreateMviewEpoch>,
}

impl CreateMviewProgressTracker {
    /// Add a command with current `epoch` and `notifiers`, that needs to wait for actors with
    /// `actors` to report finishing.
    /// If `actors` is empty, [`Notifier::notify_finished`] will be called immediately.
    pub fn add(
        &mut self,
        ddl_epoch: Epoch,
        actors: impl IntoIterator<Item = ActorId>,
        notifiers: impl IntoIterator<Item = Notifier>,
    ) {
        let actors = actors.into_iter().collect_vec();
        if actors.is_empty() {
            // The barrier can be finished immediately.
            notifiers.into_iter().for_each(Notifier::notify_finished);
            return;
        }
        // tracing::debug!(
        //     "actors to be finished for DDL with epoch {}: {:?}",
        //     ddl_epoch,
        //     actor_ids
        // );

        for &actor in &actors {
            self.actor_map.insert(actor, ddl_epoch);
        }

        let progress = Progress::new(actors);
        let notifiers = notifiers.into_iter().collect();
        let old = self.progress_map.insert(ddl_epoch, (progress, notifiers));
        assert!(old.is_none());
    }

    pub fn update(&mut self, actor: ActorId, consumed_epoch: Epoch, current_epoch: Epoch) {
        let epoch = self.actor_map.get(&actor).cloned().unwrap_or_else(|| {
            panic!(
                "bad actor {} to update progress, are we after meta recovery?",
                actor
            )
        });

        match self.progress_map.entry(epoch) {
            Entry::Occupied(mut o) => {
                let progress = &mut o.get_mut().0;
                progress.update(actor, consumed_epoch, current_epoch);

                if progress.is_done() {
                    tracing::debug!("all actors done for creating mview with epoch {}!", epoch);

                    // Clean-up the mapping from actors to DDL epoch.
                    for actor in o.get().0.actors() {
                        self.actor_map.remove(&actor);
                    }
                    // Notify about finishing.
                    let notifiers = o.remove().1;
                    notifiers.into_iter().for_each(Notifier::notify_finished);
                }
            }
            Entry::Vacant(_) => unreachable!(),
        }
    }
}
