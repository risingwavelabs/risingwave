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

use risingwave_common::error::{Result, RwError};
use tokio::sync::oneshot;

use crate::model::ActorId;

/// Used for notifying the status of a scheduled command/barrier.
#[derive(Debug, Default)]
pub(super) struct Notifier {
    /// Get notified when scheduled barrier is about to send.
    pub to_send: Option<oneshot::Sender<()>>,

    /// Get notified when scheduled barrier is collected or failed.
    pub collected: Option<oneshot::Sender<Result<()>>>,

    /// Get notified when scheduled barrier is finished.
    pub finished: Option<oneshot::Sender<()>>,
}

impl Notifier {
    /// Notify when we are about to send a barrier.
    pub fn notify_to_send(&mut self) {
        if let Some(tx) = self.to_send.take() {
            tx.send(()).ok();
        }
    }

    /// Notify when we have collected a barrier from all actors.
    pub fn notify_collected(&mut self) {
        if let Some(tx) = self.collected.take() {
            tx.send(Ok(())).ok();
        }
    }

    /// Notify when we failed to collect a barrier. This function consumes `self`.
    pub fn notify_collection_failed(self, err: RwError) {
        if let Some(tx) = self.collected {
            tx.send(Err(err)).ok();
        }
    }

    /// Notify when we have finished a barrier from all actors. This function consumes `self`.
    ///
    /// Generally when a barrier is collected, it's also finished since it does not require further
    /// report of finishing from actors.
    /// However for creating MV, this is only called when all `Chain` report it finished.
    pub fn notify_finished(self) {
        if let Some(tx) = self.finished {
            tx.send(()).ok();
        }
    }
}

/// Stores the notifiers for commands that are not finished yet. Essentially for
/// `CreateMaterializedView`.
#[derive(Default)]
pub(super) struct UnfinishedNotifiers(HashMap<u64, (HashSet<ActorId>, Vec<Notifier>)>);

impl UnfinishedNotifiers {
    /// Add a command with current `epoch` and `notifiers`, that needs to wait for actors with
    /// `actor_ids` to report finishing.
    /// If `actor_ids` is empty, [`Notifier::notify_finished`] will be called immediately.
    pub fn add(
        &mut self,
        epoch: u64,
        actor_ids: impl IntoIterator<Item = ActorId>,
        notifiers: impl IntoIterator<Item = Notifier>,
    ) {
        let actor_ids: HashSet<_> = actor_ids.into_iter().collect();

        if actor_ids.is_empty() {
            // The barrier can be finished immediately.
            notifiers.into_iter().for_each(Notifier::notify_finished);
        } else {
            tracing::debug!(
                "actors to be finished for DDL with epoch {}: {:?}",
                epoch,
                actor_ids
            );

            let notifiers = notifiers.into_iter().collect();
            let old = self.0.insert(epoch, (actor_ids, notifiers));
            assert!(old.is_none());
        }
    }

    /// Tell that the command with `epoch` has been reported to be finished on given `actors`. If
    /// we've finished on all actors, [`Notifier::notify_finished`] will be called.
    pub fn finish_actors(&mut self, epoch: u64, actors: impl IntoIterator<Item = ActorId>) {
        use std::collections::hash_map::Entry;

        match self.0.entry(epoch) {
            Entry::Occupied(mut o) => {
                actors.into_iter().for_each(|a| {
                    tracing::debug!("finish actor {} for DDL with epoch {}", a, epoch);
                    o.get_mut().0.remove(&a);
                });

                // All actors finished.
                if o.get().0.is_empty() {
                    tracing::debug!("finish all actors for DDL with epoch {}!", epoch);

                    let notifiers = o.remove().1;
                    notifiers.into_iter().for_each(Notifier::notify_finished);
                }
            }

            Entry::Vacant(_) => todo!("handle finish report after meta recovery"),
        }
    }
}
