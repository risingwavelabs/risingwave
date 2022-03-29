use std::collections::{HashMap, HashSet};

use risingwave_common::error::{Result, RwError};
use tokio::sync::oneshot;

use crate::model::ActorId;

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

#[derive(Default)]
pub(super) struct UnfinishedNotifiers(HashMap<u64, (HashSet<ActorId>, Vec<Notifier>)>);

impl UnfinishedNotifiers {
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
            let old = self
                .0
                .insert(epoch, (actor_ids, notifiers.into_iter().collect()));

            assert!(old.is_none());
        }
    }

    pub fn finish_actors(&mut self, epoch: u64, actors: impl IntoIterator<Item = ActorId>) {
        use std::collections::hash_map::Entry;

        tracing::debug!("finish actors for epoch {}", epoch);

        match self.0.entry(epoch) {
            Entry::Occupied(mut o) => {
                actors.into_iter().for_each(|a| {
                    o.get_mut().0.remove(&a);
                });

                // All actors finished.
                if o.get().0.is_empty() {
                    tracing::debug!("finish all for epoch {}", epoch);

                    let notifiers = o.remove().1;
                    notifiers.into_iter().for_each(Notifier::notify_finished);
                }
            }

            Entry::Vacant(_) => todo!("handle finish report after meta recovery"),
        }
    }
}
