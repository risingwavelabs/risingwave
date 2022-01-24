use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use itertools::Itertools;
use risingwave_common::error::Result;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use crate::executor::*;

struct ManagedBarrierState {
    epoch: u64,

    /// Notify that the collection is finished.
    collect_notifier: oneshot::Sender<()>,

    /// Actor ids remaining to be collected.
    remaining_actors: HashSet<u32>,
}

enum BarrierState {
    /// `Local` mode should be only used for tests. In this mode, barriers are not managed or
    /// collected, and there's no way to know whether or when a barrier is finished.
    Local,

    /// In `Managed` mode, barriers are sent and collected according to the request from meta
    /// service. When the barrier is finished, the caller can be notified about this.
    Managed(Option<ManagedBarrierState>),
}

/// [`LocalBarrierManager`] manages barrier control flow, used by local stream manager.
/// Specifically, [`LocalBarrierManager`] serve barrier injection from meta server, send the
/// barriers to and collect them from all actors, and finally report the progress.
pub struct LocalBarrierManager {
    /// Stores all materialized view source sender.
    senders: HashMap<u32, UnboundedSender<Message>>,

    /// Span of the current epoch.
    span: Option<tracing::Span>,

    /// Current barrier collection state.
    state: BarrierState,

    /// Last epoch of barriers.
    last_epoch: Option<u64>,
}

impl Default for LocalBarrierManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalBarrierManager {
    fn with_state(state: BarrierState) -> Self {
        Self {
            senders: HashMap::new(),
            span: None,
            state,
            last_epoch: None,
        }
    }

    /// Create a [`LocalBarrierManager`] with managed mode.
    pub fn new() -> Self {
        Self::with_state(BarrierState::Managed(None))
    }

    #[cfg(test)]
    pub fn for_test() -> Self {
        Self::with_state(BarrierState::Local)
    }

    /// Register sender for source actors, used to send barriers.
    pub fn register_sender(&mut self, actor_id: u32, sender: UnboundedSender<Message>) {
        debug!("register sender: {}", actor_id);
        self.senders.insert(actor_id, sender);
    }

    /// Broadcast a barrier to all senders. Returns a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    // TODO: async collect barrier flush state from hummock.
    pub fn send_barrier(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_collect: impl IntoIterator<Item = u32>,
    ) -> Result<Option<oneshot::Receiver<()>>> {
        let actor_ids_to_collect: HashSet<_> = actor_ids_to_collect.into_iter().collect();
        trace!(
            "send barrier {:?}, senders = {:?}, actor_ids_to_collect = {:?}",
            barrier,
            self.senders.keys(),
            actor_ids_to_collect
        );

        let rx = match &mut self.state {
            BarrierState::Local => None,

            BarrierState::Managed(state) => {
                // There should be only one epoch / barrier at a time.
                assert!(state.is_none());
                // There must be some actors to collect from.
                assert!(!actor_ids_to_collect.is_empty());

                let (tx, rx) = oneshot::channel();
                *state = Some(ManagedBarrierState {
                    epoch: barrier.epoch,
                    collect_notifier: tx,
                    remaining_actors: actor_ids_to_collect,
                });

                Some(rx)
            }
        };

        let mut barrier = barrier.clone();

        if ENABLE_BARRIER_EVENT {
            let receiver_ids = self.senders.keys().cloned().join(", ");
            // TODO: not a correct usage of span -- the span ends once it goes out of scope, but we
            // still have events in the background.
            let span = tracing::info_span!("send_barrier", epoch = barrier.epoch, mutation = ?barrier.mutation, receivers = %receiver_ids);
            barrier.span = Some(span);
        }

        for sender in self.senders.values() {
            sender.send(Message::Barrier(barrier.clone())).unwrap();
        }

        // Actors to stop should still accept this barrier, but won't get sent to in next times.
        if let Some(Mutation::Stop(actors)) = barrier.mutation.as_deref() {
            for actor in actors {
                self.senders.remove(actor);
            }
        }

        Ok(rx)
    }

    /// When a [`StreamConsumer`] (typically [`DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    pub fn collect(&mut self, actor_id: u32, barrier: &Barrier) -> Result<()> {
        match &mut self.state {
            BarrierState::Local => {}

            BarrierState::Managed(managed_state) => {
                let current_epoch = managed_state.as_ref().map(|s| s.epoch);
                let cmp = current_epoch
                    .map(|current_epoch| barrier.epoch.cmp(&current_epoch))
                    .unwrap_or(Ordering::Less);

                match cmp {
                    Ordering::Less => {
                        // TODO: there SHOULE BE no stale barriers in our system, this should panic.
                        warn!(
              "stale barrier with epoch {} from actor {}, while current epoch is {:?}, last epoch is {:?}",
              barrier.epoch, actor_id, current_epoch, self.last_epoch
            )
                    }
                    Ordering::Equal => {
                        let state = managed_state.as_mut().unwrap();
                        state.remaining_actors.remove(&actor_id);

                        trace!(
                            "collect barrier epoch {} from actor {}, remaining actors {:?}",
                            barrier.epoch,
                            actor_id,
                            state.remaining_actors
                        );

                        if state.remaining_actors.is_empty() {
                            let state = managed_state.take().unwrap();
                            self.last_epoch = Some(state.epoch);
                            // Notify about barrier finishing.
                            let tx = state.collect_notifier;
                            tx.send(()).unwrap();
                        }
                    }
                    Ordering::Greater => panic!(
            "collected barrier with a larger epoch {} from actor {}, while current epoch is {:?}",
            barrier.epoch, actor_id, current_epoch
          ),
                }
            }
        }

        Ok(())
    }

    /// Returns whether [`BarrierState`] is `Local`.
    pub fn is_local_mode(&self) -> bool {
        matches!(self.state, BarrierState::Local)
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    #[tokio::test]
    async fn test_managed_barrier_collection() -> Result<()> {
        let mut manager = LocalBarrierManager::new();
        assert!(!manager.is_local_mode());

        let register_sender = |actor_id: u32| {
            let (barrier_tx, barrier_rx) = unbounded_channel();
            manager.register_sender(actor_id, barrier_tx);
            (actor_id, barrier_rx)
        };

        // Register actors
        let actor_ids = vec![233, 234, 235];
        let count = actor_ids.len();
        let mut rxs = actor_ids
            .clone()
            .into_iter()
            .map(register_sender)
            .collect_vec();

        // Send a barrier to all actors
        let epoch = 114514;
        let barrier = Barrier::new(epoch);
        let mut collect_rx = manager.send_barrier(&barrier, actor_ids).unwrap().unwrap();

        // Collect barriers from actors
        let collected_barriers = rxs
            .iter_mut()
            .map(|(actor_id, rx)| {
                let msg = rx.try_recv().unwrap();
                let barrier = match msg {
                    Message::Barrier(b) => {
                        assert_eq!(b.epoch, epoch);
                        b
                    }
                    _ => unreachable!(),
                };
                (*actor_id, barrier)
            })
            .collect_vec();

        // Report to local barrier manager
        for (i, (actor_id, barrier)) in collected_barriers.into_iter().enumerate() {
            manager.collect(actor_id, &barrier).unwrap();
            let notified = collect_rx.try_recv().is_ok();
            assert_eq!(notified, i == count - 1);
        }

        Ok(())
    }
}
