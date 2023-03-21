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

use std::collections::{HashMap, HashSet};

use anyhow::anyhow;
use prometheus::HistogramTimer;
use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Receiver;

use self::managed_state::ManagedBarrierState;
use crate::error::{StreamError, StreamResult};
use crate::executor::*;
use crate::task::ActorId;

mod managed_state;
mod progress;
#[cfg(test)]
mod tests;

pub use progress::CreateMviewProgress;
use risingwave_common::bail;
use risingwave_storage::StateStoreImpl;

/// If enabled, all actors will be grouped in the same tracing span within one epoch.
/// Note that this option will significantly increase the overhead of tracing.
pub const ENABLE_BARRIER_AGGREGATION: bool = false;

/// Collect result of some barrier on current compute node. Will be reported to the meta service.
#[derive(Debug)]
pub struct CollectResult {
    pub create_mview_progress: Vec<PbCreateMviewProgress>,
}

enum BarrierState {
    /// `Local` mode should be only used for tests. In this mode, barriers are not managed or
    /// collected, and there's no way to know whether or when a barrier is finished.
    #[cfg(test)]
    Local,

    /// In `Managed` mode, barriers are sent and collected according to the request from meta
    /// service. When the barrier is finished, the caller can be notified about this.
    Managed(ManagedBarrierState),
}

/// [`LocalBarrierManager`] manages barrier control flow, used by local stream manager.
/// Specifically, [`LocalBarrierManager`] serve barrier injection from meta server, send the
/// barriers to and collect them from all actors, and finally report the progress.
pub struct LocalBarrierManager {
    /// Stores all streaming job source sender.
    senders: HashMap<ActorId, Vec<UnboundedSender<Barrier>>>,

    /// Span of the current epoch.
    #[expect(dead_code)]
    span: tracing::Span,

    /// Current barrier collection state.
    state: BarrierState,

    /// Save collect `CompleteReceiver`.
    collect_complete_receiver: HashMap<u64, CompleteReceiver>,
}

/// Information used after collection.
pub struct CompleteReceiver {
    /// Notify all actors of completion of collection.
    pub complete_receiver: Option<Receiver<StreamResult<CollectResult>>>,
    /// `barrier_inflight_timer`'s metrics.
    pub barrier_inflight_timer: Option<HistogramTimer>,
    /// Mark whether this is a checkpoint barrier.
    pub checkpoint: bool,
}

impl LocalBarrierManager {
    fn with_state(state: BarrierState) -> Self {
        Self {
            senders: HashMap::new(),
            span: tracing::Span::none(),
            state,
            collect_complete_receiver: HashMap::default(),
        }
    }

    /// Create a [`LocalBarrierManager`] with managed mode.
    pub fn new(state_store: StateStoreImpl) -> Self {
        Self::with_state(BarrierState::Managed(ManagedBarrierState::new(state_store)))
    }

    /// Register sender for source actors, used to send barriers.
    pub fn register_sender(&mut self, actor_id: ActorId, sender: UnboundedSender<Barrier>) {
        tracing::trace!(actor_id = actor_id, "register sender");
        self.senders.entry(actor_id).or_default().push(sender);
    }

    /// Return all senders.
    pub fn all_senders(&self) -> HashSet<ActorId> {
        self.senders.keys().cloned().collect()
    }

    /// Broadcast a barrier to all senders. Save a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    pub fn send_barrier(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
        timer: Option<HistogramTimer>,
    ) -> StreamResult<()> {
        let to_send = {
            let to_send: HashSet<ActorId> = actor_ids_to_send.into_iter().collect();
            match &self.state {
                #[cfg(test)]
                BarrierState::Local if to_send.is_empty() => self.senders.keys().cloned().collect(),
                _ => to_send,
            }
        };
        let to_collect: HashSet<ActorId> = actor_ids_to_collect.into_iter().collect();
        trace!(
            "send barrier {:?}, senders = {:?}, actor_ids_to_collect = {:?}",
            barrier,
            to_send,
            to_collect
        );

        let rx = match &mut self.state {
            #[cfg(test)]
            BarrierState::Local => None,

            BarrierState::Managed(state) => {
                // There must be some actors to collect from.
                assert!(!to_collect.is_empty());

                let (tx, rx) = oneshot::channel();
                state.transform_to_issued(barrier, to_collect, tx)?;
                Some(rx)
            }
        };

        for actor_id in to_send {
            match self.senders.get(&actor_id) {
                Some(senders) => {
                    for sender in senders {
                        if let Err(err) = sender.send(barrier.clone()) {
                            // return err to trigger recovery.
                            bail!("failed to send barrier to actor {}: {:?}", actor_id, err)
                        }
                    }
                }
                None => {
                    bail!("sender for actor {} does not exist", actor_id)
                }
            }
        }

        // Actors to stop should still accept this barrier, but won't get sent to in next times.
        if let Some(actors) = barrier.all_stop_actors() {
            trace!("remove actors {:?} from senders", actors);
            for actor in actors {
                self.senders.remove(actor);
            }
        }

        self.collect_complete_receiver.insert(
            barrier.epoch.prev,
            CompleteReceiver {
                complete_receiver: rx,
                barrier_inflight_timer: timer,
                checkpoint: barrier.checkpoint,
            },
        );
        Ok(())
    }

    /// Use `prev_epoch` to remove collect rx and return rx.
    pub fn remove_collect_rx(&mut self, prev_epoch: u64) -> StreamResult<CompleteReceiver> {
        // It's still possible that `collect_complete_receiver` does not contain the target epoch
        // when receiving collect_barrier request. Because `collect_complete_receiver` could
        // be cleared when CN is under recovering. We should return error rather than panic.
        self.collect_complete_receiver
            .remove(&prev_epoch)
            .ok_or_else(|| {
                anyhow!(
                    "barrier collect complete receiver for prev epoch {} not exists",
                    prev_epoch
                )
                .into()
            })
    }

    // remove all senders
    pub fn clear_senders(&mut self) {
        self.senders.clear();
    }

    /// remove all collect rx
    pub fn clear_collect_rx(&mut self) {
        self.collect_complete_receiver.clear();
        match &mut self.state {
            #[cfg(test)]
            BarrierState::Local => {}

            BarrierState::Managed(managed_state) => {
                managed_state.clear_all_states();
            }
        }
    }

    /// When a [`StreamConsumer`] (typically [`DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    pub fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) {
        match &mut self.state {
            #[cfg(test)]
            BarrierState::Local => {}

            BarrierState::Managed(managed_state) => {
                managed_state.collect(actor_id, barrier);
            }
        }
    }

    /// When a actor exit unexpectedly, it should report this event using this function, so meta
    /// will notice actor's exit while collecting.
    pub fn notify_failure(&mut self, actor_id: ActorId, err: StreamError) {
        match &mut self.state {
            #[cfg(test)]
            BarrierState::Local => {}

            BarrierState::Managed(managed_state) => {
                managed_state.notify_failure(actor_id, err);
            }
        }
    }
}

#[cfg(test)]
impl LocalBarrierManager {
    pub fn for_test() -> Self {
        Self::with_state(BarrierState::Local)
    }

    /// Returns whether [`BarrierState`] is `Local`.
    pub fn is_local_mode(&self) -> bool {
        !matches!(self.state, BarrierState::Managed(_))
    }
}
