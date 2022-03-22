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
use std::collections::{HashMap, HashSet};

use risingwave_common::error::Result;
use risingwave_pb::stream_service::inject_barrier_response::Finished;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

use self::managed_state::ManagedBarrierState;
use crate::executor::*;
use crate::task::ActorId;

mod managed_state;
#[cfg(test)]
mod tests;

/// If enabled, all actors will be grouped in the same tracing span within one epoch.
/// Note that this option will significantly increase the overhead of tracing.
pub const ENABLE_BARRIER_AGGREGATION: bool = false;

#[derive(Debug)]
pub struct BarrierCollectResult {
    /// Finished commands during this epoch.
    pub finished: Vec<Finished>,
}

pub type BarrierCollectTx = oneshot::Sender<BarrierCollectResult>;
pub type BarrierCollectRx = oneshot::Receiver<BarrierCollectResult>;

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
    /// Stores all materialized view source sender.
    senders: HashMap<ActorId, UnboundedSender<Message>>,

    /// Span of the current epoch.
    #[allow(dead_code)]
    span: tracing::Span,

    /// Current barrier collection state.
    state: BarrierState,

    finished: Vec<Finished>,
}

impl Default for LocalBarrierManager {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalBarrierManager {
    fn with_state(state: BarrierState) -> Self {
        Self {
            senders: Default::default(),
            span: tracing::Span::none(),
            state,
            finished: Default::default(),
        }
    }

    /// Create a [`LocalBarrierManager`] with managed mode.
    pub fn new() -> Self {
        Self::with_state(BarrierState::Managed(ManagedBarrierState::Pending {
            last_epoch: None, // TODO: specify last epoch
        }))
    }

    /// Register sender for source actors, used to send barriers.
    pub fn register_sender(&mut self, actor_id: ActorId, sender: UnboundedSender<Message>) {
        debug!("register sender: {}", actor_id);
        self.senders.insert(actor_id, sender);
    }

    /// Broadcast a barrier to all senders. Returns a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    // TODO: async collect barrier flush state from hummock.
    pub fn send_barrier(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> Result<Option<BarrierCollectRx>> {
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
                if let Some(tx) = state.transform_to_issued(barrier, to_collect, tx) {
                    self.notify(tx);
                }
                Some(rx)
            }
        };

        for actor_id in to_send {
            let sender = self
                .senders
                .get(&actor_id)
                .unwrap_or_else(|| panic!("sender for actor {} does not exist", actor_id));
            sender.send(Message::Barrier(barrier.clone())).unwrap();
        }

        // Actors to stop should still accept this barrier, but won't get sent to in next times.
        if let Some(Mutation::Stop(actors)) = barrier.mutation.as_deref() {
            for actor in actors {
                trace!("remove actor {} from senders", actor);
                self.senders.remove(actor);
            }
        }

        Ok(rx)
    }

    /// When a [`StreamConsumer`] (typically [`DispatchExecutor`]) get a barrier, it should report
    /// and collect this barrier with its own `actor_id` using this function.
    pub fn collect(&mut self, actor_id: ActorId, barrier: &Barrier) -> Result<()> {
        match &mut self.state {
            #[cfg(test)]
            BarrierState::Local => {}

            BarrierState::Managed(managed_state) => {
                if let Some(tx) = managed_state.collect(actor_id, barrier) {
                    self.notify(tx);
                }
            }
        }

        Ok(())
    }

    /// Finish the command with given `epoch` for this `actor_id`.
    pub fn finish(&mut self, epoch: u64, actor_id: ActorId) -> Result<()> {
        self.finished.push(Finished { epoch, actor_id });
        Ok(())
    }

    /// Notify about barrier collection finishing.
    fn notify(&mut self, tx: BarrierCollectTx) {
        let result = BarrierCollectResult {
            finished: std::mem::take(&mut self.finished),
        };

        if tx.send(result).is_err() {
            warn!("failed to notify barrier collection with epoch");
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
