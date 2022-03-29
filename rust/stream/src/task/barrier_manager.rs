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

use risingwave_common::error::Result;
use risingwave_pb::stream_service::inject_barrier_response::FinishedDdl as ProstFinishedDdl;
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
pub struct FinishedDdl {
    pub epoch: u64,

    pub actor_id: ActorId,
}

impl From<FinishedDdl> for ProstFinishedDdl {
    fn from(f: FinishedDdl) -> Self {
        Self {
            epoch: f.epoch,
            actor_id: f.actor_id,
        }
    }
}

pub type DdlFinishNotifierTx = oneshot::Sender<u64>;
pub type DdlFinishNotifierRx = oneshot::Receiver<u64>;

#[derive(Debug)]
pub struct CollectResult {
    pub finished_ddls: Vec<FinishedDdl>,
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
    /// Stores all materialized view source sender.
    senders: HashMap<ActorId, UnboundedSender<Message>>,

    /// Span of the current epoch.
    #[allow(dead_code)]
    span: tracing::Span,

    /// Current barrier collection state.
    state: BarrierState,
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
            span: tracing::Span::none(),
            state,
        }
    }

    /// Create a [`LocalBarrierManager`] with managed mode.
    pub fn new() -> Self {
        Self::with_state(BarrierState::Managed(ManagedBarrierState::new()))
    }

    /// Register sender for source actors, used to send barriers.
    pub fn register_sender(&mut self, actor_id: ActorId, sender: UnboundedSender<Message>) {
        debug!("register sender: {}", actor_id);
        self.senders.insert(actor_id, sender);
    }

    /// Broadcast a barrier to all senders. Returns a receiver which will get notified when this
    /// barrier is finished, in managed mode.
    pub fn send_barrier(
        &mut self,
        barrier: &Barrier,
        actor_ids_to_send: impl IntoIterator<Item = ActorId>,
        actor_ids_to_collect: impl IntoIterator<Item = ActorId>,
    ) -> Result<Option<oneshot::Receiver<CollectResult>>> {
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
                state.transform_to_issued(barrier, to_collect, tx);
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
                managed_state.collect(actor_id, barrier);
            }
        }

        Ok(())
    }

    pub fn finish_ddl(&mut self, ddl_epoch: u64, actor_id: ActorId) {
        match &mut self.state {
            #[cfg(test)]
            BarrierState::Local => {}

            BarrierState::Managed(managed_state) => managed_state.finished_ddls.push(FinishedDdl {
                epoch: ddl_epoch,
                actor_id,
            }),
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
