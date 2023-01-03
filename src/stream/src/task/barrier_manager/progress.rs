// Copyright 2023 Singularity Data
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

use std::sync::Arc;

use super::{BarrierState, LocalBarrierManager};
use crate::task::{ActorId, SharedContext};

type ConsumedEpoch = u64;

#[derive(Debug, Clone, Copy)]
pub(super) enum ChainState {
    ConsumingUpstream(ConsumedEpoch),
    Done,
}

impl LocalBarrierManager {
    fn update_create_mview_progress(
        &mut self,
        current_epoch: u64,
        actor: ActorId,
        state: ChainState,
    ) {
        match &mut self.state {
            #[cfg(test)]
            BarrierState::Local => {}

            BarrierState::Managed(managed_state) => {
                managed_state
                    .create_mview_progress
                    .entry(current_epoch)
                    .or_default()
                    .insert(actor, state);
            }
        }
    }
}

/// The progress held by the chain executors to report to the local barrier manager.
pub struct CreateMviewProgress {
    barrier_manager: Arc<parking_lot::Mutex<LocalBarrierManager>>,

    /// The id of the actor containing the chain node.
    chain_actor_id: ActorId,

    state: Option<ChainState>,
}

impl CreateMviewProgress {
    pub fn new(
        barrier_manager: Arc<parking_lot::Mutex<LocalBarrierManager>>,
        chain_actor_id: ActorId,
    ) -> Self {
        Self {
            barrier_manager,
            chain_actor_id,
            state: None,
        }
    }

    #[cfg(test)]
    pub fn for_test(barrier_manager: Arc<parking_lot::Mutex<LocalBarrierManager>>) -> Self {
        Self::new(barrier_manager, 0)
    }

    pub fn actor_id(&self) -> u32 {
        self.chain_actor_id
    }

    fn update_inner(&mut self, current_epoch: u64, state: ChainState) {
        self.state = Some(state);
        self.barrier_manager.lock().update_create_mview_progress(
            current_epoch,
            self.chain_actor_id,
            state,
        );
    }

    /// Update the progress to `ConsumingUpstream(consumed_epoch)`. The epoch must be monotonically
    /// increasing.
    /// `current_epoch` should be provided to locate the barrier under concurrent checkpoint.
    pub fn update(&mut self, current_epoch: u64, consumed_epoch: ConsumedEpoch) {
        match self.state {
            Some(ChainState::ConsumingUpstream(last)) => {
                assert!(last < consumed_epoch);
            }
            Some(ChainState::Done) => unreachable!(),
            None => {}
        }
        self.update_inner(current_epoch, ChainState::ConsumingUpstream(consumed_epoch));
    }

    /// Finish the progress. If the progress is already finished, then perform no-op.
    /// `current_epoch` should be provided to locate the barrier under concurrent checkpoint.
    pub fn finish(&mut self, current_epoch: u64) {
        if let Some(ChainState::Done) = self.state {
            return;
        }
        self.update_inner(current_epoch, ChainState::Done);
    }
}

impl SharedContext {
    /// Create a struct for reporting the progress of creating mview. The chain executors should
    /// report the progress of barrier rearranging continuously using this. The updated progress
    /// will be collected by the local barrier manager and reported to the meta service in this
    /// epoch.
    ///
    /// When all chain executors of the creating mview finish, the creation progress will be done at
    /// frontend and the mview will be exposed to the user.
    pub fn register_create_mview_progress(&self, chain_actor_id: ActorId) -> CreateMviewProgress {
        trace!("register create mview progress: {}", chain_actor_id);
        CreateMviewProgress::new(self.barrier_manager.clone(), chain_actor_id)
    }
}
