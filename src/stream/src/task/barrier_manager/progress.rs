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

use std::sync::Arc;

use super::{BarrierState, LocalBarrierManager};
use crate::task::{ActorId, SharedContext};

pub(super) type ConsumedEpoch = u64;

impl LocalBarrierManager {
    pub fn update_create_mview_progress(&mut self, actor: ActorId, consumed_epoch: ConsumedEpoch) {
        match &mut self.state {
            #[cfg(test)]
            BarrierState::Local => {}

            BarrierState::Managed(managed_state) => {
                managed_state
                    .create_mview_progress
                    .insert(actor, consumed_epoch);
            }
        }
    }
}

pub struct CreateMviewProgress {
    barrier_manager: Arc<parking_lot::Mutex<LocalBarrierManager>>,

    chain_actor_id: ActorId,

    last_consumed_epoch: Option<ConsumedEpoch>,
}

impl CreateMviewProgress {
    #[cfg(test)]
    pub fn for_test(barrier_manager: Arc<parking_lot::Mutex<LocalBarrierManager>>) -> Self {
        Self {
            barrier_manager,
            chain_actor_id: 0,
            last_consumed_epoch: None,
        }
    }

    pub fn update(&mut self, consumed_epoch: ConsumedEpoch) {
        assert!(self.last_consumed_epoch.unwrap_or_default() < consumed_epoch);
        self.last_consumed_epoch = Some(consumed_epoch);

        self.barrier_manager
            .lock()
            .update_create_mview_progress(self.chain_actor_id, consumed_epoch);
    }

    pub fn finish(self, consumed_epoch: ConsumedEpoch) {
        assert!(self.last_consumed_epoch.unwrap_or_default() < consumed_epoch);

        self.barrier_manager
            .lock()
            .update_create_mview_progress(self.chain_actor_id, consumed_epoch);
    }

    pub fn actor_id(&self) -> u32 {
        self.chain_actor_id
    }
}

impl SharedContext {
    // /// Create a notifier for Create MV DDL finish. When an executor/actor (essentially a
    // /// [`crate::executor::ChainExecutor`]) finishes its DDL job, it can report that using this
    // /// notifier. Note that a DDL of MV always corresponds to an epoch in our system.
    // ///
    // /// Creation of an MV may last for several epochs to finish.
    // /// Therefore, when the [`crate::executor::ChainExecutor`] finds that the creation is
    // /// finished, it will send the DDL epoch using this notifier, which can be collected by the
    // /// barrier manager and reported to the meta service soon.
    // pub fn register_finish_create_mview_notifier(
    //     &self,
    //     actor_id: ActorId,
    // ) -> FinishCreateMviewNotifier {
    //     debug!("register finish create mview notifier: {}", actor_id);

    //     let barrier_manager = self.barrier_manager.clone();
    //     FinishCreateMviewNotifier {
    //         barrier_manager,
    //         actor_id,
    //     }
    // }

    pub fn register_create_mview_progress(&self, chain_actor_id: ActorId) -> CreateMviewProgress {
        debug!("register create mview progress: {}", chain_actor_id);

        let barrier_manager = self.barrier_manager.clone();
        CreateMviewProgress {
            barrier_manager,
            chain_actor_id,
            last_consumed_epoch: None,
        }
    }
}

/// To notify about the finish of an DDL with the `u64` epoch.
pub struct FinishCreateMviewNotifier {
    pub barrier_manager: Arc<parking_lot::Mutex<LocalBarrierManager>>,
    pub actor_id: ActorId,
}

impl FinishCreateMviewNotifier {
    pub fn notify(self, ddl_epoch: u64) {
        todo!()
    }
}

impl std::fmt::Debug for FinishCreateMviewNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FinishCreateMviewNotifier")
            .field("actor_id", &self.actor_id)
            .finish_non_exhaustive()
    }
}
