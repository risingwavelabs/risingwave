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

use std::sync::Arc;

use super::{BarrierState, LocalBarrierManager};
use crate::task::{ActorId, SharedContext};

type ConsumedEpoch = u64;
type ConsumedRows = u64;

#[derive(Debug, Clone, Copy)]
pub(super) enum ChainState {
    ConsumingUpstream(ConsumedEpoch, ConsumedRows),
    Done(ConsumedRows),
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
///
/// Progress can be computed by
/// `total_rows_consumed` / `total_rows_upstream`.
/// This yields the (approximate) percentage of rows we are done backfilling.
///
/// For `total_rows_consumed`, the progress is tracked in the following way:
/// 1. Fetching the row count from our state table.
///    This number is the total number, NOT incremental.
///    This is done per actor.
/// 2. Refreshing this number on the meta side, on every barrier.
///    This is done by just summing up all the row counts from the actors.
///
/// For `total_rows_upstream`,
/// this is fetched from `HummockVersion`'s statistics (`TableStats::total_key_count`).
///
/// This is computed per `HummockVersion`, which is updated whenever a checkpoint is committed.
/// The `total_key_count` figure just counts the number of storage keys.
/// For example, if a key is inserted and then deleted,
/// it results two storage entries in `LSMt`, so count=2.
/// Only after compaction, the count will drop back to 0.
///
/// So the total count could be more pessimistic, than actual progress.
///
/// It is fine for this number not to be precise,
/// since we don't rely on it to update the status of a stream job internally.
///
/// TODO(kwannoel): Perhaps it is possible to get total key count of the replicated state table
/// for arrangement backfill. We can use that to estimate the progress as well, and avoid recording
/// `row_count` state for it.
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

    /// Update the progress to `ConsumingUpstream(consumed_epoch, consumed_rows)`. The epoch must be
    /// monotonically increasing.
    /// `current_epoch` should be provided to locate the barrier under concurrent checkpoint.
    /// `current_consumed_rows` is an accumulated value.
    pub fn update(
        &mut self,
        current_epoch: u64,
        consumed_epoch: ConsumedEpoch,
        current_consumed_rows: ConsumedRows,
    ) {
        match self.state {
            Some(ChainState::ConsumingUpstream(last, last_consumed_rows)) => {
                assert!(
                    last < consumed_epoch,
                    "last_epoch: {:#?} must be greater than consumed epoch: {:#?}",
                    last,
                    consumed_epoch
                );
                assert!(last_consumed_rows <= current_consumed_rows);
            }
            Some(ChainState::Done(_)) => unreachable!(),
            None => {}
        };
        self.update_inner(
            current_epoch,
            ChainState::ConsumingUpstream(consumed_epoch, current_consumed_rows),
        );
    }

    /// Finish the progress. If the progress is already finished, then perform no-op.
    /// `current_epoch` should be provided to locate the barrier under concurrent checkpoint.
    pub fn finish(&mut self, current_epoch: u64, current_consumed_rows: ConsumedRows) {
        if let Some(ChainState::Done(_)) = self.state {
            return;
        }
        self.update_inner(current_epoch, ChainState::Done(current_consumed_rows));
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
