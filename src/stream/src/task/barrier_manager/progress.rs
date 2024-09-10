// Copyright 2024 RisingWave Labs
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

use std::fmt::{Display, Formatter};

use risingwave_common::util::epoch::EpochPair;
use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;

use super::LocalBarrierManager;
use crate::task::barrier_manager::LocalBarrierEvent::ReportCreateProgress;
use crate::task::barrier_manager::LocalBarrierWorker;
use crate::task::ActorId;

type ConsumedEpoch = u64;
type ConsumedRows = u64;

#[derive(Debug, Clone, Copy)]
pub(crate) enum BackfillState {
    ConsumingUpstream(ConsumedEpoch, ConsumedRows),
    Done(ConsumedRows),
}

impl BackfillState {
    pub fn to_pb(self, actor_id: ActorId) -> PbCreateMviewProgress {
        PbCreateMviewProgress {
            backfill_actor_id: actor_id,
            done: matches!(self, BackfillState::Done(_)),
            consumed_epoch: match self {
                BackfillState::ConsumingUpstream(consumed_epoch, _) => consumed_epoch,
                BackfillState::Done(_) => 0, // unused field for done
            },
            consumed_rows: match self {
                BackfillState::ConsumingUpstream(_, consumed_rows) => consumed_rows,
                BackfillState::Done(consumed_rows) => consumed_rows,
            },
        }
    }
}

impl Display for BackfillState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillState::ConsumingUpstream(epoch, rows) => {
                write!(f, "ConsumingUpstream(epoch: {}, rows: {})", epoch, rows)
            }
            BackfillState::Done(rows) => write!(f, "Done(rows: {})", rows),
        }
    }
}

impl LocalBarrierWorker {
    pub(crate) fn update_create_mview_progress(
        &mut self,
        epoch: EpochPair,
        actor: ActorId,
        state: BackfillState,
    ) {
        if let Some(actor_state) = self.state.actor_states.get(&actor)
            && let Some(partial_graph_id) = actor_state.inflight_barriers.get(&epoch.prev)
            && let Some(graph_state) = self.state.graph_states.get_mut(partial_graph_id)
        {
            graph_state
                .create_mview_progress
                .entry(epoch.curr)
                .or_default()
                .insert(actor, state);
        } else {
            warn!(?epoch, actor, ?state, "ignore create mview progress");
        }
    }
}

impl LocalBarrierManager {
    fn update_create_mview_progress(&self, epoch: EpochPair, actor: ActorId, state: BackfillState) {
        self.send_event(ReportCreateProgress {
            epoch,
            actor,
            state,
        })
    }
}

/// The progress held by the backfill executors to report to the local barrier manager.
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
pub struct CreateMviewProgressReporter {
    barrier_manager: LocalBarrierManager,

    /// The id of the actor containing the backfill executors.
    backfill_actor_id: ActorId,

    state: Option<BackfillState>,
}

impl CreateMviewProgressReporter {
    pub fn new(barrier_manager: LocalBarrierManager, backfill_actor_id: ActorId) -> Self {
        Self {
            barrier_manager,
            backfill_actor_id,
            state: None,
        }
    }

    #[cfg(test)]
    pub fn for_test(barrier_manager: LocalBarrierManager) -> Self {
        Self::new(barrier_manager, 0)
    }

    pub fn actor_id(&self) -> u32 {
        self.backfill_actor_id
    }

    fn update_inner(&mut self, epoch: EpochPair, state: BackfillState) {
        self.state = Some(state);
        self.barrier_manager
            .update_create_mview_progress(epoch, self.backfill_actor_id, state);
    }

    /// Update the progress to `ConsumingUpstream(consumed_epoch, consumed_rows)`. The epoch must be
    /// monotonically increasing.
    /// `current_epoch` should be provided to locate the barrier under concurrent checkpoint.
    /// `current_consumed_rows` is an accumulated value.
    pub fn update(
        &mut self,
        epoch: EpochPair,
        consumed_epoch: ConsumedEpoch,
        current_consumed_rows: ConsumedRows,
    ) {
        match self.state {
            Some(BackfillState::ConsumingUpstream(last, last_consumed_rows)) => {
                assert!(
                    last < consumed_epoch,
                    "last_epoch: {:#?} must be greater than consumed epoch: {:#?}",
                    last,
                    consumed_epoch
                );
                assert!(last_consumed_rows <= current_consumed_rows);
            }
            Some(BackfillState::Done(_)) => unreachable!(),
            None => {}
        };
        self.update_inner(
            epoch,
            BackfillState::ConsumingUpstream(consumed_epoch, current_consumed_rows),
        );
    }

    /// Finish the progress. If the progress is already finished, then perform no-op.
    /// `current_epoch` should be provided to locate the barrier under concurrent checkpoint.
    pub fn finish(&mut self, epoch: EpochPair, current_consumed_rows: ConsumedRows) {
        if let Some(BackfillState::Done(_)) = self.state {
            return;
        }
        self.update_inner(epoch, BackfillState::Done(current_consumed_rows));
    }
}

impl LocalBarrierManager {
    /// Create a struct for reporting the progress of creating mview. The backfill executors should
    /// report the progress of barrier rearranging continuously using this. The updated progress
    /// will be collected by the local barrier manager and reported to the meta service in this
    /// epoch.
    ///
    /// When all backfill executors of the creating mview finish, the creation progress will be done at
    /// frontend and the mview will be exposed to the user.
    pub fn register_create_mview_progress(
        &self,
        backfill_actor_id: ActorId,
    ) -> CreateMviewProgressReporter {
        trace!("register create mview progress: {}", backfill_actor_id);
        CreateMviewProgressReporter::new(self.clone(), backfill_actor_id)
    }
}
