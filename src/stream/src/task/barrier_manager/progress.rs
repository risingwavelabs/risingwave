// Copyright 2025 RisingWave Labs
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

use std::assert_matches::assert_matches;
use std::fmt::{Display, Formatter};

use risingwave_common::util::epoch::EpochPair;
use risingwave_pb::stream_service::barrier_complete_response::PbCreateMviewProgress;

use super::LocalBarrierManager;
use crate::task::ActorId;
use crate::task::barrier_manager::LocalBarrierEvent::ReportCreateProgress;
use crate::task::barrier_manager::managed_state::DatabaseManagedBarrierState;

type ConsumedEpoch = u64;
type ConsumedRows = u64;

#[derive(Debug, Clone, Copy)]
pub(crate) enum BackfillState {
    ConsumingUpstreamTableOrSource(ConsumedEpoch, ConsumedRows),
    DoneConsumingUpstreamTableOrSource(ConsumedRows),
    ConsumingLogStore { pending_epoch_lag: u64 },
    DoneConsumingLogStore,
    // TODO(zw): impl
    // ConsumingCdcSplits(ConsumedRows),
    // DoneConsumingCdcSplits,
}

impl BackfillState {
    pub fn to_pb(self, actor_id: ActorId) -> PbCreateMviewProgress {
        let (done, consumed_epoch, consumed_rows, pending_epoch_lag) = match self {
            BackfillState::ConsumingUpstreamTableOrSource(consumed_epoch, consumed_rows) => {
                (false, consumed_epoch, consumed_rows, 0)
            }
            BackfillState::DoneConsumingUpstreamTableOrSource(consumed_rows) => {
                (true, 0, consumed_rows, 0)
            } // unused field for done
            BackfillState::ConsumingLogStore { pending_epoch_lag } => {
                (false, 0, 0, pending_epoch_lag)
            }
            BackfillState::DoneConsumingLogStore => (true, 0, 0, 0),
        };
        PbCreateMviewProgress {
            backfill_actor_id: actor_id,
            done,
            consumed_epoch,
            consumed_rows,
            pending_epoch_lag,
        }
    }
}

impl Display for BackfillState {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BackfillState::ConsumingUpstreamTableOrSource(epoch, rows) => {
                write!(
                    f,
                    "ConsumingUpstreamTable(epoch: {}, rows: {})",
                    epoch, rows
                )
            }
            BackfillState::DoneConsumingUpstreamTableOrSource(rows) => {
                write!(f, "DoneConsumingUpstreamTable(rows: {})", rows)
            }
            BackfillState::ConsumingLogStore { pending_epoch_lag } => {
                write!(
                    f,
                    "ConsumingLogStore(pending_epoch_lag: {pending_epoch_lag})"
                )
            }
            BackfillState::DoneConsumingLogStore => {
                write!(f, "DoneConsumingLogStore")
            }
        }
    }
}

impl DatabaseManagedBarrierState {
    pub(crate) fn update_create_mview_progress(
        &mut self,
        epoch: EpochPair,
        actor: ActorId,
        state: BackfillState,
    ) {
        if let Some(actor_state) = self.actor_states.get(&actor)
            && let Some(partial_graph_id) = actor_state.inflight_barriers.get(&epoch.prev)
            && let Some(graph_state) = self.graph_states.get_mut(partial_graph_id)
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
            Some(BackfillState::ConsumingUpstreamTableOrSource(last, last_consumed_rows)) => {
                assert!(
                    last <= consumed_epoch,
                    "last_epoch: {:#?} must be greater than consumed epoch: {:#?}",
                    last,
                    consumed_epoch
                );
                assert!(last_consumed_rows <= current_consumed_rows);
            }
            Some(state) => {
                panic!(
                    "should not update consuming progress at invalid state: {:?}",
                    state
                )
            }
            None => {}
        };
        tracing::debug!(
            actor_id = self.backfill_actor_id,
            ?epoch,
            consumed_epoch,
            current_consumed_rows,
            "progress update"
        );
        self.update_inner(
            epoch,
            BackfillState::ConsumingUpstreamTableOrSource(consumed_epoch, current_consumed_rows),
        );
    }

    /// The difference from [`Self::update`] (MV backfill) is that we
    /// don't care `ConsumedEpoch` here.
    pub fn update_for_source_backfill(
        &mut self,
        epoch: EpochPair,
        current_consumed_rows: ConsumedRows,
    ) {
        match self.state {
            Some(BackfillState::ConsumingUpstreamTableOrSource(
                dummy_last_epoch,
                _last_consumed_rows,
            )) => {
                debug_assert_eq!(dummy_last_epoch, 0);
            }
            Some(state) => {
                panic!(
                    "should not update consuming progress at invalid state: {:?}",
                    state
                )
            }
            None => {}
        };
        self.update_inner(
            epoch,
            // fill a dummy ConsumedEpoch
            BackfillState::ConsumingUpstreamTableOrSource(0, current_consumed_rows),
        );
    }

    /// Finish the progress. If the progress is already finished, then perform no-op.
    /// `current_epoch` should be provided to locate the barrier under concurrent checkpoint.
    pub fn finish(&mut self, epoch: EpochPair, current_consumed_rows: ConsumedRows) {
        if let Some(BackfillState::DoneConsumingUpstreamTableOrSource(_)) = self.state {
            return;
        }
        tracing::debug!(
            actor_id = self.backfill_actor_id,
            ?epoch,
            current_consumed_rows,
            "progress finish"
        );
        self.update_inner(
            epoch,
            BackfillState::DoneConsumingUpstreamTableOrSource(current_consumed_rows),
        );
    }

    pub(crate) fn update_create_mview_log_store_progress(
        &mut self,
        epoch: EpochPair,
        pending_epoch_lag: u64,
    ) {
        assert_matches!(
            self.state,
            Some(BackfillState::DoneConsumingUpstreamTableOrSource(_))
                | Some(BackfillState::ConsumingLogStore { .. })
                | None,
            "cannot update log store progress at state {:?}",
            self.state
        );
        self.update_inner(
            epoch,
            BackfillState::ConsumingLogStore { pending_epoch_lag },
        );
    }

    pub(crate) fn finish_consuming_log_store(&mut self, epoch: EpochPair) {
        assert_matches!(
            self.state,
            Some(BackfillState::DoneConsumingUpstreamTableOrSource(_))
                | Some(BackfillState::ConsumingLogStore { .. })
                | None,
            "cannot finish log store progress at state {:?}",
            self.state
        );
        self.update_inner(epoch, BackfillState::DoneConsumingLogStore);
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
    pub(crate) fn register_create_mview_progress(
        &self,
        backfill_actor_id: ActorId,
    ) -> CreateMviewProgressReporter {
        trace!("register create mview progress: {}", backfill_actor_id);
        CreateMviewProgressReporter::new(self.clone(), backfill_actor_id)
    }
}
