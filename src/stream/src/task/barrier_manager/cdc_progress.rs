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

use risingwave_common::must_match;
use risingwave_common::util::epoch::EpochPair;
use risingwave_pb::stream_service::barrier_complete_response::PbCdcTableBackfillProgress;

use crate::task::barrier_manager::LocalBarrierEvent::ReportCdcTableBackfillProgress;
use crate::task::{ActorId, FragmentId, LocalBarrierManager};

#[derive(Debug, Clone, Copy)]
pub(crate) enum CdcTableBackfillState {
    Finish {
        fragment_id: FragmentId,
        split_id_start_inclusive: i64,
        split_id_end_inclusive: i64,
        generation: u64,
    },
}

impl CdcTableBackfillState {
    pub fn to_pb(self, actor_id: ActorId, epoch: u64) -> PbCdcTableBackfillProgress {
        must_match!(self, CdcTableBackfillState::Finish {fragment_id,split_id_start_inclusive,split_id_end_inclusive,generation} => {
            PbCdcTableBackfillProgress {
                actor_id,
                epoch,
                done: true,
                split_id_start_inclusive,
                split_id_end_inclusive,
                generation,
                fragment_id,
            }
        })
    }
}

pub struct CdcProgressReporter {
    barrier_manager: LocalBarrierManager,
}

impl CdcProgressReporter {
    pub fn new(barrier_manager: LocalBarrierManager) -> Self {
        Self { barrier_manager }
    }

    pub fn finish(
        &self,
        fragment_id: FragmentId,
        actor_id: ActorId,
        epoch: EpochPair,
        generation: u64,
        split_id_range: (i64, i64),
    ) {
        self.barrier_manager.update_cdc_backfill_progress(
            actor_id,
            epoch,
            CdcTableBackfillState::Finish {
                fragment_id,
                split_id_start_inclusive: split_id_range.0,
                split_id_end_inclusive: split_id_range.1,
                generation,
            },
        );
    }
}

impl LocalBarrierManager {
    fn update_cdc_backfill_progress(
        &self,
        actor_id: ActorId,
        epoch: EpochPair,
        state: CdcTableBackfillState,
    ) {
        self.send_event(ReportCdcTableBackfillProgress {
            actor_id,
            epoch,
            state,
        })
    }
}
