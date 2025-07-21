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

use crate::task::barrier_manager::LocalBarrierEvent::ReportCdcTableBackfillProgress;
use crate::task::{ActorId, LocalBarrierManager};

pub struct CdcProgressReporter {
    barrier_manager: LocalBarrierManager,
}

impl CdcProgressReporter {
    pub fn new(barrier_manager: LocalBarrierManager) -> Self {
        Self { barrier_manager }
    }

    pub fn finish(&self, actor_id: ActorId, epoch: u64, split_id_range: (i64, i64)) {
        self.barrier_manager
            .update_cdc_backfill_progress(actor_id, epoch, split_id_range);
    }
}

impl LocalBarrierManager {
    fn update_cdc_backfill_progress(
        &self,
        actor_id: ActorId,
        epoch: u64,
        split_id_range: (i64, i64),
    ) {
        self.send_event(ReportCdcTableBackfillProgress {
            actor_id,
            epoch,
            split_id_start_inclusive: split_id_range.0,
            split_id_end_inclusive: split_id_range.1,
        })
    }
}
