// Copyright 2026 RisingWave Labs
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

use std::collections::HashMap;

use risingwave_common::id::{ActorId, SourceId};
use risingwave_meta_model::WorkerId;
use risingwave_pb::stream_service::BarrierCompleteResponse;
use risingwave_pb::stream_service::barrier_complete_response::PbLoadProgress;
use risingwave_pb::stream_service::barrier_complete_response::load_progress::Progress;

/// The progress of a [`PbLoadProgress`], from its `progress` oneof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LoadProgress {
    ListFinished,
    ReaderDone,
    Failed,
}

/// A load progress report of an actor, carried by a collected barrier. Parsed from
/// [`PbLoadProgress`], with its `load_id` and `progress` required.
#[derive(Debug, Clone, Copy)]
pub(super) struct LoadReport {
    pub actor_id: ActorId,
    pub source_id: SourceId,
    pub progress: LoadProgress,
}

impl LoadReport {
    /// Returns the load reports of a collected barrier.
    pub fn from_responses(
        resps: &HashMap<WorkerId, BarrierCompleteResponse>,
    ) -> impl Iterator<Item = Self> + '_ {
        resps
            .values()
            .flat_map(|resp| &resp.load_progress)
            .map(Self::from_pb)
    }

    fn from_pb(report: &PbLoadProgress) -> Self {
        let source_id = report
            .load_id
            .expect("load progress must have a load id")
            .source_id;
        let progress = match report.progress.expect("load progress must have a progress") {
            Progress::ListFinished(_) => LoadProgress::ListFinished,
            Progress::ReaderDone(_) => LoadProgress::ReaderDone,
            Progress::Failed(_) => LoadProgress::Failed,
        };

        Self {
            actor_id: report.reporter_actor_id,
            source_id,
            progress,
        }
    }
}
