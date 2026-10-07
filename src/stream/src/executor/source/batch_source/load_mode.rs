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

//! Start and seal rules shared by the batch Iceberg list and fetch executors.
//!
//! Refreshable tables wait for `RefreshStart` and can run multiple loads.
//! Bounded jobs run once per attempt, authorized by the first creation or recovery barrier.

use risingwave_pb::stream_plan::BoundedLoadStart;

use crate::executor::{Barrier, Mutation};
use crate::task::ActorId;

#[derive(Clone, Copy)]
pub(super) enum LoadMode {
    /// Start a new full-reload cycle on each `RefreshStart` mutation.
    FullReload,
    Bounded {
        /// The job and generation assigned to this actor by the first `AddMutation`.
        /// Meta omits the start when recovering a completed job. Such actors stay idle
        /// and forward barriers without reading the source again.
        start: Option<BoundedLoadStart>,
    },
}

impl LoadMode {
    /// Read the actor's bounded start once from its creation or recovery barrier.
    pub(super) fn new(is_bounded: bool, first_barrier: &Barrier, actor_id: ActorId) -> Self {
        if !is_bounded {
            return Self::FullReload;
        }

        let start = match first_barrier.mutation.as_deref() {
            Some(Mutation::Add(add)) => add.actor_bounded_starts.get(&actor_id).copied(),
            _ => None,
        };
        Self::Bounded { start }
    }

    /// Whether to start loading immediately after the first barrier, including on recovery.
    pub(super) fn starts_on_creation(&self) -> bool {
        matches!(self, Self::Bounded { start: Some(_) })
    }

    /// Only refreshable tables may start another load through `RefreshStart`.
    pub(super) fn accepts_refresh_start(&self) -> bool {
        matches!(self, Self::FullReload)
    }

    /// Validate the attempt tag on `ListFinish` before fetch seals its input.
    /// Full reload uses an untagged seal. Bounded loads require the same job and
    /// generation as their start; bounded actors without a start reject all seals.
    /// The fetch phase separately determines whether a load is still running.
    pub(super) fn accepts_list_finish(&self, sealed_attempt: Option<&BoundedLoadStart>) -> bool {
        match self {
            Self::FullReload => sealed_attempt.is_none(),
            Self::Bounded { start: Some(start) } => sealed_attempt == Some(start),
            Self::Bounded { start: None } => false,
        }
    }
}
