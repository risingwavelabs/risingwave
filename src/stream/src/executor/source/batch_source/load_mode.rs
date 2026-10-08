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

use risingwave_pb::stream_plan::BoundedLoadAttempt;

use crate::executor::{Barrier, Mutation};
use crate::task::ActorId;

#[derive(Clone, Copy)]
pub(super) enum LoadMode {
    /// Start a new full-reload cycle on each `RefreshStart` mutation.
    FullReload,
    Bounded {
        /// The attempt assigned to this actor by the first `AddMutation`.
        /// Meta omits the attempt when recovering a completed job. Such actors stay idle
        /// and forward barriers without reading the source again.
        attempt: Option<BoundedLoadAttempt>,
    },
}

impl LoadMode {
    /// Read the actor's bounded attempt once from its creation or recovery barrier.
    pub(super) fn new(is_bounded: bool, first_barrier: &Barrier, actor_id: ActorId) -> Self {
        if !is_bounded {
            return Self::FullReload;
        }

        let attempt = match first_barrier.mutation.as_deref() {
            Some(Mutation::Add(add)) => add.actor_bounded_starts.get(&actor_id).copied(),
            _ => None,
        };
        Self::Bounded { attempt }
    }

    /// Whether to start loading immediately after the first barrier, including on recovery.
    pub(super) fn starts_on_creation(&self) -> bool {
        matches!(self, Self::Bounded { attempt: Some(_) })
    }

    /// Only refreshable tables follow `RefreshStart` and `ListFinish`.
    pub(super) fn accepts_refresh_mutations(&self) -> bool {
        matches!(self, Self::FullReload)
    }

    /// Whether a `BoundedLoad` mutation targets this actor's attempt.
    /// Bounded actors without an attempt reject all of them.
    pub(super) fn owns_attempt(&self, attempt: &BoundedLoadAttempt) -> bool {
        matches!(self, Self::Bounded { attempt: Some(own) } if own == attempt)
    }
}
