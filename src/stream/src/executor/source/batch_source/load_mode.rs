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
//! Bounded jobs run once, started by actor initialisation in `AddMutation.bounded_load_actors`

use crate::executor::{Barrier, Mutation};
use crate::task::ActorId;

#[derive(Clone, Copy)]
pub(super) enum LoadMode {
    FullReload,
    Bounded,
}

impl LoadMode {
    pub(super) fn starts_on_creation(&self, first_barrier: &Barrier, actor_id: ActorId) -> bool {
        matches!(self, Self::Bounded)
            && matches!(
                first_barrier.mutation.as_deref(),
                Some(Mutation::Add(add)) if add.bounded_load_actors.contains(&actor_id)
            )
    }

    /// Only refreshable tables follow `RefreshStart` and `ListFinish`.
    pub(super) fn accepts_refresh_mutations(&self) -> bool {
        matches!(self, Self::FullReload)
    }

    /// Only bounded jobs follow `BoundedLoad`.
    pub(super) fn accepts_bounded_mutations(&self) -> bool {
        matches!(self, Self::Bounded)
    }
}
