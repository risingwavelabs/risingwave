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

use std::time::Duration;

use crate::executor::source::get_infinite_backoff_strategy;
use crate::executor::{Barrier, Mutation};
use crate::task::ActorId;

/// Retries a bounded load makes before it fails.
/// Refreshable tables retry forever, since their next refresh plans the latest snapshot again.
const BOUNDED_LOAD_MAX_RETRIES: usize = 10;

pub(super) type Backoff = Box<dyn Iterator<Item = Duration> + Send>;

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

    /// Delays between retries of a load. Once a bounded load's backoff runs out, the load fails.
    pub(super) fn backoff(&self) -> Backoff {
        match self {
            Self::FullReload => Box::new(get_infinite_backoff_strategy()),
            Self::Bounded => {
                Box::new(get_infinite_backoff_strategy().take(BOUNDED_LOAD_MAX_RETRIES))
            }
        }
    }
}
