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

use risingwave_common::bail;
use risingwave_common::id::TableId;

use crate::error::StreamResult;
use crate::executor::source::get_infinite_backoff_strategy;
use crate::executor::{Barrier, Mutation};
use crate::task::ActorId;

/// Retries a bounded load makes before it fails.
/// Refreshable tables retry forever, since their next refresh plans the latest snapshot again.
const BOUNDED_LOAD_MAX_RETRIES: usize = 10;

pub(super) type Backoff = Box<dyn Iterator<Item = Duration> + Send>;

#[derive(Clone, Copy, Debug)]
pub(super) enum LoadMode {
    /// A refreshable table reloads into `associated_table_id` on every refresh.
    FullReload { associated_table_id: TableId },
    /// A bounded job loads its pinned input once. Any job can consume it, so only a bounded table
    /// has an associated table.
    Bounded {
        associated_table_id: Option<TableId>,
    },
}

impl LoadMode {
    pub(super) fn new(
        is_bounded: bool,
        associated_table_id: Option<TableId>,
    ) -> StreamResult<Self> {
        match (is_bounded, associated_table_id) {
            (true, associated_table_id) => Ok(Self::Bounded {
                associated_table_id,
            }),
            (false, Some(associated_table_id)) => Ok(Self::FullReload {
                associated_table_id,
            }),
            (false, None) => bail!("full reload requires an associated table"),
        }
    }

    pub(super) fn is_bounded(&self) -> bool {
        matches!(self, Self::Bounded { .. })
    }

    /// The table this load writes into. Jobs reading a bounded source have none.
    pub(super) fn associated_table_id(&self) -> Option<TableId> {
        match self {
            Self::FullReload {
                associated_table_id,
            } => Some(*associated_table_id),
            Self::Bounded {
                associated_table_id,
            } => *associated_table_id,
        }
    }

    pub(super) fn starts_on_creation(&self, first_barrier: &Barrier, actor_id: ActorId) -> bool {
        matches!(self, Self::Bounded { .. })
            && matches!(
                first_barrier.mutation.as_deref(),
                Some(Mutation::Add(add)) if add.bounded_load_actors.contains(&actor_id)
            )
    }

    /// Only refreshable tables follow `RefreshStart` and `ListFinish`.
    pub(super) fn accepts_refresh_mutations(&self) -> bool {
        matches!(self, Self::FullReload { .. })
    }

    /// Only bounded jobs follow `BoundedLoad`, and only when it names this actor. Several jobs can
    /// read the same source, so the source id alone does not identify the load.
    pub(super) fn accepts_bounded_mutation(
        &self,
        reader_actor_ids: &[ActorId],
        actor_id: ActorId,
    ) -> bool {
        matches!(self, Self::Bounded { .. }) && reader_actor_ids.contains(&actor_id)
    }

    /// Delays between retries of a load. Once a bounded load's backoff runs out, the load fails.
    pub(super) fn backoff(&self) -> Backoff {
        match self {
            Self::FullReload { .. } => Box::new(get_infinite_backoff_strategy()),
            Self::Bounded { .. } => {
                Box::new(get_infinite_backoff_strategy().take(BOUNDED_LOAD_MAX_RETRIES))
            }
        }
    }
}
