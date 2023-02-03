// Copyright 2023 RisingWave Labs
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

use std::collections::{BTreeMap, HashMap};

use itertools::Itertools;
use risingwave_pb::common::{ActorInfo, ParallelUnit};

use crate::manager::{WorkerId, WorkerLocations};
use crate::model::ActorId;

/// [`Locations`] represents the parallel unit and worker locations of the actors.
#[cfg_attr(test, derive(Default))]
pub struct Locations {
    /// actor location map.
    pub actor_locations: BTreeMap<ActorId, ParallelUnit>,
    /// worker location map.
    pub worker_locations: WorkerLocations,
}

impl Locations {
    /// Returns all actors for every worker node.
    pub fn worker_actors(&self) -> HashMap<WorkerId, Vec<ActorId>> {
        self.actor_locations
            .iter()
            .map(|(actor_id, parallel_unit)| (parallel_unit.worker_node_id, *actor_id))
            .into_group_map()
    }

    /// Returns the `ActorInfo` map for every actor.
    pub fn actor_info_map(&self) -> HashMap<ActorId, ActorInfo> {
        self.actor_infos()
            .map(|info| (info.actor_id, info))
            .collect()
    }

    /// Returns an iterator of `ActorInfo`.
    pub fn actor_infos(&self) -> impl Iterator<Item = ActorInfo> + '_ {
        self.actor_locations
            .iter()
            .map(|(actor_id, parallel_unit)| ActorInfo {
                actor_id: *actor_id,
                host: self.worker_locations[&parallel_unit.worker_node_id]
                    .host
                    .clone(),
            })
    }
}
