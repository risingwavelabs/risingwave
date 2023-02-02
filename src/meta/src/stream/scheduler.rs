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

use risingwave_pb::common::{ActorInfo, Buffer, ParallelUnit};

use crate::manager::{WorkerId, WorkerLocations};
use crate::model::ActorId;

/// [`ScheduledLocations`] represents the location of scheduled result.
pub struct ScheduledLocations {
    /// actor location map.
    pub actor_locations: BTreeMap<ActorId, ParallelUnit>,
    /// worker location map.
    pub worker_locations: WorkerLocations,
    /// actor vnode bitmap.
    pub actor_vnode_bitmaps: HashMap<ActorId, Option<Buffer>>,
}

impl ScheduledLocations {
    #[cfg_attr(not(test), expect(dead_code))]
    pub fn new() -> Self {
        Self {
            actor_locations: BTreeMap::new(),
            worker_locations: WorkerLocations::new(),
            actor_vnode_bitmaps: HashMap::new(),
        }
    }

    /// Returns all actors for every worker node.
    pub fn worker_actors(&self) -> HashMap<WorkerId, Vec<ActorId>> {
        let mut worker_actors = HashMap::new();
        self.actor_locations
            .iter()
            .for_each(|(actor_id, parallel_unit)| {
                worker_actors
                    .entry(parallel_unit.worker_node_id)
                    .or_insert_with(Vec::new)
                    .push(*actor_id);
            });

        worker_actors
    }

    /// Returns the `ActorInfo` map for every actor.
    pub fn actor_info_map(&self) -> HashMap<ActorId, ActorInfo> {
        self.actor_locations
            .iter()
            .map(|(actor_id, parallel_unit)| {
                (
                    *actor_id,
                    ActorInfo {
                        actor_id: *actor_id,
                        host: self.worker_locations[&parallel_unit.worker_node_id]
                            .host
                            .clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>()
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
