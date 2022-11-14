// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;

use risingwave_pb::common::WorkerNode;

use crate::manager::{ActorInfos, WorkerId};
use crate::model::ActorId;

/// [`BarrierActorInfo`] resolves the actor info read from meta store for
/// [`crate::barrier::GlobalBarrierManager`].
pub struct BarrierActorInfo {
    /// node_id => node
    pub node_map: HashMap<WorkerId, WorkerNode>,

    /// node_id => actors
    pub actor_map: HashMap<WorkerId, Vec<ActorId>>,

    /// node_id => source actors
    pub actor_map_to_send: HashMap<WorkerId, Vec<ActorId>>,
}

impl BarrierActorInfo {
    // TODO: we may resolve this info as graph updating, instead of doing it every time we want to
    // send a barrier
    pub fn resolve(
        all_nodes: impl IntoIterator<Item = WorkerNode>,
        actor_infos: ActorInfos,
    ) -> Self {
        let node_map = all_nodes
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<HashMap<_, _>>();

        Self {
            node_map,
            actor_map: actor_infos.actor_maps,
            actor_map_to_send: actor_infos.source_actor_maps,
        }
    }

    // TODO: should only collect from reachable actors, for mv on mv
    pub fn actor_ids_to_collect(&self, node_id: &WorkerId) -> impl Iterator<Item = ActorId> {
        self.actor_map
            .get(node_id)
            .map(|actor_ids| actor_ids.clone().into_iter())
            .unwrap_or_else(|| vec![].into_iter())
    }

    pub fn actor_ids_to_send(&self, node_id: &WorkerId) -> impl Iterator<Item = ActorId> {
        self.actor_map_to_send
            .get(node_id)
            .map(|actor_ids| actor_ids.clone().into_iter())
            .unwrap_or_else(|| vec![].into_iter())
    }

    pub fn nothing_to_do(&self) -> bool {
        if self.actor_map.is_empty() {
            assert!(self.actor_map_to_send.is_empty());
            return true;
        }
        false
    }
}
