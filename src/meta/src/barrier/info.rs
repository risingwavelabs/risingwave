// Copyright 2024 RisingWave Labs
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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use risingwave_common::catalog::TableId;
use risingwave_pb::common::PbWorkerNode;
use tracing::warn;

use crate::barrier::Command;
use crate::manager::{
    ActiveStreamingWorkerNodes, InflightFragmentInfo, InflightGraphInfo, WorkerId,
};
use crate::model::{ActorId, FragmentId};

#[derive(Debug, Clone)]
pub(crate) enum CommandFragmentChanges {
    NewFragment(InflightFragmentInfo),
    Reschedule {
        new_actors: HashMap<ActorId, WorkerId>,
        to_remove: HashSet<ActorId>,
    },
    RemoveFragment,
}

#[derive(Default, Clone)]
pub struct InflightSubscriptionInfo {
    /// `mv_table_id` => `subscription_id` => retention seconds
    pub mv_depended_subscriptions: HashMap<TableId, HashMap<u32, u64>>,
}

/// [`InflightActorInfo`] resolves the actor info read from meta store for
/// [`crate::barrier::GlobalBarrierManager`].
#[derive(Default, Clone)]
pub struct InflightActorInfo {
    /// `node_id` => node
    pub node_map: HashMap<WorkerId, PbWorkerNode>,

    /// `node_id` => actors
    pub actor_map: HashMap<WorkerId, HashSet<ActorId>>,

    /// `actor_id` => `WorkerId`
    pub actor_location_map: HashMap<ActorId, WorkerId>,
}

impl InflightActorInfo {
    /// Resolve inflight actor info from given nodes and actors that are loaded from meta store. It will be used during recovery to rebuild all streaming actors.
    pub fn resolve(
        active_nodes: &ActiveStreamingWorkerNodes,
        graph_info: &InflightGraphInfo,
    ) -> Self {
        let node_map = active_nodes.current().clone();

        let actor_map = {
            let mut map: HashMap<_, HashSet<_>> = HashMap::new();
            for info in graph_info.fragment_infos.values() {
                for (actor_id, worker_id) in &info.actors {
                    map.entry(*worker_id).or_default().insert(*actor_id);
                }
            }
            map
        };

        let actor_location_map = graph_info
            .fragment_infos
            .values()
            .flat_map(|fragment| {
                fragment
                    .actors
                    .iter()
                    .map(|(actor_id, workder_id)| (*actor_id, *workder_id))
            })
            .collect();

        Self {
            node_map,
            actor_map,
            actor_location_map,
        }
    }

    /// Update worker nodes snapshot. We need to support incremental updates for it in the future.
    pub fn resolve_worker_nodes(&mut self, all_nodes: impl IntoIterator<Item = PbWorkerNode>) {
        let new_node_map = all_nodes
            .into_iter()
            .map(|node| (node.id, node))
            .collect::<HashMap<_, _>>();

        let mut deleted_actors = BTreeMap::new();
        for (&actor_id, &location) in &self.actor_location_map {
            if !new_node_map.contains_key(&location) {
                deleted_actors
                    .entry(location)
                    .or_insert_with(BTreeSet::new)
                    .insert(actor_id);
            }
        }
        for (node_id, actors) in deleted_actors {
            let node = self.node_map.get(&node_id);
            warn!(
                node_id,
                ?node,
                ?actors,
                "node with running actors is deleted"
            );
        }

        self.node_map = new_node_map;
    }
}

impl InflightGraphInfo {
    /// Apply some actor changes before issuing a barrier command, if the command contains any new added actors, we should update
    /// the info correspondingly.
    pub(crate) fn pre_apply(
        &mut self,
        fragment_changes: &HashMap<FragmentId, CommandFragmentChanges>,
    ) -> HashMap<ActorId, WorkerId> {
        {
            let mut to_add = HashMap::new();
            for (fragment_id, change) in fragment_changes {
                match change {
                    CommandFragmentChanges::NewFragment(info) => {
                        for (actor_id, node_id) in &info.actors {
                            assert!(to_add.insert(*actor_id, *node_id).is_none());
                        }
                        assert!(self
                            .fragment_infos
                            .insert(*fragment_id, info.clone())
                            .is_none());
                    }
                    CommandFragmentChanges::Reschedule { new_actors, .. } => {
                        let info = self
                            .fragment_infos
                            .get_mut(fragment_id)
                            .expect("should exist");
                        let actors = &mut info.actors;
                        for (actor_id, node_id) in new_actors {
                            assert!(to_add.insert(*actor_id, *node_id).is_none());
                            assert!(actors.insert(*actor_id, *node_id).is_none());
                        }
                    }
                    CommandFragmentChanges::RemoveFragment => {}
                }
            }
            to_add
        }
    }
}

impl InflightActorInfo {
    pub fn pre_apply(&mut self, actors_to_add: Option<HashMap<ActorId, WorkerId>>) {
        {
            for (actor_id, node_id) in actors_to_add.into_iter().flatten() {
                assert!(self.node_map.contains_key(&node_id));
                assert!(
                    self.actor_map.entry(node_id).or_default().insert(actor_id),
                    "duplicate actor in command changes"
                );
                assert!(
                    self.actor_location_map.insert(actor_id, node_id).is_none(),
                    "duplicate actor in command changes"
                );
            }
        }
    }
}

impl InflightSubscriptionInfo {
    pub fn pre_apply(&mut self, command: &Command) {
        if let Command::CreateSubscription {
            subscription_id,
            upstream_mv_table_id,
            retention_second,
        } = command
        {
            if let Some(prev_retiontion) = self
                .mv_depended_subscriptions
                .entry(*upstream_mv_table_id)
                .or_default()
                .insert(*subscription_id, *retention_second)
            {
                warn!(subscription_id, ?upstream_mv_table_id, mv_depended_subscriptions = ?self.mv_depended_subscriptions, prev_retiontion, "add an existing subscription id");
            }
        }
    }
}

impl InflightGraphInfo {
    /// Apply some actor changes after the barrier command is collected, if the command contains any actors that are dropped, we should
    /// remove that from the snapshot correspondingly.
    pub(crate) fn post_apply(
        &mut self,
        fragment_changes: &HashMap<FragmentId, CommandFragmentChanges>,
    ) -> HashSet<ActorId> {
        {
            let mut all_to_remove = HashSet::new();
            for (fragment_id, changes) in fragment_changes {
                match changes {
                    CommandFragmentChanges::NewFragment(_) => {}
                    CommandFragmentChanges::Reschedule { to_remove, .. } => {
                        let info = self
                            .fragment_infos
                            .get_mut(fragment_id)
                            .expect("should exist");
                        for actor_id in to_remove {
                            assert!(all_to_remove.insert(*actor_id));
                            assert!(info.actors.remove(actor_id).is_some());
                        }
                    }
                    CommandFragmentChanges::RemoveFragment => {
                        let info = self
                            .fragment_infos
                            .remove(fragment_id)
                            .expect("should exist");
                        for actor_id in info.actors.keys() {
                            assert!(all_to_remove.insert(*actor_id));
                        }
                    }
                }
            }
            all_to_remove
        }
    }
}

impl InflightActorInfo {
    pub fn post_apply(&mut self, actors_to_remove: Option<HashSet<ActorId>>) {
        {
            for actor_id in actors_to_remove.into_iter().flatten() {
                let node_id = self
                    .actor_location_map
                    .remove(&actor_id)
                    .expect("actor not found");
                let actor_ids = self.actor_map.get_mut(&node_id).expect("node not found");
                assert!(actor_ids.remove(&actor_id), "actor not found");
            }
            self.actor_map.retain(|_, actor_ids| !actor_ids.is_empty());
        }
    }
}

impl InflightSubscriptionInfo {
    pub fn post_apply(&mut self, command: &Command) {
        if let Command::DropSubscription {
            subscription_id,
            upstream_mv_table_id,
        } = command
        {
            let removed = match self.mv_depended_subscriptions.get_mut(upstream_mv_table_id) {
                Some(subscriptions) => {
                    let removed = subscriptions.remove(subscription_id).is_some();
                    if removed && subscriptions.is_empty() {
                        self.mv_depended_subscriptions.remove(upstream_mv_table_id);
                    }
                    removed
                }
                None => false,
            };
            if !removed {
                warn!(subscription_id, ?upstream_mv_table_id, mv_depended_subscriptions = ?self.mv_depended_subscriptions, "remove a non-existing subscription id");
            }
        }
    }
}

impl InflightGraphInfo {
    /// Returns actor list to collect in the target worker node.
    pub fn actor_ids_to_collect(&self, node_id: WorkerId) -> impl Iterator<Item = ActorId> + '_ {
        self.fragment_infos.values().flat_map(move |info| {
            info.actors
                .iter()
                .filter_map(move |(actor_id, actor_node_id)| {
                    if *actor_node_id == node_id {
                        Some(*actor_id)
                    } else {
                        None
                    }
                })
        })
    }

    /// Returns actor list to send in the target worker node.
    pub fn actor_ids_to_send(&self, node_id: WorkerId) -> impl Iterator<Item = ActorId> + '_ {
        self.fragment_infos
            .values()
            .filter(|info| info.is_injectable)
            .flat_map(move |info| {
                info.actors
                    .iter()
                    .filter_map(move |(actor_id, actor_node_id)| {
                        if *actor_node_id == node_id {
                            Some(*actor_id)
                        } else {
                            None
                        }
                    })
            })
    }

    pub fn existing_table_ids(&self) -> impl Iterator<Item = TableId> + '_ {
        self.fragment_infos
            .values()
            .flat_map(|info| info.state_table_ids.iter().cloned())
    }
}
