// Copyright 2025 RisingWave Labs
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

use std::collections::{HashMap, HashSet};

use risingwave_common::catalog::{FragmentTypeFlag, TableId};
pub use risingwave_common::id::ActorId;

use crate::controller::fragment::InflightFragmentInfo;
use crate::model::{FragmentId, StreamJobFragments};

#[derive(Clone, Debug, Default)]
pub struct BackfillNode {
    fragment_id: FragmentId,
    /// How many more actors need to finish,
    /// before this fragment can finish backfilling.
    remaining_actors: HashSet<ActorId>,
    /// How many more dependencies need to finish,
    /// before this fragment can be backfilled.
    remaining_dependencies: HashSet<FragmentId>,
    children: Vec<FragmentId>,
}

/// Actor done                   -> update `fragment_id` state
/// Operator done                -> update downstream operator dependency
/// Operator's dependencies done -> queue operator for backfill
#[derive(Clone, Debug, Default)]
pub struct BackfillOrderState {
    // The order plan.
    current_backfill_nodes: HashMap<FragmentId, BackfillNode>,
    // Remaining nodes to finish
    remaining_backfill_nodes: HashMap<FragmentId, BackfillNode>,
    // The mapping between actors and fragment_ids
    actor_to_fragment_id: HashMap<ActorId, FragmentId>,
    // The mapping between fragment_ids and table_ids of locality provider fragments
    locality_fragment_state_table_mapping: HashMap<FragmentId, Vec<TableId>>,
}

/// Get nodes with some dependencies.
/// These should initially be paused until their dependencies are done.
pub fn get_nodes_with_backfill_dependencies(
    backfill_orders: &HashMap<FragmentId, Vec<FragmentId>>,
) -> HashSet<FragmentId> {
    backfill_orders.values().flatten().copied().collect()
}

// constructor
impl BackfillOrderState {
    pub fn new(
        backfill_orders: &HashMap<FragmentId, Vec<FragmentId>>,
        stream_job_fragments: &StreamJobFragments,
        locality_fragment_state_table_mapping: HashMap<FragmentId, Vec<TableId>>,
    ) -> Self {
        tracing::debug!(?backfill_orders, "initialize backfill order state");
        let actor_to_fragment_id = stream_job_fragments.actor_fragment_mapping();

        let mut backfill_nodes: HashMap<FragmentId, BackfillNode> = HashMap::new();

        for fragment in stream_job_fragments.fragments() {
            if fragment.fragment_type_mask.contains_any([
                FragmentTypeFlag::StreamScan,
                FragmentTypeFlag::SourceScan,
                FragmentTypeFlag::LocalityProvider,
            ]) {
                let fragment_id = fragment.fragment_id;
                backfill_nodes.insert(
                    fragment_id,
                    BackfillNode {
                        fragment_id,
                        remaining_actors: stream_job_fragments
                            .fragment_actors(fragment_id)
                            .iter()
                            .map(|actor| actor.actor_id)
                            .collect(),
                        remaining_dependencies: Default::default(),
                        children: backfill_orders
                            .get(&fragment_id)
                            .cloned()
                            .unwrap_or_else(Vec::new),
                    },
                );
            }
        }

        for (fragment_id, children) in backfill_orders {
            for child in children {
                let child_node = backfill_nodes.get_mut(child).unwrap();
                child_node.remaining_dependencies.insert(*fragment_id);
            }
        }

        let mut current_backfill_nodes = HashMap::new();
        let mut remaining_backfill_nodes = HashMap::new();
        for (fragment_id, node) in backfill_nodes {
            if node.remaining_dependencies.is_empty() {
                current_backfill_nodes.insert(fragment_id, node);
            } else {
                remaining_backfill_nodes.insert(fragment_id, node);
            }
        }

        Self {
            current_backfill_nodes,
            remaining_backfill_nodes,
            actor_to_fragment_id,
            locality_fragment_state_table_mapping,
        }
    }

    pub fn recover_from_fragment_infos(
        backfill_orders: &HashMap<FragmentId, Vec<FragmentId>>,
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
        locality_fragment_state_table_mapping: HashMap<FragmentId, Vec<TableId>>,
    ) -> Self {
        tracing::debug!(
            ?backfill_orders,
            "initialize backfill order state from recovery"
        );
        let actor_to_fragment_id = fragment_infos
            .iter()
            .flat_map(|(fragment_id, fragment)| {
                fragment
                    .actors
                    .keys()
                    .map(|actor_id| (*actor_id, *fragment_id))
            })
            .collect();

        let mut backfill_nodes: HashMap<FragmentId, BackfillNode> = HashMap::new();

        for (fragment_id, fragment) in fragment_infos {
            if fragment.fragment_type_mask.contains_any([
                FragmentTypeFlag::StreamScan,
                FragmentTypeFlag::SourceScan,
                FragmentTypeFlag::LocalityProvider,
            ]) {
                backfill_nodes.insert(
                    *fragment_id,
                    BackfillNode {
                        fragment_id: *fragment_id,
                        remaining_actors: fragment.actors.keys().copied().collect(),
                        remaining_dependencies: Default::default(),
                        children: backfill_orders
                            .get(fragment_id)
                            .cloned()
                            .unwrap_or_else(Vec::new),
                    },
                );
            }
        }

        for (fragment_id, children) in backfill_orders {
            for child in children {
                let child_node = backfill_nodes.get_mut(child).unwrap();
                child_node.remaining_dependencies.insert(*fragment_id);
            }
        }

        let mut current_backfill_nodes = HashMap::new();
        let mut remaining_backfill_nodes = HashMap::new();
        for (fragment_id, node) in backfill_nodes {
            if node.remaining_dependencies.is_empty() {
                current_backfill_nodes.insert(fragment_id, node);
            } else {
                remaining_backfill_nodes.insert(fragment_id, node);
            }
        }

        Self {
            current_backfill_nodes,
            remaining_backfill_nodes,
            actor_to_fragment_id,
            locality_fragment_state_table_mapping,
        }
    }
}

// state transitions
impl BackfillOrderState {
    pub fn finish_actor(&mut self, actor_id: ActorId) -> Vec<FragmentId> {
        let Some(fragment_id) = self.actor_to_fragment_id.get(&actor_id) else {
            tracing::error!(%actor_id, "fragment not found for actor");
            return vec![];
        };
        // NOTE(kwannoel):
        // Backfill order are specified by the user, for instance:
        // t1->t2 means that t1 must be backfilled before t2.
        // However, each snapshot executor may finish ahead of time if there's no data to backfill.
        // For instance, if t2 has no data to backfill,
        // and t1 has a lot of data to backfill,
        // t2's scan operator might finish immediately,
        // and t2 will finish before t1.
        // In such cases, we should directly update it in remaining backfill nodes instead,
        // so we should track whether a node finished in order.
        let (node, is_in_order) = match self.current_backfill_nodes.get_mut(fragment_id) {
            Some(node) => (node, true),
            None => {
                let Some(node) = self.remaining_backfill_nodes.get_mut(fragment_id) else {
                    tracing::error!(
                        %fragment_id,
                        %actor_id,
                        "fragment not found in current_backfill_nodes or remaining_backfill_nodes"
                    );
                    return vec![];
                };
                (node, false)
            }
        };

        assert!(node.remaining_actors.remove(&actor_id), "missing actor");
        tracing::debug!(
            %actor_id,
            remaining_actors = node.remaining_actors.len(),
            %fragment_id,
            "finish_backfilling_actor"
        );
        if node.remaining_actors.is_empty() && is_in_order {
            self.finish_fragment(*fragment_id)
        } else {
            vec![]
        }
    }

    pub fn finish_fragment(&mut self, fragment_id: FragmentId) -> Vec<FragmentId> {
        let mut newly_scheduled = vec![];
        // Decrease the remaining_dependency_count of the children.
        // If the remaining_dependency_count is 0, add the child to the current_backfill_nodes.
        if let Some(node) = self.current_backfill_nodes.remove(&fragment_id) {
            for child_id in &node.children {
                let newly_scheduled_child_finished = {
                    let child = self.remaining_backfill_nodes.get_mut(child_id).unwrap();
                    assert!(
                        child.remaining_dependencies.remove(&fragment_id),
                        "missing dependency"
                    );
                    if child.remaining_dependencies.is_empty() {
                        tracing::debug!(fragment_id = ?child_id, "schedule next backfill node");
                        self.current_backfill_nodes
                            .insert(child.fragment_id, child.clone());
                        newly_scheduled.push(child.fragment_id);
                        child.remaining_actors.is_empty()
                    } else {
                        false
                    }
                };
                if newly_scheduled_child_finished {
                    newly_scheduled.extend(self.finish_fragment(*child_id));
                }
            }
        } else {
            tracing::error!(%fragment_id, "fragment not found in current_backfill_nodes");
            return vec![];
        }
        newly_scheduled
    }

    pub fn current_backfill_node_fragment_ids(&self) -> Vec<FragmentId> {
        self.current_backfill_nodes.keys().copied().collect()
    }

    pub fn get_locality_fragment_state_table_mapping(&self) -> &HashMap<FragmentId, Vec<TableId>> {
        &self.locality_fragment_state_table_mapping
    }

    /// Refresh actor mapping after reschedule and return newly scheduled fragments.
    pub fn refresh_actors(
        &mut self,
        fragment_actors: &HashMap<FragmentId, HashSet<ActorId>>,
    ) -> Vec<FragmentId> {
        self.actor_to_fragment_id = fragment_actors
            .iter()
            .flat_map(|(fragment_id, actors)| {
                actors.iter().map(|actor_id| (*actor_id, *fragment_id))
            })
            .collect();

        for node in self
            .current_backfill_nodes
            .values_mut()
            .chain(self.remaining_backfill_nodes.values_mut())
        {
            if let Some(actors) = fragment_actors.get(&node.fragment_id) {
                node.remaining_actors = actors.iter().copied().collect();
            } else {
                node.remaining_actors.clear();
            }
        }

        let finished_fragments: Vec<_> = self
            .current_backfill_nodes
            .iter()
            .filter(|(_, node)| node.remaining_actors.is_empty())
            .map(|(fragment_id, _)| *fragment_id)
            .collect();

        let mut newly_scheduled = vec![];
        for fragment_id in finished_fragments {
            newly_scheduled.extend(self.finish_fragment(fragment_id));
        }
        newly_scheduled
    }
}
