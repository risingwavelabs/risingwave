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

use risingwave_common::catalog::TableId;
pub use risingwave_common::id::ActorId;
use risingwave_common::operator::unique_operator_id_into_parts;
use risingwave_common::util::stream_graph_visitor::visit_backfill_nodes;
use risingwave_pb::id::GlobalOperatorId;
use risingwave_pb::stream_plan::stream_node::NodeBody;

use crate::controller::fragment::InflightFragmentInfo;
use crate::model::{BackfillExecutor, FragmentId};
use crate::stream::ExtendedBackfillOrder;

#[derive(Clone, Debug, Default)]
pub struct BackfillNode {
    operator_id: GlobalOperatorId,
    /// How many more actors need to finish,
    /// before this node can finish backfilling.
    remaining_actors: HashSet<ActorId>,
    /// How many more dependencies need to finish,
    /// before this node can be backfilled.
    remaining_dependencies: HashSet<GlobalOperatorId>,
    children: Vec<GlobalOperatorId>,
}

/// Executor done                -> update `operator_id` state
/// Operator done                -> update downstream operator dependency
/// Operator's dependencies done -> queue operator for backfill
#[derive(Clone, Debug, Default)]
pub struct BackfillOrderState {
    // The order plan.
    current_backfill_nodes: HashMap<GlobalOperatorId, BackfillNode>,
    // Remaining nodes to finish
    remaining_backfill_nodes: HashMap<GlobalOperatorId, BackfillNode>,
    locality_provider_state_tables: HashMap<GlobalOperatorId, TableId>,
}

/// Get nodes with some dependencies.
/// These should initially be paused until their dependencies are done.
pub fn get_nodes_with_backfill_dependencies(
    backfill_orders: &ExtendedBackfillOrder,
) -> HashSet<GlobalOperatorId> {
    backfill_orders.values().flatten().copied().collect()
}

// constructor
impl BackfillOrderState {
    pub fn new(
        backfill_orders: &ExtendedBackfillOrder,
        fragment_infos: &HashMap<FragmentId, InflightFragmentInfo>,
    ) -> Self {
        tracing::debug!(?backfill_orders, "initialize backfill order state");
        let mut backfill_nodes: HashMap<GlobalOperatorId, BackfillNode> = HashMap::new();
        let mut locality_provider_state_tables = HashMap::new();

        for (fragment_id, fragment) in fragment_infos {
            visit_backfill_nodes(
                *fragment_id,
                &fragment.nodes,
                |operator_id, upstream_type, stream_node| {
                    if !upstream_type.is_ordered() {
                        return;
                    }
                    if let Some(NodeBody::LocalityProvider(locality_provider)) =
                        stream_node.node_body.as_ref()
                    {
                        let state_table_id = locality_provider
                            .state_table
                            .as_ref()
                            .expect("must have state table")
                            .id;
                        locality_provider_state_tables.insert(operator_id, state_table_id);
                    }
                    backfill_nodes.insert(
                        operator_id,
                        BackfillNode {
                            operator_id,
                            remaining_actors: fragment.actors.keys().copied().collect(),
                            remaining_dependencies: Default::default(),
                            children: backfill_orders
                                .get(&operator_id)
                                .cloned()
                                .unwrap_or_default(),
                        },
                    );
                },
            );
        }

        for (operator_id, children) in backfill_orders.iter() {
            for child in children {
                let child_node = backfill_nodes.get_mut(child).unwrap();
                child_node.remaining_dependencies.insert(*operator_id);
            }
        }

        let mut current_backfill_nodes = HashMap::new();
        let mut remaining_backfill_nodes = HashMap::new();
        for (operator_id, node) in backfill_nodes {
            if node.remaining_dependencies.is_empty() {
                current_backfill_nodes.insert(operator_id, node);
            } else {
                remaining_backfill_nodes.insert(operator_id, node);
            }
        }

        Self {
            current_backfill_nodes,
            remaining_backfill_nodes,
            locality_provider_state_tables,
        }
    }
}

// state transitions
impl BackfillOrderState {
    pub fn finish_executor(&mut self, executor: BackfillExecutor) -> Vec<GlobalOperatorId> {
        let BackfillExecutor {
            actor_id,
            operator_id,
        } = executor;
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
        let (node, is_in_order) = match self.current_backfill_nodes.get_mut(&operator_id) {
            Some(node) => (node, true),
            None => {
                let Some(node) = self.remaining_backfill_nodes.get_mut(&operator_id) else {
                    tracing::error!(
                        %operator_id,
                        %actor_id,
                        "node not found in current_backfill_nodes or remaining_backfill_nodes"
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
            %operator_id,
            "finish_backfilling_actor"
        );
        if node.remaining_actors.is_empty() && is_in_order {
            self.finish_node(operator_id)
        } else {
            vec![]
        }
    }

    pub fn finish_node(&mut self, operator_id: GlobalOperatorId) -> Vec<GlobalOperatorId> {
        let mut newly_scheduled = vec![];
        // Decrease the remaining_dependency_count of the children.
        // If the remaining_dependency_count is 0, add the child to the current_backfill_nodes.
        if let Some(node) = self.current_backfill_nodes.remove(&operator_id) {
            for child_id in &node.children {
                let newly_scheduled_child_finished = {
                    let child = self.remaining_backfill_nodes.get_mut(child_id).unwrap();
                    assert!(
                        child.remaining_dependencies.remove(&operator_id),
                        "missing dependency"
                    );
                    if child.remaining_dependencies.is_empty() {
                        let should_schedule = !child.remaining_actors.is_empty();
                        if should_schedule {
                            tracing::debug!(operator_id = ?child_id, "schedule next backfill node");
                        }
                        self.current_backfill_nodes
                            .insert(child.operator_id, child.clone());
                        if should_schedule {
                            newly_scheduled.push(child.operator_id);
                        }
                        child.remaining_actors.is_empty()
                    } else {
                        false
                    }
                };
                if newly_scheduled_child_finished {
                    newly_scheduled.extend(self.finish_node(*child_id));
                }
            }
        } else {
            tracing::error!(%operator_id, "node not found in current_backfill_nodes");
            return vec![];
        }
        newly_scheduled
    }

    pub fn current_backfill_operator_ids(&self) -> Vec<GlobalOperatorId> {
        self.current_backfill_nodes.keys().copied().collect()
    }

    pub fn get_locality_provider_state_tables(&self) -> &HashMap<GlobalOperatorId, TableId> {
        &self.locality_provider_state_tables
    }

    /// Refresh actor mapping after reschedule and return newly scheduled nodes.
    pub fn refresh_actors(
        &mut self,
        fragment_actors: &HashMap<FragmentId, HashSet<ActorId>>,
    ) -> Vec<GlobalOperatorId> {
        for node in self
            .current_backfill_nodes
            .values_mut()
            .chain(self.remaining_backfill_nodes.values_mut())
        {
            let (fragment_id, _) = unique_operator_id_into_parts(node.operator_id);
            if let Some(actors) = fragment_actors.get(&fragment_id) {
                node.remaining_actors = actors.iter().copied().collect();
            } else {
                node.remaining_actors.clear();
            }
        }

        let finished_nodes: Vec<_> = self
            .current_backfill_nodes
            .iter()
            .filter(|(_, node)| node.remaining_actors.is_empty())
            .map(|(operator_id, _)| *operator_id)
            .collect();

        let mut newly_scheduled = vec![];
        for operator_id in finished_nodes {
            newly_scheduled.extend(self.finish_node(operator_id));
        }
        newly_scheduled
    }
}
