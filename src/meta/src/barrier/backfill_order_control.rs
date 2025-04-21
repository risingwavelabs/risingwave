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

use risingwave_pb::stream_plan::FragmentTypeFlag;

use crate::model::{FragmentId, StreamJobFragments};

/// This is the "global" `fragment_id`.
/// The local `fragment_id` is namespaced by the `fragment_id`.
pub type ActorId = u32;

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
#[derive(Clone, Debug)]
pub struct BackfillOrderState {
    // The order plan.
    current_backfill_nodes: HashMap<FragmentId, BackfillNode>,
    // Remaining nodes to finish
    remaining_backfill_nodes: HashMap<FragmentId, BackfillNode>,
    // The mapping between actors and fragment_ids
    actor_to_fragment_id: HashMap<ActorId, FragmentId>,
}

pub fn get_root_nodes(
    backfill_orders: &HashMap<FragmentId, Vec<FragmentId>>,
    stream_job_fragments: &StreamJobFragments,
) -> Vec<FragmentId> {
    let mut root_nodes = HashSet::new();
    for fragment in stream_job_fragments.fragments() {
        if fragment.fragment_type_mask
            & (FragmentTypeFlag::StreamScan as u32 | FragmentTypeFlag::SourceScan as u32)
            > 0
        {
            root_nodes.insert(fragment.fragment_id);
        }
    }

    for children in backfill_orders.values() {
        for child in children {
            root_nodes.remove(child);
        }
    }

    root_nodes.into_iter().collect()
}

// constructor
impl BackfillOrderState {
    pub fn new(
        backfill_orders: HashMap<FragmentId, Vec<FragmentId>>,
        stream_job_fragments: &StreamJobFragments,
    ) -> Self {
        tracing::debug!(?backfill_orders, "initialize backfill order state");
        let actor_to_fragment_id = stream_job_fragments.actor_fragment_mapping();

        let mut backfill_nodes: HashMap<FragmentId, BackfillNode> = HashMap::new();

        for fragment in stream_job_fragments.fragments() {
            if fragment.fragment_type_mask
                & (FragmentTypeFlag::StreamScan as u32 | FragmentTypeFlag::SourceScan as u32)
                > 0
            {
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
            for child in &children {
                let child_node = backfill_nodes.get_mut(child).unwrap();
                child_node.remaining_dependencies.insert(fragment_id);
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
        }
    }
}

// Getters
impl BackfillOrderState {
    pub fn get_current_nodes(&self) -> Vec<FragmentId> {
        self.current_backfill_nodes.keys().cloned().collect()
    }
}

// state transitions
impl BackfillOrderState {
    pub fn finish_actor(&mut self, actor_id: ActorId) -> Vec<FragmentId> {
        // Find the fragment_id of the actor.
        let fragment_id = self.actor_to_fragment_id.get(&actor_id).unwrap();
        // Decrease the remaining_actor_count of the operator.
        // If the remaining_actor_count is 0, add the operator to the current_backfill_nodes.
        let node = self.current_backfill_nodes.get_mut(fragment_id).unwrap();
        assert!(node.remaining_actors.remove(&actor_id), "missing actor");
        tracing::debug!(
            actor_id,
            remaining_actors = node.remaining_actors.len(),
            fragment_id,
            "finish_backfilling_actor"
        );
        if node.remaining_actors.is_empty() {
            return self.finish_fragment(*fragment_id);
        }
        vec![]
    }

    pub fn finish_fragment(&mut self, fragment_id: FragmentId) -> Vec<FragmentId> {
        let mut newly_scheduled = vec![];
        // Decrease the remaining_dependency_count of the children.
        // If the remaining_dependency_count is 0, add the child to the current_backfill_nodes.
        let node = self.current_backfill_nodes.remove(&fragment_id).unwrap();
        for child_id in &node.children {
            let child = self.remaining_backfill_nodes.get_mut(child_id).unwrap();
            assert!(
                child.remaining_dependencies.remove(&fragment_id),
                "missing dependency"
            );
            if child.remaining_dependencies.is_empty() {
                tracing::debug!(fragment_id = ?child_id, "schedule next backfill node");
                self.current_backfill_nodes
                    .insert(child.fragment_id, child.clone());
                newly_scheduled.push(child.fragment_id)
            }
        }
        newly_scheduled
    }
}
