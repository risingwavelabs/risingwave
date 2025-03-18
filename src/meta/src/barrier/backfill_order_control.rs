use std::collections::HashMap;

use crate::model::FragmentId;

/// This is the "global" `fragment_id`.
/// The local `fragment_id` is namespaced by the `fragment_id`.
pub type ActorId = u32;

#[derive(Clone, Debug)]
pub struct BackfillNode {
    fragment_id: FragmentId,
    /// How many more actors need to finish,
    /// before this fragment can finish backfilling.
    remaining_actor_count: usize,
    /// How many more dependencies need to finish,
    /// before this fragment can be backfilled.
    remaining_dependency_count: usize,
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

impl BackfillOrderState {
    pub fn finish_actor(&mut self, actor_id: ActorId) -> Vec<FragmentId> {
        // Find the fragment_id of the actor.
        let fragment_id = self.actor_to_fragment_id.get(&actor_id).unwrap();
        // Decrease the remaining_actor_count of the operator.
        // If the remaining_actor_count is 0, add the operator to the current_backfill_nodes.
        let node = self.current_backfill_nodes.get_mut(fragment_id).unwrap();
        node.remaining_actor_count -= 1;
        if node.remaining_actor_count == 0 {
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
            child.remaining_dependency_count -= 1;
            if child.remaining_dependency_count == 0 {
                self.current_backfill_nodes
                    .insert(child.fragment_id, child.clone());
                newly_scheduled.push(child.fragment_id)
            }
        }
        newly_scheduled
    }
}
