use std::collections::HashMap;

/// This is the "global" `operator_id`.
/// The local `operator_id` is namespaced by the `fragment_id`.
pub(crate) type OperatorId = u64;
pub(crate) type ActorId = u64;

#[derive(Clone, Debug)]
pub(crate) struct BackfillNode {
    operator_id: OperatorId,
    /// How many more actors need to finish,
    /// before this operator can finish backfilling.
    remaining_actor_count: usize,
    /// How many more dependencies need to finish,
    /// before this operator can be backfilled.
    remaining_dependency_count: usize,
    children: Vec<OperatorId>,
}

/// Actor done                   -> update `operator_id` state
/// Operator done                -> update downstream operator dependency
/// Operator's dependencies done -> queue operator for backfill
#[derive(Clone, Debug)]
pub(crate) struct BackfillOrderState {
    // The order plan.
    current_backfill_nodes: HashMap<OperatorId, BackfillNode>,
    // Remaining nodes to finish
    remaining_backfill_nodes: HashMap<OperatorId, BackfillNode>,
    // The mapping between actors and operator_ids
    actor_to_operator_id: HashMap<ActorId, OperatorId>,
}

impl BackfillOrderState {
    fn finish_actor(&mut self, actor_id: ActorId) {
        // Find the operator_id of the actor.
        let operator_id = self.actor_to_operator_id.get(&actor_id).unwrap();
        // Decrease the remaining_actor_count of the operator.
        // If the remaining_actor_count is 0, add the operator to the current_backfill_nodes.
        let node = self.current_backfill_nodes.get_mut(operator_id).unwrap();
        node.remaining_actor_count -= 1;
        if node.remaining_actor_count == 0 {
            self.finish_operator(*operator_id);
        }
    }

    fn finish_operator(&mut self, operator_id: OperatorId) {
        // Decrease the remaining_dependency_count of the children.
        // If the remaining_dependency_count is 0, add the child to the current_backfill_nodes.
        let node = self.current_backfill_nodes.remove(&operator_id).unwrap();
        for child_id in &node.children {
            let child = self.remaining_backfill_nodes.get_mut(child_id).unwrap();
            child.remaining_dependency_count -= 1;
            if child.remaining_dependency_count == 0 {
                self.current_backfill_nodes
                    .insert(child.operator_id, child.clone());
            }
        }
    }
}
