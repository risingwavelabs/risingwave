use std::rc::Rc;

use paste::paste;

use super::super::plan_node::*;
use super::PlanPass;
use crate::for_logical_plan_nodes;
use crate::planner::property::{Distribution, Order};
use crate::planner::PlanRef;

macro_rules! def_to_batch_visit_func {
  ([], $({ $convention:ident, $name:ident }),*) => {
    paste! {
      fn visit(&mut self, plan: PlanRef, required_order: Order,
      ) -> PlanRef{
        let ret = match plan.node_type() {
          $(
            PlanNodeType::[<$convention $name>] => self.[<visit_ $convention:snake _ $name:snake>](plan.downcast_ref::<[<$convention $name>]>().unwrap()),
          )*
          _=> unreachable!()
        };
        required_order.enforce_if_not_satisfies(ret)
      }
    }
  }
}

/// Traverse plan tree whose nodes all are logical, transform logical node to batch node from bottom
/// to up, and make sure the order property in the tree is correct.
pub struct LogicalToBatchPass {}
impl PlanPass for LogicalToBatchPass {
    fn pass_with_require(
        &mut self,
        plan: PlanRef,
        required_order: Order,
        _required_dist: Distribution,
    ) -> PlanRef {
        self.visit(plan, required_order)
    }
}
impl Default for LogicalToBatchPass {
    fn default() -> Self {
        Self::new()
    }
}
impl LogicalToBatchPass {
    fn new() -> Self {
        Self {}
    }
}

impl LogicalToBatchPass {
    for_logical_plan_nodes! {def_to_batch_visit_func}

    fn visit_logical_project(&mut self, plan: &LogicalProject) -> PlanRef {
        let new_child = self.visit(plan.child(), Order::any());
        let new_logical = plan.clone_with_child(new_child);
        Rc::new(BatchProject::new(new_logical))
    }

    fn visit_logical_scan(&mut self, _plan: &LogicalScan) -> PlanRef {
        Rc::new(BatchSeqScan {})
    }

    fn visit_logical_join(&mut self, plan: &LogicalJoin) -> PlanRef {
        if plan.get_predicate().is_equal_cond() {
            let new_left = self.visit(plan.left(), Order::any());
            let sort_join_required_order =
                BatchSortMergeJoin::left_required_order(plan.get_predicate());
            if new_left.order().satisfies(&sort_join_required_order) {
                let right_required_order = BatchSortMergeJoin::right_required_order_from_left_order(
                    new_left.order(),
                    plan.get_predicate(),
                );
                let new_right = self.visit(plan.left(), right_required_order);
                let new_logical = plan.clone_with_left_right(new_left, new_right);
                return Rc::new(BatchSortMergeJoin::new(new_logical));
            } else {
                let new_right = self.visit(plan.left(), Order::any());
                let new_logical = plan.clone_with_left_right(new_left, new_right);
                return Rc::new(BatchHashJoin::new(new_logical));
            }
        }
        todo!(); // nestedLoopJoin
    }
}
