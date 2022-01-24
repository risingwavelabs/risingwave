use paste::paste;

use super::super::plan_node::*;
use super::PlanPass;
use crate::for_logical_plan_nodes;
use crate::optimizer::property::{Distribution, Order};
use crate::optimizer::PlanRef;

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
    fn visit_logical_agg(&mut self, _valuse: &LogicalAgg) -> PlanRef {
        todo!()
    }

    fn visit_logical_values(&mut self, _valuse: &LogicalValues) -> PlanRef {
        todo!()
    }

    fn visit_logical_filter(&mut self, _filter: &LogicalFilter) -> PlanRef {
        todo!()
    }

    fn visit_logical_project(&mut self, plan: &LogicalProject) -> PlanRef {
        let new_input = self.visit(plan.input(), Order::any());
        let new_logical = plan.clone_with_input(new_input);
        BatchProject::new(new_logical).into_plan_ref()
    }

    fn visit_logical_scan(&mut self, _plan: &LogicalScan) -> PlanRef {
        BatchSeqScan {}.into_plan_ref()
    }

    fn visit_logical_join(&mut self, plan: &LogicalJoin) -> PlanRef {
        if plan.predicate().is_equal_cond() {
            let new_left = self.visit(plan.left(), Order::any());
            let sort_join_required_order =
                BatchSortMergeJoin::left_required_order(plan.predicate());
            if new_left.order().satisfies(&sort_join_required_order) {
                let right_required_order = BatchSortMergeJoin::right_required_order_from_left_order(
                    new_left.order(),
                    plan.predicate(),
                );
                let new_right = self.visit(plan.left(), right_required_order);
                let new_logical = plan.clone_with_left_right(new_left, new_right);
                return BatchSortMergeJoin::new(new_logical).into_plan_ref();
            } else {
                let new_right = self.visit(plan.left(), Order::any());
                let new_logical = plan.clone_with_left_right(new_left, new_right);
                return BatchHashJoin::new(new_logical).into_plan_ref();
            }
        }
        todo!(); // nestedLoopJoin
    }
}
