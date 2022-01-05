use std::rc::Rc;

use paste::paste;

use super::super::plan_node::*;
use super::PlanPass;
use crate::for_logical_plan_nodes;
use crate::planner::property::{Distribution, Order};
use crate::planner::PlanRef;

// TODO: use passThrough get a better plan

macro_rules! def_to_stream_visit_func {
  ([], $({ $convention:ident, $name:ident }),*) => {
    paste! {
      fn visit(&mut self, plan: PlanRef, required_dist: Distribution) -> PlanRef{
        let ret = match plan.node_type() {
          $(
            PlanNodeType::[<$convention $name>] => self.[<visit_ $convention:snake _ $name:snake>](plan.downcast_ref::<[<$convention $name>]>().unwrap()),
          )*
          _=> unreachable!()
        };
        required_dist.enforce_if_not_satisfies(ret, &Order::any())
      }
    }
  }
}

/// Traverse plan tree whose nodes all are logical, transform logical node to stream node from
/// bottom to up, make sure the order property in the tree is correct and make best effort to let
/// actor sharding and parallel (by inserting exchange to plan tree)
pub struct LogicalToStreamPass {}
impl PlanPass for LogicalToStreamPass {
    fn pass_with_require(
        &mut self,
        plan: PlanRef,
        _required_order: Order,
        required_dist: Distribution,
    ) -> PlanRef {
        self.visit(plan, required_dist)
    }
}
impl Default for LogicalToStreamPass {
    fn default() -> Self {
        Self::new()
    }
}
impl LogicalToStreamPass {
    fn new() -> Self {
        Self {}
    }
}

impl LogicalToStreamPass {
    for_logical_plan_nodes! {def_to_stream_visit_func}

    fn visit_logical_project(&mut self, proj: &LogicalProject) -> PlanRef {
        let new_child = self.visit(proj.child(), Distribution::any());
        let new_logical = proj.clone_with_child(new_child);
        Rc::new(StreamProject::new(new_logical))
    }

    fn visit_logical_scan(&mut self, _scan: &LogicalScan) -> PlanRef {
        Rc::new(StreamTableSource {})
    }

    fn visit_logical_join(&mut self, join: &LogicalJoin) -> PlanRef {
        // only hash shuffle join now
        // TODO: we have not Statistics to use broadcast join
        let left = self.visit(
            join.left(),
            Distribution::HashShard(join.get_predicate().left_keys()),
        );
        let right = self.visit(
            join.right(),
            Distribution::HashShard(join.get_predicate().right_keys()),
        );
        Rc::new(StreamHashJoin::new(join.clone_with_left_right(left, right)))
    }
}
