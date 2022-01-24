use paste::paste;

use super::super::plan_node::*;
use super::PlanPass;
use crate::for_batch_plan_nodes;
use crate::optimizer::property::{Distribution, Order};

macro_rules! def_batch_to_distributed_visit_func {
  ([], $({ $convention:ident, $name:ident }),*) => {
    paste! {
      fn visit(&mut self, plan: PlanRef, required_order: Order, required_dist: Distribution) -> PlanRef{
        let mut ret = match plan.node_type() {
          $(
            PlanNodeType::[<$convention $name>] => self.[<visit_ $convention:snake _ $name:snake>](plan.downcast_ref::<[<$convention $name>]>().unwrap()),
          )*
          _=> unreachable!()
        };
        ret = required_order.enforce_if_not_satisfies(ret);
        required_dist.enforce_if_not_satisfies(ret, &required_order)
      }
    }
  }
}

/// Traverse plan tree whose nodes all are batch, insert exchange from bottom to up
/// to make sure the distribution property in the tree is correct and make best effort to let
/// executor parallel,
pub struct BatchToDistributed {}
impl PlanPass for BatchToDistributed {
    fn pass_with_require(
        &mut self,
        plan: PlanRef,
        required_order: Order,
        required_dist: Distribution,
    ) -> PlanRef {
        self.visit(plan, required_order, required_dist)
    }
    fn pass(&mut self, plan: PlanRef) -> PlanRef {
        self.pass_with_require(plan, Order::any(), Distribution::Single)
    }
}
impl Default for BatchToDistributed {
    fn default() -> Self {
        Self::new()
    }
}
impl BatchToDistributed {
    fn new() -> Self {
        Self {}
    }
}

trait BatchToDistributedTrait {}
impl BatchToDistributed {
    for_batch_plan_nodes! {def_batch_to_distributed_visit_func}
    fn visit_batch_exchange(&mut self, _exchange: &BatchExchange) -> PlanRef {
        unreachable!()
    }

    fn visit_batch_seq_scan(&mut self, scan: &BatchSeqScan) -> PlanRef {
        scan.clone().into_plan_ref()
    }

    fn visit_batch_hash_join(&mut self, join: &BatchHashJoin) -> PlanRef {
        // only hash shuffle join now
        // TODO: we have not Statistics to use broadcast join
        let left = self.visit(
            join.left(),
            join.left_order_required(),
            Distribution::HashShard(join.predicate().left_keys()),
        );
        let right = self.visit(
            join.right(),
            join.right_order_required(),
            Distribution::HashShard(join.predicate().right_keys()),
        );
        join.clone_with_left_right(left, right).into_plan_ref()
    }
    fn visit_batch_sort_merge_join(&mut self, _join: &BatchSortMergeJoin) -> PlanRef {
        todo!()
    }

    fn visit_batch_sort(&mut self, _sort: &BatchSort) -> PlanRef {
        todo!()
    }

    fn visit_batch_project(&mut self, _proj: &BatchProject) -> PlanRef {
        todo!()
    }
}
