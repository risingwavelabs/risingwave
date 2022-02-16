use std::fmt;

use risingwave_common::catalog::Schema;

use super::{
    IntoPlanRef, JoinPredicate, LogicalJoin, PlanRef, PlanTreeNodeBinary, ToDistributedBatch,
};
use crate::optimizer::property::{Distribution, WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct BatchHashJoin {
    logical: LogicalJoin,
}
impl BatchHashJoin {
    pub fn new(logical: LogicalJoin) -> Self {
        Self { logical }
    }
    pub fn predicate(&self) -> &JoinPredicate {
        self.logical.predicate()
    }
}

impl fmt::Display for BatchHashJoin {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl PlanTreeNodeBinary for BatchHashJoin {
    fn left(&self) -> PlanRef {
        self.logical.left()
    }
    fn right(&self) -> PlanRef {
        self.logical.right()
    }
    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(self.logical.clone_with_left_right(left, right))
    }
    fn left_dist_required(&self) -> &Distribution {
        todo!()
    }
    fn right_dist_required(&self) -> &Distribution {
        todo!()
    }
}
impl_plan_tree_node_for_binary! {BatchHashJoin}

impl WithOrder for BatchHashJoin {}
impl WithDistribution for BatchHashJoin {}
impl WithSchema for BatchHashJoin {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}
impl ToDistributedBatch for BatchHashJoin {
    fn to_distributed(&self) -> PlanRef {
        let left = self.left().to_distributed_with_required(
            self.left_order_required(),
            &Distribution::HashShard(self.predicate().left_eq_indexes()),
        );
        let right = self.right().to_distributed_with_required(
            self.right_order_required(),
            &Distribution::HashShard(self.predicate().right_eq_indexes()),
        );

        self.clone_with_left_right(left, right).into_plan_ref()
    }
}
