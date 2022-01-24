use std::fmt;

use risingwave_common::catalog::Schema;

use super::{IntoPlanRef, JoinPredicate, LogicalJoin, PlanRef, PlanTreeNodeBinary};
use crate::optimizer::property::{WithDistribution, WithOrder, WithSchema};

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
}
impl_plan_tree_node_for_binary! {BatchHashJoin}

impl WithOrder for BatchHashJoin {}
impl WithDistribution for BatchHashJoin {}
impl WithSchema for BatchHashJoin {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}
