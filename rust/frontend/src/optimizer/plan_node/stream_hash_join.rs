use std::fmt;

use risingwave_common::catalog::Schema;

use super::{IntoPlanRef, LogicalJoin, PlanRef, PlanTreeNodeBinary, ToStreamProst};
use crate::optimizer::property::{Distribution, WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct StreamHashJoin {
    logical: LogicalJoin,
}
impl StreamHashJoin {
    pub fn new(logical: LogicalJoin) -> Self {
        Self { logical }
    }
}
impl fmt::Display for StreamHashJoin {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl PlanTreeNodeBinary for StreamHashJoin {
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
impl_plan_tree_node_for_binary! {StreamHashJoin}
impl WithSchema for StreamHashJoin {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}
impl WithDistribution for StreamHashJoin {}
impl WithOrder for StreamHashJoin {}
impl ToStreamProst for StreamHashJoin {}
