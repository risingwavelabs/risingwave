use std::fmt;

use risingwave_common::catalog::Schema;

use super::{LogicalJoin, PlanRef, PlanTreeNodeBinary, StreamBase, ToStreamProst};
use crate::optimizer::property::{Distribution, WithOrder, WithSchema};

/// `BatchHashJoin` implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct StreamHashJoin {
    pub base: StreamBase,
    logical: LogicalJoin,
}

impl StreamHashJoin {
    pub fn new(logical: LogicalJoin) -> Self {
        // TODO: derive from input
        let base = StreamBase {
            dist: Distribution::any().clone(),
        };
        Self { logical, base }
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

impl ToStreamProst for StreamHashJoin {}
