use std::fmt;

use risingwave_common::catalog::Schema;

use super::{LogicalJoin, PlanRef, PlanTreeNodeBinary, StreamBase, ToStreamProst};
use crate::optimizer::plan_node::EqJoinPredicate;
use crate::optimizer::property::{Distribution, WithSchema};

/// `BatchHashJoin` implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct StreamHashJoin {
    pub base: StreamBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but seperated into equi and non-equi
    /// parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,
}

impl StreamHashJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = StreamBase {
            dist: Distribution::any().clone(),
            id: ctx.borrow_mut().get_id(),
            ctx: ctx.clone(),
        };

        Self {
            base,
            logical,
            eq_join_predicate,
        }
    }

    /// Get a reference to the batch hash join's eq join predicate.
    pub fn eq_join_predicate(&self) -> &EqJoinPredicate {
        &self.eq_join_predicate
    }
}

impl fmt::Display for StreamHashJoin {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "StreamHashJoin(predicate: {})", self.eq_join_predicate())
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

impl_plan_tree_node_for_binary! { StreamHashJoin }

impl WithSchema for StreamHashJoin {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToStreamProst for StreamHashJoin {}
