use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::plan::plan_node::NodeBody;
use risingwave_pb::plan::HashJoinNode;

use super::{
    BatchBase, EqJoinPredicate, LogicalJoin, PlanRef, PlanTreeNodeBinary, ToBatchProst,
    ToDistributedBatch,
};
use crate::optimizer::property::{Distribution, Order, WithSchema};

/// `BatchHashJoin` implements [`super::LogicalJoin`] with hash table. It builds a hash table
/// from inner (right-side) relation and then probes with data from outer (left-side) relation to
/// get output rows.
#[derive(Debug, Clone)]
pub struct BatchHashJoin {
    pub base: BatchBase,
    logical: LogicalJoin,

    /// The join condition must be equivalent to `logical.on`, but seperated into equal and
    /// non-equal parts to facilitate execution later
    eq_join_predicate: EqJoinPredicate,
}

impl BatchHashJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        let ctx = logical.base.ctx.clone();
        // TODO: derive from input
        let base = BatchBase {
            order: Order::any().clone(),
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

impl fmt::Display for BatchHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "BatchHashJoin {{ type: {:?}, predicate: {} }}",
            self.logical.join_type(),
            self.eq_join_predicate()
        )
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
        Self::new(
            self.logical.clone_with_left_right(left, right),
            self.eq_join_predicate.clone(),
        )
    }
    fn left_dist_required(&self) -> &Distribution {
        todo!()
    }
    fn right_dist_required(&self) -> &Distribution {
        todo!()
    }
}

impl_plan_tree_node_for_binary! { BatchHashJoin }

impl WithSchema for BatchHashJoin {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchHashJoin {
    fn to_distributed(&self) -> PlanRef {
        let left = self.left().to_distributed_with_required(
            self.left_order_required(),
            &Distribution::HashShard(self.eq_join_predicate().left_eq_indexes()),
        );
        let right = self.right().to_distributed_with_required(
            self.right_order_required(),
            &Distribution::HashShard(self.eq_join_predicate().right_eq_indexes()),
        );

        self.clone_with_left_right(left, right).into()
    }
}

impl ToBatchProst for BatchHashJoin {
    fn to_batch_prost_body(&self) -> NodeBody {
        NodeBody::HashJoin(HashJoinNode {
            join_type: self.logical.join_type() as i32,
            left_key: self
                .eq_join_predicate
                .left_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            right_key: self
                .eq_join_predicate
                .right_eq_indexes()
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            left_output: (0..self.logical.left().schema().len())
                .into_iter()
                .map(|a| a as i32)
                .collect(),
            right_output: (0..self.logical.right().schema().len())
                .into_iter()
                .map(|a| a as i32)
                .collect(),
        })
    }
}
