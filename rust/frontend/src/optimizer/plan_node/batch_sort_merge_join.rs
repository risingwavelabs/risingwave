use std::fmt;

use risingwave_common::catalog::Schema;

use super::{
    BatchBase, EqJoinPredicate, LogicalJoin, PlanRef, PlanTreeNodeBinary, ToBatchProst,
    ToDistributedBatch,
};
use crate::optimizer::property::{
    Direction, Distribution, FieldOrder, Order, WithOrder, WithSchema,
};

/// `BatchSortMergeJoin` implements [`super::LogicalJoin`] by merging left & right relations in
/// a streaming manner. The input relation must have been ordered by the equi-join key(s).
#[derive(Debug, Clone)]
pub struct BatchSortMergeJoin {
    pub base: BatchBase,
    logical: LogicalJoin,
    eq_join_predicate: EqJoinPredicate,
}

impl BatchSortMergeJoin {
    pub fn new(logical: LogicalJoin, eq_join_predicate: EqJoinPredicate) -> Self {
        let order = Self::derive_order(logical.left().order(), logical.right().order());
        // TODO: derive from input
        let base = BatchBase {
            order,
            dist: Distribution::any().clone(),
        };

        Self {
            logical,
            base,
            eq_join_predicate,
        }
    }

    // Panic if input orders can't satisfy sortMergeJoin
    fn derive_order(_left: &Order, _right: &Order) -> Order {
        todo!()
    }

    pub fn left_required_order(eq_join_predicate: &EqJoinPredicate) -> Order {
        Order {
            field_order: eq_join_predicate
                .left_eq_indexes()
                .into_iter()
                .map(|index| FieldOrder {
                    index,
                    direct: Direction::Any,
                })
                .collect(),
        }
    }
    pub fn right_required_order_from_left_order(
        _left_order: &Order,
        _eq_join_predicate: &EqJoinPredicate,
    ) -> Order {
        todo!()
    }
}

impl fmt::Display for BatchSortMergeJoin {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl PlanTreeNodeBinary for BatchSortMergeJoin {
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
    fn right_order_required(&self) -> &Order {
        todo!()
    }
    fn left_order_required(&self) -> &Order {
        todo!()
    }
    fn left_dist_required(&self) -> &Distribution {
        todo!()
    }
    fn right_dist_required(&self) -> &Distribution {
        todo!()
    }
}
impl_plan_tree_node_for_binary! {BatchSortMergeJoin}

impl WithSchema for BatchSortMergeJoin {
    fn schema(&self) -> &Schema {
        self.logical.schema()
    }
}

impl ToDistributedBatch for BatchSortMergeJoin {
    fn to_distributed(&self) -> PlanRef {
        todo!()
    }
}

impl ToBatchProst for BatchSortMergeJoin {}
