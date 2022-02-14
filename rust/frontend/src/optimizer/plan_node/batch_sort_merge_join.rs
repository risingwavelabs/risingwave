use std::fmt;

use risingwave_common::catalog::Schema;

use super::{
    IntoPlanRef, JoinPredicate, LogicalJoin, PlanRef, PlanTreeNodeBinary, ToDistributedBatch,
};
use crate::optimizer::property::{
    Direction, Distribution, FieldOrder, Order, WithDistribution, WithOrder, WithSchema,
};
#[derive(Debug, Clone)]
pub struct BatchSortMergeJoin {
    logical: LogicalJoin,
    order: Order,
}
impl BatchSortMergeJoin {
    pub fn new(logical: LogicalJoin) -> Self {
        let order = Self::derive_order(logical.left().order(), logical.right().order());
        Self { logical, order }
    }

    // Panic if input orders can't satisfy sortMergeJoin
    fn derive_order(_left: &Order, _right: &Order) -> Order {
        todo!()
    }

    pub fn left_required_order(join_predicate: &JoinPredicate) -> Order {
        Order {
            field_order: join_predicate
                .left_keys()
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
        _join_predicate: &JoinPredicate,
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
        Self::new(self.logical.clone_with_left_right(left, right))
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

impl WithOrder for BatchSortMergeJoin {
    fn order(&self) -> &Order {
        &self.order
    }
}
impl WithDistribution for BatchSortMergeJoin {}
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
