use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_pb::plan::JoinType;

use super::{
    ColPrunable, IntoPlanRef, PlanRef, PlanTreeNodeBinary, StreamHashJoin, ToBatch, ToStream,
};
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::{
    BatchHashJoin, BatchSortMergeJoin, EqJoinPredicate, LogicalFilter,
};
use crate::optimizer::property::{Distribution, Order, WithDistribution, WithOrder, WithSchema};
use crate::utils::Condition;

#[derive(Debug, Clone)]
pub struct LogicalJoin {
    left: PlanRef,
    right: PlanRef,
    on: Condition,
    join_type: JoinType,
    schema: Schema,
}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl LogicalJoin {
    pub(crate) fn new(left: PlanRef, right: PlanRef, join_type: JoinType, on: Condition) -> Self {
        let schema = Self::derive_schema(left.schema(), right.schema(), join_type);

        LogicalJoin {
            left,
            right,
            schema,
            join_type,
            on,
        }
    }

    pub fn create(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on_clause: ExprImpl,
    ) -> PlanRef {
        Self::new(left, right, join_type, Condition::with_expr(on_clause)).into_plan_ref()
    }

    fn derive_schema(left: &Schema, right: &Schema, join_type: JoinType) -> Schema {
        let mut new_fields = Vec::with_capacity(left.fields.len() + right.fields.len());
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                new_fields.extend_from_slice(&left.fields);
                new_fields.extend_from_slice(&right.fields);
                Schema { fields: new_fields }
            }
            _ => unimplemented!(),
        }
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        &self.on
    }
}

impl PlanTreeNodeBinary for LogicalJoin {
    fn left(&self) -> PlanRef {
        self.left.clone()
    }

    fn right(&self) -> PlanRef {
        self.right.clone()
    }

    fn clone_with_left_right(&self, left: PlanRef, right: PlanRef) -> Self {
        Self::new(left, right, self.join_type, self.on.clone())
    }
}
impl_plan_tree_node_for_binary! {LogicalJoin}
impl WithOrder for LogicalJoin {}

impl WithDistribution for LogicalJoin {}

impl WithSchema for LogicalJoin {
    fn schema(&self) -> &Schema {
        &self.schema
    }
}

impl ColPrunable for LogicalJoin {}

impl ToBatch for LogicalJoin {
    fn to_batch_with_order_required(&self, _required_order: &Order) -> PlanRef {
        let predicate = EqJoinPredicate::create(
            self.left.schema().len(),
            self.right.schema().len(),
            self.on.clone(),
        );
        let can_pull_filter = self.join_type == JoinType::Inner;
        let has_non_eq = predicate.has_non_eq();
        let has_eq = predicate.has_eq();
        if has_eq && (!has_non_eq || can_pull_filter) {
            let need_pull_filter = has_non_eq;
            let new_left = self.left().to_batch();
            let sort_join_required_order = BatchSortMergeJoin::left_required_order(&predicate);
            let use_sort_merge_join = new_left.order().satisfies(&sort_join_required_order);

            let phy_join = if use_sort_merge_join {
                let right_required_order = BatchSortMergeJoin::right_required_order_from_left_order(
                    new_left.order(),
                    &predicate,
                );
                let new_right = self
                    .left()
                    .to_batch_with_order_required(&right_required_order);
                let new_logical = self.clone_with_left_right(new_left, new_right);
                BatchSortMergeJoin::new(new_logical, predicate.clone()).into_plan_ref()
            } else {
                let new_right = self.left().to_batch();
                let new_logical = self.clone_with_left_right(new_left, new_right);
                BatchHashJoin::new(new_logical, predicate.clone()).into_plan_ref()
            };

            if need_pull_filter {
                let _filter = LogicalFilter::new(phy_join, predicate.non_eq_cond());
                todo!()
                // TODO: add a BatchFilter on the top of the join.
            } else {
                return phy_join;
            }
        }
        todo!(); // nestedLoopJoin
    }
    fn to_batch(&self) -> PlanRef {
        self.to_batch_with_order_required(Order::any())
    }
}

impl ToStream for LogicalJoin {
    fn to_stream(&self) -> PlanRef {
        let predicate = EqJoinPredicate::create(
            self.left.schema().len(),
            self.right.schema().len(),
            self.on.clone(),
        );
        assert!(!predicate.has_non_eq());
        let left = self
            .left()
            .to_stream_with_dist_required(&Distribution::HashShard(predicate.left_eq_indexes()));
        let right = self
            .right()
            .to_stream_with_dist_required(&Distribution::HashShard(predicate.right_eq_indexes()));
        StreamHashJoin::new(self.clone_with_left_right(left, right)).into_plan_ref()
    }
}
