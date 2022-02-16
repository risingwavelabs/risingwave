use std::fmt;

use risingwave_common::catalog::Schema;
use risingwave_common::types::DataType;
use risingwave_pb::plan::JoinType;

use super::{
    ColPrunable, IntoPlanRef, JoinPredicate, PlanRef, PlanTreeNodeBinary, StreamHashJoin, ToBatch,
    ToStream,
};
use crate::expr::{assert_input_ref, Expr, ExprImpl};
use crate::optimizer::plan_node::{BatchHashJoin, BatchSortMergeJoin};
use crate::optimizer::property::{Distribution, Order, WithDistribution, WithOrder, WithSchema};

#[derive(Debug, Clone)]
pub struct LogicalJoin {
    left: PlanRef,
    right: PlanRef,
    predicate: JoinPredicate,
    join_type: JoinType,
    schema: Schema,
}
impl fmt::Display for LogicalJoin {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}
impl LogicalJoin {
    pub(crate) fn new(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        predicate: JoinPredicate,
    ) -> Self {
        let schema = Self::derive_schema(left.schema(), right.schema(), join_type);

        let left_cols_num = left.schema().fields.len();
        let input_col_num = left_cols_num + right.schema().fields.len();
        for (k1, k2) in predicate.eq_indexes() {
            assert!(k1 < left_cols_num);
            assert!(k2 >= left_cols_num);
            assert!(k2 < input_col_num);
        }

        LogicalJoin {
            left,
            right,
            schema,
            join_type,
            predicate,
        }
    }

    pub fn create(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on_clause: ExprImpl,
    ) -> PlanRef {
        let left_cols_num = left.schema().fields.len();
        let predicate = JoinPredicate::create(left_cols_num, on_clause);
        Self::new(left, right, join_type, predicate).into_plan_ref()
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
    pub fn predicate(&self) -> &JoinPredicate {
        &self.predicate
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
        Self::new(left, right, self.join_type, self.predicate.clone())
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
        // if self.predicate().is_equal_cond() {
        //     // TODO: check if use SortMergeJoin could satisfy the required_order
        //     let new_left = self.left().to_batch();
        //     let sort_join_required_order =
        //         BatchSortMergeJoin::left_required_order(self.predicate());
        //     if new_left.order().satisfies(&sort_join_required_order) {
        //         let right_required_order =
        // BatchSortMergeJoin::right_required_order_from_left_order(
        // new_left.order(),             self.predicate(),
        //         );
        //         let new_right = self
        //             .left()
        //             .to_batch_with_order_required(&right_required_order);
        //         let new_logical = self.clone_with_left_right(new_left, new_right);
        //         return BatchSortMergeJoin::new(new_logical).into_plan_ref();
        //     } else {
        //         let new_right = self.left().to_batch();
        //         let new_logical = self.clone_with_left_right(new_left, new_right);
        //         return BatchHashJoin::new(new_logical).into_plan_ref();
        //     }
        // }
        todo!(); // nestedLoopJoin
    }
    fn to_batch(&self) -> PlanRef {
        self.to_batch_with_order_required(Order::any())
    }
}
impl ToStream for LogicalJoin {
    fn to_stream(&self) -> PlanRef {
        let left = self
            .left()
            .to_stream_with_dist_required(&Distribution::HashShard(
                self.predicate().left_eq_indexes(),
            ));
        let right = self
            .right()
            .to_stream_with_dist_required(&Distribution::HashShard(
                self.predicate().right_eq_indexes(),
            ));
        StreamHashJoin::new(self.clone_with_left_right(left, right)).into_plan_ref()
    }
}
