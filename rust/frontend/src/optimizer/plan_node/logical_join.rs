use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::{Either, Itertools};
use risingwave_common::catalog::Schema;
use risingwave_pb::plan::JoinType;

use super::{
    ColPrunable, LogicalBase, LogicalProject, PlanRef, PlanTreeNodeBinary, StreamHashJoin, ToBatch,
    ToStream,
};
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::{
    BatchHashJoin, BatchSortMergeJoin, CollectRequiredCols, EqJoinPredicate, LogicalFilter,
};
use crate::optimizer::property::{Distribution, Order, WithDistribution, WithOrder, WithSchema};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalJoin` that combines two relations according to some condition.
///
/// Each output row has fields from the left and right inputs. The set of output rows is a subset
/// of the cartesian product of the two inputs; precisely which subset depends on the join
/// condition.
#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub(super) base: LogicalBase,
    left: PlanRef,
    right: PlanRef,
    on: Condition,
    join_type: JoinType,
}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        todo!()
    }
}

impl LogicalJoin {
    pub(crate) fn new(left: PlanRef, right: PlanRef, join_type: JoinType, on: Condition) -> Self {
        let schema = Self::derive_schema(left.schema(), right.schema(), join_type);
        let base = LogicalBase { schema };
        LogicalJoin {
            left,
            right,
            join_type,
            on,
            base,
        }
    }

    pub fn create(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        on_clause: ExprImpl,
    ) -> PlanRef {
        Self::new(left, right, join_type, Condition::with_expr(on_clause)).into()
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

impl ColPrunable for LogicalJoin {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        match self.join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {}
            _ => unimplemented!(),
        }

        let left_len = self.left.schema().fields.len();

        let mut visitor = CollectRequiredCols {
            required_cols: required_cols.clone(),
        };
        self.on.visit_expr(&mut visitor);

        let mut on = self.on.clone();
        let mut mapping = ColIndexMapping::with_remaining_columns(&visitor.required_cols);
        on = on.rewrite_expr(&mut mapping);

        let (left_required_cols, right_required_cols): (FixedBitSet, FixedBitSet) =
            visitor.required_cols.ones().partition_map(|i| {
                if i < left_len {
                    Either::Left(i)
                } else {
                    Either::Right(i - left_len)
                }
            });

        let join = LogicalJoin::new(
            self.left.prune_col(&left_required_cols),
            self.right.prune_col(&right_required_cols),
            self.join_type,
            on,
        );

        if required_cols == &visitor.required_cols {
            join.into()
        } else {
            LogicalProject::with_mapping(
                join.into(),
                ColIndexMapping::with_remaining_columns(
                    &required_cols.ones().map(|i| mapping.map(i)).collect(),
                ),
            )
            .into()
        }
    }
}

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
                BatchSortMergeJoin::new(new_logical, predicate.clone()).into()
            } else {
                let new_right = self.left().to_batch();
                let new_logical = self.clone_with_left_right(new_left, new_right);
                BatchHashJoin::new(new_logical, predicate.clone()).into()
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
        StreamHashJoin::new(self.clone_with_left_right(left, right)).into()
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::{Field, TableId};
    use risingwave_common::types::DataType;
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef};
    use crate::optimizer::plan_node::{LogicalScan, PlanTreeNodeUnary};

    #[test]
    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [2,3] will result in
    /// ```text
    /// Project(input_ref(1), input_ref(2))
    ///   Join(on: input_ref(0)=input_ref(2))
    ///     TableScan(v2, v3)
    ///     TableScan(v4)
    /// ```
    fn test_prune_join() {
        let ty = DataType::Int32;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field {
                data_type: ty.clone(),
                name: format!("v{}", i),
            })
            .collect();
        let left = LogicalScan::new(
            "left".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema {
                fields: fields[0..3].to_vec(),
            },
        );
        let right = LogicalScan::new(
            "right".to_string(),
            TableId::new(0),
            vec![4.into(), 5.into(), 6.into()],
            Schema {
                fields: fields[3..6].to_vec(),
            },
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        );

        // Perform the prune
        let mut required_cols = FixedBitSet::with_capacity(6);
        required_cols.extend(vec![2, 3]);
        let plan = join.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 1);
        assert_eq_input_ref!(&project.exprs()[1], 2);

        let join = project.input();
        let join = join.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 3);
        assert_eq!(join.schema().fields(), &fields[1..4]);

        match join.on.clone().to_expr() {
            ExprImpl::FunctionCall(call) => {
                assert_eq_input_ref!(&call.inputs()[0], 0);
                assert_eq_input_ref!(&call.inputs()[1], 2);
            }
            _ => panic!("Expected function call"),
        }

        let left = join.left();
        let left = left.as_logical_scan().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..3]);
        let right = join.right();
        let right = right.as_logical_scan().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    #[test]
    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [1,3] will result in
    /// ```text
    /// Join(on: input_ref(0)=input_ref(1))
    ///   TableScan(v2)
    ///   TableScan(v4)
    /// ```
    fn test_prune_join_no_project() {
        let ty = DataType::Int32;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field {
                data_type: ty.clone(),
                name: format!("v{}", i),
            })
            .collect();
        let left = LogicalScan::new(
            "left".to_string(),
            TableId::new(0),
            vec![1.into(), 2.into(), 3.into()],
            Schema {
                fields: fields[0..3].to_vec(),
            },
        );
        let right = LogicalScan::new(
            "right".to_string(),
            TableId::new(0),
            vec![4.into(), 5.into(), 6.into()],
            Schema {
                fields: fields[3..6].to_vec(),
            },
        );
        let on: ExprImpl = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, ty.clone()))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, ty))),
                ],
            )
            .unwrap(),
        ));
        let join_type = JoinType::Inner;
        let join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on),
        );

        // Perform the prune
        let mut required_cols = FixedBitSet::with_capacity(6);
        required_cols.extend(vec![1, 3]);
        let plan = join.prune_col(&required_cols);

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[1]);
        assert_eq!(join.schema().fields()[1], fields[3]);

        match join.on.clone().to_expr() {
            ExprImpl::FunctionCall(call) => {
                assert_eq_input_ref!(&call.inputs()[0], 0);
                assert_eq_input_ref!(&call.inputs()[1], 1);
            }
            _ => panic!("Expected function call"),
        }

        let left = join.left();
        let left = left.as_logical_scan().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..2]);
        let right = join.right();
        let right = right.as_logical_scan().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }
}
