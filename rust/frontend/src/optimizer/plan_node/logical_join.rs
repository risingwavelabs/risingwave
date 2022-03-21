// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use std::fmt::{self};
use std::iter;

use fixedbitset::FixedBitSet;
use itertools::{Either, Itertools};
use risingwave_common::catalog::Schema;
use risingwave_pb::plan::JoinType;

use super::{
    ColPrunable, LogicalProject, PlanBase, PlanRef, PlanTreeNodeBinary, StreamHashJoin, ToBatch,
    ToStream,
};
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::{
    BatchFilter, BatchHashJoin, CollectInputRef, EqJoinPredicate, LogicalFilter, StreamFilter,
};
use crate::optimizer::property::{Distribution, WithSchema};
use crate::utils::{ColIndexMapping, Condition};

/// `LogicalJoin` combines two relations according to some condition.
///
/// Each output row has fields from the left and right inputs. The set of output rows is a subset
/// of the cartesian product of the two inputs; precisely which subset depends on the join
/// condition.
#[derive(Debug, Clone)]
pub struct LogicalJoin {
    pub base: PlanBase,
    left: PlanRef,
    right: PlanRef,
    on: Condition,
    join_type: JoinType,
}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "LogicalJoin {{ type: {:?}, on: {} }}",
            &self.join_type, &self.on
        )
    }
}

impl LogicalJoin {
    pub(crate) fn new(left: PlanRef, right: PlanRef, join_type: JoinType, on: Condition) -> Self {
        let ctx = left.ctx();
        let schema = Self::derive_schema(
            left.schema(),
            right.schema(),
            left.pk_indices(),
            right.pk_indices(),
            join_type,
        );
        let base = PlanBase::new_logical(ctx, schema);
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

    pub fn out_column_num(left_len: usize, right_len: usize, join_type: JoinType) -> usize {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                left_len + right_len
            }
            _ => unimplemented!(),
        }
    }

    /// get the Mapping of columnIndex from output column index to left column index
    pub fn o2l_col_mapping(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::new(
                    (0..left_len)
                        .into_iter()
                        .map(|x| Some(x))
                        .chain(iter::repeat(None).take(right_len))
                        .collect_vec(),
                )
            }
            _ => unimplemented!(),
        }
    }

    /// get the Mapping of columnIndex from output column index to right column index
    pub fn o2r_col_mapping(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
            }
            _ => unimplemented!(),
        }
    }

    /// get the Mapping of columnIndex from left column index to output column index
    pub fn l2o_col_mapping(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::o2l_col_mapping(left_len, right_len, join_type).inverse()
    }

    /// get the Mapping of columnIndex from right column index to output column index
    pub fn r2o_col_mapping(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::o2r_col_mapping(left_len, right_len, join_type).inverse()
    }

    fn derive_schema(left_schema: &Schema, right_schema: &Schema, join_type: JoinType) -> Schema {
        let left_len = left_schema.len();
        let right_len = right_schema.len();
        let out_column_num = Self::out_column_num(left_len, right_len, join_type);
        let o2l = Self::o2l_col_mapping(left_len, right_len, join_type);
        let o2r = Self::o2r_col_mapping(left_len, right_len, join_type);
        let mut fields = (0..out_column_num)
            .into_iter()
            .map(|i| match (o2l.try_map(i), o2r.try_map(i)) {
                (Some(l_i), None) => left_schema.fields()[l_i],
                (None, Some(r_i)) => right_schema.fields()[r_i],
                _ => panic!(),
            })
            .collect();
        Schema { fields }
    }

    pub fn derive_pk(left_pk: &[usize], right_pk: &[usize], join_type: JoinType) -> Vec<usize> {
        let left_len = left_pk.len();
        let right_len = right_pk.len();
        let l2o = Self::l2o_col_mapping(left_len, right_len, join_type);
        let r2o = Self::r2o_col_mapping(left_len, right_len, join_type);
        left_pk
            .iter()
            .map(|index| l2o.map(*index))
            .chain(right_pk.iter().map(|iindex| r2o.map(*index)))
            .collect()
    }

    /// Get a reference to the logical join's on.
    pub fn on(&self) -> &Condition {
        &self.on
    }

    /// Get the join type of the logical join.
    pub fn join_type(&self) -> JoinType {
        self.join_type
    }

    /// Clone with new `on` condition
    pub fn clone_with_cond(&self, cond: Condition) -> Self {
        Self::new(self.left.clone(), self.right.clone(), self.join_type, cond)
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

impl_plan_tree_node_for_binary! { LogicalJoin }

impl ColPrunable for LogicalJoin {
    fn prune_col(&self, required_cols: &FixedBitSet) -> PlanRef {
        self.must_contain_columns(required_cols);

        match self.join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {}
            _ => unimplemented!(),
        }

        let left_len = self.left.schema().fields.len();

        let mut visitor = CollectInputRef {
            input_bits: required_cols.clone(),
        };
        self.on.visit_expr(&mut visitor);

        let mut on = self.on.clone();
        let mut mapping = ColIndexMapping::with_remaining_columns(&visitor.input_bits);
        on = on.rewrite_expr(&mut mapping);

        let (left_required_cols, right_required_cols): (FixedBitSet, FixedBitSet) =
            visitor.input_bits.ones().partition_map(|i| {
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

        if required_cols == &visitor.input_bits {
            join.into()
        } else {
            let mut remaining_columns = FixedBitSet::with_capacity(join.schema().fields().len());
            remaining_columns.extend(required_cols.ones().map(|i| mapping.map(i)));
            LogicalProject::with_mapping(
                join.into(),
                ColIndexMapping::with_remaining_columns(&remaining_columns),
            )
            .into()
        }
    }
}

impl ToBatch for LogicalJoin {
    fn to_batch(&self) -> PlanRef {
        let predicate = EqJoinPredicate::create(
            self.left.schema().len(),
            self.right.schema().len(),
            self.on.clone(),
        );

        let left = self.left().to_batch();
        let right = self.right().to_batch();
        let logical_join = self.clone_with_left_right(left, right);

        if predicate.has_eq() {
            // Convert to Hash Join for equal joins
            // For inner joins, pull non-equal conditions to a filter operator on top of it
            let pull_filter = self.join_type == JoinType::Inner && predicate.has_non_eq();
            if pull_filter {
                let eq_cond =
                    EqJoinPredicate::new(Condition::true_cond(), predicate.eq_keys().to_vec());
                let logical_join = logical_join.clone_with_cond(eq_cond.eq_cond());
                let hash_join = BatchHashJoin::new(logical_join, eq_cond).into();
                let logical_filter = LogicalFilter::new(hash_join, predicate.non_eq_cond());
                BatchFilter::new(logical_filter).into()
            } else {
                BatchHashJoin::new(logical_join, predicate).into()
            }
        } else {
            // Convert to Nested-loop Join for non-equal joins
            todo!("nested loop join")
        }
    }
}

impl ToStream for LogicalJoin {
    fn to_stream(&self) -> PlanRef {
        let predicate = EqJoinPredicate::create(
            self.left.schema().len(),
            self.right.schema().len(),
            self.on.clone(),
        );
        let left = self
            .left()
            .to_stream_with_dist_required(&Distribution::HashShard(predicate.left_eq_indexes()));
        let right = self
            .right()
            .to_stream_with_dist_required(&Distribution::HashShard(predicate.right_eq_indexes()));
        let logical_join = self.clone_with_left_right(left, right);

        if predicate.has_eq() {
            // Convert to Hash Join for equal joins
            // For inner joins, pull non-equal conditions to a filter operator on top of it
            let pull_filter = self.join_type == JoinType::Inner && predicate.has_non_eq();
            if pull_filter {
                let eq_cond =
                    EqJoinPredicate::new(Condition::true_cond(), predicate.eq_keys().to_vec());
                let logical_join = logical_join.clone_with_cond(eq_cond.eq_cond());
                let hash_join = StreamHashJoin::new(logical_join, eq_cond).into();
                let logical_filter = LogicalFilter::new(hash_join, predicate.non_eq_cond());
                StreamFilter::new(logical_filter).into()
            } else {
                StreamHashJoin::new(logical_join, predicate).into()
            }
        } else {
            // Convert to Nested-loop Join for non-equal joins
            todo!("nested loop join")
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use risingwave_common::catalog::{Field, TableId};
    use risingwave_common::types::{DataType, Datum};
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::plan_node::{LogicalScan, PlanTreeNodeUnary};
    use crate::optimizer::property::WithSchema;
    use crate::session::QueryContext;

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
    #[tokio::test]
    async fn test_prune_join() {
        let ty = DataType::Int32;
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
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
            ctx.clone(),
        );
        let right = LogicalScan::new(
            "right".to_string(),
            TableId::new(0),
            vec![4.into(), 5.into(), 6.into()],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
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
    #[tokio::test]
    async fn test_prune_join_no_project() {
        let ty = DataType::Int32;
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
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
            ctx.clone(),
        );
        let right = LogicalScan::new(
            "right".to_string(),
            TableId::new(0),
            vec![4.into(), 5.into(), 6.into()],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
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

    /// Convert
    /// ```text
    /// Join(on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// to
    /// ```text
    /// Filter($2 == 42)
    ///   HashJoin(on: $1 = $3)
    ///     TableScan(v1, v2, v3)
    ///     TableScan(v4, v5, v6)
    /// ```
    #[tokio::test]
    async fn test_join_to_batch() {
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        let fields: Vec<Field> = (1..7)
            .map(|i| Field {
                data_type: DataType::Int32,
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
            ctx.clone(),
        );
        let right = LogicalScan::new(
            "right".to_string(),
            TableId::new(0),
            vec![4.into(), 5.into(), 6.into()],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );

        let eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, DataType::Int32))),
                ],
            )
            .unwrap(),
        ));
        let non_eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(2, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(
                        Datum::Some(42_i32.into()),
                        DataType::Int32,
                    ))),
                ],
            )
            .unwrap(),
        ));
        // Condition: ($1 = $3) AND ($2 == 42)
        let on_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(Type::And, vec![eq_cond.clone(), non_eq_cond.clone()]).unwrap(),
        ));

        let join_type = JoinType::Inner;
        let logical_join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on_cond),
        );

        // Perform `to_batch`
        let result = logical_join.to_batch();

        // Expected plan: Filter($2 == 42) --> HashJoin($1 = $3)
        let batch_filter = result.as_batch_filter().unwrap();
        assert_eq!(batch_filter.predicate().as_expr(), non_eq_cond);

        let input = batch_filter.input();
        let hash_join = input.as_batch_hash_join().unwrap();
        assert_eq!(hash_join.eq_join_predicate().eq_cond().as_expr(), eq_cond);
    }

    /// Convert
    /// ```text
    /// Join(join_type: left outer, on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// to
    /// ```text
    /// HashJoin(join_type: left outer, on: ($1 = $3) AND ($2 == 42))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    #[tokio::test]
    async fn test_join_to_stream() {
        let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        let fields: Vec<Field> = (1..7)
            .map(|i| Field {
                data_type: DataType::Int32,
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
            ctx.clone(),
        );
        let right = LogicalScan::new(
            "right".to_string(),
            TableId::new(0),
            vec![4.into(), 5.into(), 6.into()],
            Schema {
                fields: fields[3..6].to_vec(),
            },
            ctx,
        );

        let eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
                    ExprImpl::InputRef(Box::new(InputRef::new(3, DataType::Int32))),
                ],
            )
            .unwrap(),
        ));
        let non_eq_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(
                Type::Equal,
                vec![
                    ExprImpl::InputRef(Box::new(InputRef::new(2, DataType::Int32))),
                    ExprImpl::Literal(Box::new(Literal::new(
                        Datum::Some(42_i32.into()),
                        DataType::Int32,
                    ))),
                ],
            )
            .unwrap(),
        ));
        // Condition: ($1 = $3) AND ($2 == 42)
        let on_cond = ExprImpl::FunctionCall(Box::new(
            FunctionCall::new(Type::And, vec![eq_cond, non_eq_cond]).unwrap(),
        ));

        let join_type = JoinType::LeftOuter;
        let logical_join = LogicalJoin::new(
            left.into(),
            right.into(),
            join_type,
            Condition::with_expr(on_cond.clone()),
        );

        // Perform `to_stream`
        let result = logical_join.to_stream();

        // Expected plan: HashJoin(($1 = $3) AND ($2 == 42))
        let hash_join = result.as_stream_hash_join().unwrap();
        assert_eq!(hash_join.eq_join_predicate().all_cond().as_expr(), on_cond);
    }
}
