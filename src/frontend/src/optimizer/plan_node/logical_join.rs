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

use std::fmt;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::Schema;
use risingwave_common::error::{ErrorCode, Result, RwError};
use risingwave_pb::plan_common::JoinType;

use super::{
    ColPrunable, CollectInputRef, LogicalProject, PlanBase, PlanRef, PlanTreeNodeBinary,
    PredicatePushdown, StreamHashJoin, ToBatch, ToStream,
};
use crate::expr::{ExprImpl, ExprType};
use crate::optimizer::plan_node::batch_nested_loop_join::BatchNestedLoopJoin;
use crate::optimizer::plan_node::{
    BatchFilter, BatchHashJoin, EqJoinPredicate, LogicalFilter, StreamFilter,
};
use crate::optimizer::property::RequiredDist;
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
        let schema = Self::derive_schema(left.schema(), right.schema(), join_type);
        let pk_indices = Self::derive_pk(
            left.schema().len(),
            right.schema().len(),
            left.pk_indices(),
            right.pk_indices(),
            join_type,
        );
        let base = PlanBase::new_logical(ctx, schema, pk_indices);
        LogicalJoin {
            base,
            left,
            right,
            on,
            join_type,
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
            JoinType::LeftSemi | JoinType::LeftAnti => left_len,
            JoinType::RightSemi | JoinType::RightAnti => right_len,
        }
    }

    /// get the Mapping of columnIndex from output column index to left column index
    fn o2l_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::identity_or_none(left_len + right_len, left_len)
            }

            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::identity(left_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::empty(right_len),
        }
    }

    /// get the Mapping of columnIndex from output column index to right column index
    fn o2r_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        match join_type {
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                ColIndexMapping::with_shift_offset(left_len + right_len, -(left_len as isize))
            }
            JoinType::LeftSemi | JoinType::LeftAnti => ColIndexMapping::empty(left_len),
            JoinType::RightSemi | JoinType::RightAnti => ColIndexMapping::identity(right_len),
        }
    }

    /// get the Mapping of columnIndex from left column index to output column index
    fn l2o_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::o2l_col_mapping_inner(left_len, right_len, join_type).inverse()
    }

    /// get the Mapping of columnIndex from right column index to output column index
    fn r2o_col_mapping_inner(
        left_len: usize,
        right_len: usize,
        join_type: JoinType,
    ) -> ColIndexMapping {
        Self::o2r_col_mapping_inner(left_len, right_len, join_type).inverse()
    }

    pub fn o2l_col_mapping(&self) -> ColIndexMapping {
        Self::o2l_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    pub fn o2r_col_mapping(&self) -> ColIndexMapping {
        Self::o2r_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    pub fn l2o_col_mapping(&self) -> ColIndexMapping {
        Self::l2o_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    pub fn r2o_col_mapping(&self) -> ColIndexMapping {
        Self::r2o_col_mapping_inner(
            self.left().schema().len(),
            self.right().schema().len(),
            self.join_type(),
        )
    }

    pub(super) fn derive_schema(
        left_schema: &Schema,
        right_schema: &Schema,
        join_type: JoinType,
    ) -> Schema {
        let left_len = left_schema.len();
        let right_len = right_schema.len();
        let out_column_num = Self::out_column_num(left_len, right_len, join_type);
        let o2l = Self::o2l_col_mapping_inner(left_len, right_len, join_type);
        let o2r = Self::o2r_col_mapping_inner(left_len, right_len, join_type);
        let fields = (0..out_column_num)
            .into_iter()
            .map(|i| match (o2l.try_map(i), o2r.try_map(i)) {
                (Some(l_i), None) => left_schema.fields()[l_i].clone(),
                (None, Some(r_i)) => right_schema.fields()[r_i].clone(),
                _ => panic!(),
            })
            .collect();
        Schema { fields }
    }

    pub(super) fn derive_pk(
        left_len: usize,
        right_len: usize,
        left_pk: &[usize],
        right_pk: &[usize],
        join_type: JoinType,
    ) -> Vec<usize> {
        let l2o = Self::l2o_col_mapping_inner(left_len, right_len, join_type);
        let r2o = Self::r2o_col_mapping_inner(left_len, right_len, join_type);
        left_pk
            .iter()
            .map(|index| l2o.try_map(*index))
            .chain(right_pk.iter().map(|index| r2o.try_map(*index)))
            .flatten()
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

    pub fn is_left_join(&self) -> bool {
        matches!(self.join_type(), JoinType::LeftSemi | JoinType::LeftAnti)
    }

    pub fn is_right_join(&self) -> bool {
        matches!(self.join_type(), JoinType::RightSemi | JoinType::RightAnti)
    }

    /// Try to split and pushdown `predicate` into a join's left/right child or the on clause.
    /// Returns the pushed predicates. The pushed part will be removed from the original predicate.
    ///
    /// `InputRef`s in the right `Condition` are shifted by `-left_col_num`.
    fn push_down(
        &self,
        predicate: &mut Condition,
        left_col_num: usize,
        right_col_num: usize,
        push_left: bool,
        push_right: bool,
        push_on: bool,
    ) -> (Condition, Condition, Condition) {
        let conjunctions = std::mem::take(&mut predicate.conjunctions);
        let (mut left, right, mut others) =
            Condition { conjunctions }.split(left_col_num, right_col_num);

        let mut cannot_push = vec![];

        if !push_left {
            cannot_push.extend(left);
            left = Condition::true_cond();
        };

        let right = if push_right {
            let mut mapping = ColIndexMapping::with_shift_offset(
                left_col_num + right_col_num,
                -(left_col_num as isize),
            );
            right.rewrite_expr(&mut mapping)
        } else {
            cannot_push.extend(right);
            Condition::true_cond()
        };

        let on = if push_on {
            others.conjunctions.extend(std::mem::take(&mut cannot_push));
            others
        } else {
            cannot_push.extend(others);
            Condition::true_cond()
        };

        predicate.conjunctions = cannot_push;

        (left, right, on)
    }

    fn can_push_left_from_filter(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti
        )
    }

    fn can_push_right_from_filter(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti
        )
    }

    fn can_push_on_from_filter(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi
        )
    }

    fn can_push_left_from_on(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::RightOuter | JoinType::LeftSemi
        )
    }

    fn can_push_right_from_on(&self, ty: JoinType) -> bool {
        matches!(
            ty,
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightSemi
        )
    }

    /// Try to simplify the outer join with the predicate on the top of the join
    ///
    /// now it is just a naive implementation for comparison expression, we can give a more general
    /// implementation with constant folding in future
    fn simplify_outer(
        &self,
        predicate: &Condition,
        left_col_num: usize,
        join_type: JoinType,
    ) -> JoinType {
        let (mut gen_null_in_left, mut gen_null_in_right) = match join_type {
            JoinType::LeftOuter => (false, true),
            JoinType::RightOuter => (true, false),
            JoinType::FullOuter => (true, true),
            _ => return join_type,
        };

        for expr in &predicate.conjunctions {
            if let ExprImpl::FunctionCall(func) = expr {
                match func.get_expr_type() {
                    ExprType::Equal
                    | ExprType::NotEqual
                    | ExprType::LessThan
                    | ExprType::LessThanOrEqual
                    | ExprType::GreaterThan
                    | ExprType::GreaterThanOrEqual => {
                        for input in func.inputs() {
                            if let ExprImpl::InputRef(input) = input {
                                let idx = input.index;
                                if idx < left_col_num {
                                    gen_null_in_left = false;
                                } else {
                                    gen_null_in_right = false;
                                }
                            }
                        }
                    }
                    _ => {}
                };
            }
        }

        match (gen_null_in_left, gen_null_in_right) {
            (true, true) => JoinType::FullOuter,
            (true, false) => JoinType::RightOuter,
            (false, true) => JoinType::LeftOuter,
            (false, false) => JoinType::Inner,
        }
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

    #[must_use]
    fn rewrite_with_left_right(
        &self,
        left: PlanRef,
        left_col_change: ColIndexMapping,
        right: PlanRef,
        right_col_change: ColIndexMapping,
    ) -> (Self, ColIndexMapping) {
        let new_on = {
            let (mut left_map, _) = left_col_change.clone().into_parts();
            let (mut right_map, _) = right_col_change.clone().into_parts();
            for i in right_map.iter_mut().flatten() {
                *i += left.schema().len();
            }
            left_map.append(&mut right_map);
            self.on()
                .clone()
                .rewrite_expr(&mut ColIndexMapping::new(left_map))
        };
        let join = Self::new(left, right, self.join_type, new_on);

        let old_o2l = self.o2l_col_mapping().composite(&left_col_change);
        let old_o2r = self.o2r_col_mapping().composite(&right_col_change);
        let new_l2o = join.l2o_col_mapping();
        let new_r2o = join.r2o_col_mapping();

        let out_col_change = old_o2l
            .composite(&new_l2o)
            .union(&old_o2r.composite(&new_r2o));
        (join, out_col_change)
    }
}

impl_plan_tree_node_for_binary! { LogicalJoin }

impl ColPrunable for LogicalJoin {
    fn prune_col(&self, required_cols: &[usize]) -> PlanRef {
        let left_len = self.left.schema().fields.len();

        let total_len = self.left().schema().len() + self.right().schema().len();
        let mut resized_required_cols = FixedBitSet::with_capacity(total_len);

        required_cols.iter().for_each(|&i| {
            if self.is_right_join() {
                resized_required_cols.insert(left_len + i);
            } else {
                resized_required_cols.insert(i);
            }
        });

        let mut visitor = CollectInputRef::new(resized_required_cols);
        self.on.visit_expr(&mut visitor);
        let left_right_required_cols = FixedBitSet::from(visitor).ones().collect_vec();

        let mut on = self.on.clone();
        let mut mapping =
            ColIndexMapping::with_remaining_columns(&left_right_required_cols, total_len);
        on = on.rewrite_expr(&mut mapping);

        let mut left_required_cols = Vec::new();
        let mut right_required_cols = Vec::new();
        left_right_required_cols.iter().for_each(|&i| {
            if i < left_len {
                left_required_cols.push(i);
            } else {
                right_required_cols.push(i - left_len);
            }
        });

        let join = LogicalJoin::new(
            self.left.prune_col(&left_required_cols),
            self.right.prune_col(&right_required_cols),
            self.join_type,
            on,
        );

        let required_inputs_in_output = if self.is_left_join() {
            left_required_cols
        } else if self.is_right_join() {
            right_required_cols
        } else {
            left_right_required_cols
        };
        if required_cols == required_inputs_in_output {
            join.into()
        } else {
            let mapping = ColIndexMapping::with_remaining_columns(
                &required_inputs_in_output,
                self.schema().len(),
            );
            let remaining_columns = required_cols.iter().map(|&i| mapping.map(i)).collect_vec();
            LogicalProject::with_mapping(
                join.into(),
                ColIndexMapping::with_remaining_columns(&remaining_columns, self.schema().len()),
            )
            .into()
        }
    }
}

impl PredicatePushdown for LogicalJoin {
    /// Pushes predicates above and within a join node into the join node and/or its children nodes.
    ///
    /// # Which predicates can be pushed
    ///
    /// For inner join, we can do all kinds of pushdown.
    ///
    /// For left/right semi join, we can push filter to left/right and on-clause,
    /// and push on-clause to left/right.
    ///
    /// For left/right anti join, we can push filter to left/right, but on-clause can not be pushed
    ///
    /// ## Outer Join
    ///
    /// Preserved Row table
    /// : The table in an Outer Join that must return all rows.
    ///
    /// Null Supplying table
    /// : This is the table that has nulls filled in for its columns in unmatched rows.
    ///
    /// |                          | Preserved Row table | Null Supplying table |
    /// |--------------------------|---------------------|----------------------|
    /// | Join predicate (on)      | Not Pushed          | Pushed               |
    /// | Where predicate (filter) | Pushed              | Not Pushed           |
    fn predicate_pushdown(&self, mut predicate: Condition) -> PlanRef {
        let left_col_num = self.left.schema().len();
        let right_col_num = self.right.schema().len();
        let join_type = self.simplify_outer(&predicate, left_col_num, self.join_type);

        let (left_from_filter, right_from_filter, on) = self.push_down(
            &mut predicate,
            left_col_num,
            right_col_num,
            self.can_push_left_from_filter(join_type),
            self.can_push_right_from_filter(join_type),
            self.can_push_on_from_filter(join_type),
        );

        let mut new_on = self.on.clone().and(on);
        let (left_from_on, right_from_on, on) = self.push_down(
            &mut new_on,
            left_col_num,
            right_col_num,
            self.can_push_left_from_on(join_type),
            self.can_push_right_from_on(join_type),
            false,
        );
        assert!(
            on.always_true(),
            "On-clause should not be pushed to on-clause."
        );

        let left_predicate = left_from_filter.and(left_from_on);
        let right_predicate = right_from_filter.and(right_from_on);

        let new_left = self.left.predicate_pushdown(left_predicate);
        let new_right = self.right.predicate_pushdown(right_predicate);
        let new_join = LogicalJoin::new(new_left, new_right, join_type, new_on);
        LogicalFilter::create(new_join.into(), predicate)
    }
}

impl ToBatch for LogicalJoin {
    fn to_batch(&self) -> Result<PlanRef> {
        let predicate = EqJoinPredicate::create(
            self.left.schema().len(),
            self.right.schema().len(),
            self.on.clone(),
        );

        let left = self.left().to_batch()?;
        let right = self.right().to_batch()?;
        let logical_join = self.clone_with_left_right(left, right);

        if predicate.has_eq() {
            // Convert to Hash Join for equal joins
            // For inner joins, pull non-equal conditions to a filter operator on top of it
            let pull_filter = self.join_type == JoinType::Inner && predicate.has_non_eq();
            if pull_filter {
                let eq_cond = EqJoinPredicate::new(
                    Condition::true_cond(),
                    predicate.eq_keys().to_vec(),
                    self.left.schema().len(),
                );
                let logical_join = logical_join.clone_with_cond(eq_cond.eq_cond());
                let hash_join = BatchHashJoin::new(logical_join, eq_cond).into();
                let logical_filter = LogicalFilter::new(hash_join, predicate.non_eq_cond());
                Ok(BatchFilter::new(logical_filter).into())
            } else {
                Ok(BatchHashJoin::new(logical_join, predicate).into())
            }
        } else {
            // Convert to Nested-loop Join for non-equal joins
            Ok(BatchNestedLoopJoin::new(logical_join).into())
        }
    }
}

impl ToStream for LogicalJoin {
    fn to_stream(&self) -> Result<PlanRef> {
        let predicate = EqJoinPredicate::create(
            self.left.schema().len(),
            self.right.schema().len(),
            self.on.clone(),
        );

        let right = self
            .right()
            .to_stream_with_dist_required(&RequiredDist::shard_by_key(
                self.right().schema().len(),
                &predicate.right_eq_indexes(),
            ))?;

        let r2l =
            predicate.r2l_eq_columns_mapping(self.left().schema().len(), right.schema().len());

        let left_dist = r2l.rewrite_required_distribution(&RequiredDist::PhysicalDist(
            right.distribution().clone(),
        ));

        let left = self.left().to_stream_with_dist_required(&left_dist)?;
        let logical_join = self.clone_with_left_right(left, right);

        if predicate.has_eq() {
            // Convert to Hash Join for equal joins
            // For inner joins, pull non-equal conditions to a filter operator on top of it
            let pull_filter = self.join_type == JoinType::Inner && predicate.has_non_eq();
            if pull_filter {
                let eq_cond = EqJoinPredicate::new(
                    Condition::true_cond(),
                    predicate.eq_keys().to_vec(),
                    self.left.schema().len(),
                );
                let logical_join = logical_join.clone_with_cond(eq_cond.eq_cond());
                let hash_join = StreamHashJoin::new(logical_join, eq_cond).into();
                let logical_filter = LogicalFilter::new(hash_join, predicate.non_eq_cond());
                Ok(StreamFilter::new(logical_filter).into())
            } else {
                Ok(StreamHashJoin::new(logical_join, predicate).into())
            }
        } else {
            Err(RwError::from(ErrorCode::NotImplemented(
                "stream nested-loop join".to_string(),
                None.into(),
            )))
        }
    }

    fn logical_rewrite_for_stream(&self) -> Result<(PlanRef, ColIndexMapping)> {
        let (left, left_col_change) = self.left.logical_rewrite_for_stream()?;
        let (right, right_col_change) = self.right.logical_rewrite_for_stream()?;
        let (join, out_col_change) =
            self.rewrite_with_left_right(left, left_col_change, right, right_col_change);
        Ok((join.into(), out_col_change))
    }
}

#[cfg(test)]
mod tests {

    use risingwave_common::catalog::Field;
    use risingwave_common::types::{DataType, Datum};
    use risingwave_pb::expr::expr_node::Type;

    use super::*;
    use crate::expr::{assert_eq_input_ref, FunctionCall, InputRef, Literal};
    use crate::optimizer::plan_node::{LogicalValues, PlanTreeNodeUnary};
    use crate::session::OptimizerContext;

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
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
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
        let required_cols = vec![2, 3];
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

        let expr: ExprImpl = join.on.clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 2);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..3]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }

    /// Semi join panicked previously at `prune_col`. Add test to prevent regression.
    #[tokio::test]
    async fn test_prune_semi_join() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
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
                    ExprImpl::InputRef(Box::new(InputRef::new(4, ty))),
                ],
            )
            .unwrap(),
        ));
        for join_type in [
            JoinType::LeftSemi,
            JoinType::RightSemi,
            JoinType::LeftAnti,
            JoinType::RightAnti,
        ] {
            let join = LogicalJoin::new(
                left.clone().into(),
                right.clone().into(),
                join_type,
                Condition::with_expr(on.clone()),
            );

            let offset = if join.is_right_join() { 3 } else { 0 };

            // Perform the prune
            let required_cols = vec![0];
            // key 0 is never used in the join (always key 1)
            let plan = join.prune_col(&required_cols);
            // Check that the join has been wrapped in a projection
            let as_plan = plan.as_logical_project().unwrap();
            // Check the result
            assert_eq!(as_plan.schema().fields().len(), 1);
            assert_eq!(as_plan.schema().fields()[0], fields[offset]);

            // Perform the prune
            let required_cols = vec![0, 1, 2];
            // should not panic here
            let plan = join.prune_col(&required_cols);
            // Check that the join has not been wrapped in a projection
            let as_plan = plan.as_logical_join().unwrap();
            // Check the result
            assert_eq!(as_plan.schema().fields().len(), 3);
            assert_eq!(as_plan.schema().fields()[0], fields[offset]);
            assert_eq!(as_plan.schema().fields()[1], fields[offset + 1]);
            assert_eq!(as_plan.schema().fields()[2], fields[offset + 2]);
        }
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
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
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
        let required_cols = vec![1, 3];
        let plan = join.prune_col(&required_cols);

        // Check the result
        let join = plan.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 2);
        assert_eq!(join.schema().fields()[0], fields[1]);
        assert_eq!(join.schema().fields()[1], fields[3]);

        let expr: ExprImpl = join.on.clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 1);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..2]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
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
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(DataType::Int32, format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
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
        let result = logical_join.to_batch().unwrap();

        // Expected plan: Filter($2 == 42) --> HashJoin($1 = $3)
        let batch_filter = result.as_batch_filter().unwrap();
        assert_eq!(
            ExprImpl::from(batch_filter.predicate().clone()),
            non_eq_cond
        );

        let input = batch_filter.input();
        let hash_join = input.as_batch_hash_join().unwrap();
        assert_eq!(
            ExprImpl::from(hash_join.eq_join_predicate().eq_cond()),
            eq_cond
        );
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
    #[ignore] // ignore due to refactor logical scan, but the test seem to duplicate with the explain test
              // framework, maybe we will remove it?
    async fn test_join_to_stream() {
        // let ctx = Rc::new(RefCell::new(QueryContext::mock().await));
        // let fields: Vec<Field> = (1..7)
        //     .map(|i| Field {
        //         data_type: DataType::Int32,
        //         name: format!("v{}", i),
        //     })
        //     .collect();
        // let left = LogicalScan::new(
        //     "left".to_string(),
        //     TableId::new(0),
        //     vec![1.into(), 2.into(), 3.into()],
        //     Schema {
        //         fields: fields[0..3].to_vec(),
        //     },
        //     ctx.clone(),
        // );
        // let right = LogicalScan::new(
        //     "right".to_string(),
        //     TableId::new(0),
        //     vec![4.into(), 5.into(), 6.into()],
        //     Schema {
        //         fields: fields[3..6].to_vec(),
        //     },
        //     ctx,
        // );
        // let eq_cond = ExprImpl::FunctionCall(Box::new(
        //     FunctionCall::new(
        //         Type::Equal,
        //         vec![
        //             ExprImpl::InputRef(Box::new(InputRef::new(1, DataType::Int32))),
        //             ExprImpl::InputRef(Box::new(InputRef::new(3, DataType::Int32))),
        //         ],
        //     )
        //     .unwrap(),
        // ));
        // let non_eq_cond = ExprImpl::FunctionCall(Box::new(
        //     FunctionCall::new(
        //         Type::Equal,
        //         vec![
        //             ExprImpl::InputRef(Box::new(InputRef::new(2, DataType::Int32))),
        //             ExprImpl::Literal(Box::new(Literal::new(
        //                 Datum::Some(42_i32.into()),
        //                 DataType::Int32,
        //             ))),
        //         ],
        //     )
        //     .unwrap(),
        // ));
        // // Condition: ($1 = $3) AND ($2 == 42)
        // let on_cond = ExprImpl::FunctionCall(Box::new(
        //     FunctionCall::new(Type::And, vec![eq_cond, non_eq_cond]).unwrap(),
        // ));

        // let join_type = JoinType::LeftOuter;
        // let logical_join = LogicalJoin::new(
        //     left.into(),
        //     right.into(),
        //     join_type,
        //     Condition::with_expr(on_cond.clone()),
        // );

        // // Perform `to_stream`
        // let result = logical_join.to_stream();

        // // Expected plan: HashJoin(($1 = $3) AND ($2 == 42))
        // let hash_join = result.as_stream_hash_join().unwrap();
        // assert_eq!(hash_join.eq_join_predicate().all_cond().as_expr(), on_cond);
    }
    /// Pruning
    /// ```text
    /// Join(on: input_ref(1)=input_ref(3))
    ///   TableScan(v1, v2, v3)
    ///   TableScan(v4, v5, v6)
    /// ```
    /// with required columns [3, 2] will result in
    /// ```text
    /// Project(input_ref(2), input_ref(1))
    ///   Join(on: input_ref(0)=input_ref(2))
    ///     TableScan(v2, v3)
    ///     TableScan(v4)
    /// ```
    #[tokio::test]
    async fn test_join_column_prune_with_order_required() {
        let ty = DataType::Int32;
        let ctx = OptimizerContext::mock().await;
        let fields: Vec<Field> = (1..7)
            .map(|i| Field::with_name(ty.clone(), format!("v{}", i)))
            .collect();
        let left = LogicalValues::new(
            vec![],
            Schema {
                fields: fields[0..3].to_vec(),
            },
            ctx.clone(),
        );
        let right = LogicalValues::new(
            vec![],
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
        let required_cols = vec![3, 2];
        let plan = join.prune_col(&required_cols);

        // Check the result
        let project = plan.as_logical_project().unwrap();
        assert_eq!(project.exprs().len(), 2);
        assert_eq_input_ref!(&project.exprs()[0], 2);
        assert_eq_input_ref!(&project.exprs()[1], 1);

        let join = project.input();
        let join = join.as_logical_join().unwrap();
        assert_eq!(join.schema().fields().len(), 3);
        assert_eq!(join.schema().fields(), &fields[1..4]);

        let expr: ExprImpl = join.on.clone().into();
        let call = expr.as_function_call().unwrap();
        assert_eq_input_ref!(&call.inputs()[0], 0);
        assert_eq_input_ref!(&call.inputs()[1], 2);

        let left = join.left();
        let left = left.as_logical_values().unwrap();
        assert_eq!(left.schema().fields(), &fields[1..3]);
        let right = join.right();
        let right = right.as_logical_values().unwrap();
        assert_eq!(right.schema().fields(), &fields[3..4]);
    }
}
