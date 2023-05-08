// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_common::types::DataType::Boolean;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{
    CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall,
    InputRef,
};
use crate::optimizer::plan_node::{LogicalApply, LogicalFilter, LogicalJoin, PlanTreeNodeBinary};
use crate::optimizer::plan_visitor::{ExprCorrelatedIdFinder, PlanCorrelatedIdFinder};
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

/// Transpose `LogicalApply` and `LogicalJoin`.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  Domain      LogicalJoin
///                /      \
///               T1     T2
/// ```
///
/// `push_apply_both_side`:
///
/// D Apply (T1 join< p > T2)  ->  (D Apply T1) join< p and natural join D > (D Apply T2)
///
/// After:
///
/// ```text
///           LogicalJoin
///         /            \
///  LogicalApply     LogicalApply
///   /      \           /      \
/// Domain   T1        Domain   T2
/// ```
///
/// `push_apply_left_side`:
///
/// D Apply (T1 join< p > T2)  ->  (D Apply T1) join< p > T2
///
/// After:
///
/// ```text
///        LogicalJoin
///      /            \
///  LogicalApply    T2
///   /      \
/// Domain   T1
/// ```
///
/// `push_apply_right_side`:
///
/// D Apply (T1 join< p > T2)  ->  T1 join< p > (D Apply T2)
///
/// After:
///
/// ```text
///        LogicalJoin
///      /            \
///    T1         LogicalApply
///                /      \
///              Domain   T2
/// ```
pub struct ApplyJoinTransposeRule {}
impl Rule for ApplyJoinTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (
            apply_left,
            apply_right,
            apply_on,
            apply_join_type,
            correlated_id,
            correlated_indices,
            max_one_row,
        ) = apply.clone().decompose();

        if max_one_row {
            return None;
        }

        assert_eq!(apply_join_type, JoinType::Inner);
        let join: &LogicalJoin = apply_right.as_logical_join()?;

        let mut finder = ExprCorrelatedIdFinder::default();
        join.on().visit_expr(&mut finder);
        let join_cond_has_correlated_id = finder.contains(&correlated_id);
        let join_left_has_correlated_id =
            PlanCorrelatedIdFinder::find_correlated_id(join.left(), &correlated_id);
        let join_right_has_correlated_id =
            PlanCorrelatedIdFinder::find_correlated_id(join.right(), &correlated_id);

        // Shortcut
        // Check whether correlated_input_ref with same correlated_id exists below apply.
        // If no, bail out and leave for ApplyScan rule to deal with.
        if !join_cond_has_correlated_id
            && !join_left_has_correlated_id
            && !join_right_has_correlated_id
        {
            return None;
        }

        let (push_left, push_right) = match join.join_type() {
            // `LeftSemi`, `LeftAnti`, `LeftOuter` can only push to left side if it's right side has
            // no correlated id. Otherwise push to both sides.
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftOuter => {
                if !join_right_has_correlated_id {
                    (true, false)
                } else {
                    (true, true)
                }
            }
            // `RightSemi`, `RightAnti`, `RightOuter` can only push to right side if it's left side
            // has no correlated id. Otherwise push to both sides.
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightOuter => {
                if !join_left_has_correlated_id {
                    (false, true)
                } else {
                    (true, true)
                }
            }
            // `Inner` can push to one side if the other side is not dependent on it.
            JoinType::Inner => {
                if join_cond_has_correlated_id
                    && !join_right_has_correlated_id
                    && !join_left_has_correlated_id
                {
                    (true, false)
                } else {
                    (join_left_has_correlated_id, join_right_has_correlated_id)
                }
            }
            // `FullOuter` should always push to both sides.
            JoinType::FullOuter => (true, true),
            JoinType::Unspecified => unreachable!(),
        };

        let out = if push_left && push_right {
            self.push_apply_both_side(
                apply_left,
                join,
                apply_on,
                apply_join_type,
                correlated_id,
                correlated_indices,
            )
        } else if push_left {
            self.push_apply_left_side(
                apply_left,
                join,
                apply_on,
                apply_join_type,
                correlated_id,
                correlated_indices,
            )
        } else if push_right {
            self.push_apply_right_side(
                apply_left,
                join,
                apply_on,
                apply_join_type,
                correlated_id,
                correlated_indices,
            )
        } else {
            unreachable!();
        };
        assert_eq!(out.schema(), plan.schema());
        Some(out)
    }
}

impl ApplyJoinTransposeRule {
    fn push_apply_left_side(
        &self,
        apply_left: PlanRef,
        join: &LogicalJoin,
        apply_on: Condition,
        apply_join_type: JoinType,
        correlated_id: CorrelatedId,
        correlated_indices: Vec<usize>,
    ) -> PlanRef {
        let apply_left_len = apply_left.schema().len();
        let join_left_len = join.left().schema().len();
        let mut rewriter = Rewriter {
            join_left_len,
            join_left_offset: apply_left_len as isize,
            join_right_offset: apply_left_len as isize,
            index_mapping: ColIndexMapping::new(
                correlated_indices
                    .clone()
                    .into_iter()
                    .map(Some)
                    .collect_vec(),
            )
            .inverse()
            .expect("must be invertible"),
            correlated_id,
        };

        // Rewrite join on condition
        let new_join_condition = Condition {
            conjunctions: join
                .on()
                .clone()
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .collect_vec(),
        };

        let mut left_apply_condition: Vec<ExprImpl> = vec![];
        let mut other_condition: Vec<ExprImpl> = vec![];

        match join.join_type() {
            JoinType::LeftSemi | JoinType::LeftAnti => {
                left_apply_condition.extend(apply_on);
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                let apply_len = apply_left_len + join.schema().len();
                let mut d_t1_bit_set = FixedBitSet::with_capacity(apply_len);
                d_t1_bit_set.set_range(0..apply_left_len + join_left_len, true);

                let (left, other): (Vec<_>, Vec<_>) = apply_on
                    .into_iter()
                    .partition(|expr| expr.collect_input_refs(apply_len).is_subset(&d_t1_bit_set));
                left_apply_condition.extend(left);
                other_condition.extend(other);
            }
            JoinType::RightSemi | JoinType::RightAnti | JoinType::Unspecified => unreachable!(),
        }

        let new_join_left = LogicalApply::create(
            apply_left,
            join.left(),
            apply_join_type,
            Condition {
                conjunctions: left_apply_condition,
            },
            correlated_id,
            correlated_indices,
            false,
        );

        let new_join = LogicalJoin::new(
            new_join_left,
            join.right(),
            join.join_type(),
            new_join_condition,
        );

        // Leave other condition for predicate push down to deal with
        LogicalFilter::create(
            new_join.into(),
            Condition {
                conjunctions: other_condition,
            },
        )
    }

    fn push_apply_right_side(
        &self,
        apply_left: PlanRef,
        join: &LogicalJoin,
        apply_on: Condition,
        apply_join_type: JoinType,
        correlated_id: CorrelatedId,
        correlated_indices: Vec<usize>,
    ) -> PlanRef {
        let apply_left_len = apply_left.schema().len();
        let join_left_len = join.left().schema().len();
        let mut rewriter = Rewriter {
            join_left_len,
            join_left_offset: 0,
            join_right_offset: apply_left_len as isize,
            index_mapping: ColIndexMapping::new(
                correlated_indices
                    .clone()
                    .into_iter()
                    .map(Some)
                    .collect_vec(),
            )
            .inverse()
            .expect("must be invertible"),
            correlated_id,
        };

        // Rewrite join on condition
        let new_join_condition = Condition {
            conjunctions: join
                .on()
                .clone()
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .collect_vec(),
        };

        let mut right_apply_condition: Vec<ExprImpl> = vec![];
        let mut other_condition: Vec<ExprImpl> = vec![];

        match join.join_type() {
            JoinType::RightSemi | JoinType::RightAnti => {
                right_apply_condition.extend(apply_on);
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                let apply_len = apply_left_len + join.schema().len();
                let mut d_t2_bit_set = FixedBitSet::with_capacity(apply_len);
                d_t2_bit_set.set_range(0..apply_left_len, true);
                d_t2_bit_set.set_range(apply_left_len + join_left_len..apply_len, true);

                let (right, other): (Vec<_>, Vec<_>) = apply_on
                    .into_iter()
                    .partition(|expr| expr.collect_input_refs(apply_len).is_subset(&d_t2_bit_set));
                right_apply_condition.extend(right);
                other_condition.extend(other);

                // rewrite right condition
                let mut right_apply_condition_rewriter = Rewriter {
                    join_left_len: apply_left_len,
                    join_left_offset: 0,
                    join_right_offset: -(join_left_len as isize),
                    index_mapping: ColIndexMapping::empty(0, 0),
                    correlated_id,
                };

                right_apply_condition = right_apply_condition
                    .into_iter()
                    .map(|expr| right_apply_condition_rewriter.rewrite_expr(expr))
                    .collect_vec();
            }
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::Unspecified => unreachable!(),
        }

        let new_join_right = LogicalApply::create(
            apply_left,
            join.right(),
            apply_join_type,
            Condition {
                conjunctions: right_apply_condition,
            },
            correlated_id,
            correlated_indices,
            false,
        );
        let output_indices: Vec<_> = {
            let (apply_left_len, join_right_len) = match apply_join_type {
                JoinType::LeftSemi | JoinType::LeftAnti => (apply_left_len, 0),
                JoinType::RightSemi | JoinType::RightAnti => (0, join.right().schema().len()),
                _ => (apply_left_len, join.right().schema().len()),
            };

            let left_iter = join_left_len..join_left_len + apply_left_len;
            let right_iter = (0..join_left_len).chain(
                join_left_len + apply_left_len..join_left_len + apply_left_len + join_right_len,
            );

            match join.join_type() {
                JoinType::LeftSemi | JoinType::LeftAnti => left_iter.collect(),
                JoinType::RightSemi | JoinType::RightAnti => right_iter.collect(),
                _ => left_iter.chain(right_iter).collect(),
            }
        };
        let mut output_indices_mapping =
            ColIndexMapping::new(output_indices.iter().map(|x| Some(*x)).collect());
        let new_join = LogicalJoin::new(
            join.left(),
            new_join_right,
            join.join_type(),
            new_join_condition,
        )
        .clone_with_output_indices(output_indices);

        // Leave other condition for predicate push down to deal with
        LogicalFilter::create(
            new_join.into(),
            Condition {
                conjunctions: other_condition,
            }
            .rewrite_expr(&mut output_indices_mapping),
        )
    }

    fn push_apply_both_side(
        &self,
        apply_left: PlanRef,
        join: &LogicalJoin,
        apply_on: Condition,
        apply_join_type: JoinType,
        correlated_id: CorrelatedId,
        correlated_indices: Vec<usize>,
    ) -> PlanRef {
        let apply_left_len = apply_left.schema().len();
        let join_left_len = join.left().schema().len();
        let mut rewriter = Rewriter {
            join_left_len,
            join_left_offset: apply_left_len as isize,
            join_right_offset: 2 * apply_left_len as isize,
            index_mapping: ColIndexMapping::new(
                correlated_indices
                    .clone()
                    .into_iter()
                    .map(Some)
                    .collect_vec(),
            )
            .inverse()
            .expect("must be invertible"),
            correlated_id,
        };

        // Rewrite join on condition and add natural join condition
        let natural_conjunctions = apply_left
            .schema()
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                Self::create_null_safe_equal_expr(
                    i,
                    field.data_type.clone(),
                    i + join_left_len + apply_left_len,
                    field.data_type.clone(),
                )
            })
            .collect_vec();
        let new_join_condition = Condition {
            conjunctions: join
                .on()
                .clone()
                .into_iter()
                .map(|expr| rewriter.rewrite_expr(expr))
                .chain(natural_conjunctions.into_iter())
                .collect_vec(),
        };

        let mut left_apply_condition: Vec<ExprImpl> = vec![];
        let mut right_apply_condition: Vec<ExprImpl> = vec![];
        let mut other_condition: Vec<ExprImpl> = vec![];

        match join.join_type() {
            JoinType::LeftSemi | JoinType::LeftAnti => {
                left_apply_condition.extend(apply_on);
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                right_apply_condition.extend(apply_on);
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                let apply_len = apply_left_len + join.schema().len();
                let mut d_t1_bit_set = FixedBitSet::with_capacity(apply_len);
                let mut d_t2_bit_set = FixedBitSet::with_capacity(apply_len);
                d_t1_bit_set.set_range(0..apply_left_len + join_left_len, true);
                d_t2_bit_set.set_range(0..apply_left_len, true);
                d_t2_bit_set.set_range(apply_left_len + join_left_len..apply_len, true);

                for (key, group) in &apply_on.into_iter().group_by(|expr| {
                    let collect_bit_set = expr.collect_input_refs(apply_len);
                    if collect_bit_set.is_subset(&d_t1_bit_set) {
                        0
                    } else if collect_bit_set.is_subset(&d_t2_bit_set) {
                        1
                    } else {
                        2
                    }
                }) {
                    let vec = group.collect_vec();
                    match key {
                        0 => left_apply_condition.extend(vec),
                        1 => right_apply_condition.extend(vec),
                        2 => other_condition.extend(vec),
                        _ => unreachable!(),
                    }
                }

                // Rewrite right condition
                let mut right_apply_condition_rewriter = Rewriter {
                    join_left_len: apply_left_len,
                    join_left_offset: 0,
                    join_right_offset: -(join_left_len as isize),
                    index_mapping: ColIndexMapping::empty(0, 0),
                    correlated_id,
                };

                right_apply_condition = right_apply_condition
                    .into_iter()
                    .map(|expr| right_apply_condition_rewriter.rewrite_expr(expr))
                    .collect_vec();
            }
            JoinType::Unspecified => unreachable!(),
        }

        let new_join_left = LogicalApply::create(
            apply_left.clone(),
            join.left(),
            apply_join_type,
            Condition {
                conjunctions: left_apply_condition,
            },
            correlated_id,
            correlated_indices.clone(),
            false,
        );
        let new_join_right = LogicalApply::create(
            apply_left,
            join.right(),
            apply_join_type,
            Condition {
                conjunctions: right_apply_condition,
            },
            correlated_id,
            correlated_indices,
            false,
        );

        let output_indices: Vec<_> = {
            let (apply_left_len, join_right_len) = match apply_join_type {
                JoinType::LeftSemi | JoinType::LeftAnti => (apply_left_len, 0),
                JoinType::RightSemi | JoinType::RightAnti => (0, join.right().schema().len()),
                _ => (apply_left_len, join.right().schema().len()),
            };

            let left_iter = 0..join_left_len + apply_left_len;
            let right_iter = join_left_len + apply_left_len * 2
                ..join_left_len + apply_left_len * 2 + join_right_len;

            match join.join_type() {
                JoinType::LeftSemi | JoinType::LeftAnti => left_iter.collect(),
                JoinType::RightSemi | JoinType::RightAnti => right_iter.collect(),
                _ => left_iter.chain(right_iter).collect(),
            }
        };
        let new_join = LogicalJoin::new(
            new_join_left,
            new_join_right,
            join.join_type(),
            new_join_condition,
        )
        .clone_with_output_indices(output_indices.clone());

        match join.join_type() {
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti => {
                new_join.into()
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                let mut output_indices_mapping =
                    ColIndexMapping::new(output_indices.iter().map(|x| Some(*x)).collect());
                // Leave other condition for predicate push down to deal with
                LogicalFilter::create(
                    new_join.into(),
                    Condition {
                        conjunctions: other_condition,
                    }
                    .rewrite_expr(&mut output_indices_mapping),
                )
            }
            JoinType::Unspecified => unreachable!(),
        }
    }

    fn create_null_safe_equal_expr(
        left: usize,
        left_data_type: DataType,
        right: usize,
        right_data_type: DataType,
    ) -> ExprImpl {
        // use null-safe equal
        ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
            ExprType::IsNotDistinctFrom,
            vec![
                ExprImpl::InputRef(Box::new(InputRef::new(left, left_data_type))),
                ExprImpl::InputRef(Box::new(InputRef::new(right, right_data_type))),
            ],
            Boolean,
        )))
    }
}

impl ApplyJoinTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyJoinTransposeRule {})
    }
}

/// Convert `CorrelatedInputRef` to `InputRef` and shift `InputRef` with offset.
struct Rewriter {
    join_left_len: usize,
    join_left_offset: isize,
    join_right_offset: isize,
    index_mapping: ColIndexMapping,
    correlated_id: CorrelatedId,
}
impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        if correlated_input_ref.correlated_id() == self.correlated_id {
            InputRef::new(
                self.index_mapping.map(correlated_input_ref.index()),
                correlated_input_ref.return_type(),
            )
            .into()
        } else {
            correlated_input_ref.into()
        }
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        if input_ref.index < self.join_left_len {
            InputRef::new(
                (input_ref.index() as isize + self.join_left_offset) as usize,
                input_ref.return_type(),
            )
            .into()
        } else {
            InputRef::new(
                (input_ref.index() as isize + self.join_right_offset) as usize,
                input_ref.return_type(),
            )
            .into()
        }
    }
}
