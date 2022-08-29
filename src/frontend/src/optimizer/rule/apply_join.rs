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

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::types::DataType;
use risingwave_common::types::DataType::Boolean;
use risingwave_pb::plan_common::JoinType;

use super::{BoxedRule, Rule};
use crate::expr::{
    CollectInputRef, CorrelatedId, CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, ExprType,
    ExprVisitor, FunctionCall, InputRef,
};
use crate::optimizer::plan_correlated_id_finder::{ExprCorrelatedIdFinder, PlanCorrelatedIdFinder};
use crate::optimizer::plan_node::{
    LogicalApply, LogicalFilter, LogicalJoin, LogicalProject, PlanTreeNodeBinary,
};
use crate::optimizer::PlanRef;
use crate::utils::{ColIndexMapping, Condition};

/// Push `LogicalJoin` down `LogicalApply`
///
/// `push_apply_both_side`:
///
/// D Apply (T1 join< p > T2)  ->  (D Apply T1) join< p and natural join D > (D Apply T2)
///
/// `push_apply_left_side`:
///
/// D Apply (T1 join< p > T2)  ->  (D Apply T1) join< p > T2
///
/// `push_apply_right_side`:
///
/// D Apply (T1 join< p > T2)  ->  T1 join< p > (D Apply T2)
pub struct ApplyJoinRule {}
impl Rule for ApplyJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (apply_left, apply_right, apply_on, apply_join_type, correlated_id, correlated_indices) =
            apply.clone().decompose();
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

        if push_left && push_right {
            Some(self.push_apply_both_side(
                apply_left,
                join,
                apply_on,
                apply_join_type,
                correlated_id,
                correlated_indices,
            ))
        } else if push_left {
            Some(self.push_apply_left_side(
                apply_left,
                join,
                apply_on,
                apply_join_type,
                correlated_id,
                correlated_indices,
            ))
        } else if push_right {
            Some(self.push_apply_right_side(
                apply_left,
                join,
                apply_on,
                apply_join_type,
                correlated_id,
                correlated_indices,
            ))
        } else {
            unreachable!();
        }
    }
}

impl ApplyJoinRule {
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
            .inverse(),
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

                for (key, group) in &apply_on.into_iter().group_by(|expr| {
                    let mut visitor = CollectInputRef::with_capacity(apply_len);
                    visitor.visit_expr(expr);
                    let collect_bit_set = FixedBitSet::from(visitor);
                    if collect_bit_set.is_subset(&d_t1_bit_set) {
                        0
                    } else {
                        1
                    }
                }) {
                    let vec = group.collect_vec();
                    match key {
                        0 => left_apply_condition.extend(vec),
                        1 => other_condition.extend(vec),
                        _ => unreachable!(),
                    }
                }
            }
            JoinType::RightSemi | JoinType::RightAnti | JoinType::Unspecified => unreachable!(),
        }

        let new_join_left = LogicalApply::create(
            apply_left.clone(),
            join.left().clone(),
            apply_join_type,
            Condition {
                conjunctions: left_apply_condition,
            },
            correlated_id,
            correlated_indices,
        );

        let new_join = LogicalJoin::new(
            new_join_left.clone(),
            join.right().clone(),
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
            .inverse(),
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

                for (key, group) in &apply_on.into_iter().group_by(|expr| {
                    let mut visitor = CollectInputRef::with_capacity(apply_len);
                    visitor.visit_expr(expr);
                    let collect_bit_set = FixedBitSet::from(visitor);
                    if collect_bit_set.is_subset(&d_t2_bit_set) {
                        0
                    } else {
                        1
                    }
                }) {
                    let vec = group.collect_vec();
                    match key {
                        0 => right_apply_condition.extend(vec),
                        1 => other_condition.extend(vec),
                        _ => unreachable!(),
                    }
                }

                // rewrite right condition
                let mut right_apply_condition_rewriter = Rewriter {
                    join_left_len: apply_left_len,
                    join_left_offset: 0,
                    join_right_offset: -(join_left_len as isize),
                    index_mapping: ColIndexMapping::empty(0),
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
            apply_left.clone(),
            join.right().clone(),
            apply_join_type,
            Condition {
                conjunctions: right_apply_condition,
            },
            correlated_id,
            correlated_indices,
        );
        let new_join = LogicalJoin::new(
            join.left().clone(),
            new_join_right.clone(),
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
            .inverse(),
            correlated_id,
        };

        // Rewrite join on condition and add natural join condition
        let natural_conjunctions = apply_left
            .schema()
            .fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                Self::create_equal_expr(
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
                    let mut visitor = CollectInputRef::with_capacity(apply_len);
                    visitor.visit_expr(expr);
                    let collect_bit_set = FixedBitSet::from(visitor);
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
                    index_mapping: ColIndexMapping::empty(0),
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
            join.left().clone(),
            apply_join_type,
            Condition {
                conjunctions: left_apply_condition,
            },
            correlated_id,
            correlated_indices.clone(),
        );
        let new_join_right = LogicalApply::create(
            apply_left.clone(),
            join.right().clone(),
            apply_join_type,
            Condition {
                conjunctions: right_apply_condition,
            },
            correlated_id,
            correlated_indices,
        );
        let new_join = LogicalJoin::new(
            new_join_left.clone(),
            new_join_right.clone(),
            join.join_type(),
            new_join_condition,
        );

        match join.join_type() {
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::RightSemi | JoinType::RightAnti => {
                new_join.into()
            }
            JoinType::Inner | JoinType::LeftOuter | JoinType::RightOuter | JoinType::FullOuter => {
                // Use project to provide a natural join
                let mut project_exprs: Vec<ExprImpl> = vec![];

                let d_offset = if join.join_type() == JoinType::RightOuter {
                    new_join_left.schema().len()
                } else {
                    0
                };

                project_exprs.extend(
                    apply_left
                        .schema()
                        .fields
                        .iter()
                        .enumerate()
                        .map(|(i, field)| {
                            ExprImpl::InputRef(Box::new(InputRef::new(
                                i + d_offset,
                                field.data_type.clone(),
                            )))
                        })
                        .collect_vec(),
                );

                project_exprs.extend(
                    new_join_left
                        .schema()
                        .fields
                        .iter()
                        .enumerate()
                        .skip(apply_left_len)
                        .map(|(i, field)| {
                            ExprImpl::InputRef(Box::new(InputRef::new(i, field.data_type.clone())))
                        })
                        .collect_vec(),
                );
                project_exprs.extend(
                    new_join_right
                        .schema()
                        .fields
                        .iter()
                        .enumerate()
                        .skip(apply_left_len)
                        .map(|(i, field)| {
                            ExprImpl::InputRef(Box::new(InputRef::new(
                                i + new_join_left.schema().len(),
                                field.data_type.clone(),
                            )))
                        })
                        .collect_vec(),
                );

                let new_project = LogicalProject::create(new_join.into(), project_exprs);

                // Leave other condition for predicate push down to deal with
                LogicalFilter::create(
                    new_project,
                    Condition {
                        conjunctions: other_condition,
                    },
                )
            }
            JoinType::Unspecified => unreachable!(),
        }
    }

    fn create_equal_expr(
        left: usize,
        left_data_type: DataType,
        right: usize,
        right_data_type: DataType,
    ) -> ExprImpl {
        // TODO: use is not distinct from instead of equal
        ExprImpl::FunctionCall(Box::new(FunctionCall::new_unchecked(
            ExprType::Equal,
            vec![
                ExprImpl::InputRef(Box::new(InputRef::new(left, left_data_type))),
                ExprImpl::InputRef(Box::new(InputRef::new(right, right_data_type))),
            ],
            Boolean,
        )))
    }
}

impl ApplyJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyJoinRule {})
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
