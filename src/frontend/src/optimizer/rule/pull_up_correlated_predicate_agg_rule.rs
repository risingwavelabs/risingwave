// Copyright 2024 RisingWave Labs
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
use itertools::{Either, Itertools};
use risingwave_common::types::DataType;
use risingwave_common::util::column_index_mapping::ColIndexMapping;
use risingwave_expr::aggregate::{AggType, PbAggKind};
use risingwave_pb::plan_common::JoinType;

use super::prelude::{PlanRef, *};
use crate::expr::{
    CorrelatedId, Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef, Literal,
};
use crate::optimizer::plan_expr_visitor::Strong;
use crate::optimizer::plan_node::generic::{Agg, GenericPlanNode, GenericPlanRef};
use crate::optimizer::plan_node::*;
use crate::optimizer::plan_visitor::{PlanCorrelatedIdFinder, PlanVisitor};
use crate::utils::{Condition, IndexSet};

/// Pull up correlated predicates from the right agg side of Apply to the `on` clause of Join.
///
/// Before:
///
/// ```text
///        Filter
///          |
///     LogicalApply
///    /            \
///  LHS          Project
///                 |
///                Agg [group by nothing]
///                 |
///               Project
///                 |
///               Filter [correlated_input_ref(yyy) = xxx]
/// ```
///
/// After:
///
/// ```text
///        Filter
///          |
///     LogicalJoin [yyy = xxx]
///    /            \
///  LHS          Project
///                 |
///                Agg [group by xxx]
///                 |
///               Project
///                 |
///               Filter
/// ```
///
/// Grouping by `xxx` drops the row the scalar aggregate yields for an empty input. The rule
/// therefore requires the top `Project` to fold to a constant on an empty group (`count` is `0`,
/// other aggregates are NULL): if the constant is NULL or the top `Filter` rejects it, the join
/// type is kept as is; otherwise the join becomes `LeftOuter` and a `Project` above it restores
/// the constant (e.g. `ARRAY[]` for `ARRAY(...)`, `0` for `count(*)`) when the right side is
/// null-padded.
pub struct PullUpCorrelatedPredicateAggRule {}
impl Rule<Logical> for PullUpCorrelatedPredicateAggRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_filter = if let Some(top_filter) = plan.as_logical_filter() {
            top_filter.clone()
        } else {
            LogicalFilter::new(plan, Condition::true_cond())
        };
        let top_filter_input = top_filter.input();
        let apply = top_filter_input.as_logical_apply()?;
        let (apply_left, apply_right, apply_on, join_type, correlated_id, correlated_indices, _) =
            apply.clone().decompose();

        if let Some(plan) = self.split_cross_joined_subqueries(
            apply_left.clone(),
            apply_right.clone(),
            &apply_on,
            join_type,
            correlated_id,
            &correlated_indices,
        ) {
            return Some(LogicalFilter::create(plan, top_filter.predicate().clone()));
        }

        let top_project = if let Some(project) = apply_right.as_logical_project() {
            project.clone()
        } else {
            LogicalProject::with_mapping(
                apply_right.clone(),
                ColIndexMapping::identity(apply_right.schema().len()),
            )
        };
        let (top_proj_exprs, _) = top_project.clone().decompose();

        let input = top_project.input();
        let agg: &LogicalAgg = input.as_logical_agg()?;
        let (agg_calls, group_key, grouping_sets, input, _enable_two_phase) =
            agg.clone().decompose();
        // It could be too restrictive to require the group key to be empty. We can relax this in the future if necessary.
        if !group_key.is_empty() {
            return None;
        }
        // An aggregate without grouping returns exactly one row, so a scalar Apply's
        // max-one-row check is also satisfied after joining on the correlated group keys.
        assert!(grouping_sets.is_empty());
        let bottom_project = if let Some(project) = input.as_logical_project() {
            project.clone()
        } else {
            LogicalProject::with_mapping(
                input.clone(),
                ColIndexMapping::identity(input.schema().len()),
            )
        };
        let (mut bottom_proj_exprs, _) = bottom_project.clone().decompose();
        let bottom_project_input = bottom_project.input();
        let bottom_filter: &LogicalFilter = bottom_project_input.as_logical_filter()?;

        // Split predicates in LogicalFilter into correlated expressions and uncorrelated
        // expressions.
        let (cor_exprs, uncor_exprs): (Vec<ExprImpl>, Vec<ExprImpl>) = bottom_filter
            .predicate()
            .clone()
            .into_iter()
            .partition_map(|expr| {
                if expr.has_correlated_input_ref_by_correlated_id(correlated_id) {
                    Either::Left(expr)
                } else {
                    Either::Right(expr)
                }
            });

        // Ensure all correlated expressions look like [correlated_input_ref = input_ref]
        let mut cor_eq_exprs = vec![];
        for cor_expr in &cor_exprs {
            if let Some((input_ref, cor_input_ref)) = cor_expr.as_eq_correlated_input_ref() {
                if cor_input_ref.correlated_id() == correlated_id {
                    cor_eq_exprs.push((input_ref, cor_input_ref));
                } else {
                    return None;
                }
            } else {
                return None;
            }
        }
        let cor_eq_exprs_len = cor_eq_exprs.len();

        let filter = LogicalFilter::create(
            bottom_filter.input(),
            Condition {
                conjunctions: uncor_exprs,
            },
        );

        // Append `InputRef`s in the predicate expression to be pulled to the project, so that they
        // are accessible by the expression after it is pulled.
        bottom_proj_exprs.extend(
            cor_eq_exprs
                .iter()
                .map(|(input_ref, _)| ExprImpl::InputRef(input_ref.clone().into())),
        );

        let new_bottom_proj: PlanRef = LogicalProject::new(filter, bottom_proj_exprs).into();

        // Grouping by the correlated key loses the row that a scalar agg produces for an empty
        // input. We can apply this rule only if we know what the top project yields for that
        // row (a constant, e.g. NULL, `0` for `count`, `ARRAY[]` for `ARRAY(...)`), so that we
        // can either prove the top filter rejects it or re-create it above a left outer join.
        let empty_values = top_proj_exprs
            .iter()
            .map(|expr| eval_on_empty_group(expr.clone(), &agg_calls))
            .collect::<Option<Vec<_>>>()?;
        let apply_left_len = apply_left.schema().len();
        // Shift the top project expressions to the right by apply_left schema len, because it is
        // used to check null-rejected by the top filter.
        let mut top_proj_null_bitset =
            FixedBitSet::with_capacity(top_project.base.schema().len() + apply_left_len);
        for (i, value) in empty_values.iter().enumerate() {
            if value.is_null() {
                top_proj_null_bitset.insert(i + apply_left_len);
            }
        }
        let top_proj_all_null = top_proj_null_bitset.count_ones(..) == empty_values.len();
        let top_filter_rejects_null = top_filter
            .predicate()
            .conjunctions
            .iter()
            .any(|expr| Strong::is_null(expr, top_proj_null_bitset.clone()));
        // Whether the left-padded nulls of the left outer join must be replaced by the values
        // the subquery yields on an empty group.
        let fill_empty_values = !top_proj_all_null && !top_filter_rejects_null;
        // Restoring the empty-group values needs a group key to tell null-padded rows apart.
        if fill_empty_values && cor_eq_exprs_len == 0 {
            return None;
        }
        // Semi and anti joins don't pad a missing row with NULLs, so the row the scalar agg
        // yields for an empty input can only be dropped if the apply's `on` rejects it.
        if matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti)
            && !apply_on
                .conjunctions
                .iter()
                .any(|expr| Strong::is_null(expr, top_proj_null_bitset.clone()))
        {
            return None;
        }

        // New agg with group key extracted from the cor_eq_exprs.
        let new_agg = Agg::new(
            agg_calls,
            IndexSet::from_iter(
                new_bottom_proj.schema().len() - cor_eq_exprs_len..new_bottom_proj.schema().len(),
            ),
            new_bottom_proj,
        );

        let mut shift_input_ref_rewriter = ShiftInputRefRewriter {
            offset: cor_eq_exprs_len,
        };
        // Shift the top project expressions to the right by cor_eq_exprs_len.
        let mut top_proj_exprs = top_proj_exprs
            .into_iter()
            .map(|expr| shift_input_ref_rewriter.rewrite_expr(expr))
            .collect_vec();
        top_proj_exprs.extend((0..new_agg.group_key.len()).map(|i| {
            ExprImpl::InputRef(
                InputRef::new(i, new_agg.schema().fields[i].data_type.clone()).into(),
            )
        }));

        let new_top_proj: PlanRef = LogicalProject::new(new_agg.into(), top_proj_exprs).into();

        let cor_eq_exprs = cor_eq_exprs
            .into_iter()
            .enumerate()
            .map(|(i, (input_ref, correlated_input_ref))| {
                ExprImpl::FunctionCall(
                    FunctionCall::new_unchecked(
                        ExprType::Equal,
                        vec![
                            InputRef::new(
                                correlated_input_ref.index(),
                                correlated_input_ref.return_type(),
                            )
                            .into(),
                            InputRef::new(
                                new_top_proj.schema().len() - cor_eq_exprs_len
                                    + i
                                    + apply_left.schema().len(),
                                input_ref.return_type(),
                            )
                            .into(),
                        ],
                        DataType::Boolean,
                    )
                    .into(),
                )
            })
            .collect_vec();

        // Check whether correlated_input_ref with same correlated_id exists for the join right
        // side. If yes, bail out and leave for general subquery unnesting to deal with
        let mut plan_correlated_id_finder = PlanCorrelatedIdFinder::default();
        plan_correlated_id_finder.visit(new_top_proj.clone());
        if plan_correlated_id_finder.contains(&correlated_id) {
            return None;
        }

        let correlated_predicate = Condition {
            conjunctions: cor_eq_exprs,
        };
        let (join_type, on, filter) =
            if top_filter_rejects_null || (join_type != JoinType::Inner && top_proj_all_null) {
                (
                    join_type,
                    apply_on.and(correlated_predicate),
                    top_filter.predicate().clone(),
                )
            } else if join_type == JoinType::Inner
                || (join_type == JoinType::LeftOuter && apply_on.always_true())
            {
                // A scalar aggregate returns one row even for empty input. Grouping by the
                // correlated columns removes that row, so preserve it with a left outer join.
                // The original Apply's ON condition must still filter the resulting row.
                (
                    JoinType::LeftOuter,
                    correlated_predicate,
                    top_filter.predicate().clone().and(apply_on),
                )
            } else {
                return None;
            };

        let apply_len = apply.schema().len();
        let new_join: PlanRef = if fill_empty_values {
            // The row for an empty group is null-padded by the left outer join; restore the
            // values the subquery yields on an empty group. The right side's first group key
            // (right after the Apply's columns) is null iff the group is empty.
            let new_join: PlanRef =
                LogicalJoin::new(apply_left, new_top_proj, join_type, on).into();
            let group_key_is_null: ExprImpl = FunctionCall::new_unchecked(
                ExprType::IsNull,
                vec![
                    InputRef::new(
                        apply_len,
                        new_join.schema().fields[apply_len].data_type.clone(),
                    )
                    .into(),
                ],
                DataType::Boolean,
            )
            .into();
            let exprs = new_join.schema().fields[..apply_len]
                .iter()
                .enumerate()
                .map(|(i, field)| {
                    let col: ExprImpl = InputRef::new(i, field.data_type.clone()).into();
                    match i.checked_sub(apply_left_len).map(|j| &empty_values[j]) {
                        Some(value) if !value.is_null() => FunctionCall::new_unchecked(
                            ExprType::Case,
                            vec![group_key_is_null.clone(), value.clone(), col],
                            field.data_type.clone(),
                        )
                        .into(),
                        _ => col,
                    }
                })
                .collect_vec();
            LogicalProject::new(new_join, exprs).into()
        } else {
            LogicalJoin::with_output_indices(
                apply_left,
                new_top_proj,
                join_type,
                on,
                (0..apply_len).collect(),
            )
            .into()
        };

        Some(LogicalFilter::create(new_join, filter))
    }
}

/// Value of `expr`, defined over the output of `agg_calls`, when the aggregate input is empty:
/// `count` and `approx_count_distinct` yield `0`, every other built-in aggregate yields NULL.
/// `None` if an aggregate's empty-group value is unknown (user-defined) or `expr` does not fold
/// to a constant.
fn eval_on_empty_group(expr: ExprImpl, agg_calls: &[PlanAggCall]) -> Option<ExprImpl> {
    if agg_calls
        .iter()
        .any(|call| !matches!(call.agg_type, AggType::Builtin(_)))
    {
        return None;
    }
    struct EmptyGroupRewriter<'a>(&'a [PlanAggCall]);
    impl ExprRewriter for EmptyGroupRewriter<'_> {
        fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
            if matches!(
                self.0[input_ref.index()].agg_type,
                AggType::Builtin(PbAggKind::Count | PbAggKind::ApproxCountDistinct)
            ) {
                ExprImpl::literal_bigint(0)
            } else {
                ExprImpl::literal_null(input_ref.return_type())
            }
        }
    }
    let expr = EmptyGroupRewriter(agg_calls).rewrite_expr(expr);
    let datum = expr.try_fold_const()?.ok()?;
    Some(Literal::new(datum, expr.return_type()).into())
}

struct ShiftInputRefRewriter {
    offset: usize,
}
impl ExprRewriter for ShiftInputRefRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}

impl PullUpCorrelatedPredicateAggRule {
    pub fn create() -> BoxedRule {
        Box::new(PullUpCorrelatedPredicateAggRule {})
    }

    /// Several subqueries in one select list are planned as a single `Apply` over a
    /// `FullOuter` cross join of their (scalar aggregate) subplans. Split it into one `Apply`
    /// per subquery, but only if every one of them is then pulled up by this rule, so that
    /// we never fall back to the (more expensive when nested) general decorrelation.
    fn split_cross_joined_subqueries(
        &self,
        apply_left: PlanRef,
        apply_right: PlanRef,
        apply_on: &Condition,
        join_type: JoinType,
        correlated_id: CorrelatedId,
        correlated_indices: &[usize],
    ) -> Option<PlanRef> {
        let join = apply_right.as_logical_join()?;
        if join.join_type() != JoinType::FullOuter
            || !join.on().always_true()
            || !join.output_indices_are_trivial()
            || !apply_on.always_true()
            || !matches!(join_type, JoinType::Inner | JoinType::LeftOuter)
        {
            return None;
        }
        let mut left = apply_left;
        for right in [join.left(), join.right()] {
            let apply = LogicalApply::create(
                left,
                right,
                join_type,
                Condition::true_cond(),
                correlated_id,
                correlated_indices.to_vec(),
                true,
            );
            left = self.apply(apply)?;
        }
        Some(left)
    }
}
