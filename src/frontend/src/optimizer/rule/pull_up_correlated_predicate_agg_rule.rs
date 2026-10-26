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
use crate::expr::{Expr, ExprImpl, ExprRewriter, ExprType, FunctionCall, InputRef};
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
        let (apply_left, apply_right, apply_on, join_type, correlated_id, _, _) =
            apply.clone().decompose();

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

        // After the rewrite, an outer row without matching rows finds no group. Before it, the
        // subquery returned one row for such an outer row: the top project over the aggregate
        // results of empty input. Find the columns of that row that are null.
        let empty_results = agg_calls.iter().map(EmptyAggResult::of).collect_vec();
        let apply_left_len = apply_left.schema().len();
        let mut empty_row_all_null = true;
        let mut empty_row_null_bitset =
            FixedBitSet::with_capacity(apply_left_len + top_proj_exprs.len());
        for (i, expr) in top_proj_exprs.iter().enumerate() {
            if EmptyAggResult::is_null_for_empty_input(expr, &empty_results) {
                empty_row_null_bitset.insert(apply_left_len + i);
            } else {
                empty_row_all_null = false;
            }
        }
        let rejects_empty_row = |condition: &Condition| {
            condition
                .conjunctions
                .iter()
                .any(|expr| Strong::is_null(expr, empty_row_null_bitset.clone()))
        };
        let top_filter_rejects_null = rejects_empty_row(top_filter.predicate());

        // The aggregate can be joined directly if replacing that row with a null row, or
        // dropping it, does not change the output:
        // - for inner and left outer apply, the row is all null, or the top filter rejects it;
        // - for semi and anti apply, the apply condition is not true for the row, so the outer
        //   row is dropped by the semi join and kept by the anti join either way.
        let join_directly = match join_type {
            JoinType::LeftSemi | JoinType::LeftAnti => rejects_empty_row(&apply_on),
            _ => empty_row_all_null || top_filter_rejects_null,
        };
        // Without correlated equalities, there is nothing to group by, and the subquery is left
        // to the other rules.
        if !join_directly && cor_eq_exprs.is_empty() {
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
        let mut shifted_top_proj_exprs = top_proj_exprs
            .iter()
            .map(|expr| shift_input_ref_rewriter.rewrite_expr(expr.clone()))
            .collect_vec();
        shifted_top_proj_exprs.extend((0..new_agg.group_key.len()).map(|i| {
            ExprImpl::InputRef(
                InputRef::new(i, new_agg.schema().fields[i].data_type.clone()).into(),
            )
        }));

        let new_top_proj: PlanRef =
            LogicalProject::new(new_agg.clone().into(), shifted_top_proj_exprs).into();

        // Check whether correlated_input_ref with same correlated_id exists for the join right
        // side. If yes, bail out and leave for general subquery unnesting to deal with
        let mut plan_correlated_id_finder = PlanCorrelatedIdFinder::default();
        plan_correlated_id_finder.visit(new_top_proj.clone());
        if plan_correlated_id_finder.contains(&correlated_id) {
            return None;
        }

        // `correlated_input_ref = group_key[i]`, where the group keys start at `group_key_offset`
        // of the join output.
        let correlated_predicate = |group_key_offset: usize| Condition {
            conjunctions: cor_eq_exprs
                .iter()
                .enumerate()
                .map(|(i, (input_ref, correlated_input_ref))| {
                    FunctionCall::new_unchecked(
                        ExprType::Equal,
                        vec![
                            InputRef::new(
                                correlated_input_ref.index(),
                                correlated_input_ref.return_type(),
                            )
                            .into(),
                            InputRef::new(group_key_offset + i, input_ref.return_type()).into(),
                        ],
                        DataType::Boolean,
                    )
                    .into()
                })
                .collect(),
        };

        if !join_directly {
            return Self::join_with_empty_results(
                apply_left,
                new_agg.into(),
                correlated_predicate(apply_left_len),
                top_proj_exprs,
                &empty_results,
                join_type,
                apply_on,
                top_filter.predicate().clone(),
            );
        }

        let correlated_predicate =
            correlated_predicate(apply_left_len + new_top_proj.schema().len() - cor_eq_exprs_len);
        let (join_type, on, filter) = if join_type == JoinType::Inner && !top_filter_rejects_null {
            // A scalar aggregate returns one row even for empty input. Grouping by the
            // correlated columns removes that row, so preserve it with a left outer join.
            // The original Apply's ON condition must still filter the resulting row.
            (
                JoinType::LeftOuter,
                correlated_predicate,
                top_filter.predicate().clone().and(apply_on),
            )
        } else {
            (
                join_type,
                apply_on.and(correlated_predicate),
                top_filter.predicate().clone(),
            )
        };

        let new_join = LogicalJoin::with_output_indices(
            apply_left,
            new_top_proj,
            join_type,
            on,
            (0..apply.schema().len()).collect(),
        )
        .into();

        Some(LogicalFilter::create(new_join, filter))
    }
}

impl PullUpCorrelatedPredicateAggRule {
    pub fn create() -> BoxedRule {
        Box::new(PullUpCorrelatedPredicateAggRule {})
    }

    /// Joins every outer row with its group of the aggregate, then evaluates the subquery's top
    /// project above the join. An outer row without a group gets the aggregate results over empty
    /// input, such as `0` for `count`, so the subquery returns the same row as before the rewrite.
    ///
    /// ```text
    ///         Filter [top filter]
    ///           |
    ///    (Filter [apply on])
    ///           |
    ///   Project [LHS, top project with coalesce(count, 0)]
    ///           |
    ///   LogicalJoin [LeftOuter, xxx = yyy]
    ///    /            \
    ///  LHS          Agg [group by xxx]
    /// ```
    fn join_with_empty_results(
        apply_left: PlanRef,
        new_agg: PlanRef,
        correlated_predicate: Condition,
        top_proj_exprs: Vec<ExprImpl>,
        empty_results: &[EmptyAggResult],
        join_type: JoinType,
        apply_on: Condition,
        top_filter: Condition,
    ) -> Option<PlanRef> {
        // The result of an unknown aggregate over empty input cannot be filled in.
        if empty_results
            .iter()
            .any(|result| matches!(result, EmptyAggResult::Unknown))
        {
            return None;
        }
        // Only an always-true condition keeps every outer row of a left outer apply paired with
        // its subquery row.
        if join_type == JoinType::LeftOuter && !apply_on.always_true() {
            return None;
        }

        let apply_left_len = apply_left.schema().len();
        let group_key_len = correlated_predicate.conjunctions.len();
        let join: PlanRef = LogicalJoin::new(
            apply_left,
            new_agg,
            JoinType::LeftOuter,
            correlated_predicate,
        )
        .into();

        let mut rewriter = FillEmptyAggResultRewriter {
            agg_offset: apply_left_len + group_key_len,
            empty_results,
        };
        let exprs = join.schema().fields()[..apply_left_len]
            .iter()
            .enumerate()
            .map(|(i, field)| InputRef::new(i, field.data_type.clone()).into())
            .chain(
                top_proj_exprs
                    .into_iter()
                    .map(|expr| rewriter.rewrite_expr(expr)),
            )
            .collect();
        // One row for each outer row: the outer row followed by its subquery row.
        let paired = LogicalProject::create(join, exprs);

        let output = match join_type {
            JoinType::Inner => LogicalFilter::create(paired, apply_on),
            JoinType::LeftOuter => paired,
            JoinType::LeftSemi => LogicalProject::with_out_col_idx(
                LogicalFilter::create(paired, apply_on),
                0..apply_left_len,
            )
            .into(),
            JoinType::LeftAnti => LogicalProject::with_out_col_idx(
                LogicalFilter::create_with_expr(
                    paired,
                    FunctionCall::new_unchecked(
                        ExprType::IsNotTrue,
                        vec![apply_on.into()],
                        DataType::Boolean,
                    )
                    .into(),
                ),
                0..apply_left_len,
            )
            .into(),
            _ => return None,
        };
        Some(LogicalFilter::create(output, top_filter))
    }
}

/// The result of an aggregate over empty input.
enum EmptyAggResult {
    Null,
    /// A non-null value. The aggregate never returns null for non-empty input, so a null result
    /// after the left outer join means the group is empty.
    Value(ExprImpl),
    Unknown,
}

impl EmptyAggResult {
    /// Returns whether `expr` over the aggregate outputs is null when the aggregate input is
    /// empty, by substituting the aggregate results of empty input and folding the constant.
    fn is_null_for_empty_input(expr: &ExprImpl, empty_results: &[Self]) -> bool {
        struct Substitute<'a> {
            empty_results: &'a [EmptyAggResult],
        }
        impl ExprRewriter for Substitute<'_> {
            fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
                match &self.empty_results[input_ref.index()] {
                    EmptyAggResult::Null => ExprImpl::literal_null(input_ref.return_type()),
                    EmptyAggResult::Value(value) => value.clone(),
                    // Not a constant, so the expression is not folded.
                    EmptyAggResult::Unknown => input_ref.into(),
                }
            }
        }
        let expr = Substitute { empty_results }.rewrite_expr(expr.clone());
        matches!(expr.try_fold_const(), Some(Ok(None)))
    }

    fn of(agg_call: &PlanAggCall) -> Self {
        let AggType::Builtin(kind) = &agg_call.agg_type else {
            return Self::Unknown;
        };
        match kind {
            PbAggKind::Count | PbAggKind::ApproxCountDistinct
                if agg_call.return_type == DataType::Int64 =>
            {
                Self::Value(ExprImpl::literal_bigint(0))
            }
            PbAggKind::Sum
            | PbAggKind::Min
            | PbAggKind::Max
            | PbAggKind::Avg
            | PbAggKind::StringAgg
            | PbAggKind::ArrayAgg
            | PbAggKind::FirstValue
            | PbAggKind::LastValue
            | PbAggKind::VarPop
            | PbAggKind::VarSamp
            | PbAggKind::StddevPop
            | PbAggKind::StddevSamp
            | PbAggKind::BitAnd
            | PbAggKind::BitOr
            | PbAggKind::BitXor
            | PbAggKind::BoolAnd
            | PbAggKind::BoolOr
            | PbAggKind::JsonbAgg
            | PbAggKind::JsonbObjectAgg
            | PbAggKind::PercentileCont
            | PbAggKind::PercentileDisc
            | PbAggKind::Mode
            | PbAggKind::ApproxPercentile
            | PbAggKind::ArgMin
            | PbAggKind::ArgMax => Self::Null,
            _ => Self::Unknown,
        }
    }
}

/// Rewrites the subquery's top project to read the aggregate results from the join output,
/// replacing the null of an empty group with the aggregate result over empty input.
struct FillEmptyAggResultRewriter<'a> {
    agg_offset: usize,
    empty_results: &'a [EmptyAggResult],
}

impl ExprRewriter for FillEmptyAggResultRewriter<'_> {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let agg_result: ExprImpl =
            InputRef::new(self.agg_offset + input_ref.index(), input_ref.return_type()).into();
        match &self.empty_results[input_ref.index()] {
            EmptyAggResult::Value(empty_value) => FunctionCall::new_unchecked(
                ExprType::Coalesce,
                vec![agg_result, empty_value.clone()],
                input_ref.return_type(),
            )
            .into(),
            EmptyAggResult::Null | EmptyAggResult::Unknown => agg_result,
        }
    }
}

struct ShiftInputRefRewriter {
    offset: usize,
}
impl ExprRewriter for ShiftInputRefRewriter {
    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        InputRef::new(input_ref.index() + self.offset, input_ref.return_type()).into()
    }
}
