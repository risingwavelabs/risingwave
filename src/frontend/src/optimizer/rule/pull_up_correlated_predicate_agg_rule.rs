// Copyright 2025 RisingWave Labs
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
        let (apply_left, apply_right, apply_on, join_type, correlated_id, _, max_one_row) =
            apply.clone().decompose();

        if max_one_row {
            return None;
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

        // We can apply this rule only if:
        // 1. The `group by + proj` returns null for empty input
        // 2. OR the top filter is null for empty input
        {
            // When group input is empty, if the agg is not `count`, it would return null.
            let null_agg_pos = agg_calls
                .iter()
                .positions(|agg_call| {
                    !matches!(agg_call.agg_type, AggType::Builtin(PbAggKind::Count))
                })
                .collect_vec();

            // We don't have null args, so the output will never be null. Bail out.
            if null_agg_pos.is_empty() {
                return None;
            }

            // Try to prove that the expression is null-rejected by the top filter when not-count-aggs are null.
            let mut agg_null_bitset = FixedBitSet::with_capacity(agg.base.schema().len());
            for pos in null_agg_pos {
                agg_null_bitset.insert(pos);
            }

            // Shift the top project expressions to the right by apply_left schema len, because it is used to check null-rejected by the top filter.
            let apply_left_schema = apply_left.schema().len();
            let mut top_proj_all_null = true;
            let mut top_proj_null_bitset =
                FixedBitSet::with_capacity(top_project.base.schema().len() + apply_left_schema);
            for (i, expr) in top_proj_exprs.iter().enumerate() {
                if Strong::is_null(expr, agg_null_bitset.clone()) {
                    top_proj_null_bitset.insert(i + apply_left_schema);
                } else {
                    top_proj_all_null = false;
                }
            }

            let top_filter_any_null = top_filter
                .predicate()
                .conjunctions
                .iter()
                .any(|expr| Strong::is_null(expr, top_proj_null_bitset.clone()));
            let can_apply = top_proj_all_null || top_filter_any_null;

            if !can_apply {
                return None;
            }
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

        // Merge these expressions with LogicalApply into LogicalJoin.
        let on = apply_on.and(Condition {
            conjunctions: cor_eq_exprs,
        });

        let new_join = LogicalJoin::with_output_indices(
            apply_left,
            new_top_proj,
            join_type,
            on,
            (0..apply.schema().len()).collect(),
        )
        .into();

        if top_filter.predicate().always_true() {
            Some(new_join)
        } else {
            Some(top_filter.clone_with_input(new_join).into())
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

impl PullUpCorrelatedPredicateAggRule {
    pub fn create() -> BoxedRule {
        Box::new(PullUpCorrelatedPredicateAggRule {})
    }
}
