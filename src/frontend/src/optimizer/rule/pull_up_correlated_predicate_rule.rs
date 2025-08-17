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

use itertools::{Either, Itertools};
use risingwave_common::util::column_index_mapping::ColIndexMapping;

use super::correlated_expr_rewriter::PredicateRewriter;
use super::prelude::{PlanRef, *};
use crate::expr::ExprRewriter;
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::*;
use crate::optimizer::plan_visitor::{PlanCorrelatedIdFinder, PlanVisitor};
use crate::utils::Condition;

/// Pull up correlated predicates from the right side of Apply to the `on` clause of Join.
///
/// Before:
///
/// ```text
///     LogicalApply
///    /            \
///  LHS          Project
///                 |
///               Filter [correlated_input_ref(yyy) = xxx]
///                 |
///               ...
/// ```
///
/// After:
///
/// ```text
///     LogicalJoin [yyy = xxx]
///    /            \
///  LHS          Project
///                 |
///               Filter
///                 |
///               ...
/// ```
///
/// This rule is for pattern: Apply->Project->Filter.
///
/// To unnest, we just pull predicates contain correlated variables in Filter into Apply, and
/// convert it into corresponding type of Join.
pub struct PullUpCorrelatedPredicateRule {}
impl Rule<Logical> for PullUpCorrelatedPredicateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let (apply_left, apply_right, apply_on, join_type, correlated_id, _, max_one_row) =
            apply.clone().decompose();

        if max_one_row {
            return None;
        }

        let project = if let Some(project) = apply_right.as_logical_project() {
            project.clone()
        } else {
            LogicalProject::with_mapping(
                apply_right.clone(),
                ColIndexMapping::identity(apply_right.schema().len()),
            )
        };
        let (mut proj_exprs, _) = project.clone().decompose();

        let input = project.input();
        let filter = input.as_logical_filter()?;

        let mut rewriter = PredicateRewriter::new(
            correlated_id,
            proj_exprs.len() + apply_left.schema().fields().len(),
        );
        // Split predicates in LogicalFilter into correlated expressions and uncorrelated
        // expressions.
        let (cor_exprs, uncor_exprs) =
            filter
                .predicate()
                .clone()
                .into_iter()
                .partition_map(|expr| {
                    if expr.has_correlated_input_ref_by_correlated_id(correlated_id) {
                        Either::Left(rewriter.rewrite_expr(expr))
                    } else {
                        Either::Right(expr)
                    }
                });
        // Append `InputRef`s in the predicate expression to be pulled to the project, so that they
        // are accessible by the expression after it is pulled.
        proj_exprs.extend(
            rewriter
                .input_refs
                .drain(..)
                .map(|input_ref| input_ref.into()),
        );

        let filter = LogicalFilter::create(
            filter.input(),
            Condition {
                conjunctions: uncor_exprs,
            },
        );

        let project: PlanRef = LogicalProject::new(filter, proj_exprs).into();

        // Check whether correlated_input_ref with same correlated_id exists for the join right
        // side. If yes, bail out and leave for general subquery unnesting to deal with
        let mut plan_correlated_id_finder = PlanCorrelatedIdFinder::default();
        plan_correlated_id_finder.visit(project.clone());
        if plan_correlated_id_finder.contains(&correlated_id) {
            return None;
        }

        // Merge these expressions with LogicalApply into LogicalJoin.
        let on = apply_on.and(Condition {
            conjunctions: cor_exprs,
        });
        Some(
            LogicalJoin::with_output_indices(
                apply_left,
                project,
                join_type,
                on,
                (0..apply.schema().len()).collect(),
            )
            .into(),
        )
    }
}

impl PullUpCorrelatedPredicateRule {
    pub fn create() -> BoxedRule {
        Box::new(PullUpCorrelatedPredicateRule {})
    }
}
