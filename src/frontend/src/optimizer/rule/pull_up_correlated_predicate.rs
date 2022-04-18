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

use itertools::{Either, Itertools};

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::expr::{CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

/// This rule is for pattern: Apply->Project->Filter.
///
/// To unnest, we just pull predicates contain correlated variables in Filter into Apply, and
/// convert it into corresponding type of Join.
pub struct PullUpCorrelatedPredicate {}
impl Rule for PullUpCorrelatedPredicate {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;

        let right = apply.right();
        let project = right.as_logical_project()?;
        let (mut exprs, mut expr_alias, _) = project.clone().decompose();
        let begin = exprs.len();

        let input = project.input();
        let filter = input.as_logical_filter()?;

        // Split predicates in LogicalFilter into correlated expressions and uncorrelated
        // expressions.
        let (cor_exprs, uncor_exprs) =
            filter
                .predicate()
                .clone()
                .into_iter()
                .partition_map(|expr| {
                    let mut rewriter = Rewriter {
                        has_correlated_input_ref: false,
                        input_refs: vec![],
                        index: exprs.len() + apply.left().schema().fields().len(),
                    };
                    let rewritten_expr = rewriter.rewrite_expr(expr.clone());
                    if rewriter.has_correlated_input_ref {
                        // Append input_refs in expression which contains correlated_input_ref to
                        // `exprs` used to construct new LogicalProject.
                        exprs.extend(
                            rewriter
                                .input_refs
                                .drain(..)
                                .map(|input_ref| input_ref.into()),
                        );
                        Either::Left(rewritten_expr)
                    } else {
                        Either::Right(expr)
                    }
                });

        // TODO: remove LogicalFilter with always true condition.
        let filter = LogicalFilter::new(
            filter.input(),
            Condition {
                conjunctions: uncor_exprs,
            },
        );

        // Add columns involved in expressions removed from LogicalFilter to LogicalProject.
        let end = exprs.len();
        expr_alias.extend(vec![None; end - begin].into_iter());

        let project = LogicalProject::new(filter.into(), exprs, expr_alias);

        // Merge these expressions with LogicalApply into LogicalJoin.
        let on = Condition {
            conjunctions: cor_exprs,
        };
        Some(LogicalJoin::new(apply.left(), project.into(), apply.join_type(), on).into())
    }
}

/// Rewritter is responsible for rewriting `correlated_input_ref` to `input_ref` and shifting
/// `input_ref`.
struct Rewriter {
    // This flag is used to indicate whether the expression has correlated_input_ref.
    has_correlated_input_ref: bool,

    // All uncorrelated input_refs in the expression.
    pub input_refs: Vec<InputRef>,

    pub index: usize,
}

impl ExprRewriter for Rewriter {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        self.has_correlated_input_ref = true;

        // Convert correlated_input_ref to input_ref.
        // TODO: use LiftCorrelatedInputRef here.
        InputRef::new(
            correlated_input_ref.index(),
            correlated_input_ref.return_type(),
        )
        .into()
    }

    fn rewrite_input_ref(&mut self, input_ref: InputRef) -> ExprImpl {
        let data_type = input_ref.return_type();

        // It will be appended to exprs in LogicalProject, so its index remain the same.
        self.input_refs.push(input_ref);

        // Rewrite input_ref's index to its new location.
        let input_ref = InputRef::new(self.index, data_type);
        self.index += 1;
        input_ref.into()
    }
}

impl PullUpCorrelatedPredicate {
    pub fn create() -> BoxedRule {
        Box::new(PullUpCorrelatedPredicate {})
    }
}
