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

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::expr::{CorrelatedInputRef, Expr, ExprImpl, ExprRewriter, InputRef};
use crate::optimizer::PlanRef;
use crate::utils::Condition;

/// This rule is for pattern: Apply->Project->Filter.
/// To unnest, we just pull predicates contain correlated variables in Filter into Apply, and
/// convert it into corresponding type of Join.
pub struct ApplyProjFilterRule {}
impl Rule for ApplyProjFilterRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;

        let right = apply.right();
        let project = right.as_logical_project()?;
        let (mut exprs, mut expr_alias) = project.clone().decompose();
        let begin = exprs.len();

        let input = project.input();
        let filter = input.as_logical_filter()?;

        // Remove expressions contain correlated_input_ref from LogicalFilter.
        let mut cor_exprs = vec![];
        let mut uncor_exprs = vec![];
        filter.predicate().clone().into_iter().for_each(|expr| {
            let mut split_expressions = SplitExpressions {
                flag: false,
                input_refs: vec![],
                index: exprs.len() + apply.left().schema().fields().len(),
            };
            let rewritten_expr = split_expressions.rewrite_expr(expr.clone());

            if split_expressions.flag {
                cor_exprs.push(rewritten_expr);
                // Append input_refs in expression which contains correlated_input_ref to `exprs`
                // used to construct new LogicalProject.
                exprs.extend(
                    split_expressions
                        .input_refs
                        .drain(..)
                        .map(|input_ref| input_ref.into()),
                );
            } else {
                uncor_exprs.push(expr);
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

struct SplitExpressions {
    // This flag is used to indicate whether the expression has correlated_input_ref.
    flag: bool,

    // All uncorrelated input_refs in the expression.
    pub input_refs: Vec<InputRef>,

    pub index: usize,
}

impl ExprRewriter for SplitExpressions {
    fn rewrite_correlated_input_ref(
        &mut self,
        correlated_input_ref: CorrelatedInputRef,
    ) -> ExprImpl {
        self.flag = true;

        // Convert correlated_input_ref to input_ref.
        // Need to change.
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

impl ApplyProjFilterRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyProjFilterRule {})
    }
}
