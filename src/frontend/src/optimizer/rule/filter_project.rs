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
use crate::expr::{ExprImpl, ExprVisitor, InputRef};
use crate::utils::{Condition, Substitute};

/// Push what can be pused from [`LogicalFilter`] through [`LogicalProject`].
pub struct FilterProjectRule {}
impl Rule for FilterProjectRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter = plan.as_logical_filter()?;
        let input = filter.input();
        let project = input.as_logical_project()?;

        let mut visitor = Visitor {
            can_be_pushed: true,
            proj_exprs: project.exprs().clone(),
        };
        let (pushed_exprs, not_pushed_exprs) = filter
            .predicate()
            .clone()
            .into_iter()
            .partition_map(|expr| {
                visitor.visit_expr(&expr);
                if visitor.can_be_pushed {
                    Either::Left(expr)
                } else {
                    visitor.can_be_pushed = true;
                    Either::Right(expr)
                }
            });

        let pushed_predicates = Condition {
            conjunctions: pushed_exprs,
        };
        // convert the predicate to one that references the child of the project
        let mut subst = Substitute {
            mapping: visitor.proj_exprs,
        };
        let pushed_predicates = pushed_predicates.rewrite_expr(&mut subst);
        let input_filter = LogicalFilter::create(project.input(), pushed_predicates);

        let project = project.clone_with_input(input_filter);

        let not_pushed_predicates = Condition {
            conjunctions: not_pushed_exprs,
        };
        let filter = LogicalFilter::create(project.into(), not_pushed_predicates);

        Some(filter)
    }
}

impl FilterProjectRule {
    pub fn create() -> BoxedRule {
        Box::new(FilterProjectRule {})
    }
}
/// `Visitor` is used to check whether an expression can be pushed.
struct Visitor {
    pub can_be_pushed: bool,
    pub proj_exprs: Vec<ExprImpl>,
}
impl ExprVisitor for Visitor {
    fn visit_input_ref(&mut self, input_ref: &InputRef) {
        if !matches!(self.proj_exprs[input_ref.index()], ExprImpl::InputRef(_)) {
            self.can_be_pushed = false;
        }
    }
}
