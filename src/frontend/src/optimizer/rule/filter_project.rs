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
use itertools::{Either, Itertools};

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::expr::{ExprImpl, ExprRewriter};
use crate::utils::Condition;

/// Push what can be pused from [`LogicalFilter`] through [`LogicalProject`].
pub struct FilterProjectRule {}
impl Rule for FilterProjectRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter = plan.as_logical_filter()?;
        let input = filter.input();
        let project = input.as_logical_project()?;

        // Collect the indics of `InputRef` in LogicalProject.
        let input_ref_set: FixedBitSet = project
            .exprs()
            .iter()
            .enumerate()
            .filter_map(|(idx, expr)| {
                if matches!(expr, ExprImpl::InputRef(_)) {
                    Some(idx)
                } else {
                    None
                }
            })
            .collect();
        let proj_exprs_len = project.exprs().len();
        let (pushed_exprs, not_pushed_exprs): (Vec<ExprImpl>, Vec<ExprImpl>) = filter
            .predicate()
            .clone()
            .into_iter()
            .partition_map(|expr| {
                // If all the expressions referenced by the predicate are `InputRef`, then this
                // predicate can be pushed.
                if expr
                    .collect_input_refs(proj_exprs_len)
                    .is_subset(&input_ref_set)
                {
                    Either::Left(expr)
                } else {
                    Either::Right(expr)
                }
            });

        let pushed_predicate = Condition {
            conjunctions: {
                let mut proj_o2i_mapping = project.o2i_col_mapping();
                pushed_exprs
                    .into_iter()
                    .map(|expr| proj_o2i_mapping.rewrite_expr(expr))
                    .collect()
            },
        };
        let input_filter = LogicalFilter::create(project.input(), pushed_predicate);

        let project = project.clone_with_input(input_filter);

        let not_pushed_predicate = Condition {
            conjunctions: not_pushed_exprs,
        };
        let filter = LogicalFilter::create(project.into(), not_pushed_predicate);

        Some(filter)
    }
}

impl FilterProjectRule {
    pub fn create() -> BoxedRule {
        Box::new(FilterProjectRule {})
    }
}
