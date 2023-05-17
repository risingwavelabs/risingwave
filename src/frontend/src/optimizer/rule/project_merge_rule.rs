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

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::expr::ExprRewriter;
use crate::utils::Substitute;

/// Merge contiguous [`LogicalProject`] nodes.
pub struct ProjectMergeRule {}
impl Rule for ProjectMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let outer_project = plan.as_logical_project()?;
        let mut input = outer_project.input();
        // just to check if the plan can be applied
        let _ = input.as_logical_project()?;
        let mut exprs = outer_project.exprs().clone();

        while let Some(inner_project) = input.as_logical_project() {
            let mut subst = Substitute {
                mapping: inner_project.exprs().clone(),
            };
            for expr in &mut exprs {
                *expr = subst.rewrite_expr(expr.clone());
            }

            input = inner_project.input();
        }

        Some(LogicalProject::new(input, exprs).into())
    }
}

impl ProjectMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(ProjectMergeRule {})
    }
}
