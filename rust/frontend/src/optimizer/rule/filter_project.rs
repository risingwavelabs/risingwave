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
//
use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::utils::Substitute;

/// Pushes a [`LogicalFilter`] past a [`LogicalProject`].
pub struct FilterProjectRule {}
impl Rule for FilterProjectRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter = plan.as_logical_filter()?;
        let input = filter.input();
        let project = input.as_logical_project()?;

        // convert the predicate to one that references the child of the project
        let mut subst = Substitute {
            mapping: project.exprs().clone(),
        };
        let predicate = filter.predicate().clone().rewrite_expr(&mut subst);

        let input = project.input();
        let pushed_filter = LogicalFilter::new(input, predicate);
        Some(project.clone_with_input(pushed_filter.into()).into())
    }
}

impl FilterProjectRule {
    pub fn create() -> BoxedRule {
        Box::new(FilterProjectRule {})
    }
}
