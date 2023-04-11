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

/// Eliminate useless (identity) [`LogicalProject`] nodes.
pub struct ProjectEliminateRule {}
impl Rule for ProjectEliminateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project = plan.as_logical_project()?;
        let dml = |input: PlanRef| {
            input.as_logical_insert().is_some()
                || input.as_logical_delete().is_some()
                || input.as_logical_update().is_some()
        };
        if project.is_identity() && !dml(project.input()) {
            Some(project.input())
        } else {
            None
        }
    }
}

impl ProjectEliminateRule {
    pub fn create() -> BoxedRule {
        Box::new(ProjectEliminateRule {})
    }
}
