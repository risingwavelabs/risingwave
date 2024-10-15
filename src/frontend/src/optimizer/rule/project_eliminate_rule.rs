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

use super::super::plan_node::*;
use super::{BoxedRule, Result, Rule};

/// Eliminate useless (identity) [`LogicalProject`] nodes.
pub struct ProjectEliminateRule {}
impl Rule for ProjectEliminateRule {
    fn apply(&self, plan: PlanRef) -> Result<Option<PlanRef>> {
        let project = plan.as_logical_project();
        if project.is_none() {
            return Ok(None);
        }
        let project = project.unwrap();

        if project.is_identity() {
            Ok(Some(project.input()))
        } else {
            Ok(None)
        }
    }
}

impl ProjectEliminateRule {
    pub fn create() -> BoxedRule {
        Box::new(ProjectEliminateRule {})
    }
}
