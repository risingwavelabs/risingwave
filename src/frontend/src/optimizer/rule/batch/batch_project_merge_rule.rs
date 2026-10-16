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

use super::prelude::*;
use crate::optimizer::plan_node::{BatchProject, PlanTreeNodeUnary};
use crate::optimizer::rule::ProjectMergeRule;

/// Merge contiguous [`BatchProject`] nodes.
pub struct BatchProjectMergeRule {}
impl Rule<Batch> for BatchProjectMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let outer_project = plan.as_batch_project()?;
        let input = outer_project.input();
        let inner_project = input.as_batch_project()?;

        let mut core = outer_project.core().clone();
        core.exprs = ProjectMergeRule::merge_project_exprs(
            outer_project.exprs(),
            inner_project.exprs(),
            true,
        )?;
        core.input = inner_project.input();

        Some(BatchProject::new(core).into())
    }
}

impl BatchProjectMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(BatchProjectMergeRule {})
    }
}
