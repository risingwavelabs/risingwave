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
use super::stream_project_split_rule::{has_mixed_computations, is_trivial_expr};
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::{PlanTreeNodeUnary, StreamProject};
use crate::optimizer::rule::ProjectMergeRule;

/// Merge contiguous [`StreamProject`] nodes.
pub struct StreamProjectMergeRule {}
impl Rule<Stream> for StreamProjectMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let outer_project = plan.as_stream_project()?;
        let input = outer_project.input();
        let inner_project = input.as_stream_project()?;

        // Keep evaluated async results as column references. Inlining them into a conditional
        // or lambda can change which rows evaluate the call, even without duplicating its tree.
        if inner_project.exprs().iter().any(ExprImpl::is_async)
            && outer_project
                .exprs()
                .iter()
                .any(|expr| !is_trivial_expr(expr))
        {
            return None;
        }

        let mut core = outer_project.core().clone();
        core.exprs = ProjectMergeRule::merge_project_exprs(
            outer_project.exprs(),
            inner_project.exprs(),
            true,
        )?;
        if has_mixed_computations(&core.exprs) {
            return None;
        }
        core.input = inner_project.input();

        // If either of the projects has the hint, we should keep it.
        let noop_update_hint = outer_project.noop_update_hint() || inner_project.noop_update_hint();

        Some(
            StreamProject::new(core)
                .with_noop_update_hint(noop_update_hint)
                .into(),
        )
    }
}

impl StreamProjectMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(StreamProjectMergeRule {})
    }
}
