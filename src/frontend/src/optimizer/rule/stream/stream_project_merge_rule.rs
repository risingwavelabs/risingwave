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

use super::super::super::plan_node::*;
use crate::expr::ExprRewriter;
use crate::optimizer::plan_node::{LogicalProject, PlanTreeNodeUnary, StreamProject};
use crate::optimizer::{BoxedRule, PlanRef, Rule};
use crate::utils::Substitute;

/// Merge contiguous [`StreamProject`] nodes.
pub struct StreamProjectMergeRule {}
impl Rule for StreamProjectMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let outer_project = plan.as_stream_project()?;
        let input = outer_project.input();
        let inner_project = input.as_stream_project()?;

        let mut subst = Substitute {
            mapping: inner_project.exprs().clone(),
        };
        let exprs = outer_project
            .exprs()
            .iter()
            .cloned()
            .map(|expr| subst.rewrite_expr(expr))
            .collect();
        let logical_project = LogicalProject::new(inner_project.input(), exprs).into();
        Some(StreamProject::new(logical_project).into())
    }
}

impl StreamProjectMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(StreamProjectMergeRule {})
    }
}
