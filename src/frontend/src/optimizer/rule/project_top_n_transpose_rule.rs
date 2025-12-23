// Copyright 2025 RisingWave Labs
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

use super::prelude::{PlanRef, *};
use crate::optimizer::plan_node::{LogicalProject, LogicalTopN, PlanTreeNodeUnary};
use crate::utils::ColIndexMappingRewriteExt;

/// Transpose `LogicalProject` and `LogicalTopN` when the project is a pure projection and
/// preserves all columns required by TopN order and group keys.
pub struct ProjectTopNTransposeRule {}

impl Rule<Logical> for ProjectTopNTransposeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project: &LogicalProject = plan.as_logical_project()?;
        let top_n: LogicalTopN = project.input().as_logical_top_n()?.to_owned();
        project.try_as_projection()?;

        let mapping = project.i2o_col_mapping();
        let new_order = mapping.rewrite_required_order(top_n.topn_order())?;
        let new_group_key = mapping.rewrite_dist_key(top_n.group_key())?;

        let limit_attr = top_n.limit_attr();
        let new_project = project.clone_with_input(top_n.input());
        let new_top_n = LogicalTopN::new(
            new_project.into(),
            limit_attr.limit(),
            top_n.offset(),
            limit_attr.with_ties(),
            new_order,
            new_group_key,
        );
        Some(new_top_n.into())
    }
}

impl ProjectTopNTransposeRule {
    pub fn create() -> BoxedRule {
        Box::new(ProjectTopNTransposeRule {})
    }
}
