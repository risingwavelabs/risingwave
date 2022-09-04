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

use super::Rule;
use crate::optimizer::plan_node::PlanTreeNodeUnary;
use crate::PlanRef;

/// Transforms the following pattern to group TopN
///
/// ```sql
/// SELECT .. from
///   (SELECT .., ROW_NUMBER() OVER(PARTITION BY .. ORDER BY ..) rank from ..)
/// WHERE rank < ..;
/// ```
pub struct WindowAggToTopNRule;

impl WindowAggToTopNRule {
    pub fn create() -> Box<dyn Rule> {
        Box::new(WindowAggToTopNRule)
    }
}

impl Rule for WindowAggToTopNRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project_upper = plan.as_logical_project()?;
        let plan = project_upper.input();
        let filter = plan.as_logical_filter()?;
        let plan = filter.input();
        let project_lower = plan.as_logical_project()?;
        let plan = project_lower.input();
        let window_agg = plan.as_logical_window_agg()?;
        let window_agg_len = window_agg.schema().len();
        let input = window_agg.input();

        if project_upper.exprs().iter().any(|expr| {
            expr.collect_input_refs(window_agg_len)
                .contains(window_agg_len - 1)
        }) {
            // TopN with ranking output is not supported yet.
            return None;
        }

        todo!()
    }
}
