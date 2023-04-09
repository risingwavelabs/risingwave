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

/// Merge [`LogicalAgg`] <- [`LogicalProject`] to [`LogicalAgg`].
pub struct AggProjectMergeRule {}
impl Rule for AggProjectMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        let (mut agg_calls, mut agg_group_keys, input) = agg.clone().decompose();
        let proj = input.as_logical_project()?;

        // only apply when the input proj is all input-ref
        if !proj.is_all_inputref() {
            return None;
        }

        let proj_o2i = proj.o2i_col_mapping();
        let new_input = proj.input();

        // modify group key according to projection
        agg_group_keys
            .iter_mut()
            .for_each(|x| *x = proj_o2i.map(*x));

        // modify agg calls according to projection
        agg_calls
            .iter_mut()
            .for_each(|x| x.rewrite_input_index(proj_o2i.clone()));

        Some(LogicalAgg::new(agg_calls, agg_group_keys, new_input).into())
    }
}

impl AggProjectMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(AggProjectMergeRule {})
    }
}
