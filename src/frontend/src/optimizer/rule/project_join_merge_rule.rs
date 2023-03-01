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
pub struct ProjectJoinMergeRule {}

impl ProjectJoinMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}

impl Rule for ProjectJoinMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project = plan.as_logical_project()?;
        let input = project.input();
        let join = input.as_logical_join()?;
        let outer_output_indices = project.try_as_projection()?;
        let inner_output_indices = join.output_indices();

        // We cannot deal with repeated output indices in join
        if has_repeated_element(&outer_output_indices) {
            return None;
        }
        let output_indices: Vec<usize> = outer_output_indices
            .into_iter()
            .map(|i| inner_output_indices[i])
            .collect();
        Some(join.clone_with_output_indices(output_indices).into())
    }
}

pub(crate) fn has_repeated_element(slice: &[usize]) -> bool {
    (1..slice.len()).any(|i| slice[i..].contains(&slice[i - 1]))
}
