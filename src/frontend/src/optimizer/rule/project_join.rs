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

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
pub struct ProjectJoinRule {}

impl ProjectJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(Self {})
    }
}

impl Rule for ProjectJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let project = plan.as_logical_project()?;
        let input = project.input();
        let join = input.as_logical_join()?;
        if project.exprs().iter().all(|e| e.as_input_ref().is_some()) {
            let out_indices = project
                .exprs()
                .iter()
                .map(|e| e.as_input_ref().unwrap().index());
            let mapping = join.o2i_col_mapping();
            let new_output_indices = out_indices.map(|idx| mapping.map(idx)).collect();
            Some(join.clone_with_output_indices(new_output_indices).into())
        } else {
            None
        }
    }
}
