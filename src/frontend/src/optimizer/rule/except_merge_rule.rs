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

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalExcept, PlanTreeNode};
use crate::optimizer::PlanRef;

pub struct ExceptMergeRule {}
impl Rule for ExceptMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_except: &LogicalExcept = plan.as_logical_except()?;
        let top_all = top_except.all();
        let mut new_inputs = vec![];
        let mut has_merge = false;
        for input in top_except.inputs() {
            if let Some(bottom_except) = input.as_logical_except() && bottom_except.all() == top_all {
                new_inputs.extend(bottom_except.inputs());
                has_merge = true;
            } else {
                new_inputs.push(input);
            }
        }

        if has_merge {
            Some(top_except.clone_with_inputs(&new_inputs))
        } else {
            None
        }
    }
}

impl ExceptMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(ExceptMergeRule {})
    }
}
