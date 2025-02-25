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

use super::{BoxedRule, Rule};
use crate::optimizer::PlanRef;
use crate::optimizer::plan_node::{LogicalUnion, PlanTreeNode};

pub struct UnionMergeRule {}
impl Rule for UnionMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let top_union: &LogicalUnion = plan.as_logical_union()?;
        let top_all = top_union.all();
        let mut new_inputs = vec![];
        let mut has_merge = false;
        for input in top_union.inputs() {
            if let Some(bottom_union) = input.as_logical_union()
                && bottom_union.all() == top_all
            {
                new_inputs.extend(bottom_union.inputs());
                has_merge = true;
            } else {
                new_inputs.push(input);
            }
        }

        if has_merge {
            Some(top_union.clone_with_inputs(&new_inputs))
        } else {
            None
        }
    }
}

impl UnionMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(UnionMergeRule {})
    }
}
