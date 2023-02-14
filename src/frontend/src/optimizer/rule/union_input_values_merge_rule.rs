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
use crate::optimizer::plan_node::LogicalValues;
use crate::optimizer::PlanRef;
use crate::optimizer::PlanTreeNode;

pub struct UnionInputValuesMergeRule {}
impl Rule for UnionInputValuesMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let union = plan.as_logical_union()?;
        // !union.all() is already handled by [`UnionToDistinctRule`]
        if union.inputs().iter().all(|p| p.as_logical_values().is_some()) && union.all() {
            let mut rows = vec![];
            union.inputs().iter().for_each(|v| {
                rows.extend_from_slice(v.as_logical_values().unwrap().rows());
            });
            Some(LogicalValues::new(rows, union.schema().clone(), union.ctx()).into())
        } else {
            None
        }
    }
}

impl UnionInputValuesMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(UnionInputValuesMergeRule {})
    }
}
