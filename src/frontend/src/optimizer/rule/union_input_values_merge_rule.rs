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
use crate::optimizer::{PlanRef, PlanTreeNode};

pub struct UnionInputValuesMergeRule {}
impl Rule for UnionInputValuesMergeRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let union = plan.as_logical_union()?;
        // !union.all() is already handled by [`UnionToDistinctRule`]
        if !union.all() {
            return None;
        }

        let mut rows = vec![];
        for v in union.inputs() {
            rows.extend_from_slice(v.as_logical_values()?.rows());
        }
        Some(LogicalValues::new(rows, union.schema().clone(), union.ctx()).into())
    }
}

impl UnionInputValuesMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(UnionInputValuesMergeRule {})
    }
}
