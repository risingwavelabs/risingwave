// Copyright 2024 RisingWave Labs
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

use super::{BoxedRule, Result, Rule};
use crate::optimizer::plan_node::generic::GenericPlanRef;
use crate::optimizer::plan_node::LogicalValues;
use crate::optimizer::{PlanRef, PlanTreeNode};

pub struct UnionInputValuesMergeRule {}
impl Rule for UnionInputValuesMergeRule {
    fn apply(&self, plan: PlanRef) -> Result<Option<PlanRef>> {
        let union = plan.as_logical_union();
        if union.is_none() {
            return Ok(None);
        }
        let union = union.unwrap();

        // !union.all() is already handled by [`UnionToDistinctRule`]
        if !union.all() {
            return Ok(None);
        }

        let mut rows = vec![];
        for v in union.inputs() {
            let v = v.as_logical_values();
            if v.is_none() {
                return Ok(None);
            }
            let v = v.unwrap();
            rows.extend_from_slice(v.rows());
        }
        Ok(Some(
            LogicalValues::new(rows, union.schema().clone(), union.ctx()).into(),
        ))
    }
}

impl UnionInputValuesMergeRule {
    pub fn create() -> BoxedRule {
        Box::new(UnionInputValuesMergeRule {})
    }
}
