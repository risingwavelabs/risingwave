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

use super::super::plan_node::*;
use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::generic::GenericPlanRef;

/// Remove empty output [`LogicalAgg`] nodes.
pub struct EmptyAggRemoveRule {}
impl Rule for EmptyAggRemoveRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let agg = plan.as_logical_agg()?;
        if agg.agg_calls().is_empty() && agg.group_key().is_empty() {
            assert!(agg.schema().is_empty());
            // empty simple agg should produce a single empty row
            Some(LogicalValues::new(vec![vec![]], agg.schema().clone(), agg.ctx()).into())
        } else {
            None
        }
    }
}

impl EmptyAggRemoveRule {
    pub fn create() -> BoxedRule {
        Box::new(EmptyAggRemoveRule {})
    }
}
