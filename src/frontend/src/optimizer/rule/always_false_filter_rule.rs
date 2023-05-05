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

use risingwave_common::types::ScalarImpl;

use super::Rule;
use crate::optimizer::plan_node::{LogicalFilter, LogicalValues};
use crate::PlanRef;

pub struct AlwaysFalseFilterRule;

impl Rule for AlwaysFalseFilterRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let filter: &LogicalFilter = plan.as_logical_filter()?;
        let always_false = filter
            .predicate()
            .conjunctions
            .iter()
            .filter_map(|e| e.eval_row_const().transpose().ok().flatten())
            .any(|s| s.unwrap_or(ScalarImpl::Bool(true)) == ScalarImpl::Bool(false));
        if always_false {
            Some(LogicalValues::create(
                vec![],
                filter.schema().clone(),
                filter.ctx(),
            ))
        } else {
            None
        }
    }
}

impl AlwaysFalseFilterRule {
    pub fn create() -> Box<dyn Rule> {
        Box::new(AlwaysFalseFilterRule)
    }
}
