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
use crate::optimizer::plan_node::{LogicalLimit, LogicalValues, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;

pub struct LimitEliminateRule {}
impl Rule for LimitEliminateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let limit: &LogicalLimit = plan.as_logical_limit()?;
        let limit_input = limit.input();
        let values: &LogicalValues = limit_input.as_logical_values()?;
        if limit.offset() == 0 && limit.limit() >= values.rows().len() as u64 {
            Some(values.clone().into())
        } else {
            None
        }
    }
}

impl LimitEliminateRule {
    pub fn create() -> BoxedRule {
        Box::new(LimitEliminateRule {})
    }
}
