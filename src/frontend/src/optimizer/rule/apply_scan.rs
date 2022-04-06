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
use crate::utils::Condition;

pub struct ApplyScanRule {}
impl Rule for ApplyScanRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply = plan.as_logical_apply()?;
        let right = apply.right();
        let _scan = right.as_logical_scan()?;
        Some(
            LogicalJoin::new(
                apply.left(),
                apply.right(),
                apply.join_type(),
                Condition::true_cond(),
            )
            .into(),
        )
    }
}

impl ApplyScanRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyScanRule {})
    }
}
