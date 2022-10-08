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

use super::{BoxedRule, Rule};
use crate::optimizer::plan_node::{LogicalApply, LogicalJoin};
use crate::optimizer::PlanRef;

/// Convert `LogicalApply` to `LogicalJoin` if it is uncorrelated and doesn't require max one row
/// restriction.
pub struct ApplyToJoinRule {}
impl Rule for ApplyToJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, _correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();

        if !max_one_row && correlated_indices.is_empty() {
            Some(LogicalJoin::new(left, right, join_type, on).into())
        } else {
            None
        }
    }
}

impl ApplyToJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyToJoinRule {})
    }
}
