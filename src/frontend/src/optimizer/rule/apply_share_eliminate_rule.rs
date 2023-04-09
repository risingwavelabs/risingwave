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
use crate::optimizer::plan_node::{LogicalApply, LogicalShare, PlanTreeNodeUnary};
use crate::optimizer::PlanRef;

/// Eliminate `LogicalShare` for `LogicalApply`.
pub struct ApplyShareEliminateRule {}
impl Rule for ApplyShareEliminateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();

        let share: &LogicalShare = right.as_logical_share()?;
        // Eliminate the share operator
        Some(LogicalApply::create(
            left,
            share.input(),
            join_type,
            on,
            correlated_id,
            correlated_indices,
            max_one_row,
        ))
    }
}

impl ApplyShareEliminateRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyShareEliminateRule {})
    }
}
