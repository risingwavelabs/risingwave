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
use crate::optimizer::max_one_row_visitor::MaxOneRowVisitor;
use crate::optimizer::plan_node::LogicalApply;
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::optimizer::PlanRef;

/// Eliminate max one row restriction from `LogicalApply`.
pub struct MaxOneRowEliminateRule {}
impl Rule for MaxOneRowEliminateRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();

        // Try to eliminate max one row.
        if max_one_row && MaxOneRowVisitor.visit(right.clone()) {
            Some(LogicalApply::create(
                left,
                right,
                join_type,
                on,
                correlated_id,
                correlated_indices,
                false,
            ))
        } else {
            None
        }
    }
}

impl MaxOneRowEliminateRule {
    pub fn create() -> BoxedRule {
        Box::new(MaxOneRowEliminateRule {})
    }
}
