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
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalApply, LogicalExpand, LogicalHopWindow, LogicalJoin, LogicalProjectSet,
    LogicalTopN, PlanTreeNodeUnary,
};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::optimizer::PlanRef;

/// Convert `LogicalApply` to `LogicalJoin`
pub struct ApplyToJoinRule {}
impl Rule for ApplyToJoinRule {
    fn apply(&self, plan: PlanRef) -> Option<PlanRef> {
        let apply: &LogicalApply = plan.as_logical_apply()?;
        let (left, right, on, join_type, _correlated_id, correlated_indices, max_one_row) =
            apply.clone().decompose();

        // Not a simple apply, bail out.
        if !correlated_indices.is_empty() {
            return None;
        }

        if max_one_row {
            // Try to eliminate max one row restoration.
            let mut max_one_row_visitor = MaxOneRowVisitor();
            if !max_one_row_visitor.visit(right.clone()) {
                return None;
            }
        }

        Some(LogicalJoin::new(left, right, join_type, on).into())
    }
}

impl ApplyToJoinRule {
    pub fn create() -> BoxedRule {
        Box::new(ApplyToJoinRule {})
    }
}

struct MaxOneRowVisitor();

/// Return true if we can determine at most one row returns by the plan, otherwise false.
impl PlanVisitor<bool> for MaxOneRowVisitor {
    fn merge(a: bool, b: bool) -> bool {
        a & b
    }

    fn visit_logical_agg(&mut self, plan: &LogicalAgg) -> bool {
        plan.group_key().is_empty()
    }

    fn visit_logical_top_n(&mut self, plan: &LogicalTopN) -> bool {
        plan.limit() == 1
    }

    fn visit_logical_expand(&mut self, plan: &LogicalExpand) -> bool {
        plan.column_subsets().len() == 1 && self.visit(plan.input())
    }

    fn visit_logical_project_set(&mut self, plan: &LogicalProjectSet) -> bool {
        plan.select_list().len() == 1 && self.visit(plan.input())
    }

    fn visit_logical_hop_window(&mut self, _plan: &LogicalHopWindow) -> bool {
        false
    }
}
