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

use crate::optimizer::plan_node::{
    LogicalAgg, LogicalApply, LogicalExpand, LogicalHopWindow, LogicalLimit, LogicalProjectSet,
    LogicalTopN, LogicalUnion, LogicalValues, PlanTreeNodeUnary,
};
use crate::optimizer::plan_visitor::PlanVisitor;

pub struct MaxOneRowVisitor();

/// Return true if we can determine at most one row returns by the plan, otherwise false.
impl PlanVisitor<bool> for MaxOneRowVisitor {
    fn merge(a: bool, b: bool) -> bool {
        a & b
    }

    fn visit_logical_values(&mut self, plan: &LogicalValues) -> bool {
        plan.rows().len() <= 1
    }

    fn visit_logical_agg(&mut self, plan: &LogicalAgg) -> bool {
        plan.group_key().is_empty()
    }

    fn visit_logical_limit(&mut self, plan: &LogicalLimit) -> bool {
        plan.limit() <= 1
    }

    fn visit_logical_top_n(&mut self, plan: &LogicalTopN) -> bool {
        plan.limit() <= 1
    }

    fn visit_logical_union(&mut self, _plan: &LogicalUnion) -> bool {
        false
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

pub struct HasMaxOneRowUncorrelatedApply();

impl PlanVisitor<bool> for HasMaxOneRowUncorrelatedApply {
    fn merge(a: bool, b: bool) -> bool {
        a | b
    }

    fn visit_logical_apply(&mut self, plan: &LogicalApply) -> bool {
        plan.correlated_indices().is_empty() && plan.max_one_row()
    }
}

pub struct HasMaxOneRowApply();

impl PlanVisitor<bool> for HasMaxOneRowApply {
    fn merge(a: bool, b: bool) -> bool {
        a | b
    }

    fn visit_logical_apply(&mut self, plan: &LogicalApply) -> bool {
        plan.max_one_row()
    }
}
