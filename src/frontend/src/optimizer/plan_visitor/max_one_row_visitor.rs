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

use std::collections::HashSet;

use crate::catalog::system_catalog::pg_catalog::PG_NAMESPACE_TABLE_NAME;
use crate::optimizer::plan_node::{
    LogicalAgg, LogicalApply, LogicalExpand, LogicalFilter, LogicalHopWindow, LogicalLimit,
    LogicalNow, LogicalProject, LogicalProjectSet, LogicalScan, LogicalTopN, LogicalUnion,
    LogicalValues, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::plan_visitor::PlanVisitor;
use crate::optimizer::PlanTreeNode;

pub struct MaxOneRowVisitor;

/// Return true if we can determine at most one row returns by the plan, otherwise false.
impl PlanVisitor<bool> for MaxOneRowVisitor {
    fn merge(a: bool, b: bool) -> bool {
        a & b
    }

    fn visit_logical_values(&mut self, plan: &LogicalValues) -> bool {
        plan.rows().len() <= 1
    }

    fn visit_logical_agg(&mut self, plan: &LogicalAgg) -> bool {
        plan.group_key().is_empty() || self.visit(plan.input())
    }

    fn visit_logical_limit(&mut self, plan: &LogicalLimit) -> bool {
        plan.limit() <= 1 || self.visit(plan.input())
    }

    fn visit_logical_now(&mut self, _plan: &LogicalNow) -> bool {
        true
    }

    fn visit_logical_project(&mut self, plan: &LogicalProject) -> bool {
        self.visit(plan.input())
    }

    fn visit_logical_top_n(&mut self, plan: &LogicalTopN) -> bool {
        (plan.limit() <= 1 && !plan.with_ties()) || self.visit(plan.input())
    }

    fn visit_logical_filter(&mut self, plan: &LogicalFilter) -> bool {
        let mut eq_set = HashSet::new();
        for pred in &plan.predicate().conjunctions {
            if let Some((input_ref, _value)) = pred.as_eq_const() {
                eq_set.insert(input_ref.index);
            } else if let Some((input_ref, _correlated_input_ref)) =
                pred.as_eq_correlated_input_ref()
            {
                eq_set.insert(input_ref.index);
            }
        }
        let input = plan.input();
        eq_set.is_superset(&input.logical_pk().iter().copied().collect())
            || {
                // We don't have UNIQUE key now. So we hack here to support some complex queries on
                // system tables.
                if let Some(scan) = input.as_logical_scan() && scan.is_sys_table() && scan.table_name() == PG_NAMESPACE_TABLE_NAME {
                    let nspname = scan.output_col_idx().iter().find(|i| scan.table_desc().columns[**i].name == "nspname").unwrap();
                    let unique_key = [
                        *nspname
                    ];
                    eq_set.is_superset(&unique_key.into_iter().collect())
                } else {
                    false
                }
            }
            || self.visit(input)
    }

    fn visit_logical_union(&mut self, _plan: &LogicalUnion) -> bool {
        false
    }

    fn visit_logical_expand(&mut self, plan: &LogicalExpand) -> bool {
        plan.column_subsets().len() == 1 && self.visit(plan.input())
    }

    fn visit_logical_project_set(&mut self, _plan: &LogicalProjectSet) -> bool {
        false
    }

    fn visit_logical_scan(&mut self, _plan: &LogicalScan) -> bool {
        false
    }

    fn visit_logical_hop_window(&mut self, _plan: &LogicalHopWindow) -> bool {
        false
    }
}

pub struct HasMaxOneRowApply();

impl PlanVisitor<bool> for HasMaxOneRowApply {
    fn merge(a: bool, b: bool) -> bool {
        a | b
    }

    fn visit_logical_apply(&mut self, plan: &LogicalApply) -> bool {
        plan.max_one_row() | self.visit(plan.left()) | self.visit(plan.right())
    }
}

pub struct CountRows;

impl PlanVisitor<Option<usize>> for CountRows {
    fn merge(_a: Option<usize>, _b: Option<usize>) -> Option<usize> {
        // Impossible to determine count e.g. after a join
        None
    }

    fn visit_logical_agg(&mut self, plan: &LogicalAgg) -> Option<usize> {
        if plan.group_key().is_empty() {
            Some(1)
        } else {
            None
        }
    }

    fn visit_logical_values(&mut self, plan: &LogicalValues) -> Option<usize> {
        Some(plan.rows().len())
    }

    fn visit_logical_project(&mut self, plan: &LogicalProject) -> Option<usize> {
        self.visit(plan.input())
    }

    fn visit_logical_union(&mut self, plan: &LogicalUnion) -> Option<usize> {
        if !plan.all() {
            // We cannot deal with deduplication
            return None;
        }
        plan.inputs()
            .iter()
            .fold(Some(0), |init, i| match (init, self.visit(i.clone())) {
                (None, _) => None,
                (_, None) => None,
                (Some(a), Some(b)) => Some(a + b),
            })
    }

    fn visit_logical_filter(&mut self, _plan: &LogicalFilter) -> Option<usize> {
        None
    }

    fn visit_logical_now(&mut self, _plan: &LogicalNow) -> Option<usize> {
        Some(1)
    }
}
