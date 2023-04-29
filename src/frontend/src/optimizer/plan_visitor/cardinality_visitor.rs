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
use std::ops::{Mul, Sub};

use risingwave_pb::plan_common::JoinType;

use super::{DefaultBehavior, DefaultValue, PlanVisitor};
use crate::catalog::system_catalog::pg_catalog::PG_NAMESPACE_TABLE_NAME;
use crate::optimizer::plan_node::generic::Limit;
use crate::optimizer::plan_node::{self, PlanTreeNode, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanRef;
use crate::optimizer::property::Cardinality;

/// A visitor that computes the cardinality of a plan node.
pub struct CardinalityVisitor;

impl PlanVisitor<Cardinality> for CardinalityVisitor {
    type DefaultBehavior = impl DefaultBehavior<Cardinality>;

    fn default_behavior() -> Self::DefaultBehavior {
        // returns unknown cardinality for default behavior, which is always correct
        DefaultValue
    }

    fn visit_logical_values(&mut self, plan: &plan_node::LogicalValues) -> Cardinality {
        plan.rows().len().into()
    }

    fn visit_logical_agg(&mut self, plan: &plan_node::LogicalAgg) -> Cardinality {
        let input = self.visit(plan.input());

        if plan.group_key().is_empty() {
            input.min(1)
        } else {
            input.min(1..)
        }
    }

    fn visit_logical_limit(&mut self, plan: &plan_node::LogicalLimit) -> Cardinality {
        self.visit(plan.input()).min(plan.limit() as usize)
    }

    fn visit_logical_project(&mut self, plan: &plan_node::LogicalProject) -> Cardinality {
        self.visit(plan.input())
    }

    fn visit_logical_top_n(&mut self, plan: &plan_node::LogicalTopN) -> Cardinality {
        let input = self.visit(plan.input());

        match plan.limit_attr() {
            Limit::Simple(limit) => input.sub(plan.offset() as usize).min(limit as usize),
            Limit::WithTies(limit) => {
                assert_eq!(plan.offset(), 0, "ties with offset is not supported yet");
                input.min((limit as usize)..)
            }
        }
    }

    fn visit_logical_filter(&mut self, plan: &plan_node::LogicalFilter) -> Cardinality {
        let input = plan.input();
        let eq_set: HashSet<_> = plan
            .predicate()
            .collect_input_refs(input.schema().len())
            .ones()
            .collect();

        let mut unique_keys: Vec<HashSet<_>> = vec![input.logical_pk().iter().copied().collect()];

        // We don't have UNIQUE key now. So we hack here to support some complex queries on
        // system tables.
        if let Some(scan) = input.as_logical_scan()
            && scan.is_sys_table()
            && scan.table_name() == PG_NAMESPACE_TABLE_NAME
        {
            let nspname = scan
                .output_col_idx()
                .iter()
                .find(|i| scan.table_desc().columns[**i].name == "nspname")
                .unwrap();
            unique_keys.push([*nspname].into_iter().collect());
        }

        let input = self.visit(input);
        if unique_keys
            .iter()
            .any(|unique_key| eq_set.is_superset(unique_key))
        {
            input.min(0..=1)
        } else {
            input.min(0..)
        }
    }

    fn visit_logical_union(&mut self, plan: &plan_node::LogicalUnion) -> Cardinality {
        let all = plan
            .inputs()
            .into_iter()
            .map(|input| self.visit(input))
            .fold(Cardinality::default(), std::ops::Add::add);

        if plan.all() {
            all
        } else {
            all.min(1..)
        }
    }

    fn visit_logical_join(&mut self, plan: &plan_node::LogicalJoin) -> Cardinality {
        let left = self.visit(plan.left());
        let right = self.visit(plan.right());

        match plan.join_type() {
            JoinType::Unspecified => unreachable!(),

            // For each row from one side, we match `0..=(right.hi)` rows from the other side.
            JoinType::Inner => left.mul(right.min(0..)),

            // For each row from one side, we match `1..=max(right.hi, 1)` rows from the other side,
            // since we can at least match a `NULL` row.
            JoinType::LeftOuter => left.mul(right.max(1).min(1..)),
            JoinType::RightOuter => right.mul(left.max(1).min(1..)),

            // For each row in the result set, it must belong to the given side.
            JoinType::LeftSemi | JoinType::LeftAnti => left.min(0..),
            JoinType::RightSemi | JoinType::RightAnti => right.min(0..),

            // TODO: refine the cardinality of full outer join
            JoinType::FullOuter => Cardinality::default(),
        }
    }

    fn visit_logical_now(&mut self, _plan: &plan_node::LogicalNow) -> Cardinality {
        1.into()
    }

    fn visit_logical_expand(&mut self, plan: &plan_node::LogicalExpand) -> Cardinality {
        self.visit(plan.input()) * plan.column_subsets().len()
    }
}

#[easy_ext::ext(LogicalCardinalityExt)]
pub impl PlanRef {
    /// Returns `true` if the plan node is guaranteed to yield at most one row.
    fn max_one_row(&self) -> bool {
        CardinalityVisitor.visit(self.clone()).is_at_most(1)
    }

    /// Returns the number of rows the plan node is guaranteed to yield, if known exactly.
    fn row_count(&self) -> Option<usize> {
        CardinalityVisitor.visit(self.clone()).get_exact()
    }
}
