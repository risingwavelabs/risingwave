// Copyright 2025 RisingWave Labs
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
use crate::optimizer::plan_node::generic::TopNLimit;
use crate::optimizer::plan_node::{
    self, PlanNode, PlanTreeNode, PlanTreeNodeBinary, PlanTreeNodeUnary,
};
use crate::optimizer::plan_visitor::PlanRef;
use crate::optimizer::property::Cardinality;

/// A visitor that computes the cardinality of a plan node.
pub struct CardinalityVisitor;

impl CardinalityVisitor {
    /// Used for `Filter` and `Scan` with predicate.
    fn visit_predicate(
        input: &dyn PlanNode,
        input_card: Cardinality,
        eq_set: HashSet<usize>,
    ) -> Cardinality {
        // TODO: there could be more unique keys than the stream key after we support it.
        let unique_keys: Vec<HashSet<_>> = input
            .stream_key()
            .into_iter()
            .map(|s| s.iter().copied().collect())
            .collect();

        if unique_keys
            .iter()
            .any(|unique_key| eq_set.is_superset(unique_key))
        {
            input_card.min(0..=1)
        } else {
            input_card.min(0..)
        }
    }
}

impl PlanVisitor for CardinalityVisitor {
    type Result = Cardinality;

    type DefaultBehavior = impl DefaultBehavior<Self::Result>;

    fn default_behavior() -> Self::DefaultBehavior {
        // returns unknown cardinality for default behavior, which is always correct
        DefaultValue
    }

    fn visit_logical_values(&mut self, plan: &plan_node::LogicalValues) -> Cardinality {
        plan.rows().len().into()
    }

    fn visit_logical_share(&mut self, plan: &plan_node::LogicalShare) -> Cardinality {
        self.visit(plan.input())
    }

    fn visit_logical_dedup(&mut self, plan: &plan_node::LogicalDedup) -> Cardinality {
        let input = self.visit(plan.input());
        if plan.dedup_cols().is_empty() {
            input.min(1)
        } else {
            input
        }
    }

    fn visit_logical_over_window(&mut self, plan: &super::LogicalOverWindow) -> Self::Result {
        self.visit(plan.input())
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

    fn visit_logical_max_one_row(&mut self, plan: &plan_node::LogicalMaxOneRow) -> Cardinality {
        self.visit(plan.input()).min(1)
    }

    fn visit_logical_project(&mut self, plan: &plan_node::LogicalProject) -> Cardinality {
        self.visit(plan.input())
    }

    fn visit_logical_top_n(&mut self, plan: &plan_node::LogicalTopN) -> Cardinality {
        let input = self.visit(plan.input());

        let each_group = match plan.limit_attr() {
            TopNLimit::Simple(limit) => input.sub(plan.offset() as usize).min(limit as usize),
            TopNLimit::WithTies(limit) => {
                assert_eq!(plan.offset(), 0, "ties with offset is not supported yet");
                input.min((limit as usize)..)
            }
        };

        if plan.group_key().is_empty() {
            each_group
        } else {
            let group_number = input.min(1..);
            each_group
                .mul(group_number)
                // the output cardinality will never be more than the input, thus `.min(input)`
                .min(input)
        }
    }

    fn visit_logical_filter(&mut self, plan: &plan_node::LogicalFilter) -> Cardinality {
        let eq_set = plan
            .predicate()
            .collect_input_refs(plan.input().schema().len())
            .ones()
            .collect();
        Self::visit_predicate(&*plan.input(), self.visit(plan.input()), eq_set)
    }

    fn visit_logical_scan(&mut self, plan: &plan_node::LogicalScan) -> Cardinality {
        let eq_set = plan
            .predicate()
            .collect_input_refs(plan.table_desc().columns.len())
            .ones()
            .collect();
        Self::visit_predicate(plan, plan.table_cardinality(), eq_set)
    }

    fn visit_logical_union(&mut self, plan: &plan_node::LogicalUnion) -> Cardinality {
        let all = plan
            .inputs()
            .into_iter()
            .map(|input| self.visit(input))
            .fold(Cardinality::unknown(), std::ops::Add::add);

        if plan.all() { all } else { all.min(1..) }
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
            JoinType::FullOuter => {
                if left.is_at_most(1) && right.is_at_most(1) {
                    Cardinality::new(0, 1)
                } else {
                    Cardinality::unknown()
                }
            }

            // For each row from one side, we match `0..=1` rows from the other side.
            JoinType::AsofInner => left.mul(right.min(0..=1)),
            // For each row from left side, we match exactly 1 row from the right side or a `NULL` row`.
            JoinType::AsofLeftOuter => left,
        }
    }

    fn visit_logical_now(&mut self, plan: &plan_node::LogicalNow) -> Cardinality {
        if plan.max_one_row() {
            1.into()
        } else {
            Cardinality::unknown()
        }
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
