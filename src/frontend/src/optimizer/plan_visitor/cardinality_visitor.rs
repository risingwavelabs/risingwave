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

use self::card::Cardinality;
use super::{DefaultBehavior, DefaultValue, PlanVisitor};
use crate::catalog::system_catalog::pg_catalog::PG_NAMESPACE_TABLE_NAME;
use crate::optimizer::plan_node::generic::Limit;
use crate::optimizer::plan_node::{self, PlanTreeNode, PlanTreeNodeBinary, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanRef;

pub mod card {
    use std::cmp::{min, Ordering};
    use std::ops::{Add, Mul, Sub};

    /// The upper bound of the [`Cardinality`].
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Hi {
        Limited(usize),
        Unlimited,
    }

    impl PartialOrd for Hi {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for Hi {
        fn cmp(&self, other: &Self) -> Ordering {
            match (self, other) {
                (Self::Unlimited, Self::Unlimited) => Ordering::Equal,
                (Self::Unlimited, Self::Limited(_)) => Ordering::Greater,
                (Self::Limited(_), Self::Unlimited) => Ordering::Less,
                (Self::Limited(lhs), Self::Limited(rhs)) => lhs.cmp(rhs),
            }
        }
    }

    impl From<Option<usize>> for Hi {
        fn from(value: Option<usize>) -> Self {
            value.map_or(Self::Unlimited, Self::Limited)
        }
    }

    impl From<usize> for Hi {
        fn from(value: usize) -> Self {
            Self::Limited(value)
        }
    }

    /// The cardinality of the output rows of a plan node. Bounds are inclusive.
    ///
    /// The default value is `0..`, i.e. the number of rows is unknown.
    // TODO: Make this the property of each plan node.
    #[derive(Clone, Copy, Debug)]
    pub struct Cardinality {
        lo: usize,
        hi: Hi,
    }

    impl Default for Cardinality {
        fn default() -> Self {
            Self {
                lo: 0,
                hi: None.into(),
            }
        }
    }

    impl Cardinality {
        /// Creates a new [`Cardinality`] with the given lower and upper bounds.
        pub fn new(lo: usize, hi: impl Into<Hi>) -> Self {
            let hi: Hi = hi.into();
            debug_assert!(hi >= Hi::from(lo));

            Self { lo, hi }
        }

        /// Returns the lower bound of the cardinality.
        pub fn lo(self) -> usize {
            self.lo
        }

        /// Returns the upper bound of the cardinality, `None` if the upper bound is unlimited.
        pub fn hi(self) -> Option<usize> {
            match self.hi {
                Hi::Limited(hi) => Some(hi),
                Hi::Unlimited => None,
            }
        }

        /// Creates a new [`Cardinality`] with exactly `count` rows.
        pub fn exact(count: usize) -> Self {
            Self::new(count, Some(count))
        }

        /// Creates a new [`Cardinality`] with at least `count` rows.
        pub fn at_least(count: usize) -> Self {
            Self::new(count, None)
        }

        /// Returns the minimum of the two cardinalities, where the lower and upper bounds are
        /// respectively the minimum of the lower and upper bounds of the two cardinalities.
        pub fn min(self, rhs: Self) -> Self {
            Self::new(min(self.lo(), rhs.lo()), min(self.hi, rhs.hi))
        }

        /// Returns the cardinality with both lower and upper bounds limited to `limit`.
        pub fn limit_to(self, limit: usize) -> Self {
            self.min(Self::exact(limit))
        }

        /// Returns the cardinality with the lower bound limited to `limit`.
        pub fn as_low_as(self, limit: usize) -> Self {
            self.min(Self::at_least(limit))
        }
    }

    impl Add<Cardinality> for Cardinality {
        type Output = Self;

        /// Returns the sum of the two cardinalities, where the lower and upper bounds are
        /// respectively the sum of the lower and upper bounds of the two cardinalities.
        fn add(self, rhs: Self) -> Self::Output {
            let lo = self.lo().saturating_add(rhs.lo());
            let hi = if let (Some(lhs), Some(rhs)) = (self.hi(), rhs.hi()) {
                lhs.checked_add(rhs)
            } else {
                None
            };
            Self::new(lo, hi)
        }
    }

    impl Sub<usize> for Cardinality {
        type Output = Self;

        /// Returns the cardinality with both lower and upper bounds subtracted by `rhs`.
        fn sub(self, rhs: usize) -> Self::Output {
            let lo = self.lo().saturating_sub(rhs);
            let hi = self.hi().map(|hi| hi.saturating_sub(rhs));
            Self::new(lo, hi)
        }
    }

    impl Mul<Cardinality> for Cardinality {
        type Output = Cardinality;

        /// Returns the product of the two cardinalities, where the lower and upper bounds are
        /// respectively the product of the lower and upper bounds of the two cardinalities.
        fn mul(self, rhs: Cardinality) -> Self::Output {
            let lo = self.lo().saturating_mul(rhs.lo());
            let hi = if let (Some(lhs), Some(rhs)) = (self.hi(), rhs.hi()) {
                lhs.checked_mul(rhs)
            } else {
                None
            };
            Self::new(lo, hi)
        }
    }

    impl Mul<usize> for Cardinality {
        type Output = Self;

        /// Returns the cardinality with both lower and upper bounds multiplied by `rhs`.
        fn mul(self, rhs: usize) -> Self::Output {
            let lo = self.lo().saturating_mul(rhs);
            let hi = self.hi().and_then(|hi| hi.checked_mul(rhs));
            Self::new(lo, hi)
        }
    }

    impl Cardinality {
        /// Returns the cardinality if it is exact, `None` otherwise.
        pub fn get_exact(self) -> Option<usize> {
            self.hi().filter(|hi| *hi == self.lo())
        }

        /// Returns `true` if the cardinality is at most `count` rows.
        pub fn is_at_most(self, count: usize) -> bool {
            self.hi().is_some_and(|hi| hi <= count)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_limit_to() {
            let c = Cardinality::new(5, None);
            let c1 = c.limit_to(3);
            assert_eq!(c1.lo(), 3);
            assert_eq!(c1.hi(), Some(3));
            assert_eq!(c1.get_exact(), Some(3));

            let c = Cardinality::new(5, None);
            let c1 = c.limit_to(10);
            assert_eq!(c1.lo(), 5);
            assert_eq!(c1.hi(), Some(10));
            assert_eq!(c1.get_exact(), None);
        }

        #[test]
        fn test_as_low_as() {
            let c = Cardinality::new(5, None);
            let c1 = c.as_low_as(3);
            assert_eq!(c1.lo(), 3);
            assert_eq!(c1.hi(), None);

            let c = Cardinality::new(5, 10);
            let c1 = c.as_low_as(3);
            assert_eq!(c1.lo(), 3);
            assert_eq!(c1.hi(), Some(10));
        }

        #[test]
        fn test_ops() {
            // Sub
            let c = Cardinality::new(5, 10);
            let c1 = c.sub(7);
            assert_eq!(c1.lo(), 0);
            assert_eq!(c1.hi(), Some(3));

            // Add
            let c = Cardinality::new(5, 10);
            let c1 = Cardinality::new(10, None);
            let c2 = c + c1;
            assert_eq!(c2.lo(), 15);
            assert_eq!(c2.hi(), None);

            let c = Cardinality::new(5, usize::MAX - 1);
            let c1 = Cardinality::exact(2);
            let c2 = c + c1;
            assert_eq!(c2.lo(), 7);
            assert_eq!(c2.hi(), None);

            // Mul
            let c = Cardinality::new(5, usize::MAX - 1);
            let c1 = c * 2;
            assert_eq!(c1.lo(), 10);
            assert_eq!(c1.hi(), None);
        }
    }
}

/// A visitor that computes the cardinality of a plan node.
pub struct CardinalityVisitor;

impl PlanVisitor<Cardinality> for CardinalityVisitor {
    type DefaultBehavior = impl DefaultBehavior<Cardinality>;

    fn default_behavior() -> Self::DefaultBehavior {
        // returns unknown cardinality for default behavior, which is always correct
        DefaultValue
    }

    fn visit_logical_values(&mut self, plan: &plan_node::LogicalValues) -> Cardinality {
        Cardinality::exact(plan.rows().len())
    }

    fn visit_logical_agg(&mut self, plan: &plan_node::LogicalAgg) -> Cardinality {
        let input = self.visit(plan.input());

        if plan.group_key().is_empty() {
            input.limit_to(1)
        } else {
            input.as_low_as(1)
        }
    }

    fn visit_logical_limit(&mut self, plan: &plan_node::LogicalLimit) -> Cardinality {
        self.visit(plan.input()).limit_to(plan.limit() as usize)
    }

    fn visit_logical_project(&mut self, plan: &plan_node::LogicalProject) -> Cardinality {
        self.visit(plan.input())
    }

    fn visit_logical_top_n(&mut self, plan: &plan_node::LogicalTopN) -> Cardinality {
        let input = self.visit(plan.input());

        match plan.limit_attr() {
            Limit::Simple(limit) => input.sub(plan.offset() as usize).limit_to(limit as usize),
            Limit::WithTies(limit) => {
                assert_eq!(plan.offset(), 0, "ties with offset is not supported yet");
                input.as_low_as(limit as usize)
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
        if let Some(scan) = input.as_logical_scan() && scan.is_sys_table() && scan.table_name() == PG_NAMESPACE_TABLE_NAME {
            let nspname = scan.output_col_idx().iter().find(|i| scan.table_desc().columns[**i].name == "nspname").unwrap();
            unique_keys.push([*nspname].into_iter().collect());
        }

        let input = self.visit(input);
        if unique_keys
            .iter()
            .any(|unique_key| eq_set.is_superset(unique_key))
        {
            input.limit_to(1).as_low_as(0)
        } else {
            input.as_low_as(0)
        }
    }

    fn visit_logical_union(&mut self, plan: &plan_node::LogicalUnion) -> Cardinality {
        if plan.all() {
            plan.inputs()
                .into_iter()
                .map(|input| self.visit(input))
                .reduce(std::ops::Add::add)
                .unwrap_or_default()
        } else {
            Cardinality::default()
        }
    }

    fn visit_logical_join(&mut self, plan: &plan_node::LogicalJoin) -> Cardinality {
        let left = self.visit(plan.left());
        let right = self.visit(plan.right());

        match plan.join_type() {
            JoinType::Unspecified => unreachable!(),

            JoinType::Inner => left.mul(right).as_low_as(0),

            JoinType::LeftOuter | JoinType::LeftSemi | JoinType::LeftAnti => left.as_low_as(0),
            JoinType::RightOuter | JoinType::RightSemi | JoinType::RightAnti => right.as_low_as(0),

            // TODO: refine the cardinality of full outer join
            JoinType::FullOuter => Cardinality::default(),
        }
    }

    fn visit_logical_now(&mut self, _plan: &plan_node::LogicalNow) -> Cardinality {
        Cardinality::exact(1)
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
