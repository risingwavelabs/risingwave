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
use std::ops::Sub;

use self::count::Count;
use super::{DefaultBehavior, DefaultValue, PlanVisitor};
use crate::catalog::system_catalog::pg_catalog::PG_NAMESPACE_TABLE_NAME;
use crate::optimizer::plan_node::generic::Limit;
use crate::optimizer::plan_node::{self, PlanTreeNode, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanRef;

pub mod count {
    use std::cmp::{min, Ordering};
    use std::ops::{Add, Mul, Sub};

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Hi(Option<usize>);

    impl PartialOrd for Hi {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Ord for Hi {
        fn cmp(&self, other: &Self) -> Ordering {
            match (self.0, other.0) {
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Greater,
                (Some(_), None) => Ordering::Less,
                (Some(lhs), Some(rhs)) => lhs.cmp(&rhs),
            }
        }
    }

    impl From<Option<usize>> for Hi {
        fn from(value: Option<usize>) -> Self {
            Self(value)
        }
    }

    impl From<usize> for Hi {
        fn from(value: usize) -> Self {
            Self(Some(value))
        }
    }

    #[derive(Clone, Copy, Debug)]
    pub struct Count {
        lo: usize,
        hi: Hi,
    }

    impl Default for Count {
        fn default() -> Self {
            Self {
                lo: 0,
                hi: None.into(),
            }
        }
    }

    impl Count {
        pub fn new(lo: usize, hi: impl Into<Hi>) -> Self {
            let hi: Hi = hi.into();
            if let Some(hi) = hi.0 {
                debug_assert!(hi >= lo);
            }

            Self { lo, hi }
        }

        pub fn lo(self) -> usize {
            self.lo
        }

        pub fn hi(self) -> Option<usize> {
            self.hi.0
        }

        pub fn exact(count: usize) -> Self {
            Self::new(count, Some(count))
        }

        pub fn at_least(count: usize) -> Self {
            Self::new(count, None)
        }

        pub fn min(self, rhs: Self) -> Self {
            Self::new(min(self.lo(), rhs.lo()), min(self.hi, rhs.hi))
        }

        pub fn limit_to(self, limit: usize) -> Self {
            self.min(Self::exact(limit))
        }

        pub fn as_low_as(self, limit: usize) -> Self {
            self.min(Self::at_least(limit))
        }
    }

    impl Add<Count> for Count {
        type Output = Self;

        fn add(self, rhs: Self) -> Self::Output {
            let lo = self.lo() + rhs.lo();
            let hi = if let (Some(lhs), Some(rhs)) = (self.hi(), rhs.hi()) {
                lhs.checked_add(rhs)
            } else {
                None
            };
            Self::new(lo, hi)
        }
    }

    impl Sub<usize> for Count {
        type Output = Self;

        fn sub(self, rhs: usize) -> Self::Output {
            let lo = self.lo().saturating_sub(rhs);
            let hi = self.hi().map(|hi| hi.saturating_sub(rhs));
            Self::new(lo, hi)
        }
    }

    impl Mul<usize> for Count {
        type Output = Self;

        fn mul(self, rhs: usize) -> Self::Output {
            let lo = self.lo().saturating_mul(rhs);
            let hi = self.hi().and_then(|hi| hi.checked_mul(rhs));
            Self::new(lo, hi)
        }
    }

    impl Count {
        pub fn get_exact(self) -> Option<usize> {
            self.hi().filter(|hi| *hi == self.lo())
        }

        pub fn is_at_most(self, count: usize) -> bool {
            self.hi().is_some_and(|hi| hi <= count)
        }
    }
}

pub struct CountRowsVisitor;

impl PlanVisitor<Count> for CountRowsVisitor {
    type DefaultBehavior = impl DefaultBehavior<Count>;

    fn default_behavior() -> Self::DefaultBehavior {
        DefaultValue
    }

    fn visit_logical_values(&mut self, plan: &plan_node::LogicalValues) -> Count {
        Count::exact(plan.rows().len())
    }

    fn visit_logical_agg(&mut self, plan: &plan_node::LogicalAgg) -> Count {
        let input = self.visit(plan.input());

        if plan.group_key().is_empty() {
            input.limit_to(1)
        } else {
            input.as_low_as(1)
        }
    }

    fn visit_logical_limit(&mut self, plan: &plan_node::LogicalLimit) -> Count {
        self.visit(plan.input()).limit_to(plan.limit() as usize)
    }

    fn visit_logical_project(&mut self, plan: &plan_node::LogicalProject) -> Count {
        self.visit(plan.input())
    }

    fn visit_logical_top_n(&mut self, plan: &plan_node::LogicalTopN) -> Count {
        let input = self.visit(plan.input());

        match plan.limit_attr() {
            Limit::Simple(limit) => input.sub(plan.offset() as usize).limit_to(limit as usize),
            Limit::WithTies(limit) => {
                assert_eq!(plan.offset(), 0, "ties with offset is not supported yet");
                input.as_low_as(limit as usize)
            }
        }
    }

    fn visit_logical_filter(&mut self, plan: &plan_node::LogicalFilter) -> Count {
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

    fn visit_logical_union(&mut self, plan: &plan_node::LogicalUnion) -> Count {
        if plan.all() {
            plan.inputs()
                .into_iter()
                .map(|input| self.visit(input))
                .reduce(std::ops::Add::add)
                .unwrap_or_default()
        } else {
            Count::default()
        }
    }

    fn visit_logical_now(&mut self, _plan: &plan_node::LogicalNow) -> Count {
        Count::exact(1)
    }

    fn visit_logical_expand(&mut self, plan: &plan_node::LogicalExpand) -> Count {
        self.visit(plan.input()) * plan.column_subsets().len()
    }
}

#[easy_ext::ext(LogicalCountRows)]
pub impl PlanRef {
    fn max_one_row(&self) -> bool {
        CountRowsVisitor.visit(self.clone()).is_at_most(1)
    }

    fn row_count(&self) -> Option<usize> {
        CountRowsVisitor.visit(self.clone()).get_exact()
    }
}
