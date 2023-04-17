use std::collections::HashSet;

use self::count::Count;
use super::PlanVisitor;
use crate::catalog::system_catalog::pg_catalog::PG_NAMESPACE_TABLE_NAME;
use crate::optimizer::plan_node::generic::Limit;
use crate::optimizer::plan_node::{self, PlanTreeNode, PlanTreeNodeUnary};
use crate::optimizer::plan_visitor::PlanRef;

pub mod count {
    use std::cmp::{min, Ordering};

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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

    #[derive(Clone, Copy, Debug, Default)]
    pub struct Count {
        lo: usize,
        hi: Hi,
        _private: (),
    }

    impl Count {
        pub fn new(lo: usize, hi: impl Into<Hi>) -> Self {
            let hi: Hi = hi.into();
            if let Some(hi) = hi.0 {
                assert!(hi >= lo);
            }

            Self {
                lo,
                hi,
                _private: (),
            }
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

        pub fn at_most(count: usize) -> Self {
            Self::new(0, Some(count))
        }

        pub fn limit_to(self, limit: usize) -> Self {
            Self::new(min(limit, self.lo()), min(Hi(Some(limit)), self.hi))
        }

        pub fn add(self, other: Self) -> Self {
            let lo = self.lo() + other.lo();
            let hi = if let (Some(lhs), Some(rhs)) = (self.hi(), other.hi()) {
                lhs.checked_add(rhs)
            } else {
                None
            };
            Self::new(lo, hi)
        }

        pub fn multiply(self, m: usize) -> Self {
            let lo = self.lo().saturating_mul(m);
            let hi = self.hi().and_then(|hi| hi.checked_mul(m));
            Self::new(lo, hi)
        }
    }
}

pub struct CountRowsVisitor;

impl PlanVisitor<Count> for CountRowsVisitor {
    fn merge(_a: Count, _b: Count) -> Count {
        Count::default()
    }

    fn visit_logical_values(&mut self, plan: &plan_node::LogicalValues) -> Count {
        Count::exact(plan.rows().len())
    }

    fn visit_logical_agg(&mut self, plan: &plan_node::LogicalAgg) -> Count {
        if plan.group_key().is_empty() {
            Count::at_most(1)
        } else {
            Count::new(0, self.visit(plan.input()).hi())
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

        // TODO: further constrain it with `offset`
        if let Limit::Simple(limit) = plan.limit_attr() {
            input.limit_to(limit as usize)
        } else {
            input
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

        if unique_keys
            .iter()
            .any(|unique_key| eq_set.is_superset(unique_key))
        {
            Count::at_most(1)
        } else {
            self.visit(input)
        }
    }

    fn visit_logical_union(&mut self, plan: &plan_node::LogicalUnion) -> Count {
        if plan.all() {
            plan.inputs()
                .into_iter()
                .map(|input| self.visit(input))
                .reduce(Count::add)
                .unwrap_or_default()
        } else {
            Count::default()
        }
    }

    fn visit_logical_now(&mut self, _plan: &plan_node::LogicalNow) -> Count {
        Count::exact(1)
    }

    fn visit_logical_expand(&mut self, plan: &plan_node::LogicalExpand) -> Count {
        self.visit(plan.input())
            .multiply(plan.column_subsets().len())
    }
}

#[easy_ext::ext(LogicalCountRows)]
pub impl PlanRef {
    fn max_one_row(&self) -> bool {
        CountRowsVisitor.visit(self.clone()).hi() <= Some(1)
    }

    fn row_count(&self) -> Option<usize> {
        let count = CountRowsVisitor.visit(self.clone());
        count.hi().filter(|hi| *hi == count.lo())
    }
}
