use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use crate::binder::BoundSelect;
use crate::expr::{AggCall, ExprVisitor};
pub use crate::optimizer::plan_node::LogicalFilter;
use crate::optimizer::plan_node::{LogicalAgg, LogicalProject, LogicalValues, PlanRef};
use crate::planner::Planner;
impl Planner {
    pub(super) fn plan_select(&mut self, select: BoundSelect) -> Result<PlanRef> {
        // Plan the FROM clause.
        let mut root = match select.from {
            None => self.create_dummy_values()?,
            Some(t) => self.plan_table_ref(t)?,
        };
        // Plan the WHERE clause.
        root = match select.where_clause {
            None => root,
            Some(t) => LogicalFilter::create(root, t)?,
        };
        // Plan the SELECT clause.
        let mut has_aggs = HasAggs { has_aggs: false };
        select
            .select_items
            .iter()
            .for_each(|expr| has_aggs.visit_expr(expr));
        if !select.group_by.is_empty() || has_aggs.has_aggs {
            LogicalAgg::create(select.select_items, select.aliases, select.group_by, root)
        } else {
            Ok(LogicalProject::create(
                root,
                select.select_items,
                select.aliases,
            ))
        }
    }

    /// Helper to create a dummy node as child of LogicalProject.
    /// For example, `select 1+2, 3*4` will be `Project([1+2, 3+4]) - Values([[]])`.
    fn create_dummy_values(&self) -> Result<PlanRef> {
        Ok(LogicalValues::create(
            vec![vec![]],
            Schema::default(),
            self.ctx.clone(),
        ))
    }
}

struct HasAggs {
    pub has_aggs: bool,
}

impl ExprVisitor for HasAggs {
    fn visit_agg_call(&mut self, _agg_call: &AggCall) {
        self.has_aggs = true;
    }
}
