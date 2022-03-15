use risingwave_common::catalog::Schema;
use risingwave_common::error::Result;

use crate::binder::BoundSelect;
pub use crate::optimizer::plan_node::LogicalFilter;
use crate::optimizer::plan_node::{LogicalProject, LogicalValues, PlanRef};
use crate::planner::Planner;
impl Planner {
    pub(super) fn plan_select(&mut self, select: BoundSelect) -> Result<PlanRef> {
        // Plan FROM clause.
        let mut root = match select.from {
            None => self.create_dummy_values()?,
            Some(t) => self.plan_table_ref(t)?,
        };
        // Plan WHERE clause.
        root = match select.selection {
            None => root,
            Some(t) => LogicalFilter::create(root, t)?,
        };
        // Plan SELECT caluse.
        Ok(LogicalProject::create(
            root,
            select.select_items,
            select.aliases,
        ))
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
