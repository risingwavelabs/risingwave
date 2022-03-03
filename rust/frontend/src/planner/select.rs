use risingwave_common::error::Result;

use crate::binder::BoundSelect;
use crate::optimizer::plan_node::PlanRef;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_select(&mut self, select: BoundSelect) -> Result<PlanRef> {
        let root = match select.from {
            None => self.create_dummy_values()?,
            Some(t) => self.plan_table_ref(t)?,
        };
        // mut root with LogicalFilter and LogicalProject here
        Ok(root)
    }

    /// Helper to create a dummy node as child of LogicalProject.
    /// For example, `select 1+2, 3*4` will be `Project([1+2, 3+4]) - Values([[0]])`.
    ///
    /// Ideally `Values([[1+2, 3+4]])` is better if it can be done here cleanly rather than relying
    /// on optimizer.
    fn create_dummy_values(&self) -> Result<PlanRef> {
        todo!()
    }
}
