use risingwave_common::error::Result;

use crate::binder::BoundSelect;
use crate::expr::ExprImpl;
pub use crate::optimizer::plan_node::LogicalFilter;
use crate::optimizer::plan_node::{LogicalProject, PlanRef};
use crate::planner::Planner;
impl Planner {
    pub(super) fn plan_select(&mut self, select: BoundSelect) -> Result<PlanRef> {
        let mut root = match select.from {
            None => self.create_dummy_values()?,
            Some(t) => self.plan_table_ref(t)?,
        };
        root = match select.selection {
            None => root,
            Some(t) => self.plan_local_filter(root, t)?,
        };
        root = self.plan_projection(root, select.projection)?;
        // mut root with LogicalFilter and LogicalProject here
        Ok(root)
    }
    pub(super) fn plan_local_filter(
        &mut self,
        TableRef: PlanRef,
        predicate: ExprImpl,
    ) -> Result<PlanRef> {
        LogicalFilter::create(TableRef, predicate)
    }
    /// Helper to create a dummy node as child of LogicalProject.
    /// For example, `select 1+2, 3*4` will be `Project([1+2, 3+4]) - Values([[0]])`.
    ///
    /// Ideally `Values([[1+2, 3+4]])` is better if it can be done here cleanly rather than relying
    /// on optimizer.
    fn create_dummy_values(&self) -> Result<PlanRef> {
        todo!()
    }

    fn plan_projection(&mut self, input: PlanRef, projection: Vec<ExprImpl>) -> Result<PlanRef> {
        // TODO: support alias.
        let expr_alias = vec![None; projection.len()];
        Ok(LogicalProject::create(input, projection, expr_alias))
    }
}
