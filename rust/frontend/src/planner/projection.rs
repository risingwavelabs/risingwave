use risingwave_common::error::Result;

use super::Planner;
use crate::expr::ExprImpl;
use crate::optimizer::plan_node::LogicalProject;
use crate::optimizer::PlanRef;

impl Planner {
    pub(super) fn plan_projection(
        &mut self,
        input: PlanRef,
        projection: Vec<ExprImpl>,
    ) -> Result<PlanRef> {
        // TODO: support alias.
        let expr_alias = vec![None; projection.len()];
        Ok(LogicalProject::create(input, projection, expr_alias))
    }
}
