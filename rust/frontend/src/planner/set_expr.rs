use risingwave_common::error::Result;

use crate::binder::BoundSetExpr;
use crate::optimizer::plan_node::PlanRef;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_set_expr(&mut self, set_expr: BoundSetExpr) -> Result<PlanRef> {
        match set_expr {
            BoundSetExpr::Select(s) => self.plan_select(*s),
            BoundSetExpr::Values(v) => self.plan_values(*v),
        }
    }
}
