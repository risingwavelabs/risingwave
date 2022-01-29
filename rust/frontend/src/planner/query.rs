use risingwave_common::error::Result;

use crate::binder::BoundQuery;
use crate::optimizer::plan_node::PlanRef;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_query(&mut self, query: BoundQuery) -> Result<PlanRef> {
        self.plan_set_expr(query.body)
        // plan order and limit here
    }
}
