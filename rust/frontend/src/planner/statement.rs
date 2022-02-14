use risingwave_common::error::Result;

use crate::binder::BoundStatement;
use crate::optimizer::plan_node::PlanRef;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_statement(&mut self, stmt: BoundStatement) -> Result<PlanRef> {
        match stmt {
            BoundStatement::Query(q) => self.plan_query(*q),
            BoundStatement::Insert(i) => self.plan_insert(*i),
        }
    }
}
