use risingwave_common::error::Result;

use crate::binder::BoundStatement;
use crate::optimizer::PlanRoot;
use crate::planner::Planner;

impl Planner {
    pub(super) fn plan_statement(&mut self, stmt: BoundStatement) -> Result<PlanRoot> {
        match stmt {
            BoundStatement::Insert(i) => self.plan_insert(*i),
            BoundStatement::Delete(d) => self.plan_delete(*d),
            BoundStatement::Query(q) => self.plan_query(*q),
        }
    }
}
