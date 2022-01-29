use risingwave_common::error::Result;

use crate::binder::BoundStatement;
use crate::optimizer::plan_node::PlanRef;

mod query;
mod set_expr;
mod statement;
mod values;

pub struct Planner {}

impl Planner {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Planner {
        Planner {}
    }
    pub fn plan(&mut self, stmt: BoundStatement) -> Result<PlanRef> {
        self.plan_statement(stmt)
    }
}
