use risingwave_common::error::Result;

use crate::binder::BoundStatement;
use crate::optimizer::PlanRoot;

mod insert;
mod query;
mod select;
mod set_expr;
mod statement;
mod table_ref;
mod values;

/// `Planner` converts a bounded statement to a [`crate::optimizer::plan_node::PlanNode`] tree
pub struct Planner {}

impl Planner {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Planner {
        Planner {}
    }
    pub fn plan(&mut self, stmt: BoundStatement) -> Result<PlanRoot> {
        self.plan_statement(stmt)
    }
}
