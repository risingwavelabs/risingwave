use risingwave_common::error::Result;

use crate::binder::BoundStatement;
use crate::optimizer::PlanRoot;
use crate::session::QueryContextRef;

mod insert;
mod query;
mod select;
mod set_expr;
mod statement;
mod table_ref;
mod values;

/// `Planner` converts a bound statement to a [`crate::optimizer::plan_node::PlanNode`] tree
pub struct Planner {
    ctx: QueryContextRef,
}

impl Planner {
    #[allow(clippy::new_without_default)]
    pub fn new(ctx: QueryContextRef) -> Planner {
        Planner { ctx }
    }
    pub fn plan(&mut self, stmt: BoundStatement) -> Result<PlanRoot> {
        self.plan_statement(stmt)
    }
    pub fn ctx(&self) -> QueryContextRef {
        self.ctx.clone()
    }
}
