use risingwave_common::error::Result;

use crate::binder::BoundStatement;
use crate::optimizer::PlanRoot;
use crate::session::QueryContextRef;

mod delete;
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
    pub fn new(ctx: QueryContextRef) -> Planner {
        Planner { ctx }
    }

    /// Plan a [`BoundStatement`]. Need to bind a statement before plan.
    pub fn plan(&mut self, stmt: BoundStatement) -> Result<PlanRoot> {
        self.plan_statement(stmt)
    }

    pub fn ctx(&self) -> QueryContextRef {
        self.ctx.clone()
    }
}
