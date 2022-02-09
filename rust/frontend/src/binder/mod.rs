use risingwave_common::error::Result;
use risingwave_sqlparser::ast::Statement;

mod expr;
mod query;
mod set_expr;
mod statement;
mod values;

pub use query::BoundQuery;
pub use set_expr::BoundSetExpr;
pub use statement::BoundStatement;
pub use values::BoundValues;

pub struct Binder {}

impl Binder {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Binder {
        Binder {}
    }
    pub async fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt).await
    }
}
