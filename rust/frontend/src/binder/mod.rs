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

use crate::session::RwSession;

pub struct Binder<'session> {
    #[allow(dead_code)]
    session: &'session RwSession,
}

impl Binder<'_> {
    pub fn new(session: &RwSession) -> Binder {
        Binder { session }
    }
    pub async fn bind(&mut self, stmt: Statement) -> Result<BoundStatement> {
        self.bind_statement(stmt).await
    }
}
