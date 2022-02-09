use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use crate::binder::{Binder, BoundQuery};

#[derive(Debug)]
pub enum BoundStatement {
    Query(Box<BoundQuery>),
}

impl Binder<'_> {
    pub(super) async fn bind_statement(&mut self, stmt: Statement) -> Result<BoundStatement> {
        match stmt {
            Statement::Query(q) => Ok(BoundStatement::Query(Box::new(self.bind_query(*q).await?))),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", stmt)).into()),
        }
    }
}
