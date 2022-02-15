use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use crate::binder::{Binder, BoundInsert, BoundQuery};

#[derive(Debug)]
pub enum BoundStatement {
    Insert(Box<BoundInsert>),
    Query(Box<BoundQuery>),
}

impl Binder {
    pub(super) fn bind_statement(&mut self, stmt: Statement) -> Result<BoundStatement> {
        match stmt {
            Statement::Insert {
                table_name,
                columns,
                source,
            } => Ok(BoundStatement::Insert(Box::new(
                self.bind_insert(table_name, columns, *source)?,
            ))),
            Statement::Query(q) => Ok(BoundStatement::Query(Box::new(self.bind_query(*q)?))),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", stmt)).into()),
        }
    }
}
