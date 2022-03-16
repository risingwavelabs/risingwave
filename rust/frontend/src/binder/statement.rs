use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Statement;

use super::delete::BoundDelete;
use crate::binder::{Binder, BoundInsert, BoundQuery};

#[derive(Debug)]
pub enum BoundStatement {
    Insert(Box<BoundInsert>),
    Delete(Box<BoundDelete>),
    Query(Box<BoundQuery>),
    CreateView(Box<BoundQuery>),
}

impl Binder {
    pub(super) fn bind_statement(&mut self, stmt: Statement) -> Result<BoundStatement> {
        match stmt {
            Statement::Insert {
                table_name,
                columns,
                source,
            } => Ok(BoundStatement::Insert(
                self.bind_insert(table_name, columns, *source)?.into(),
            )),

            Statement::Delete {
                table_name,
                selection,
            } => Ok(BoundStatement::Delete(
                self.bind_delete(table_name, selection)?.into(),
            )),

            Statement::Query(q) => Ok(BoundStatement::Query(self.bind_query(*q)?.into())),

            _ => Err(
                ErrorCode::NotImplementedError(format!("unsupported statement {:?}", stmt)).into(),
            ),
        }
    }
}
