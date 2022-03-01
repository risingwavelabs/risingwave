use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Ident;

use crate::binder::Binder;
use crate::expr::{ExprImpl, InputRef};

impl Binder {
    pub fn bind_column(&mut self, idents: &[Ident]) -> Result<ExprImpl> {
        // TODO: check quote style of `ident`.
        let (_schema_name, table_name, column_name) = match idents {
            [column] => (None, None, &column.value),
            [table, column] => (None, Some(&table.value), &column.value),
            [schema, table, column] => (Some(&schema.value), Some(&table.value), &column.value),
            _ => {
                return Err(
                    ErrorCode::InternalError(format!("Too many idents: {:?}", idents)).into(),
                )
            }
        };
        match table_name {
            Some(table_name) => match self.context.tables.get(table_name) {
                Some(columns) => match columns.get(column_name) {
                    Some(column) => Ok(ExprImpl::InputRef(Box::new(InputRef::new(
                        column.id,
                        column.data_type.clone(),
                    )))),
                    None => Err(ErrorCode::ItemNotFound(format!(
                        "Invalid column: {}",
                        column_name
                    ))
                    .into()),
                },
                None => {
                    Err(ErrorCode::ItemNotFound(format!("Invalid table: {}", table_name)).into())
                }
            },
            None => {
                for columns in self.context.tables.values() {
                    if let Some(column) = columns.get(column_name) {
                        return Ok(ExprImpl::InputRef(Box::new(InputRef::new(
                            column.id,
                            column.data_type.clone(),
                        ))));
                    }
                }
                Err(ErrorCode::ItemNotFound(format!("Invalid column: {}", column_name)).into())
            }
        }
    }
}
