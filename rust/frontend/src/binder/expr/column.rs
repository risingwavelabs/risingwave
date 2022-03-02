use risingwave_common::array::RwError;
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
        let columns = self.context.columns.get(column_name).ok_or_else(|| {
            RwError::from(ErrorCode::ItemNotFound(format!(
                "Invalid column: {}",
                column_name
            )))
        })?;
        match table_name {
            Some(table_name) => {
                match columns
                    .iter()
                    .find(|column| column.table_name == *table_name)
                {
                    Some(column) => Ok(ExprImpl::InputRef(Box::new(InputRef::new(
                        column.index,
                        column.data_type.clone(),
                    )))),
                    None => Err(
                        ErrorCode::ItemNotFound(format!("Invalid table: {}", table_name)).into(),
                    ),
                }
            }
            None => {
                if columns.len() > 1 {
                    Err(ErrorCode::InternalError("Ambiguous column name".into()).into())
                } else {
                    Ok(ExprImpl::InputRef(Box::new(InputRef::new(
                        columns[0].index,
                        columns[0].data_type.clone(),
                    ))))
                }
            }
        }
    }
}
