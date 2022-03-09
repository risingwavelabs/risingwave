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
        let index = match table_name {
            Some(table_name) => self
                .context
                .get_index_with_table_name(column_name, table_name)?,
            None => self.context.get_index(column_name)?,
        };
        let column = &self.context.columns[index];
        Ok(InputRef::new(
            column.index,
            column.data_type.clone(),
        ).into())
    }

    pub fn bind_all_columns(&mut self) -> Result<Vec<ExprImpl>> {
        let bound_columns = self
            .context
            .columns
            .iter()
            .map(|column| InputRef::new(column.index, column.data_type.clone()).into())
            .collect();
        Ok(bound_columns)
    }
}
