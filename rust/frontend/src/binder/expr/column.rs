use risingwave_common::array::RwError;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Ident;

use crate::binder::Binder;
use crate::expr::{ExprImpl, InputRef};

impl Binder {
    pub fn bind_column(&mut self, ident: Ident) -> Result<ExprImpl> {
        // TODO: check quote style of `ident`.

        // let column = ident.value.to_lowercase();
        if let Some(column) = self.context.columns.get(&ident.value) {
            Ok(ExprImpl::InputRef(Box::new(InputRef::new(
                column.id,
                column.data_type,
            ))))
        } else {
            Err(RwError::from(ErrorCode::InternalError(format!(
                "Invalid column name: {}",
                ident.value
            ))))
        }
    }
}
