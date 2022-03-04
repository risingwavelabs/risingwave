use std::fmt::Debug;

use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::Select;

use crate::binder::{Binder, TableRef};
use crate::expr::{Expr, ExprImpl};

#[derive(Debug)]
pub struct BoundSelect {
    pub distinct: bool,
    pub projection: Vec<ExprImpl>,
    pub from: Option<TableRef>,
    pub selection: Option<ExprImpl>,
}

impl Binder {
    pub(super) fn bind_select(&mut self, select: Select) -> Result<BoundSelect> {
        let from = self.bind_vec_table_with_joins(select.from)?;
        let selection = select
            .selection
            .map(|expr| self.bind_expr(expr))
            .transpose()?;
        if let Some(selection) = &selection {
            if !matches!(selection.return_type(), DataType::Boolean) {
                return Err(ErrorCode::InternalError(
                    "The return type must be boolean!".to_string(),
                )
                .into());
            }
        }
        let projection = self.bind_projection(select.projection)?;
        Ok(BoundSelect {
            distinct: select.distinct,
            projection,
            from,
            selection,
        })
    }
}
