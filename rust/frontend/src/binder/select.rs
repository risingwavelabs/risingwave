use std::fmt::Debug;
use risingwave_common::error::{ErrorCode,Result};
use risingwave_common::types::DataType;
use crate::expr::{Expr, ExprImpl};
use risingwave_sqlparser::ast::Select;

use crate::binder::{Binder, TableRef};

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
        // let return_type= select.selection.clone().unwrap();
        // let return_type = match select.selection{
        //     Some(expr)=>expr.return_type(),
        //     None=>None,
        // }
        // let return_type = select.selection.map(|expr| expr.return_type())
        let return_typ = select
        .selection
        .map(|expr| self.bind_expr(expr))
        .unwrap();
        .Result;
        if return_typ != DataType::Boolean {
            return ErrorCode::InvalidInputSyntax("argument of WHERE (must be type boolean)".to_string(),return_type).transpose();
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
