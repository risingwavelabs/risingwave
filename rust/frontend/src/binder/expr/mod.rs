use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Expr;

use crate::binder::Binder;
use crate::expr::ExprImpl;

mod binary_op;
mod value;

impl Binder<'_> {
    pub(super) fn bind_expr(&mut self, expr: Expr) -> Result<ExprImpl> {
        match expr {
            Expr::Value(v) => Ok(ExprImpl::Literal(Box::new(self.bind_value(v)?))),
            Expr::BinaryOp { left, op, right } => Ok(ExprImpl::FunctionCall(Box::new(
                self.bind_binary_op(*left, op, *right)?,
            ))),
            Expr::Nested(expr) => self.bind_expr(*expr),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", expr)).into()),
        }
    }
}
