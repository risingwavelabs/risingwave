use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::Expr;

use crate::binder::Binder;
use crate::expr::BoundExprImpl;

mod binary_op;
mod value;

impl Binder {
    pub(super) fn bind_expr(&mut self, expr: Expr) -> Result<BoundExprImpl> {
        match expr {
            Expr::Value(v) => Ok(BoundExprImpl::Literal(Box::new(self.bind_value(v)?))),
            Expr::BinaryOp { left, op, right } => Ok(BoundExprImpl::FunctionCall(Box::new(
                self.bind_binary_op(*left, op, *right)?,
            ))),
            Expr::Nested(expr) => self.bind_expr(*expr),
            _ => Err(ErrorCode::NotImplementedError(format!("{:?}", expr)).into()),
        }
    }
}
