use risingwave_common::error::{ErrorCode, Result};
use risingwave_sqlparser::ast::{BinaryOperator, Expr};

use crate::binder::Binder;
use crate::expr::{BoundExpr as _, BoundFunctionCall, ExprType};

impl Binder {
    pub(super) fn bind_binary_op(
        &mut self,
        left: Expr,
        op: BinaryOperator,
        right: Expr,
    ) -> Result<BoundFunctionCall> {
        let bound_left = self.bind_expr(left)?;
        let bound_right = self.bind_expr(right)?;
        let func_type = match op {
            BinaryOperator::Plus => ExprType::Add,
            _ => return Err(ErrorCode::NotImplementedError(format!("{:?}", op)).into()),
        };
        let desc = format!(
            "{:?} {:?} {:?}",
            bound_left.return_type(),
            op,
            bound_right.return_type(),
        );
        BoundFunctionCall::new(func_type, vec![bound_left, bound_right])
            .ok_or_else(|| ErrorCode::NotImplementedError(desc).into())
    }
}
