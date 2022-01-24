use risingwave_common::types::DataTypeKind;

use super::{infer_type, BoundExpr, BoundExprImpl};
use crate::expr::ExprType;

#[derive(Clone, Debug)]
pub struct BoundFunctionCall {
    func_type: ExprType,
    return_type: DataTypeKind,
    inputs: Vec<BoundExprImpl>,
}
impl BoundFunctionCall {
    pub fn new(func_type: ExprType, inputs: Vec<BoundExprImpl>) -> Option<Self> {
        let return_type = infer_type(
            func_type,
            inputs.iter().map(|expr| expr.return_type()).collect(),
        )?; // should be derived from inputs
        Some(Self::new_with_return_type(func_type, inputs, return_type))
    }

    /// used for expressions like cast
    pub fn new_with_return_type(
        func_type: ExprType,
        inputs: Vec<BoundExprImpl>,
        return_type: DataTypeKind,
    ) -> Self {
        BoundFunctionCall {
            func_type,
            return_type,
            inputs,
        }
    }

    pub fn decompose(self) -> (ExprType, Vec<BoundExprImpl>) {
        (self.func_type, self.inputs)
    }
    pub fn get_expr_type(&self) -> ExprType {
        self.func_type
    }
}
impl BoundExpr for BoundFunctionCall {
    fn return_type(&self) -> DataTypeKind {
        self.return_type
    }
    fn bound_expr(self) -> BoundExprImpl {
        BoundExprImpl::FunctionCall(Box::new(self))
    }
}
