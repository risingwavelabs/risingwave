use risingwave_common::types::DataType;

use super::{infer_type, Expr, ExprImpl};
use crate::expr::ExprType;

#[derive(Clone, Debug)]
pub struct FunctionCall {
    func_type: ExprType,
    return_type: DataType,
    inputs: Vec<ExprImpl>,
}
impl FunctionCall {
    pub fn new(func_type: ExprType, inputs: Vec<ExprImpl>) -> Option<Self> {
        let return_type = infer_type(
            func_type,
            inputs.iter().map(|expr| expr.return_type()).collect(),
        )?; // should be derived from inputs
        Some(Self::new_with_return_type(func_type, inputs, return_type))
    }

    /// used for expressions like cast
    pub fn new_with_return_type(
        func_type: ExprType,
        inputs: Vec<ExprImpl>,
        return_type: DataType,
    ) -> Self {
        FunctionCall {
            func_type,
            return_type,
            inputs,
        }
    }

    pub fn decompose(self) -> (ExprType, Vec<ExprImpl>) {
        (self.func_type, self.inputs)
    }
    pub fn decompose_as_binary(self) -> (ExprType, ExprImpl, ExprImpl) {
        assert_eq!(self.inputs.len(), 2);
        let mut iter = self.inputs.into_iter();
        let left = iter.next().unwrap();
        let right = iter.next().unwrap();
        (self.func_type, left, right)
    }
    pub fn decompose_as_unary(self) -> (ExprType, ExprImpl) {
        assert_eq!(self.inputs.len(), 1);
        let mut iter = self.inputs.into_iter();
        let input = iter.next().unwrap();
        (self.func_type, input)
    }

    pub fn get_expr_type(&self) -> ExprType {
        self.func_type
    }

    /// Get a reference to the function call's inputs.
    pub fn inputs(&self) -> &[ExprImpl] {
        self.inputs.as_ref()
    }
}
impl Expr for FunctionCall {
    fn return_type(&self) -> DataType {
        self.return_type
    }
    fn to_expr_impl(self) -> ExprImpl {
        ExprImpl::FunctionCall(Box::new(self))
    }
}
