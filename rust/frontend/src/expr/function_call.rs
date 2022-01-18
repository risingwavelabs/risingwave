use risingwave_common::types::DataTypeKind;
use risingwave_pb::expr::expr_node;

use super::{infer_type, BoundExpr, BoundExprImpl};

#[derive(Clone, Debug)]
pub struct BoundFunctionCall {
    func_type: expr_node::Type,
    return_type: DataTypeKind,
    inputs: Vec<BoundExprImpl>,
}
impl BoundFunctionCall {
    pub fn new(func_type: expr_node::Type, inputs: Vec<BoundExprImpl>) -> Option<Self> {
        let return_type = infer_type(
            func_type,
            inputs.iter().map(|expr| expr.return_type()).collect(),
        )?; // should be derived from inputs
        Some(Self::new_with_return_type(func_type, inputs, return_type))
    }

    /// used for expressions like cast
    pub fn new_with_return_type(
        func_type: expr_node::Type,
        inputs: Vec<BoundExprImpl>,
        return_type: DataTypeKind,
    ) -> Self {
        BoundFunctionCall {
            func_type,
            return_type,
            inputs,
        }
    }

    pub fn decompose(self) -> (expr_node::Type, Vec<BoundExprImpl>) {
        (self.func_type, self.inputs)
    }
    pub fn get_expr_type(&self) -> expr_node::Type {
        self.func_type
    }
}
impl BoundExpr for BoundFunctionCall {
    fn return_type(&self) -> DataTypeKind {
        self.return_type
    }
}
