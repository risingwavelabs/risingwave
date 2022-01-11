use risingwave_pb::data::DataType;
use risingwave_pb::expr::expr_node;

use super::{infer_type, BoundExpr, BoundExprImpl};

#[derive(Clone, Debug)]
pub struct BoundFunctionCall {
    func_type: expr_node::Type,
    return_type: DataType,
    children: Vec<BoundExprImpl>,
}
impl BoundFunctionCall {
    pub fn new(func_type: expr_node::Type, children: Vec<BoundExprImpl>) -> Option<Self> {
        let return_type = infer_type(
            func_type,
            children.iter().map(|expr| expr.return_type()).collect(),
        )?; // should be derived from children
        Some(Self::new_with_return_type(func_type, children, return_type))
    }

    /// used for expressions like cast
    pub fn new_with_return_type(
        func_type: expr_node::Type,
        children: Vec<BoundExprImpl>,
        return_type: DataType,
    ) -> Self {
        BoundFunctionCall {
            func_type,
            return_type,
            children,
        }
    }

    pub fn decompose(self) -> (expr_node::Type, Vec<BoundExprImpl>) {
        (self.func_type, self.children)
    }
    pub fn get_expr_type(&self) -> expr_node::Type {
        self.func_type
    }
}
impl BoundExpr for BoundFunctionCall {
    fn return_type(&self) -> DataType {
        self.return_type.clone()
    }
}
