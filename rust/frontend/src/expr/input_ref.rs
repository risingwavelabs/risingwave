use risingwave_common::types::DataType;

use super::{Expr, ExprImpl};
use crate::expr::ExprType;
#[derive(Clone)]
pub struct InputRef {
    index: usize,
    data_type: DataType,
}
impl InputRef {
    pub fn new(index: usize, data_type: DataType) -> Self {
        InputRef { index, data_type }
    }
    pub fn get_expr_type(&self) -> ExprType {
        ExprType::InputRef
    }
}
impl Expr for InputRef {
    fn return_type(&self) -> DataType {
        self.data_type
    }
    fn bound_expr(self) -> ExprImpl {
        ExprImpl::InputRef(Box::new(self))
    }
}
impl std::fmt::Debug for InputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.index)
    }
}
