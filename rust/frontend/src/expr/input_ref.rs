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

    /// Get a reference to the input ref's index.
    pub fn index(&self) -> usize {
        self.index
    }

    /// Get a reference to the input ref's data type.
    pub fn data_type(&self) -> DataType {
        self.data_type.clone()
    }
}
impl Expr for InputRef {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }
    fn to_expr_impl(self) -> ExprImpl {
        ExprImpl::InputRef(Box::new(self))
    }
}
impl std::fmt::Debug for InputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.index)
    }
}
