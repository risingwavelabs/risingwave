use std::fmt;

use risingwave_common::types::DataType;

use super::Expr;
use crate::expr::ExprType;
#[derive(Clone, Debug, PartialEq)]
pub struct InputRef {
    index: usize,
    data_type: DataType,
}

impl fmt::Display for InputRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "${}", self.index + 1)
    }
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
}
