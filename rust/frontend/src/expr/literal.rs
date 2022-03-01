use risingwave_common::types::{DataType, Datum};

use super::{Expr, ExprImpl};
use crate::expr::ExprType;
#[derive(Clone, Debug, PartialEq)]
pub struct Literal {
    #[allow(dead_code)]
    data: Datum,
    data_type: DataType,
}
impl Literal {
    pub fn new(data: Datum, data_type: DataType) -> Self {
        Literal { data, data_type }
    }
    pub fn get_expr_type(&self) -> ExprType {
        ExprType::ConstantValue
    }
}
impl Expr for Literal {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }
    fn to_expr_impl(self) -> ExprImpl {
        ExprImpl::Literal(Box::new(self))
    }
}
