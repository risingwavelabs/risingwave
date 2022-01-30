use risingwave_common::types::{DataTypeKind, Datum};

use super::{Expr, ExprImpl};
use crate::expr::ExprType;
#[derive(Clone, Debug, PartialEq)]
pub struct Literal {
    #[allow(dead_code)]
    data: Datum,
    data_type: DataTypeKind,
}
impl Literal {
    pub fn new(data: Datum, data_type: DataTypeKind) -> Self {
        Literal { data, data_type }
    }
    pub fn get_expr_type(&self) -> ExprType {
        ExprType::ConstantValue
    }
}
impl Expr for Literal {
    fn return_type(&self) -> DataTypeKind {
        self.data_type
    }
    fn bound_expr(self) -> ExprImpl {
        ExprImpl::Literal(Box::new(self))
    }
}
