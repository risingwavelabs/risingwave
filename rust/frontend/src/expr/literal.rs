use risingwave_common::types::{DataTypeKind, Datum};

use super::{BoundExpr, BoundExprImpl};
use crate::expr::ExprType;
#[derive(Clone, Debug, PartialEq)]
pub struct BoundLiteral {
    #[allow(dead_code)]
    data: Datum,
    data_type: DataTypeKind,
}
impl BoundLiteral {
    pub fn new(data: Datum, data_type: DataTypeKind) -> Self {
        BoundLiteral { data, data_type }
    }
    pub fn get_expr_type(&self) -> ExprType {
        ExprType::ConstantValue
    }
}
impl BoundExpr for BoundLiteral {
    fn return_type(&self) -> DataTypeKind {
        self.data_type
    }
    fn bound_expr(self) -> BoundExprImpl {
        BoundExprImpl::Literal(Box::new(self))
    }
}
