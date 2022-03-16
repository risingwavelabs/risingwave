use risingwave_common::types::{DataType, Datum};

use super::Expr;
use crate::expr::ExprType;
#[derive(Clone, PartialEq)]
pub struct Literal {
    #[allow(dead_code)]
    data: Datum,
    data_type: DataType,
}

impl std::fmt::Debug for Literal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            f.debug_struct("Literal")
                .field("data", &self.data)
                .field("data_type", &self.data_type)
                .finish()
        } else {
            use risingwave_common::for_all_scalar_variants;
            use risingwave_common::types::ScalarImpl::*;
            macro_rules! scalar_write_inner {
            ([], $( { $variant_name:ident, $suffix_name:ident, $scalar:ty, $scalar_ref:ty } ),*) => {
                match &self.data {
                    None => write!(f, "null"),
                    $( Some($variant_name(v)) => write!(f, "{:?}", v) ),*
                }?;
            };
        }
            for_all_scalar_variants! { scalar_write_inner }
            write!(f, ":{:?}", self.data_type)
        }
    }
}

impl Literal {
    pub fn new(data: Datum, data_type: DataType) -> Self {
        Literal { data, data_type }
    }
    pub fn get_expr_type(&self) -> ExprType {
        ExprType::ConstantValue
    }
    pub fn get_data(&self) -> &Datum {
        &self.data
    }
}
impl Expr for Literal {
    fn return_type(&self) -> DataType {
        self.data_type.clone()
    }
}
