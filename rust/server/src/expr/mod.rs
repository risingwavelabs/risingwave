mod agg;
mod arithmetic_expr;
mod cmp;
mod conjunction;
mod input_ref;
mod literal;
mod type_cast;
pub use agg::AggKind;
pub use literal::*;

use crate::array2::ArrayRef;
use crate::array2::DataChunk;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
// use crate::expr::arithmetic_expr::ArithmeticExpression;
// use crate::expr::cmp::CompareExpression;
// use crate::expr::conjunction::ConjunctionExpression;
use crate::expr::input_ref::InputRefExpression;
use crate::expr::type_cast::TypeCastExpression;

use crate::types::{DataType, DataTypeRef};
use risingwave_proto::expr::{
    ExprNode,
    ExprNode_ExprNodeType::{CAST, CONSTANT_VALUE, INPUT_REF},
};
use std::convert::TryFrom;
use std::sync::Arc;

pub type ExpressionRef = Arc<dyn Expression>;

pub trait Expression: Sync + Send {
    fn return_type(&self) -> &dyn DataType;
    fn return_type_ref(&self) -> DataTypeRef;
    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef>;
}

pub type BoxedExpression = Box<dyn Expression>;

macro_rules! build_expression {
  ($proto: expr, $($proto_type_name:path => $data_type:ty),*) => {
    match $proto.get_expr_type() {
      $(
        $proto_type_name => {
          <$data_type>::try_from($proto).map(|d| Box::new(d) as BoxedExpression)
        },
      )*
      _ => Err(InternalError(format!("Unsupported expression type: {:?}", $proto.get_expr_type())).into())
    }
  }
}

pub fn build_from_proto(proto: &ExprNode) -> Result<BoxedExpression> {
    build_expression! {proto,
      CONSTANT_VALUE => LiteralExpression,
      INPUT_REF => InputRefExpression,
      CAST => TypeCastExpression
      // AND => ConjunctionExpression,
      // OR => ConjunctionExpression,
      // NOT => ConjunctionExpression,
      // EQUAL => CompareExpression,
      // NOT_EQUAL => CompareExpression,
      // LESS_THAN => CompareExpression,
      // LESS_THAN_OR_EQUAL => CompareExpression,
      // GREATER_THAN => CompareExpression,
      // GREATER_THAN_OR_EQUAL => CompareExpression,
      // ADD => ArithmeticExpression,
      // SUBTRACT => ArithmeticExpression,
      // MULTIPLY => ArithmeticExpression,
      // DIVIDE => ArithmeticExpression,
      // MODULUS => ArithmeticExpression
    }
}

pub(crate) fn new_input_ref(return_type: DataTypeRef, idx: usize) -> BoxedExpression {
    Box::new(InputRefExpression::new(return_type, idx))
}

pub fn build_from_proto_option(proto: Option<&ExprNode>) -> Result<BoxedExpression> {
    match proto {
        Some(expr_node) => build_from_proto(expr_node),
        None => Err(InternalError("Expression build error.".to_string()).into()),
    }
}
