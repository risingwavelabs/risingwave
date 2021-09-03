mod agg;
mod arithmetic_expr;
mod input_ref;
mod literal;
mod type_cast;
pub(crate) use agg::{AggExpression, AggKind};
pub(crate) use literal::*;

use crate::array::ArrayRef;
use crate::array::DataChunk;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::expr::arithmetic_expr::ArithmeticExpression;
use crate::expr::input_ref::InputRefExpression;
use crate::expr::type_cast::TypeCastExpression;
use crate::types::{DataType, DataTypeRef};
use risingwave_proto::expr::{
    ExprNode,
    ExprNode_ExprNodeType::{ADD, DIVIDE, MODULUS, MULTIPLY, SUBTRACT},
    ExprNode_ExprNodeType::{CAST, CONSTANT_VALUE, INPUT_REF},
};
use std::convert::TryFrom;
use std::sync::Arc;

pub(crate) type ExpressionRef = Arc<dyn Expression>;

pub(crate) trait Expression: Sync + Send {
    fn return_type(&self) -> &dyn DataType;
    fn return_type_ref(&self) -> DataTypeRef;
    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef>;
}

pub(crate) type BoxedExpression = Box<dyn Expression>;

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

pub(crate) fn build_from_proto(proto: &ExprNode) -> Result<BoxedExpression> {
    build_expression! {proto,
      CONSTANT_VALUE => LiteralExpression,
      INPUT_REF => InputRefExpression,
      CAST => TypeCastExpression,
      ADD => ArithmeticExpression,
      SUBTRACT => ArithmeticExpression,
      MULTIPLY => ArithmeticExpression,
      DIVIDE => ArithmeticExpression,
      MODULUS => ArithmeticExpression
    }
}

pub(crate) fn build_from_proto_option(proto: Option<&ExprNode>) -> Result<BoxedExpression> {
    match proto {
        Some(expr_node) => build_from_proto(expr_node),
        None => Err(InternalError("Expression build error.".to_string()).into()),
    }
}
