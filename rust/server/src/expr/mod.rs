mod input_ref;
mod literal;
pub(crate) use literal::*;

use crate::array::ArrayRef;
use crate::array::DataChunk;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::expr::input_ref::InputRefExpression;
use crate::types::{DataType, DataTypeRef};
use risingwave_proto::expr::{
    ExprNode,
    ExprNode_ExprNodeType::{CONSTANT_VALUE, INPUT_REF},
};
use std::convert::TryFrom;

pub(crate) enum ExpressionOutput<'a> {
    /// Returned by literal expression
    Literal(&'a Datum),
    Array(ArrayRef),
}

pub(crate) trait Expression: Sync + Send {
    fn return_type(&self) -> &dyn DataType;
    fn return_type_ref(&self) -> DataTypeRef;
    fn eval(&mut self, input: &DataChunk) -> Result<ExpressionOutput>;
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
      INPUT_REF => InputRefExpression
    }
}
