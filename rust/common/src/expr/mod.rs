mod agg;
pub mod binary_expr;
mod binary_expr_bytes;
mod binary_expr_nullable;
pub mod expr_factory;
mod expr_tmpl;
mod input_ref;
mod literal;
mod ternary_expr_bytes;
pub mod unary_expr;

pub use agg::AggKind;
pub use input_ref::InputRefExpression;
pub use literal::*;

use crate::array::ArrayRef;
use crate::array::DataChunk;
use crate::error::ErrorCode::InternalError;
use crate::error::Result;

use crate::expr::expr_factory::{
    build_binary_expr_prost, build_length_expr, build_like_expr, build_ltrim_expr,
    build_position_expr, build_replace_expr, build_rtrim_expr, build_substr_expr, build_trim_expr,
    build_unary_expr_prost,
};

use crate::types::{DataType, DataTypeRef};
use risingwave_pb::expr::expr_node::Type::{
    {Add, Divide, Modulus, Multiply, Subtract}, {And, Not, Or}, {Cast, Upper},
    {Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NotEqual},
};
use risingwave_pb::expr::ExprNode as ProstExprNode;
use risingwave_pb::ToProst;
use risingwave_pb::ToProto;
use risingwave_proto::expr::{
    ExprNode,
    ExprNode_Type::{
        CONSTANT_VALUE, INPUT_REF, LENGTH, LIKE, LTRIM, POSITION, REPLACE, RTRIM, SUBSTR, TRIM,
    },
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
    // TODO: Read from proto in a consistent way.
    match proto.get_expr_type() {
        SUBSTR => return build_substr_expr(proto),
        LENGTH => return build_length_expr(proto),
        REPLACE => return build_replace_expr(proto),
        LIKE => return build_like_expr(proto),
        TRIM => return build_trim_expr(proto),
        LTRIM => return build_ltrim_expr(proto),
        RTRIM => return build_rtrim_expr(proto),
        POSITION => return build_position_expr(proto),
        CONSTANT_VALUE => (),
        INPUT_REF => (),
        _other => return build_from_prost(&proto.to_prost::<ProstExprNode>()),
    };
    build_expression! {proto,
      CONSTANT_VALUE => LiteralExpression,
      INPUT_REF => InputRefExpression
    }
}

pub fn build_from_prost(prost: &ProstExprNode) -> Result<BoxedExpression> {
    match prost.get_expr_type() {
        Cast | Upper | Not => build_unary_expr_prost(prost),
        Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => {
            build_binary_expr_prost(prost)
        }
        Add | Subtract | Multiply | Divide | Modulus | And | Or => build_binary_expr_prost(prost),

        _ => build_from_proto(&prost.to_proto::<ExprNode>()),
    }
}

pub fn build_from_proto_option(proto: Option<&ExprNode>) -> Result<BoxedExpression> {
    match proto {
        Some(expr_node) => build_from_proto(expr_node),
        None => Err(InternalError("Expression build error.".to_string()).into()),
    }
}

#[cfg(test)]
mod tests;
