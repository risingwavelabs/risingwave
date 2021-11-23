mod agg;
pub mod build_expr_from_prost;
mod expr_binary_bytes;
pub mod expr_binary_nonnull;
mod expr_binary_nullable;
mod expr_case;
mod expr_input_ref;
mod expr_literal;
mod expr_ternary_bytes;
pub mod expr_unary_nonnull;
mod template;
use crate::array::{ArrayRef, DataChunk};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::expr::build_expr_from_prost::*;
use crate::types::{DataType, DataTypeRef};
pub use agg::AggKind;
pub use expr_input_ref::InputRefExpression;
pub use expr_literal::*;
use risingwave_pb::expr::ExprNode as ProstExprNode;
use std::convert::TryFrom;
use std::sync::Arc;

pub type ExpressionRef = Arc<dyn Expression>;

pub trait Expression: Sync + Send {
    fn return_type(&self) -> &dyn DataType;
    fn return_type_ref(&self) -> DataTypeRef;
    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef>;
}

pub type BoxedExpression = Box<dyn Expression>;

pub fn build_from_prost(prost: &ProstExprNode) -> Result<BoxedExpression> {
    use risingwave_pb::expr::expr_node::Type::*;

    match prost.get_expr_type() {
        Cast | Upper | Not => build_unary_expr_prost(prost),
        Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => {
            build_binary_expr_prost(prost)
        }
        Add | Subtract | Multiply | Divide | Modulus | And | Or => build_binary_expr_prost(prost),
        Extract => build_binary_expr_prost(prost),
        StreamNullByRowCount => build_nullable_binary_expr_prost(prost),
        Substr => build_substr_expr(prost),
        Length => build_length_expr(prost),
        Replace => build_replace_expr(prost),
        Like => build_like_expr(prost),
        Trim => build_trim_expr(prost),
        Ltrim => build_ltrim_expr(prost),
        Rtrim => build_rtrim_expr(prost),
        Position => build_position_expr(prost),
        ConstantValue => LiteralExpression::try_from(prost).map(|d| Box::new(d) as BoxedExpression),
        InputRef => InputRefExpression::try_from(prost).map(|d| Box::new(d) as BoxedExpression),
        Case => build_case_expr(prost),
        _ => Err(InternalError(format!(
            "Unsupported expression type: {:?}",
            prost.get_expr_type()
        ))
        .into()),
    }
}

#[cfg(test)]
mod test_utils;
