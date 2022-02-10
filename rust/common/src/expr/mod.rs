mod agg;
pub mod build_expr_from_prost;
pub mod data_types;
mod expr_binary_bytes;
pub mod expr_binary_nonnull;
pub mod expr_binary_nullable;
mod expr_case;
mod expr_input_ref;
mod expr_is_null;
mod expr_literal;
mod expr_search;
mod expr_ternary_bytes;
pub mod expr_unary;
mod pg_sleep;
mod template;

use std::convert::TryFrom;
use std::slice;
use std::sync::Arc;

pub use agg::AggKind;
pub use expr_input_ref::InputRefExpression;
pub use expr_literal::*;
use risingwave_pb::expr::ExprNode;

use crate::array::{ArrayRef, DataChunk, Row};
use crate::error::ErrorCode::InternalError;
use crate::error::Result;
use crate::expr::build_expr_from_prost::*;
use crate::types::DataType;

pub type ExpressionRef = Arc<dyn Expression>;

/// Instance of an expression
pub trait Expression: std::fmt::Debug + Sync + Send {
    fn return_type(&self) -> DataType;

    /// Evaluate the expression
    ///
    /// # Arguments
    ///
    /// * `input` - input data of the Project Executor
    fn eval(&mut self, input: &DataChunk) -> Result<ArrayRef>;
}

pub type BoxedExpression = Box<dyn Expression>;

pub fn build_from_prost(prost: &ExprNode) -> Result<BoxedExpression> {
    use risingwave_pb::expr::expr_node::Type::*;

    match prost.get_expr_type()? {
        Cast | Upper | Lower | Not | PgSleep | IsTrue | IsNotTrue | IsFalse | IsNotFalse
        | IsNull | IsNotNull | Neg => build_unary_expr_prost(prost),
        Equal | NotEqual | LessThan | LessThanOrEqual | GreaterThan | GreaterThanOrEqual => {
            build_binary_expr_prost(prost)
        }
        Add | Subtract | Multiply | Divide | Modulus => build_binary_expr_prost(prost),
        Extract | RoundDigit => build_binary_expr_prost(prost),
        StreamNullByRowCount | And | Or => build_nullable_binary_expr_prost(prost),
        Substr => build_substr_expr(prost),
        Length => build_length_expr(prost),
        Replace => build_replace_expr(prost),
        Like => build_like_expr(prost),
        Trim => build_trim_expr(prost),
        Ltrim => build_ltrim_expr(prost),
        Rtrim => build_rtrim_expr(prost),
        Position => build_position_expr(prost),
        Ascii => build_ascii_expr(prost),
        ConstantValue => LiteralExpression::try_from(prost).map(|d| Box::new(d) as BoxedExpression),
        InputRef => InputRefExpression::try_from(prost).map(|d| Box::new(d) as BoxedExpression),
        Case => build_case_expr(prost),
        Translate => build_translate_expr(prost),
        Search => build_search_expr(prost),
        _ => Err(InternalError(format!(
            "Unsupported expression type: {:?}",
            prost.get_expr_type()
        ))
        .into()),
    }
}

#[derive(Debug)]
/// Simply wrap a row level expression as an array level expression
pub struct RowExpression {
    expr: BoxedExpression,
}

impl RowExpression {
    pub fn new(expr: BoxedExpression) -> Self {
        Self { expr }
    }

    pub fn eval(&mut self, row: &Row, data_types: &[DataType]) -> Result<ArrayRef> {
        let input = DataChunk::from_rows(slice::from_ref(row), data_types)?;
        self.expr.eval(&input)
    }

    fn return_type(&self) -> DataType {
        self.expr.return_type()
    }
}

#[cfg(test)]
mod test_utils;
