// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
use risingwave_common::types::{DataType, Datum, Scalar, ScalarImpl};
mod input_ref;
pub use input_ref::*;
mod literal;
pub use literal::*;
mod function_call;
pub use function_call::*;
mod agg_call;
pub use agg_call::*;
mod type_inference;
use risingwave_pb::expr::ExprNode;
pub use type_inference::*;
mod utils;
pub use utils::*;
mod expr_rewriter;
pub use expr_rewriter::*;
mod expr_visitor;
pub use expr_visitor::*;
pub type ExprType = risingwave_pb::expr::expr_node::Type;

use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::{ConstantValue, InputRefExpr};

/// the trait of bound exprssions
pub trait Expr: Into<ExprImpl> {
    /// Get the return type of the expr
    fn return_type(&self) -> DataType;

    /// Serialize the expression
    fn to_protobuf(&self) -> ExprNode;
}

#[derive(Clone, PartialEq)]
pub enum ExprImpl {
    // ColumnRef(Box<BoundColumnRef>), might be used in binder.
    InputRef(Box<InputRef>),
    Literal(Box<Literal>),
    FunctionCall(Box<FunctionCall>),
    AggCall(Box<AggCall>),
}

impl ExprImpl {
    /// A literal int value.
    #[inline(always)]
    pub fn literal_int(v: i32) -> Self {
        Self::Literal(Box::new(Literal::new(
            Some(v.to_scalar_value()),
            DataType::Int32,
        )))
    }

    /// A literal boolean value.
    #[inline(always)]
    pub fn literal_bool(v: bool) -> Self {
        Self::Literal(Box::new(Literal::new(
            Some(v.to_scalar_value()),
            DataType::Boolean,
        )))
    }

    /// Serialize to protobuf.
    pub fn to_protobuf(&self) -> ExprNode {
        use risingwave_pb::expr::FunctionCall as ProstFunctionCall;

        match self {
            ExprImpl::InputRef(e) => ExprNode {
                expr_type: e.get_expr_type() as i32,
                return_type: Some(e.return_type().to_protobuf()),
                rex_node: Some(RexNode::InputRef(InputRefExpr {
                    column_idx: e.index() as i32,
                })),
            },
            ExprImpl::Literal(e) => ExprNode {
                expr_type: e.get_expr_type() as i32,
                return_type: Some(e.return_type().to_protobuf()),
                rex_node: literal_to_protobuf(e.get_data()),
            },
            ExprImpl::FunctionCall(e) => ExprNode {
                expr_type: e.get_expr_type() as i32,
                return_type: Some(e.return_type().to_protobuf()),
                rex_node: Some(RexNode::FuncCall(ProstFunctionCall {
                    children: e.inputs().iter().map(|arg| arg.to_protobuf()).collect(),
                })),
            },
            // This function is always called on the physical planning step, where
            // `ExprImpl::AggCall` must have been rewritten to aggregate operators.
            ExprImpl::AggCall(e) => {
                panic!(
                    "AggCall {:?} has not been rewritten to physical aggregate operators",
                    e
                )
            }
        }
    }
}

/// Convert a literal value (datum) into protobuf.
fn literal_to_protobuf(d: &Datum) -> Option<RexNode> {
    if d.is_none() {
        return None;
    }
    let body = match d.as_ref().unwrap() {
        ScalarImpl::Int16(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Int32(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Int64(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Float32(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Float64(v) => v.to_be_bytes().to_vec(),
        ScalarImpl::Utf8(s) => s.as_bytes().to_vec(),
        ScalarImpl::Bool(v) => (*v as i8).to_be_bytes().to_vec(),
        ScalarImpl::Decimal(v) => v.to_string().as_bytes().to_vec(),
        ScalarImpl::Interval(_) => todo!(),
        ScalarImpl::NaiveDate(_) => todo!(),
        ScalarImpl::NaiveDateTime(_) => todo!(),
        ScalarImpl::NaiveTime(_) => todo!(),
        ScalarImpl::Struct(_) => todo!(),
        ScalarImpl::List(_) => todo!(),
    };
    Some(RexNode::Constant(ConstantValue { body }))
}

impl Expr for ExprImpl {
    fn return_type(&self) -> DataType {
        match self {
            ExprImpl::InputRef(expr) => expr.return_type(),
            ExprImpl::Literal(expr) => expr.return_type(),
            ExprImpl::FunctionCall(expr) => expr.return_type(),
            ExprImpl::AggCall(expr) => expr.return_type(),
        }
    }

    fn to_protobuf(&self) -> ExprNode {
        match self {
            ExprImpl::InputRef(e) => e.to_protobuf(),
            ExprImpl::Literal(e) => e.to_protobuf(),
            ExprImpl::FunctionCall(e) => e.to_protobuf(),
            ExprImpl::AggCall(e) => e.to_protobuf(),
        }
    }
}

impl From<InputRef> for ExprImpl {
    fn from(input_ref: InputRef) -> Self {
        ExprImpl::InputRef(Box::new(input_ref))
    }
}

impl From<Literal> for ExprImpl {
    fn from(literal: Literal) -> Self {
        ExprImpl::Literal(Box::new(literal))
    }
}

impl From<FunctionCall> for ExprImpl {
    fn from(func_call: FunctionCall) -> Self {
        ExprImpl::FunctionCall(Box::new(func_call))
    }
}

impl From<AggCall> for ExprImpl {
    fn from(agg_call: AggCall) -> Self {
        ExprImpl::AggCall(Box::new(agg_call))
    }
}

/// A custom Debug implementation that is more concise and suitable to use with
/// [`std::fmt::Formatter::debug_list`] in plan nodes. If the verbose output is preferred, it is
/// still available via `{:#?}`.
impl std::fmt::Debug for ExprImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            return match self {
                Self::InputRef(arg0) => f.debug_tuple("InputRef").field(arg0).finish(),
                Self::Literal(arg0) => f.debug_tuple("Literal").field(arg0).finish(),
                Self::FunctionCall(arg0) => f.debug_tuple("FunctionCall").field(arg0).finish(),
                Self::AggCall(arg0) => f.debug_tuple("AggCall").field(arg0).finish(),
            };
        }
        match self {
            Self::InputRef(x) => write!(f, "{:?}", x),
            Self::Literal(x) => write!(f, "{:?}", x),
            Self::FunctionCall(x) => write!(f, "{:?}", x),
            Self::AggCall(x) => write!(f, "{:?}", x),
        }
    }
}

#[cfg(test)]
/// Asserts that the expression is an [`InputRef`] with the given index.
macro_rules! assert_eq_input_ref {
    ($e:expr, $index:expr) => {
        match $e {
            ExprImpl::InputRef(i) => assert_eq!(i.index(), $index),
            _ => assert!(false, "Expected input ref, found {:?}", $e),
        }
    };
}

#[cfg(test)]
pub(crate) use assert_eq_input_ref;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr_debug_alternate() {
        let mut e = InputRef::new(1, DataType::Boolean).into();
        e = FunctionCall::new(ExprType::Not, vec![e]).unwrap().into();
        let s = format!("{:#?}", e);
        assert!(s.contains("return_type: Boolean"))
    }
}
