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

use fixedbitset::FixedBitSet;
use risingwave_common::expr::AggKind;
use risingwave_common::types::{DataType, Scalar};
mod input_ref;
pub use input_ref::*;
mod literal;
pub use literal::*;
mod function_call;
pub use function_call::*;
mod agg_call;
pub use agg_call::*;
mod subquery;
pub use subquery::*;
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
    Subquery(Box<Subquery>),
}

impl ExprImpl {
    /// A literal int value.
    #[inline(always)]
    pub fn literal_int(v: i32) -> Self {
        Literal::new(Some(v.to_scalar_value()), DataType::Int32).into()
    }

    /// A literal boolean value.
    #[inline(always)]
    pub fn literal_bool(v: bool) -> Self {
        Literal::new(Some(v.to_scalar_value()), DataType::Boolean).into()
    }

    /// A `count(*)` aggregate function.
    #[inline(always)]
    pub fn count_star() -> Self {
        AggCall::new(AggKind::Count, vec![]).unwrap().into()
    }

    /// Collect all `InputRef`s' indexes in the expression.
    ///
    /// # Panics
    /// Panics if `input_ref >= input_col_num`.
    pub fn collect_input_refs(&self, input_col_num: usize) -> FixedBitSet {
        let mut visitor = CollectInputRef::with_capacity(input_col_num);
        visitor.visit_expr(self);
        visitor.collect()
    }

    pub fn has_agg_call(&self) -> bool {
        struct HasAggCall {
            has_agg_call: bool,
        }

        impl ExprVisitor for HasAggCall {
            fn visit_agg_call(&mut self, _agg_call: &AggCall) {
                self.has_agg_call = true;
            }
        }

        let mut visitor = HasAggCall {
            has_agg_call: false,
        };
        visitor.visit_expr(self);
        visitor.has_agg_call
    }

    pub fn has_subquery(&self) -> bool {
        struct HasSubquery {
            has_subquery: bool,
        }

        impl ExprVisitor for HasSubquery {
            fn visit_subquery(&mut self, _subquery: &Subquery) {
                self.has_subquery = true;
            }
        }

        let mut visitor = HasSubquery {
            has_subquery: false,
        };
        visitor.visit_expr(self);
        visitor.has_subquery
    }
}

impl Expr for ExprImpl {
    fn return_type(&self) -> DataType {
        match self {
            ExprImpl::InputRef(expr) => expr.return_type(),
            ExprImpl::Literal(expr) => expr.return_type(),
            ExprImpl::FunctionCall(expr) => expr.return_type(),
            ExprImpl::AggCall(expr) => expr.return_type(),
            ExprImpl::Subquery(expr) => expr.return_type(),
        }
    }

    fn to_protobuf(&self) -> ExprNode {
        match self {
            ExprImpl::InputRef(e) => e.to_protobuf(),
            ExprImpl::Literal(e) => e.to_protobuf(),
            ExprImpl::FunctionCall(e) => e.to_protobuf(),
            ExprImpl::AggCall(e) => e.to_protobuf(),
            ExprImpl::Subquery(e) => e.to_protobuf(),
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

impl From<Subquery> for ExprImpl {
    fn from(subquery: Subquery) -> Self {
        ExprImpl::Subquery(Box::new(subquery))
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
                Self::Subquery(arg0) => f.debug_tuple("Subquery").field(arg0).finish(),
            };
        }
        match self {
            Self::InputRef(x) => write!(f, "{:?}", x),
            Self::Literal(x) => write!(f, "{:?}", x),
            Self::FunctionCall(x) => write!(f, "{:?}", x),
            Self::AggCall(x) => write!(f, "{:?}", x),
            Self::Subquery(x) => write!(f, "{:?}", x),
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
