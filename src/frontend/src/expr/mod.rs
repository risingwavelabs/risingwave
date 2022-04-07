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

use enum_as_inner::EnumAsInner;
use fixedbitset::FixedBitSet;
use risingwave_common::types::{DataType, Scalar};
use risingwave_expr::expr::AggKind;

use crate::binder::BoundSetExpr;
mod input_ref;
pub use input_ref::*;
mod correlated_input_ref;
pub use correlated_input_ref::*;
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

use paste::paste;

/// the trait of bound exprssions
pub trait Expr: Into<ExprImpl> {
    /// Get the return type of the expr
    fn return_type(&self) -> DataType;

    /// Serialize the expression
    fn to_protobuf(&self) -> ExprNode;
}

#[derive(Clone, Eq, PartialEq, Hash, EnumAsInner)]
pub enum ExprImpl {
    // ColumnRef(Box<BoundColumnRef>), might be used in binder.
    InputRef(Box<InputRef>),
    CorrelatedInputRef(Box<CorrelatedInputRef>),
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

    /// Check whether self is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, ExprImpl::Literal(literal) if literal.get_data().is_none())
    }

    /// Check if cast needs to be inserted.
    /// TODO: check castiblility with context.
    pub fn ensure_type(self, ty: DataType) -> ExprImpl {
        if self.is_null() {
            ExprImpl::Literal(Box::new(Literal::new(None, ty)))
        } else if ty == self.return_type() {
            self
        } else {
            ExprImpl::FunctionCall(Box::new(FunctionCall::new_with_return_type(
                ExprType::Cast,
                vec![self],
                ty,
            )))
        }
    }
}

/// Implement helper functions which recursively checks whether an variant is included in the
/// expression. e.g., `has_subquery(&self) -> bool`
///
/// It will not traverse inside subqueries.
macro_rules! impl_has_variant {
    ( $($variant:ident),* ) => {
        paste! {
            impl ExprImpl {
                $(
                    pub fn [<has_ $variant:snake>](&self) -> bool {
                        struct Has {
                            has: bool,
                        }

                        impl ExprVisitor for Has {
                            fn [<visit_ $variant:snake>](&mut self, _: &$variant) {
                                self.has = true;
                            }
                        }

                        let mut visitor = Has {
                            has: false,
                        };
                        visitor.visit_expr(self);
                        visitor.has
                    }
                )*
            }
        }
    };
}

impl_has_variant! {InputRef, Literal, FunctionCall, AggCall, Subquery}

impl ExprImpl {
    // We need to traverse inside subqueries.
    pub fn has_correlated_input_ref(&self) -> bool {
        struct Has {
            has: bool,
        }

        impl ExprVisitor for Has {
            fn visit_correlated_input_ref(&mut self, _: &CorrelatedInputRef) {
                self.has = true;
            }

            fn visit_subquery(&mut self, subquery: &Subquery) {
                match &subquery.query.body {
                    BoundSetExpr::Select(select) => select
                        .select_items
                        .iter()
                        .chain(select.group_by.iter())
                        .chain(select.where_clause.iter())
                        .for_each(|expr| self.visit_expr(expr)),
                    BoundSetExpr::Values(_) => {}
                }
            }
        }

        let mut visitor = Has { has: false };
        visitor.visit_expr(self);
        visitor.has
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
            ExprImpl::CorrelatedInputRef(expr) => expr.return_type(),
        }
    }

    fn to_protobuf(&self) -> ExprNode {
        match self {
            ExprImpl::InputRef(e) => e.to_protobuf(),
            ExprImpl::Literal(e) => e.to_protobuf(),
            ExprImpl::FunctionCall(e) => e.to_protobuf(),
            ExprImpl::AggCall(e) => e.to_protobuf(),
            ExprImpl::Subquery(e) => e.to_protobuf(),
            ExprImpl::CorrelatedInputRef(e) => e.to_protobuf(),
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

impl From<CorrelatedInputRef> for ExprImpl {
    fn from(correlated_input_ref: CorrelatedInputRef) -> Self {
        ExprImpl::CorrelatedInputRef(Box::new(correlated_input_ref))
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
                Self::CorrelatedInputRef(arg0) => {
                    f.debug_tuple("CorrelatedInputRef").field(arg0).finish()
                }
            };
        }
        match self {
            Self::InputRef(x) => write!(f, "{:?}", x),
            Self::Literal(x) => write!(f, "{:?}", x),
            Self::FunctionCall(x) => write!(f, "{:?}", x),
            Self::AggCall(x) => write!(f, "{:?}", x),
            Self::Subquery(x) => write!(f, "{:?}", x),
            Self::CorrelatedInputRef(x) => write!(f, "{:?}", x),
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
