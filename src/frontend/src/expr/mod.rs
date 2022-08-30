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
use paste::paste;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_expr::expr::{build_from_prost, AggKind};
use risingwave_pb::expr::{ExprNode, ProjectSetSelectItem};

mod agg_call;
mod correlated_input_ref;
mod function_call;
mod input_ref;
mod literal;
mod subquery;
mod table_function;

mod expr_mutator;
mod expr_rewriter;
mod expr_visitor;
mod type_inference;
mod utils;

pub use agg_call::{AggCall, AggOrderBy, AggOrderByExpr};
pub use correlated_input_ref::{CorrelatedId, CorrelatedInputRef, Depth};
pub use function_call::{FunctionCall, FunctionCallDisplay};
pub use input_ref::{input_ref_to_column_indices, InputRef, InputRefDisplay};
pub use literal::Literal;
pub use subquery::{Subquery, SubqueryKind};
pub use table_function::{TableFunction, TableFunctionType};

pub type ExprType = risingwave_pb::expr::expr_node::Type;

pub use expr_rewriter::ExprRewriter;
pub use expr_visitor::ExprVisitor;
pub use type_inference::{
    agg_func_sigs, align_types, cast_map_array, cast_ok, cast_sigs, func_sigs, infer_type,
    least_restrictive, AggFuncSig, CastContext, CastSig, DataTypeName, FuncSign,
};
pub use utils::*;

/// the trait of bound exprssions
pub trait Expr: Into<ExprImpl> {
    /// Get the return type of the expr
    fn return_type(&self) -> DataType;

    /// Serialize the expression
    fn to_expr_proto(&self) -> ExprNode;
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
    TableFunction(Box<TableFunction>),
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

    /// A literal varchar value.
    #[inline(always)]
    pub fn literal_varchar(v: String) -> Self {
        Literal::new(Some(v.to_scalar_value()), DataType::Varchar).into()
    }

    /// A `count(*)` aggregate function.
    #[inline(always)]
    pub fn count_star() -> Self {
        AggCall::new(
            AggKind::Count,
            vec![],
            false,
            AggOrderBy::any(),
            Condition::true_cond(),
        )
        .unwrap()
        .into()
    }

    /// Collect all `InputRef`s' indexes in the expression.
    ///
    /// # Panics
    /// Panics if `input_ref >= input_col_num`.
    pub fn collect_input_refs(&self, input_col_num: usize) -> FixedBitSet {
        let mut visitor = CollectInputRef::with_capacity(input_col_num);
        visitor.visit_expr(self);
        visitor.into()
    }

    /// Check whether self is literal NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, ExprImpl::Literal(literal) if literal.get_data().is_none())
    }

    /// Check whether self is a literal NULL or literal string.
    pub fn is_unknown(&self) -> bool {
        matches!(self, ExprImpl::Literal(literal) if literal.return_type() == DataType::Varchar)
    }

    /// Shorthand to create cast expr to `target` type in implicit context.
    pub fn cast_implicit(self, target: DataType) -> Result<ExprImpl> {
        FunctionCall::new_cast(self, target, CastContext::Implicit)
    }

    /// Shorthand to create cast expr to `target` type in assign context.
    pub fn cast_assign(self, target: DataType) -> Result<ExprImpl> {
        FunctionCall::new_cast(self, target, CastContext::Assign)
    }

    /// Shorthand to create cast expr to `target` type in explicit context.
    pub fn cast_explicit(self, target: DataType) -> Result<ExprImpl> {
        FunctionCall::new_cast(self, target, CastContext::Explicit)
    }

    /// Create "cast" expr to string (`varchar`) type. This is different from a real cast, as
    /// boolean is converted to a single char rather than full word.
    ///
    /// Choose between `cast_output` and `cast_{assign,explicit}(Varchar)` based on `PostgreSQL`'s
    /// behavior on bools. For example, `concat(':', true)` is `:t` but `':' || true` is `:true`.
    /// All other types have the same behavior when formatting to output and casting to string.
    ///
    /// References in `PostgreSQL`:
    /// * [cast](https://github.com/postgres/postgres/blob/a3ff08e0b08dbfeb777ccfa8f13ebaa95d064c04/src/include/catalog/pg_cast.dat#L437-L444)
    /// * [impl](https://github.com/postgres/postgres/blob/27b77ecf9f4d5be211900eda54d8155ada50d696/src/backend/utils/adt/bool.c#L204-L209)
    pub fn cast_output(self) -> Result<ExprImpl> {
        if self.return_type() == DataType::Boolean {
            return Ok(FunctionCall::new(ExprType::BoolOut, vec![self])?.into());
        }
        // Use normal cast for other types. Both `assign` and `explicit` can pass the castability
        // check and there is no difference.
        self.cast_assign(DataType::Varchar)
    }

    /// Evaluate the expression on the given input.
    ///
    /// TODO: This is a naive implementation. We should avoid proto ser/de.
    /// Tracking issue: <https://github.com/singularity-data/risingwave/issues/3479>
    fn eval_row(&self, input: &Row) -> Result<Datum> {
        let backend_expr = build_from_prost(&self.to_expr_proto())?;
        backend_expr.eval_row(input).map_err(Into::into)
    }

    /// Evaluate a constant expression.
    pub fn eval_row_const(&self) -> Result<Datum> {
        assert!(self.is_const());
        self.eval_row(Row::empty())
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

                        impl ExprVisitor<()> for Has {
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

impl_has_variant! {InputRef, Literal, FunctionCall, AggCall, Subquery, TableFunction}

impl ExprImpl {
    /// This function is not meant to be called. In most cases you would want
    /// [`ExprImpl::has_correlated_input_ref_by_depth`].
    ///
    /// When an expr contains a [`CorrelatedInputRef`] with lower depth, the whole expr is still
    /// considered to be uncorrelated, and can be checked with [`ExprImpl::has_subquery`] as well.
    /// See examples on [`crate::binder::BoundQuery::is_correlated`] for details.
    ///
    /// This is a placeholder to trigger a compiler error when a trivial implementation checking for
    /// enum variant is generated by accident. It cannot be called either because you cannot pass
    /// `Infallible` to it.
    pub fn has_correlated_input_ref(&self, _: std::convert::Infallible) -> bool {
        unreachable!()
    }

    /// Used to check whether the expression has [`CorrelatedInputRef`].
    ///
    /// This is the core logic that supports [`crate::binder::BoundQuery::is_correlated`]. Check the
    /// doc of it for examples of `depth` being equal, less or greater.
    // We need to traverse inside subqueries.
    pub fn has_correlated_input_ref_by_depth(&self) -> bool {
        struct Has {
            has: bool,
            depth: usize,
        }

        impl ExprVisitor<()> for Has {
            fn visit_correlated_input_ref(&mut self, correlated_input_ref: &CorrelatedInputRef) {
                if correlated_input_ref.depth() >= self.depth {
                    self.has = true;
                }
            }

            fn visit_subquery(&mut self, subquery: &Subquery) {
                use crate::binder::BoundSetExpr;

                self.depth += 1;
                match &subquery.query.body {
                    BoundSetExpr::Select(select) => {
                        select.exprs().for_each(|expr| self.visit_expr(expr))
                    }
                    BoundSetExpr::Values(values) => {
                        values.exprs().for_each(|expr| self.visit_expr(expr))
                    }
                }
                self.depth -= 1;
            }
        }

        let mut visitor = Has {
            has: false,
            depth: 1,
        };
        visitor.visit_expr(self);
        visitor.has
    }

    pub fn has_correlated_input_ref_by_correlated_id(&self, correlated_id: CorrelatedId) -> bool {
        struct Has {
            has: bool,
            correlated_id: CorrelatedId,
        }

        impl ExprVisitor<()> for Has {
            fn visit_correlated_input_ref(&mut self, correlated_input_ref: &CorrelatedInputRef) {
                if correlated_input_ref.correlated_id() == self.correlated_id {
                    self.has = true;
                }
            }

            fn visit_subquery(&mut self, subquery: &Subquery) {
                use crate::binder::BoundSetExpr;
                match &subquery.query.body {
                    BoundSetExpr::Select(select) => {
                        select.exprs().for_each(|expr| self.visit_expr(expr))
                    }
                    BoundSetExpr::Values(values) => {
                        values.exprs().for_each(|expr| self.visit_expr(expr))
                    }
                }
            }
        }

        let mut visitor = Has {
            has: false,
            correlated_id,
        };
        visitor.visit_expr(self);
        visitor.has
    }

    /// Collect `CorrelatedInputRef`s in `ExprImpl` by relative `depth`, return their indices, and
    /// assign absolute `correlated_id` for them.
    pub fn collect_correlated_indices_by_depth_and_assign_id(
        &mut self,
        depth: Depth,
        correlated_id: CorrelatedId,
    ) -> Vec<usize> {
        struct Collector {
            depth: Depth,
            correlated_indices: Vec<usize>,
            correlated_id: CorrelatedId,
        }

        impl ExprMutator for Collector {
            fn visit_correlated_input_ref(
                &mut self,
                correlated_input_ref: &mut CorrelatedInputRef,
            ) {
                if correlated_input_ref.depth() == self.depth {
                    self.correlated_indices.push(correlated_input_ref.index());
                    correlated_input_ref.set_correlated_id(self.correlated_id);
                }
            }

            fn visit_subquery(&mut self, subquery: &mut Subquery) {
                use crate::binder::BoundSetExpr;

                self.depth += 1;
                match &mut subquery.query.body {
                    BoundSetExpr::Select(select) => {
                        select.exprs_mut().for_each(|expr| self.visit_expr(expr))
                    }
                    BoundSetExpr::Values(values) => {
                        values.exprs_mut().for_each(|expr| self.visit_expr(expr))
                    }
                }
                self.depth -= 1;
            }
        }

        let mut collector = Collector {
            depth,
            correlated_indices: vec![],
            correlated_id,
        };
        collector.visit_expr(self);
        collector.correlated_indices
    }

    /// Checks whether this is a constant expr that can be evaluated over a dummy chunk.
    /// Equivalent to `!has_input_ref && !has_agg_call && !has_subquery &&
    /// !has_correlated_input_ref` but checks them in one pass.
    pub fn is_const(&self) -> bool {
        struct Has {
            has: bool,
        }
        impl ExprVisitor<()> for Has {
            fn visit_expr(&mut self, expr: &ExprImpl) {
                match expr {
                    ExprImpl::Literal(_inner) => {}
                    ExprImpl::FunctionCall(inner) => self.visit_function_call(inner),
                    _ => self.has = true,
                }
            }
        }
        let mut visitor = Has { has: false };
        visitor.visit_expr(self);
        !visitor.has
    }

    /// Returns the `InputRefs` of an Equality predicate if it matches
    /// ordered by the canonical ordering (lower, higher), else returns None
    pub fn as_eq_cond(&self) -> Option<(InputRef, InputRef)> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.get_expr_type() == ExprType::Equal
            && let (_, ExprImpl::InputRef(x), ExprImpl::InputRef(y)) = function_call.clone().decompose_as_binary()
        {
            if x.index() < y.index() {
                Some((*x, *y))
            } else {
                Some((*y, *x))
            }
        } else {
            None
        }
    }

    pub fn as_is_not_distinct_from_cond(&self) -> Option<(InputRef, InputRef)> {
        if let ExprImpl::FunctionCall(function_call) = self
            && function_call.get_expr_type() == ExprType::IsNotDistinctFrom
            && let (_, ExprImpl::InputRef(x), ExprImpl::InputRef(y)) = function_call.clone().decompose_as_binary()
        {
            if x.index() < y.index() {
                Some((*x, *y))
            } else {
                Some((*y, *x))
            }
        } else {
            None
        }
    }

    pub fn as_comparison_cond(&self) -> Option<(InputRef, ExprType, InputRef)> {
        fn reverse_comparison(comparison: ExprType) -> ExprType {
            match comparison {
                ExprType::LessThan => ExprType::GreaterThan,
                ExprType::LessThanOrEqual => ExprType::GreaterThanOrEqual,
                ExprType::GreaterThan => ExprType::LessThan,
                ExprType::GreaterThanOrEqual => ExprType::LessThanOrEqual,
                _ => unreachable!(),
            }
        }

        if let ExprImpl::FunctionCall(function_call) = self {
            match function_call.get_expr_type() {
                ty @ (ExprType::LessThan
                | ExprType::LessThanOrEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual) => {
                    let (_, op1, op2) = function_call.clone().decompose_as_binary();
                    if let (ExprImpl::InputRef(x), ExprImpl::InputRef(y)) = (op1, op2) {
                        if x.index < y.index {
                            Some((*x, ty, *y))
                        } else {
                            Some((*y, reverse_comparison(ty), *x))
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn as_eq_const(&self) -> Option<(InputRef, ExprImpl)> {
        if let ExprImpl::FunctionCall(function_call) = self &&
        function_call.get_expr_type() == ExprType::Equal{
            match function_call.clone().decompose_as_binary() {
                (_, ExprImpl::InputRef(x), y) if y.is_const() => Some((*x, y)),
                (_, x, ExprImpl::InputRef(y)) if x.is_const() => Some((*y, x)),
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn as_comparison_const(&self) -> Option<(InputRef, ExprType, ExprImpl)> {
        fn reverse_comparison(comparison: ExprType) -> ExprType {
            match comparison {
                ExprType::LessThan => ExprType::GreaterThan,
                ExprType::LessThanOrEqual => ExprType::GreaterThanOrEqual,
                ExprType::GreaterThan => ExprType::LessThan,
                ExprType::GreaterThanOrEqual => ExprType::LessThanOrEqual,
                _ => unreachable!(),
            }
        }

        if let ExprImpl::FunctionCall(function_call) = self {
            match function_call.get_expr_type() {
                ty @ (ExprType::LessThan
                | ExprType::LessThanOrEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual) => {
                    let (_, op1, op2) = function_call.clone().decompose_as_binary();
                    match (op1, op2) {
                        (ExprImpl::InputRef(x), y) if y.is_const() => Some((*x, ty, y)),
                        (x, ExprImpl::InputRef(y)) if x.is_const() => {
                            Some((*y, reverse_comparison(ty), x))
                        }
                        _ => None,
                    }
                }
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn as_in_const_list(&self) -> Option<(InputRef, Vec<ExprImpl>)> {
        if let ExprImpl::FunctionCall(function_call) = self &&
        function_call.get_expr_type() == ExprType::In {
            let mut inputs = function_call.inputs().iter().cloned();
            let input_ref= match inputs.next().unwrap() {
                ExprImpl::InputRef(i) => *i,
                _ => { return None }
            };
            let list: Vec<_> = inputs.map(|expr|{
                // Non constant IN will be bound to OR
                assert!(expr.is_const());
                expr
            }).collect();

           Some((input_ref, list))
        } else {
            None
        }
    }

    pub fn as_or_disjunctions(&self) -> Option<Vec<ExprImpl>> {
        if let ExprImpl::FunctionCall(function_call) = self &&
            function_call.get_expr_type() == ExprType::Or {
            Some(to_disjunctions(self.clone()))
        } else {
            None
        }
    }

    pub fn to_project_set_select_item_proto(&self) -> ProjectSetSelectItem {
        use risingwave_pb::expr::project_set_select_item::SelectItem::*;

        ProjectSetSelectItem {
            select_item: Some(match self {
                ExprImpl::TableFunction(tf) => TableFunction(tf.to_protobuf()),
                expr => Expr(expr.to_expr_proto()),
            }),
        }
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
            ExprImpl::TableFunction(expr) => expr.return_type(),
        }
    }

    fn to_expr_proto(&self) -> ExprNode {
        match self {
            ExprImpl::InputRef(e) => e.to_expr_proto(),
            ExprImpl::Literal(e) => e.to_expr_proto(),
            ExprImpl::FunctionCall(e) => e.to_expr_proto(),
            ExprImpl::AggCall(e) => e.to_expr_proto(),
            ExprImpl::Subquery(e) => e.to_expr_proto(),
            ExprImpl::CorrelatedInputRef(e) => e.to_expr_proto(),
            ExprImpl::TableFunction(_e) => {
                unreachable!("Table function should not be converted to ExprNode")
            }
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

impl From<TableFunction> for ExprImpl {
    fn from(tf: TableFunction) -> Self {
        ExprImpl::TableFunction(Box::new(tf))
    }
}

impl From<Condition> for ExprImpl {
    fn from(c: Condition) -> Self {
        merge_expr_by_binary(
            c.conjunctions.into_iter(),
            ExprType::And,
            ExprImpl::literal_bool(true),
        )
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
                Self::TableFunction(arg0) => f.debug_tuple("TableFunction").field(arg0).finish(),
            };
        }
        match self {
            Self::InputRef(x) => write!(f, "{:?}", x),
            Self::Literal(x) => write!(f, "{:?}", x),
            Self::FunctionCall(x) => write!(f, "{:?}", x),
            Self::AggCall(x) => write!(f, "{:?}", x),
            Self::Subquery(x) => write!(f, "{:?}", x),
            Self::CorrelatedInputRef(x) => write!(f, "{:?}", x),
            Self::TableFunction(x) => write!(f, "{:?}", x),
        }
    }
}

pub struct ExprDisplay<'a> {
    pub expr: &'a ExprImpl,
    pub input_schema: &'a Schema,
}

impl std::fmt::Debug for ExprDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let that = self.expr;
        match that {
            ExprImpl::InputRef(x) => write!(
                f,
                "{:?}",
                InputRefDisplay {
                    input_ref: x,
                    input_schema: self.input_schema
                }
            ),
            ExprImpl::Literal(x) => write!(f, "{:?}", x),
            ExprImpl::FunctionCall(x) => write!(
                f,
                "{:?}",
                FunctionCallDisplay {
                    function_call: x,
                    input_schema: self.input_schema
                }
            ),
            ExprImpl::AggCall(x) => write!(f, "{:?}", x),
            ExprImpl::Subquery(x) => write!(f, "{:?}", x),
            ExprImpl::CorrelatedInputRef(x) => write!(f, "{:?}", x),
            ExprImpl::TableFunction(x) => {
                // TODO: TableFunctionCallVerboseDisplay
                write!(f, "{:?}", x)
            }
        }
    }
}

impl std::fmt::Display for ExprDisplay<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        (self as &dyn std::fmt::Debug).fmt(f)
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
use risingwave_common::catalog::Schema;

use crate::expr::expr_mutator::ExprMutator;
use crate::utils::Condition;

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
