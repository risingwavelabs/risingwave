// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use enum_as_inner::EnumAsInner;
use fixedbitset::FixedBitSet;
use futures::FutureExt;
use paste::paste;
use risingwave_common::array::ListValue;
use risingwave_common::error::Result as RwResult;
use risingwave_common::types::{DataType, Datum, Scalar};
use risingwave_expr::expr::build_from_prost;
use risingwave_expr::function::aggregate::AggKind;
use risingwave_pb::expr::expr_node::RexNode;
use risingwave_pb::expr::{ExprNode, ProjectSetSelectItem};

mod agg_call;
mod correlated_input_ref;
mod function_call;
mod input_ref;
mod literal;
mod parameter;
mod pure;
mod subquery;
mod table_function;
mod user_defined_function;
mod window_function;

mod order_by_expr;
pub use order_by_expr::{OrderBy, OrderByExpr};

mod expr_mutator;
mod expr_rewriter;
mod expr_visitor;
mod session_timezone;
mod type_inference;
mod utils;

pub use agg_call::AggCall;
pub use correlated_input_ref::{CorrelatedId, CorrelatedInputRef, Depth};
pub use expr_mutator::ExprMutator;
pub use expr_rewriter::ExprRewriter;
pub use expr_visitor::ExprVisitor;
pub use function_call::{is_row_function, FunctionCall, FunctionCallDisplay};
pub use input_ref::{input_ref_to_column_indices, InputRef, InputRefDisplay};
pub use literal::Literal;
pub use parameter::Parameter;
pub use pure::*;
pub use risingwave_pb::expr::expr_node::Type as ExprType;
pub use session_timezone::SessionTimezone;
pub use subquery::{Subquery, SubqueryKind};
pub use table_function::{TableFunction, TableFunctionType};
pub use type_inference::{
    agg_func_sigs, align_types, cast_map_array, cast_ok, cast_sigs, func_sigs, infer_some_all,
    infer_type, least_restrictive, AggFuncSig, CastContext, CastSig, FuncSign,
};
pub use user_defined_function::UserDefinedFunction;
pub use utils::*;
pub use window_function::{WindowFunction, WindowFunctionType};

/// the trait of bound expressions
pub trait Expr: Into<ExprImpl> {
    /// Get the return type of the expr
    fn return_type(&self) -> DataType;

    /// Serialize the expression
    fn to_expr_proto(&self) -> ExprNode;
}

macro_rules! impl_expr_impl {
    ($($t:ident),*) => {
        #[derive(Clone, Eq, PartialEq, Hash, EnumAsInner)]
        pub enum ExprImpl {
            $($t(Box<$t>),)*
        }
        $(
        impl From<$t> for ExprImpl {
            fn from(o: $t) -> ExprImpl {
                ExprImpl::$t(Box::new(o))
            }
        })*
    };
}

impl_expr_impl!(
    // BoundColumnRef, might be used in binder.
    CorrelatedInputRef,
    InputRef,
    Literal,
    FunctionCall,
    AggCall,
    Subquery,
    TableFunction,
    WindowFunction,
    UserDefinedFunction,
    Parameter
);

impl ExprImpl {
    /// A literal int value.
    #[inline(always)]
    pub fn literal_int(v: i32) -> Self {
        Literal::new(Some(v.to_scalar_value()), DataType::Int32).into()
    }

    /// A literal float64 value.
    #[inline(always)]
    pub fn literal_f64(v: f64) -> Self {
        Literal::new(Some(v.into()), DataType::Float64).into()
    }

    /// A literal boolean value.
    #[inline(always)]
    pub fn literal_bool(v: bool) -> Self {
        Literal::new(Some(v.to_scalar_value()), DataType::Boolean).into()
    }

    /// A literal varchar value.
    #[inline(always)]
    pub fn literal_varchar(v: String) -> Self {
        Literal::new(Some(v.into()), DataType::Varchar).into()
    }

    /// A literal null value.
    #[inline(always)]
    pub fn literal_null(element_type: DataType) -> Self {
        Literal::new(None, element_type).into()
    }

    /// A literal list value.
    #[inline(always)]
    pub fn literal_list(v: ListValue, element_type: DataType) -> Self {
        Literal::new(
            Some(v.to_scalar_value()),
            DataType::List {
                datatype: Box::new(element_type),
            },
        )
        .into()
    }

    /// A `count(*)` aggregate function.
    #[inline(always)]
    pub fn count_star() -> Self {
        AggCall::new(
            AggKind::Count,
            vec![],
            false,
            OrderBy::any(),
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

    /// Check if the expression has no side effects and output is deterministic
    pub fn is_pure(&self) -> bool {
        is_pure(self)
    }

    pub fn is_impure(&self) -> bool {
        is_impure(self)
    }

    /// Count `Now`s in the expression.
    pub fn count_nows(&self) -> usize {
        let mut visitor = CountNow::default();
        visitor.visit_expr(self)
    }

    /// Check whether self is literal NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, ExprImpl::Literal(literal) if literal.get_data().is_none())
    }

    /// Check whether self is a literal NULL or literal string.
    pub fn is_unknown(&self) -> bool {
        matches!(self, ExprImpl::Literal(literal) if literal.return_type() == DataType::Varchar)
            || matches!(self, ExprImpl::Parameter(parameter) if !parameter.has_infer())
    }

    /// Shorthand to create cast expr to `target` type in implicit context.
    pub fn cast_implicit(self, target: DataType) -> Result<ExprImpl, CastError> {
        FunctionCall::new_cast(self, target, CastContext::Implicit)
    }

    /// Shorthand to create cast expr to `target` type in assign context.
    pub fn cast_assign(self, target: DataType) -> Result<ExprImpl, CastError> {
        FunctionCall::new_cast(self, target, CastContext::Assign)
    }

    /// Shorthand to create cast expr to `target` type in explicit context.
    pub fn cast_explicit(self, target: DataType) -> Result<ExprImpl, CastError> {
        FunctionCall::new_cast(self, target, CastContext::Explicit)
    }

    /// Shorthand to enforce implicit cast to boolean
    pub fn enforce_bool_clause(self, clause: &str) -> RwResult<ExprImpl> {
        if self.is_unknown() {
            let inner = self.cast_implicit(DataType::Boolean)?;
            return Ok(inner);
        }
        let return_type = self.return_type();
        if return_type != DataType::Boolean {
            bail!(
                "argument of {} must be boolean, not type {:?}",
                clause,
                return_type
            )
        }
        Ok(self)
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
    pub fn cast_output(self) -> RwResult<ExprImpl> {
        if self.return_type() == DataType::Boolean {
            return Ok(FunctionCall::new(ExprType::BoolOut, vec![self])?.into());
        }
        // Use normal cast for other types. Both `assign` and `explicit` can pass the castability
        // check and there is no difference.
        self.cast_assign(DataType::Varchar)
            .map_err(|err| err.into())
    }

    /// Evaluate the expression on the given input.
    ///
    /// TODO: This is a naive implementation. We should avoid proto ser/de.
    /// Tracking issue: <https://github.com/risingwavelabs/risingwave/issues/3479>
    async fn eval_row(&self, input: &OwnedRow) -> RwResult<Datum> {
        let backend_expr = build_from_prost(&self.to_expr_proto())?;
        Ok(backend_expr.eval_row(input).await?)
    }

    /// Evaluate a constant expression.
    pub fn eval_row_const(&self) -> RwResult<Datum> {
        assert!(self.is_const());
        self.eval_row(&OwnedRow::empty())
            .now_or_never()
            .expect("constant expression should not be async")
    }
}

/// Implement helper functions which recursively checks whether an variant is included in the
/// expression. e.g., `has_subquery(&self) -> bool`
///
/// It will not traverse inside subqueries.
macro_rules! impl_has_variant {
    ( $($variant:ty),* ) => {
        paste! {
            impl ExprImpl {
                $(
                    pub fn [<has_ $variant:snake>](&self) -> bool {
                        struct Has {}

                        impl ExprVisitor<bool> for Has {

                            fn merge(a: bool, b: bool) -> bool {
                                a | b
                            }

                            fn [<visit_ $variant:snake>](&mut self, _: &$variant) -> bool {
                                true
                            }
                        }

                        let mut visitor = Has {};
                        visitor.visit_expr(self)
                    }
                )*
            }
        }
    };
}

impl_has_variant! {InputRef, Literal, FunctionCall, AggCall, Subquery, TableFunction, WindowFunction}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InequalityInputPair {
    /// Input index of greater side of inequality.
    pub(crate) key_required_larger: usize,
    /// Input index of less side of inequality.
    pub(crate) key_required_smaller: usize,
    /// greater >= less + delta_expression
    pub(crate) delta_expression: Option<(ExprType, ExprImpl)>,
    /// Whether the inequality includes equality.
    pub(crate) can_equal: bool,
}

impl InequalityInputPair {
    fn new(
        key_required_larger: usize,
        key_required_smaller: usize,
        delta_expression: Option<(ExprType, ExprImpl)>,
        can_equal: bool,
    ) -> Self {
        Self {
            key_required_larger,
            key_required_smaller,
            delta_expression,
            can_equal,
        }
    }
}

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
    pub fn has_correlated_input_ref_by_depth(&self, depth: Depth) -> bool {
        struct Has {
            depth: usize,
        }

        impl ExprVisitor<bool> for Has {
            fn merge(a: bool, b: bool) -> bool {
                a | b
            }

            fn visit_correlated_input_ref(
                &mut self,
                correlated_input_ref: &CorrelatedInputRef,
            ) -> bool {
                correlated_input_ref.depth() == self.depth
            }

            fn visit_subquery(&mut self, subquery: &Subquery) -> bool {
                self.depth += 1;
                let has = self.visit_bound_set_expr(&subquery.query.body);
                self.depth -= 1;

                has
            }
        }

        impl Has {
            fn visit_bound_set_expr(&mut self, set_expr: &BoundSetExpr) -> bool {
                let mut has = false;
                match set_expr {
                    BoundSetExpr::Select(select) => {
                        select.exprs().for_each(|expr| has |= self.visit_expr(expr));
                        has |= match select.from.as_ref() {
                            Some(from) => from.is_correlated(self.depth),
                            None => false,
                        };
                    }
                    BoundSetExpr::Values(values) => {
                        values.exprs().for_each(|expr| has |= self.visit_expr(expr))
                    }
                    BoundSetExpr::Query(query) => {
                        self.depth += 1;
                        has = self.visit_bound_set_expr(&query.body);
                        self.depth -= 1;
                    }
                    BoundSetExpr::SetOperation { left, right, .. } => {
                        has |= self.visit_bound_set_expr(left);
                        has |= self.visit_bound_set_expr(right);
                    }
                };
                has
            }
        }

        let mut visitor = Has { depth };
        visitor.visit_expr(self)
    }

    pub fn has_correlated_input_ref_by_correlated_id(&self, correlated_id: CorrelatedId) -> bool {
        struct Has {
            correlated_id: CorrelatedId,
        }

        impl ExprVisitor<bool> for Has {
            fn merge(a: bool, b: bool) -> bool {
                a | b
            }

            fn visit_correlated_input_ref(
                &mut self,
                correlated_input_ref: &CorrelatedInputRef,
            ) -> bool {
                correlated_input_ref.correlated_id() == self.correlated_id
            }

            fn visit_subquery(&mut self, subquery: &Subquery) -> bool {
                self.visit_bound_set_expr(&subquery.query.body)
            }
        }

        impl Has {
            fn visit_bound_set_expr(&mut self, set_expr: &BoundSetExpr) -> bool {
                match set_expr {
                    BoundSetExpr::Select(select) => select
                        .exprs()
                        .map(|expr| self.visit_expr(expr))
                        .reduce(Self::merge)
                        .unwrap_or_default(),
                    BoundSetExpr::Values(values) => values
                        .exprs()
                        .map(|expr| self.visit_expr(expr))
                        .reduce(Self::merge)
                        .unwrap_or_default(),
                    BoundSetExpr::Query(query) => self.visit_bound_set_expr(&query.body),
                    BoundSetExpr::SetOperation { left, right, .. } => {
                        self.visit_bound_set_expr(left) | self.visit_bound_set_expr(right)
                    }
                }
            }
        }

        let mut visitor = Has { correlated_id };
        visitor.visit_expr(self)
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
                self.depth += 1;
                self.visit_bound_set_expr(&mut subquery.query.body);
                self.depth -= 1;
            }
        }

        impl Collector {
            fn visit_bound_set_expr(&mut self, set_expr: &mut BoundSetExpr) {
                match set_expr {
                    BoundSetExpr::Select(select) => {
                        select.exprs_mut().for_each(|expr| self.visit_expr(expr));
                        if let Some(from) = select.from.as_mut() {
                            self.correlated_indices.extend(
                                from.collect_correlated_indices_by_depth_and_assign_id(
                                    self.depth,
                                    self.correlated_id,
                                ),
                            );
                        };
                    }
                    BoundSetExpr::Values(values) => {
                        values.exprs_mut().for_each(|expr| self.visit_expr(expr))
                    }
                    BoundSetExpr::Query(query) => {
                        self.depth += 1;
                        self.visit_bound_set_expr(&mut query.body);
                        self.depth -= 1;
                    }
                    BoundSetExpr::SetOperation { left, right, .. } => {
                        self.visit_bound_set_expr(&mut *left);
                        self.visit_bound_set_expr(&mut *right);
                    }
                }
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
            fn merge(_: (), _: ()) {}

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

    pub fn reverse_comparison(comparison: ExprType) -> ExprType {
        match comparison {
            ExprType::LessThan => ExprType::GreaterThan,
            ExprType::LessThanOrEqual => ExprType::GreaterThanOrEqual,
            ExprType::GreaterThan => ExprType::LessThan,
            ExprType::GreaterThanOrEqual => ExprType::LessThanOrEqual,
            ExprType::Equal | ExprType::IsNotDistinctFrom => comparison,
            _ => unreachable!(),
        }
    }

    pub fn as_comparison_cond(&self) -> Option<(InputRef, ExprType, InputRef)> {
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
                            Some((*y, Self::reverse_comparison(ty), *x))
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

    /// Accepts expressions of the form `input_expr cmp now() [+- const_expr]` or
    /// `now() [+- const_expr] cmp input_expr`, where `input_expr` contains an
    /// `InputRef` and contains no `now()`.
    ///
    /// Canonicalizes to the first ordering and returns `(input_expr, cmp, now_expr)`
    pub fn as_now_comparison_cond(&self) -> Option<(ExprImpl, ExprType, ExprImpl)> {
        if let ExprImpl::FunctionCall(function_call) = self {
            match function_call.get_expr_type() {
                ty @ (ExprType::LessThan
                | ExprType::LessThanOrEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual) => {
                    let (_, op1, op2) = function_call.clone().decompose_as_binary();
                    if op1.count_nows() == 0
                        && op1.has_input_ref()
                        && op2.count_nows() > 0
                        && op2.is_now_offset()
                    {
                        Some((op1, ty, op2))
                    } else if op2.count_nows() == 0
                        && op2.has_input_ref()
                        && op1.count_nows() > 0
                        && op1.is_now_offset()
                    {
                        Some((op2, Self::reverse_comparison(ty), op1))
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

    /// Accepts expressions of the form `InputRef cmp InputRef [+- const_expr]` or
    /// `InputRef [+- const_expr] cmp InputRef`.
    pub(crate) fn as_input_comparison_cond(&self) -> Option<InequalityInputPair> {
        if let ExprImpl::FunctionCall(function_call) = self {
            match function_call.get_expr_type() {
                ty @ (ExprType::LessThan
                | ExprType::LessThanOrEqual
                | ExprType::GreaterThan
                | ExprType::GreaterThanOrEqual) => {
                    let can_equal =
                        matches!(ty, ExprType::LessThanOrEqual | ExprType::GreaterThanOrEqual);
                    let (_, mut op1, mut op2) = function_call.clone().decompose_as_binary();
                    if matches!(ty, ExprType::LessThan | ExprType::LessThanOrEqual) {
                        std::mem::swap(&mut op1, &mut op2);
                    }
                    if let (Some((lft_input, lft_offset)), Some((rht_input, rht_offset))) =
                        (op1.as_input_offset(), op2.as_input_offset())
                    {
                        match (lft_offset, rht_offset) {
                            (Some(_), Some(_)) => None,
                            (None, rht_offset @ Some(_)) => Some(InequalityInputPair::new(
                                lft_input, rht_input, rht_offset, can_equal,
                            )),
                            (Some((operator, operand)), None) => Some(InequalityInputPair::new(
                                lft_input,
                                rht_input,
                                Some((
                                    if operator == ExprType::Add {
                                        ExprType::Subtract
                                    } else {
                                        ExprType::Add
                                    },
                                    operand,
                                )),
                                can_equal,
                            )),
                            (None, None) => Some(InequalityInputPair::new(
                                lft_input, rht_input, None, can_equal,
                            )),
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

    /// Checks if expr is of the form `now() [+- const_expr]`
    fn is_now_offset(&self) -> bool {
        if let ExprImpl::FunctionCall(f) = self {
            match f.get_expr_type() {
                ExprType::Now => true,
                ExprType::Add | ExprType::Subtract => {
                    let (_, lhs, rhs) = f.clone().decompose_as_binary();
                    lhs.as_function_call()
                        .map(|f| f.get_expr_type() == ExprType::Now)
                        .unwrap_or(false)
                        && rhs.is_const()
                }
                _ => false,
            }
        } else {
            false
        }
    }

    /// Returns the `InputRef` and offset of a predicate if it matches
    /// the form `InputRef [+- const_expr]`, else returns None.
    fn as_input_offset(&self) -> Option<(usize, Option<(ExprType, ExprImpl)>)> {
        match self {
            ExprImpl::InputRef(input_ref) => Some((input_ref.index(), None)),
            ExprImpl::FunctionCall(function_call) => {
                let expr_type = function_call.get_expr_type();
                match expr_type {
                    ExprType::Add | ExprType::Subtract => {
                        let (_, mut lhs, mut rhs) = function_call.clone().decompose_as_binary();
                        if expr_type == ExprType::Add && lhs.is_const() {
                            std::mem::swap(&mut lhs, &mut rhs);
                        }
                        if let ExprImpl::InputRef(input_ref) = &lhs && rhs.is_const() {
                            Some((input_ref.index(), Some((expr_type, rhs))))
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub fn recursively_input_offset(&self) -> Option<usize> {
        let mut cur = self.clone();
        loop {
            match cur {
                ExprImpl::InputRef(input_ref) => return Some(input_ref.index()),
                ExprImpl::FunctionCall(function_call) => {
                    let expr_type = function_call.get_expr_type();
                    match expr_type {
                        ExprType::Add | ExprType::Subtract | ExprType::AtTimeZone => {
                            let (_, mut lhs, mut rhs) = function_call.clone().decompose_as_binary();
                            if expr_type == ExprType::Add && lhs.is_const() {
                                std::mem::swap(&mut lhs, &mut rhs);
                            }
                            if rhs.is_const() {
                                cur = lhs;
                            } else {
                                return None;
                            }
                        }
                        _ => return None,
                    }
                }
                _ => return None,
            }
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

    pub fn as_eq_correlated_input_ref(&self) -> Option<(InputRef, CorrelatedInputRef)> {
        if let ExprImpl::FunctionCall(function_call) = self &&
            function_call.get_expr_type() == ExprType::Equal{
            match function_call.clone().decompose_as_binary() {
                (_, ExprImpl::InputRef(x), ExprImpl::CorrelatedInputRef(y)) => Some((*x, *y)),
                (_, ExprImpl::CorrelatedInputRef(x), ExprImpl::InputRef(y)) => Some((*y, *x)),
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn as_is_null(&self) -> Option<InputRef> {
        if let ExprImpl::FunctionCall(function_call) = self &&
            function_call.get_expr_type() == ExprType::IsNull{
            match function_call.clone().decompose_as_unary() {
                (_, ExprImpl::InputRef(x)) => Some(*x),
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

    pub fn from_expr_proto(proto: &ExprNode) -> RwResult<Self> {
        let rex_node = proto.get_rex_node()?;
        let ret_type = proto.get_return_type()?.into();
        let expr_type = proto.get_expr_type()?;
        Ok(match rex_node {
            RexNode::InputRef(column_index) => Self::InputRef(Box::new(InputRef::from_expr_proto(
                *column_index as _,
                ret_type,
            )?)),
            RexNode::Constant(_) => Self::Literal(Box::new(Literal::from_expr_proto(proto)?)),
            RexNode::Udf(udf) => Self::UserDefinedFunction(Box::new(
                UserDefinedFunction::from_expr_proto(udf, ret_type)?,
            )),
            RexNode::FuncCall(function_call) => Self::FunctionCall(Box::new(
                FunctionCall::from_expr_proto(function_call, expr_type, ret_type)?,
            )),
        })
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
            ExprImpl::WindowFunction(expr) => expr.return_type(),
            ExprImpl::UserDefinedFunction(expr) => expr.return_type(),
            ExprImpl::Parameter(expr) => expr.return_type(),
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
            ExprImpl::WindowFunction(_e) => {
                unreachable!("Window function should not be converted to ExprNode")
            }
            ExprImpl::UserDefinedFunction(e) => e.to_expr_proto(),
            ExprImpl::Parameter(e) => e.to_expr_proto(),
        }
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
                Self::WindowFunction(arg0) => f.debug_tuple("WindowFunction").field(arg0).finish(),
                Self::UserDefinedFunction(arg0) => {
                    f.debug_tuple("UserDefinedFunction").field(arg0).finish()
                }
                Self::Parameter(arg0) => f.debug_tuple("Parameter").field(arg0).finish(),
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
            Self::WindowFunction(x) => write!(f, "{:?}", x),
            Self::UserDefinedFunction(x) => write!(f, "{:?}", x),
            Self::Parameter(x) => write!(f, "{:?}", x),
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
            ExprImpl::WindowFunction(x) => {
                // TODO: WindowFunctionCallVerboseDisplay
                write!(f, "{:?}", x)
            }
            ExprImpl::UserDefinedFunction(x) => write!(f, "{:?}", x),
            ExprImpl::Parameter(x) => write!(f, "{:?}", x),
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
use risingwave_common::bail;
use risingwave_common::catalog::Schema;
use risingwave_common::row::OwnedRow;

use self::function_call::CastError;
use crate::binder::BoundSetExpr;
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
