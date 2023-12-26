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

//! Expressions in RisingWave.
//!
//! All expressions are implemented under the [`Expression`] trait.
//!
//! ## Construction
//!
//! Expressions can be constructed by [`build_func()`] function, which returns a
//! [`BoxedExpression`].
//!
//! They can also be transformed from the prost [`ExprNode`] using the [`build_from_prost()`]
//! function.
//!
//! ## Evaluation
//!
//! Expressions can be evaluated using the [`eval`] function.
//!
//! [`ExprNode`]: risingwave_pb::expr::ExprNode
//! [`eval`]: Expression::eval

// These modules define concrete expression structures.
mod and_or;
mod expr_input_ref;
mod expr_literal;
mod expr_some_all;
pub(crate) mod expr_udf;
pub(crate) mod wrapper;

mod build;
pub mod test_utils;
mod value;

use futures_util::TryFutureExt;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};

pub use self::build::*;
pub use self::expr_input_ref::InputRefExpression;
pub use self::expr_literal::LiteralExpression;
pub use self::value::{ValueImpl, ValueRef};
pub use self::wrapper::*;
pub use super::{ExprError, Result};

/// Interface of an expression.
///
/// There're two functions to evaluate an expression: `eval` and `eval_v2`, exactly one of them
/// should be implemented. Prefer calling and implementing `eval_v2` instead of `eval` if possible,
/// to gain the performance benefit of scalar expression.
#[async_trait::async_trait]
#[auto_impl::auto_impl(&, Box)]
pub trait Expression: std::fmt::Debug + Sync + Send {
    /// Get the return data type.
    fn return_type(&self) -> DataType;

    /// Evaluate the expression in vectorized execution. Returns an array.
    ///
    /// The default implementation calls `eval_v2` and always converts the result to an array.
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let value = self.eval_v2(input).await?;
        Ok(match value {
            ValueImpl::Array(array) => array,
            ValueImpl::Scalar { value, capacity } => {
                let mut builder = self.return_type().create_array_builder(capacity);
                builder.append_n(capacity, value);
                builder.finish().into()
            }
        })
    }

    /// Evaluate the expression in vectorized execution. Returns a value that can be either an
    /// array, or a scalar if all values in the array are the same.
    ///
    /// The default implementation calls `eval` and puts the result into the `Array` variant.
    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        self.eval(input).map_ok(ValueImpl::Array).await
    }

    /// Evaluate the expression in row-based execution. Returns a nullable scalar.
    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum>;

    /// Evaluate if the expression is constant.
    fn eval_const(&self) -> Result<Datum> {
        Err(ExprError::NotConstant)
    }

    /// Get the index if the expression is an `InputRef`.
    fn input_ref_index(&self) -> Option<usize> {
        None
    }
}

/// An owned dynamically typed [`Expression`].
pub type BoxedExpression = Box<dyn Expression>;

/// Extension trait for boxing expressions.
///
/// This is not directly made into [`Expression`] trait because...
/// - an expression does not have to be `'static`,
/// - and for the ease of `auto_impl`.
#[easy_ext::ext(ExpressionBoxExt)]
impl<E: Expression + 'static> E {
    /// Wrap the expression in a Box.
    pub fn boxed(self) -> BoxedExpression {
        Box::new(self)
    }
}

/// An type-safe wrapper that indicates the inner expression can be evaluated in a non-strict
/// manner, i.e., developers can directly call `eval_infallible` and `eval_row_infallible` without
/// checking the result.
///
/// This is usually created by non-strict build functions like [`crate::expr::build_non_strict_from_prost`]
/// and [`crate::expr::build_func_non_strict`]. It can also be created directly by
/// [`NonStrictExpression::new_topmost`], where only the evaluation of the topmost level expression
/// node is non-strict and should be treated as a TODO.
///
/// Compared to [`crate::expr::wrapper::non_strict::NonStrict`], this is more like an indicator
/// applied on the root of an expression tree, while the latter is a wrapper that can be applied on
/// each node of the tree and actually changes the behavior. As a result, [`NonStrictExpression`]
/// does not implement [`Expression`] trait and instead deals directly with developers.
#[derive(Debug)]
pub struct NonStrictExpression<E = BoxedExpression>(E);

impl<E> NonStrictExpression<E>
where
    E: Expression,
{
    /// Create a non-strict expression directly wrapping the given expression.
    ///
    /// Should only be used in tests as evaluation may panic.
    pub fn for_test(inner: E) -> NonStrictExpression
    where
        E: 'static,
    {
        NonStrictExpression(inner.boxed())
    }

    /// Create a non-strict expression from the given expression, where only the evaluation of the
    /// topmost level expression node is non-strict (which is subtly different from
    /// [`crate::expr::build_non_strict_from_prost`] where every node is non-strict).
    ///
    /// This should be used as a TODO.
    pub fn new_topmost(
        inner: E,
        error_report: impl EvalErrorReport,
    ) -> NonStrictExpression<impl Expression> {
        let inner = wrapper::non_strict::NonStrict::new(inner, error_report);
        NonStrictExpression(inner)
    }

    /// Get the return data type.
    pub fn return_type(&self) -> DataType {
        self.0.return_type()
    }

    /// Evaluate the expression in vectorized execution and assert it succeeds. Returns an array.
    ///
    /// Use with expressions built in non-strict mode.
    pub async fn eval_infallible(&self, input: &DataChunk) -> ArrayRef {
        self.0.eval(input).await.expect("evaluation failed")
    }

    /// Evaluate the expression in row-based execution and assert it succeeds. Returns a nullable
    /// scalar.
    ///
    /// Use with expressions built in non-strict mode.
    pub async fn eval_row_infallible(&self, input: &OwnedRow) -> Datum {
        self.0.eval_row(input).await.expect("evaluation failed")
    }

    /// Unwrap the inner expression.
    pub fn into_inner(self) -> E {
        self.0
    }

    /// Get a reference to the inner expression.
    pub fn inner(&self) -> &E {
        &self.0
    }
}

/// An optional context that can be used in a function.
///
/// # Example
/// ```ignore
/// #[function("foo(int4) -> int8")]
/// fn foo(a: i32, ctx: &Context) -> i64 {
///    assert_eq!(ctx.arg_types[0], DataType::Int32);
///    assert_eq!(ctx.return_type, DataType::Int64);
///    // ...
/// }
/// ```
#[derive(Debug)]
pub struct Context {
    pub arg_types: Vec<DataType>,
    pub return_type: DataType,
}
