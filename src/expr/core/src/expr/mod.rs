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
//! All expressions are implemented under the [`SyncExpression`] or [`AsyncExpression`] trait.
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
//! [`eval`]: BoxedExpression::eval

#[macro_export]
macro_rules! forward {
    (sync, $expr:expr, $method:ident($($arg:expr),* $(,)?)) => {
        ($expr).$method($($arg),*)
    };
    (async, $expr:expr, $method:ident($($arg:expr),* $(,)?)) => {
        ($expr).$method($($arg),*).await
    };
}

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

use std::future::Future;
use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};

pub use self::build::*;
pub use self::expr_input_ref::InputRefExpression;
pub use self::expr_literal::LiteralExpression;
pub use self::value::{ValueImpl, ValueRef};
pub use self::wrapper::*;
pub use super::{ExprError, Result};

/// Common metadata of an expression.
#[auto_impl::auto_impl(&, Box, Arc)]
pub trait ExpressionInfo: std::fmt::Debug + Sync + Send {
    /// Get the return data type.
    fn return_type(&self) -> DataType;

    /// Get the index if the expression is an `InputRef`.
    fn input_ref_index(&self) -> Option<usize> {
        None
    }
}

/// Interface of a synchronous expression.
///
/// There're two functions to evaluate an expression: `eval` and `eval_v2`, exactly one of them
/// should be implemented. Prefer calling and implementing `eval_v2` instead of `eval` if possible,
/// to gain the performance benefit of scalar expression.
#[auto_impl::auto_impl(&, Box, Arc)]
pub trait SyncExpression: ExpressionInfo {
    /// Evaluate the expression in vectorized execution. Returns an array.
    ///
    /// The default implementation calls `eval_v2` and always converts the result to an array.
    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let value = self.eval_v2(input)?;
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
    fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        self.eval(input).map(ValueImpl::Array)
    }

    /// Evaluate the expression in row-based execution. Returns a nullable scalar.
    fn eval_row(&self, input: &OwnedRow) -> Result<Datum>;

    /// Evaluate if the expression is constant.
    fn eval_const(&self) -> Result<Datum> {
        Err(ExprError::NotConstant)
    }
}

/// Interface of an asynchronous expression.
pub trait AsyncExpression: ExpressionInfo {
    /// Evaluate the expression in vectorized execution. Returns an array.
    ///
    /// The default implementation calls `eval_v2` and always converts the result to an array.
    fn eval<'a>(
        &'a self,
        input: &'a DataChunk,
    ) -> impl Future<Output = Result<ArrayRef>> + Send + 'a {
        async move {
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
    }

    /// Evaluate the expression in vectorized execution. Returns a value that can be either an
    /// array, or a scalar if all values in the array are the same.
    ///
    /// The default implementation calls `eval` and puts the result into the `Array` variant.
    fn eval_v2<'a>(
        &'a self,
        input: &'a DataChunk,
    ) -> impl Future<Output = Result<ValueImpl>> + Send + 'a {
        async move { self.eval(input).await.map(ValueImpl::Array) }
    }

    /// Evaluate the expression in row-based execution. Returns a nullable scalar.
    fn eval_row<'a>(
        &'a self,
        input: &'a OwnedRow,
    ) -> impl Future<Output = Result<Datum>> + Send + 'a;
}

/// Object-safe adapter for asynchronous expressions.
#[async_trait::async_trait]
pub trait AsyncDynExpression: ExpressionInfo {
    /// Evaluate the expression in vectorized execution. Returns an array.
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef>;

    /// Evaluate the expression in vectorized execution. Returns a value that can be either an
    /// array, or a scalar if all values in the array are the same.
    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl>;

    /// Evaluate the expression in row-based execution. Returns a nullable scalar.
    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum>;
}

#[async_trait::async_trait]
impl<E> AsyncDynExpression for E
where
    E: AsyncExpression,
{
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        AsyncExpression::eval(self, input).await
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        AsyncExpression::eval_v2(self, input).await
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        AsyncExpression::eval_row(self, input).await
    }
}

impl AsyncExpression for Arc<dyn AsyncDynExpression> {
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        AsyncDynExpression::eval(self.as_ref(), input).await
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        AsyncDynExpression::eval_v2(self.as_ref(), input).await
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        AsyncDynExpression::eval_row(self.as_ref(), input).await
    }
}

/// An owned dynamically typed expression.
#[derive(Clone, Debug)]
pub enum BoxedExpression {
    Sync(Arc<dyn SyncExpression>),
    Async(Arc<dyn AsyncDynExpression>),
}

/// Try to unwrap boxed expressions into sync expressions.
///
/// Returns the original boxed expression list if any expression is async.
pub fn try_into_sync_exprs(
    exprs: Vec<BoxedExpression>,
) -> std::result::Result<Vec<Arc<dyn SyncExpression>>, Vec<BoxedExpression>> {
    try_convert_all(
        exprs,
        |expr| match expr {
            BoxedExpression::Sync(expr) => Ok(expr),
            expr @ BoxedExpression::Async(_) => Err(expr),
        },
        BoxedExpression::Sync,
    )
}

/// Try to convert all items in a vector, or return the original vector if any conversion fails.
pub fn try_convert_all<T, U>(
    items: Vec<T>,
    mut try_convert: impl FnMut(T) -> std::result::Result<U, T>,
    recover: impl Fn(U) -> T,
) -> std::result::Result<Vec<U>, Vec<T>> {
    let mut converted = Vec::with_capacity(items.len());
    let mut items = items.into_iter();
    loop {
        let Some(item) = items.next() else {
            return Ok(converted);
        };
        match try_convert(item) {
            Ok(item) => converted.push(item),
            Err(item) => {
                let items = converted
                    .into_iter()
                    .map(recover)
                    .chain(std::iter::once(item))
                    .chain(items)
                    .collect();
                return Err(items);
            }
        }
    }
}

impl BoxedExpression {
    /// Get the return data type.
    pub fn return_type(&self) -> DataType {
        match self {
            Self::Sync(expr) => expr.return_type(),
            Self::Async(expr) => expr.return_type(),
        }
    }

    /// Get the index if the expression is an `InputRef`.
    pub fn input_ref_index(&self) -> Option<usize> {
        match self {
            Self::Sync(expr) => expr.input_ref_index(),
            Self::Async(expr) => expr.input_ref_index(),
        }
    }

    /// Evaluate if the expression is constant.
    pub fn eval_const(&self) -> Result<Datum> {
        match self {
            Self::Sync(expr) => expr.eval_const(),
            Self::Async(_) => Err(ExprError::NotConstant),
        }
    }

    /// Evaluate the expression in vectorized execution. Returns an array.
    pub async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        match self {
            Self::Sync(expr) => expr.eval(input),
            Self::Async(expr) => AsyncDynExpression::eval(expr.as_ref(), input).await,
        }
    }

    /// Evaluate the expression in vectorized execution. Returns a value that can be either an
    /// array, or a scalar if all values in the array are the same.
    pub async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        match self {
            Self::Sync(expr) => expr.eval_v2(input),
            Self::Async(expr) => AsyncDynExpression::eval_v2(expr.as_ref(), input).await,
        }
    }

    /// Evaluate the expression in row-based execution. Returns a nullable scalar.
    pub async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        match self {
            Self::Sync(expr) => expr.eval_row(input),
            Self::Async(expr) => AsyncDynExpression::eval_row(expr.as_ref(), input).await,
        }
    }
}

impl<E> From<E> for BoxedExpression
where
    E: SyncExpression + 'static,
{
    fn from(expr: E) -> Self {
        Self::Sync(Arc::new(expr))
    }
}

impl ExpressionInfo for BoxedExpression {
    fn return_type(&self) -> DataType {
        self.return_type()
    }

    fn input_ref_index(&self) -> Option<usize> {
        self.input_ref_index()
    }
}

impl AsyncExpression for BoxedExpression {
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        match self {
            Self::Sync(expr) => expr.eval(input),
            Self::Async(expr) => AsyncDynExpression::eval(expr.as_ref(), input).await,
        }
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        match self {
            Self::Sync(expr) => expr.eval_v2(input),
            Self::Async(expr) => AsyncDynExpression::eval_v2(expr.as_ref(), input).await,
        }
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        match self {
            Self::Sync(expr) => expr.eval_row(input),
            Self::Async(expr) => expr.as_ref().eval_row(input).await,
        }
    }
}

/// Extension trait for boxing expressions.
///
/// This is not directly made into expression traits because...
/// - an expression does not have to be `'static`,
/// - and for the ease of `auto_impl`.
#[easy_ext::ext(SyncExpressionBoxExt)]
impl<E: SyncExpression + 'static> E {
    /// Wrap the expression in a [`BoxedExpression::Sync`].
    pub fn boxed(self) -> BoxedExpression {
        BoxedExpression::Sync(Arc::new(self))
    }
}

/// Extension trait for boxing async expressions.
#[easy_ext::ext(AsyncExpressionBoxExt)]
impl<E: AsyncExpression + 'static> E {
    /// Wrap the expression in a [`BoxedExpression::Async`].
    pub fn boxed(self) -> BoxedExpression {
        BoxedExpression::Async(Arc::new(self))
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
/// does not implement expression traits and instead deals directly with developers.
#[derive(Debug)]
pub enum NonStrictExpression {
    Sync(Arc<dyn SyncExpression>),
    Async(Arc<dyn AsyncDynExpression>),
}

impl NonStrictExpression {
    /// Create a non-strict expression directly wrapping the given expression.
    ///
    /// Should only be used in tests as evaluation may panic.
    pub fn for_test(inner: impl Into<BoxedExpression>) -> NonStrictExpression {
        match inner.into() {
            BoxedExpression::Sync(inner) => Self::Sync(inner),
            BoxedExpression::Async(inner) => Self::Async(inner),
        }
    }

    /// Create a non-strict expression from the given expression, where only the evaluation of the
    /// topmost level expression node is non-strict (which is subtly different from
    /// [`crate::expr::build_non_strict_from_prost`] where every node is non-strict).
    ///
    /// This should be used as a TODO.
    pub fn new_topmost(
        inner: impl Into<BoxedExpression>,
        error_report: impl EvalErrorReport + 'static,
    ) -> NonStrictExpression {
        match inner.into() {
            BoxedExpression::Sync(inner) => {
                let inner = wrapper::non_strict::NonStrict::new(inner, error_report);
                Self::Sync(Arc::new(inner))
            }
            BoxedExpression::Async(inner) => {
                let inner = wrapper::non_strict::NonStrict::new(inner, error_report);
                Self::Async(Arc::new(inner))
            }
        }
    }

    /// Get the return data type.
    pub fn return_type(&self) -> DataType {
        match self {
            Self::Sync(expr) => expr.return_type(),
            Self::Async(expr) => expr.return_type(),
        }
    }

    /// Evaluate the expression in vectorized execution and assert it succeeds. Returns an array.
    ///
    /// Use with expressions built in non-strict mode.
    pub async fn eval_infallible(&self, input: &DataChunk) -> ArrayRef {
        match self {
            Self::Sync(expr) => expr.eval(input),
            Self::Async(expr) => AsyncDynExpression::eval(expr.as_ref(), input).await,
        }
        .expect("evaluation failed")
    }

    /// Evaluate the expression in row-based execution and assert it succeeds. Returns a nullable
    /// scalar.
    ///
    /// Use with expressions built in non-strict mode.
    pub async fn eval_row_infallible(&self, input: &OwnedRow) -> Datum {
        match self {
            Self::Sync(expr) => expr.eval_row(input),
            Self::Async(expr) => AsyncDynExpression::eval_row(expr.as_ref(), input).await,
        }
        .expect("evaluation failed")
    }

    /// Unwrap the inner expression.
    pub fn into_inner(self) -> BoxedExpression {
        match self {
            Self::Sync(expr) => BoxedExpression::Sync(expr),
            Self::Async(expr) => BoxedExpression::Async(expr),
        }
    }

    /// Get a reference to the inner expression.
    pub fn inner(&self) -> &dyn ExpressionInfo {
        match self {
            Self::Sync(expr) => expr.as_ref(),
            Self::Async(expr) => expr.as_ref(),
        }
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
    /// Whether the function is variadic.
    pub variadic: bool,
}
