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
//! Expressions can be constructed by functions like [`new_binary_expr`],
//! which returns a [`BoxedExpression`].
//!
//! They can also be transformed from the prost [`ExprNode`] using the [`build_from_prost`]
//! function.
//!
//! ## Evaluation
//!
//! Expressions can be evaluated using the [`eval`] function.
//!
//! [`ExprNode`]: risingwave_pb::expr::ExprNode
//! [`eval`]: Expression::eval

// These modules define concrete expression structures.
mod expr_array_concat;
mod expr_array_distinct;
mod expr_array_length;
mod expr_array_to_string;
mod expr_binary_nonnull;
mod expr_binary_nullable;
mod expr_cardinality;
mod expr_case;
mod expr_coalesce;
mod expr_concat_ws;
mod expr_field;
mod expr_in;
mod expr_input_ref;
mod expr_is_null;
mod expr_jsonb_access;
mod expr_literal;
mod expr_nested_construct;
mod expr_now;
mod expr_proctime;
pub mod expr_regexp;
mod expr_some_all;
mod expr_to_char_const_tmpl;
mod expr_to_timestamp_const_tmpl;
mod expr_udf;
mod expr_unary;
mod expr_vnode;

mod agg;
mod build;
pub(crate) mod data_types;
pub(crate) mod template;
pub(crate) mod template_fast;
pub mod test_utils;
mod value;

use std::sync::Arc;

use futures_util::TryFutureExt;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum};
use risingwave_common::util::epoch::Epoch;
use static_assertions::const_assert;

pub use self::agg::AggKind;
pub use self::build::*;
pub use self::expr_input_ref::InputRefExpression;
pub use self::expr_literal::LiteralExpression;
pub use self::value::{ValueImpl, ValueRef};
use super::{ExprError, Result};

/// Interface of an expression.
///
/// There're two functions to evaluate an expression: `eval` and `eval_v2`, exactly one of them
/// should be implemented. Prefer calling and implementing `eval_v2` instead of `eval` if possible,
/// to gain the performance benefit of scalar expression.
#[async_trait::async_trait]
pub trait Expression: std::fmt::Debug + Sync + Send {
    /// Get the return data type.
    fn return_type(&self) -> DataType;

    /// Eval the result with extra checks.
    async fn eval_checked(&self, input: &DataChunk) -> Result<ArrayRef> {
        let res = self.eval(input).await?;

        // TODO: Decide to use assert or debug_assert by benchmarks.
        assert_eq!(res.len(), input.capacity());

        Ok(res)
    }

    /// Evaluate the expression in vectorized execution. Returns an array.
    ///
    /// The default implementation calls `eval_v2` and always converts the result to an array.
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        let value = self.eval_v2(input).await?;
        Ok(match value {
            ValueImpl::Array(array) => array,
            ValueImpl::Scalar { value, capacity } => {
                let mut builder = self.return_type().create_array_builder(capacity);
                builder.append_datum_n(capacity, value);
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

    /// Wrap the expression in a Box.
    fn boxed(self) -> BoxedExpression
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

impl dyn Expression {
    pub async fn eval_infallible(&self, input: &DataChunk, on_err: impl Fn(ExprError)) -> ArrayRef {
        const_assert!(!STRICT_MODE);

        if let Ok(array) = self.eval(input).await {
            return array;
        }

        // When eval failed, recompute in row-based execution
        // and pad with NULL for each failed row.
        let mut array_builder = self.return_type().create_array_builder(input.cardinality());
        for row in input.rows_with_holes() {
            if let Some(row) = row {
                let datum = self
                    .eval_row_infallible(&row.into_owned_row(), &on_err)
                    .await;
                array_builder.append_datum(&datum);
            } else {
                array_builder.append_null();
            }
        }
        Arc::new(array_builder.finish())
    }

    pub async fn eval_row_infallible(&self, input: &OwnedRow, on_err: impl Fn(ExprError)) -> Datum {
        const_assert!(!STRICT_MODE);

        self.eval_row(input).await.unwrap_or_else(|err| {
            on_err(err);
            None
        })
    }
}

/// An owned dynamically typed [`Expression`].
pub type BoxedExpression = Box<dyn Expression>;

/// A reference to a dynamically typed [`Expression`].
pub type ExpressionRef = Arc<dyn Expression>;

/// Controls the behavior when a compute error happens.
///
/// - If set to `false`, `NULL` will be inserted.
/// - TODO: If set to `true`, The MV will be suspended and removed from further checkpoints. It can
///   still be used to serve outdated data without corruption.
///
/// See also <https://github.com/risingwavelabs/risingwave/issues/4625>.
#[allow(dead_code)]
const STRICT_MODE: bool = false;

/// The context used by expressions.
#[derive(Clone)]
pub struct ExprContext {
    /// The epoch that an executor currently in.
    curr_epoch: Epoch,
}

impl ExprContext {
    pub fn new(curr_epoch: Epoch) -> Self {
        Self { curr_epoch }
    }

    pub fn get_unix_micro(&self) -> u64 {
        self.curr_epoch.as_unix_millis() * 1000
    }
}

tokio::task_local! {
    pub static CONTEXT: ExprContext;
}
