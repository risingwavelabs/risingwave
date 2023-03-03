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
mod expr_array_to_string;
mod expr_array_distinct;
mod expr_binary_bytes;
mod expr_binary_nonnull;
mod expr_binary_nullable;
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
mod expr_quaternary_bytes;
pub mod expr_regexp;
mod expr_some_all;
mod expr_ternary_bytes;
mod expr_to_char_const_tmpl;
mod expr_to_timestamp_const_tmpl;
mod expr_udf;
mod expr_unary;
mod expr_vnode;

mod agg;
mod build_expr_from_prost;
pub(crate) mod data_types;
mod template;
mod template_fast;
pub mod test_utils;

use std::sync::Arc;

use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};

pub use self::agg::AggKind;
pub use self::build_expr_from_prost::build_from_prost;
pub use self::expr_binary_nonnull::new_binary_expr;
pub use self::expr_input_ref::InputRefExpression;
pub use self::expr_literal::LiteralExpression;
pub use self::expr_unary::new_unary_expr;
use super::Result;

/// Instance of an expression
pub trait Expression: std::fmt::Debug + Sync + Send {
    /// Get the return data type.
    fn return_type(&self) -> DataType;

    /// Eval the result with extra checks.
    fn eval_checked(&self, input: &DataChunk) -> Result<ArrayRef> {
        let res = self.eval(input)?;

        // TODO: Decide to use assert or debug_assert by benchmarks.
        assert_eq!(res.len(), input.capacity());

        Ok(res)
    }

    /// Evaluate the expression
    ///
    /// # Arguments
    ///
    /// * `input` - input data of the Project Executor
    fn eval(&self, input: &DataChunk) -> Result<ArrayRef>;

    /// Evaluate the expression in row-based execution.
    fn eval_row(&self, input: &OwnedRow) -> Result<Datum>;

    /// Wrap the expression in a Box.
    fn boxed(self) -> BoxedExpression
    where
        Self: Sized + Send + 'static,
    {
        Box::new(self)
    }
}

/// An owned dynamically typed [`Expression`].
pub type BoxedExpression = Box<dyn Expression>;

/// A reference to a dynamically typed [`Expression`].
pub type ExpressionRef = Arc<dyn Expression>;
