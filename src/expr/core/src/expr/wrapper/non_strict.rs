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

use async_trait::async_trait;
use auto_impl::auto_impl;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::row::{OwnedRow, Row};
use risingwave_common::types::{DataType, Datum};
use thiserror_ext::AsReport;

use crate::error::Result;
use crate::expr::{Expression, ValueImpl};
use crate::ExprError;

/// Report an error during evaluation.
#[auto_impl(&, Arc)]
pub trait EvalErrorReport: Clone + Send + Sync {
    /// Perform the error reporting.
    ///
    /// Called when an error occurs during row-level evaluation of a non-strict expression,
    /// that is, wrapped by [`NonStrict`].
    fn report(&self, error: ExprError);
}

/// A dummy implementation that panics when called.
///
/// Used as the type parameter for the expression builder when non-strict evaluation is not
/// required.
impl EvalErrorReport for ! {
    fn report(&self, _error: ExprError) {
        unreachable!()
    }
}

/// Log the error to report an error during evaluation.
#[derive(Clone)]
pub struct LogReport;

impl EvalErrorReport for LogReport {
    fn report(&self, error: ExprError) {
        tracing::error!(error=%error.as_report(), "failed to evaluate expression");
    }
}

/// A wrapper of [`Expression`] that evaluates in a non-strict way. Basically...
/// - When an error occurs during chunk-level evaluation, recompute in row-based execution and pad
///   with NULL for each failed row.
/// - Report all error occurred during row-level evaluation to the [`EvalErrorReport`].
pub(crate) struct NonStrict<E, R> {
    inner: E,
    report: R,
}

impl<E, R> std::fmt::Debug for NonStrict<E, R>
where
    E: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonStrict")
            .field("inner", &self.inner)
            .field("report", &std::any::type_name::<R>())
            .finish()
    }
}

impl<E, R> NonStrict<E, R>
where
    E: Expression,
    R: EvalErrorReport,
{
    pub fn new(inner: E, report: R) -> Self {
        Self { inner, report }
    }

    /// Evaluate expression in row-based execution with `eval_row_infallible`.
    async fn eval_chunk_infallible_by_row(&self, input: &DataChunk) -> ArrayRef {
        let mut array_builder = self.return_type().create_array_builder(input.capacity());
        for row in input.rows_with_holes() {
            if let Some(row) = row {
                let datum = self.eval_row_infallible(&row.into_owned_row()).await; // TODO: use `Row` trait
                array_builder.append(&datum);
            } else {
                array_builder.append_null();
            }
        }
        array_builder.finish().into()
    }

    /// Evaluate expression on a single row, report error and return NULL if failed.
    async fn eval_row_infallible(&self, input: &OwnedRow) -> Datum {
        match self.inner.eval_row(input).await {
            Ok(datum) => datum,
            Err(error) => {
                self.report.report(error);
                None // NULL
            }
        }
    }
}

// TODO: avoid the overhead of extra boxing.
#[async_trait]
impl<E, R> Expression for NonStrict<E, R>
where
    E: Expression,
    R: EvalErrorReport,
{
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        Ok(match self.inner.eval(input).await {
            Ok(array) => array,
            Err(_e) => self.eval_chunk_infallible_by_row(input).await,
        })
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        Ok(match self.inner.eval_v2(input).await {
            Ok(value) => value,
            Err(_e) => self.eval_chunk_infallible_by_row(input).await.into(),
        })
    }

    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        Ok(self.eval_row_infallible(input).await)
    }

    fn eval_const(&self) -> Result<Datum> {
        self.inner.eval_const() // do not handle error
    }

    fn input_ref_index(&self) -> Option<usize> {
        self.inner.input_ref_index()
    }
}
