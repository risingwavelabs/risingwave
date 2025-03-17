// Copyright 2025 RisingWave Labs
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

use std::sync::LazyLock;

use async_trait::async_trait;
use auto_impl::auto_impl;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::log::LogSuppresser;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use thiserror_ext::AsReport;

use crate::ExprError;
use crate::error::Result;
use crate::expr::{Expression, ValueImpl};

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
        static LOG_SUPPERSSER: LazyLock<LogSuppresser> = LazyLock::new(LogSuppresser::default);
        if let Ok(suppressed_count) = LOG_SUPPERSSER.check() {
            tracing::error!(error=%error.as_report(), suppressed_count, "failed to evaluate expression");
        }
    }
}

/// A wrapper of [`Expression`] that evaluates in a non-strict way. Basically...
/// - When an error occurs during chunk-level evaluation, pad with NULL for each failed row.
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
            Err(ExprError::Multiple(array, errors)) => {
                for error in errors {
                    self.report.report(error);
                }
                array
            }
            Err(e) => {
                self.report.report(e);
                let mut builder = self.return_type().create_array_builder(input.capacity());
                builder.append_n_null(input.capacity());
                builder.finish().into()
            }
        })
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        Ok(match self.inner.eval_v2(input).await {
            Ok(array) => array,
            Err(ExprError::Multiple(array, errors)) => {
                for error in errors {
                    self.report.report(error);
                }
                array.into()
            }
            Err(e) => {
                self.report.report(e);
                ValueImpl::Scalar {
                    value: None,
                    capacity: input.capacity(),
                }
            }
        })
    }

    /// Evaluate expression on a single row, report error and return NULL if failed.
    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        Ok(match self.inner.eval_row(input).await {
            Ok(datum) => datum,
            Err(error) => {
                self.report.report(error);
                None // NULL
            }
        })
    }

    fn eval_const(&self) -> Result<Datum> {
        self.inner.eval_const() // do not handle error
    }

    fn input_ref_index(&self) -> Option<usize> {
        self.inner.input_ref_index()
    }
}
