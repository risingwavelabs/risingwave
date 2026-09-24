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

use std::sync::LazyLock;

use auto_impl::auto_impl;
use risingwave_common::array::{ArrayRef, DataChunk};
use risingwave_common::log::LogSuppressor;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, Datum};
use thiserror_ext::AsReport;

use crate::ExprError;
use crate::error::Result;
use crate::expr::{AsyncExpression, ExpressionInfo, SyncExpression, ValueImpl};

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
        static LOG_SUPPRESSOR: LazyLock<LogSuppressor> = LazyLock::new(LogSuppressor::default);
        if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
            tracing::error!(error=%error.as_report(), suppressed_count, "failed to evaluate expression");
        }
    }
}

/// A wrapper of an expression that evaluates in a non-strict way. Basically...
/// - When an error occurs during chunk-level evaluation, pad with NULL for each failed row.
/// - Report ordinary row-level errors to the [`EvalErrorReport`].
/// - Propagate fatal external-operation failures instead of manufacturing NULL results.
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
    E: ExpressionInfo,
    R: EvalErrorReport,
{
    pub fn new(inner: E, report: R) -> Self {
        Self { inner, report }
    }
}

impl<E, R> ExpressionInfo for NonStrict<E, R>
where
    E: ExpressionInfo,
    R: EvalErrorReport,
{
    fn return_type(&self) -> DataType {
        self.inner.return_type()
    }

    fn input_ref_index(&self) -> Option<usize> {
        self.inner.input_ref_index()
    }
}

macro_rules! non_strict_eval_array {
    ($mode:ident, $this:expr, $input:expr) => {{
        Ok(match forward!($mode, $this.inner, eval($input)) {
            Ok(array) => array,
            Err(error) if error.is_fatal() => return Err(error),
            Err(ExprError::Multiple(array, errors)) => {
                for error in errors {
                    $this.report.report(error);
                }
                array
            }
            Err(e) => {
                $this.report.report(e);
                let mut builder = $this.return_type().create_array_builder($input.capacity());
                builder.append_n_null($input.capacity());
                builder.finish().into()
            }
        })
    }};
}

macro_rules! non_strict_eval_value {
    ($mode:ident, $this:expr, $input:expr) => {{
        Ok(match forward!($mode, $this.inner, eval_v2($input)) {
            Ok(array) => array,
            Err(error) if error.is_fatal() => return Err(error),
            Err(ExprError::Multiple(array, errors)) => {
                for error in errors {
                    $this.report.report(error);
                }
                array.into()
            }
            Err(e) => {
                $this.report.report(e);
                ValueImpl::Scalar {
                    value: None,
                    capacity: $input.capacity(),
                }
            }
        })
    }};
}

macro_rules! non_strict_eval_row {
    ($mode:ident, $this:expr, $input:expr) => {{
        Ok(match forward!($mode, $this.inner, eval_row($input)) {
            Ok(datum) => datum,
            Err(error) if error.is_fatal() => return Err(error),
            Err(error) => {
                $this.report.report(error);
                None // NULL
            }
        })
    }};
}

impl<E, R> SyncExpression for NonStrict<E, R>
where
    E: SyncExpression,
    R: EvalErrorReport,
{
    fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        non_strict_eval_array!(sync, self, input)
    }

    fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        non_strict_eval_value!(sync, self, input)
    }

    /// Evaluate expression on a single row, report error and return NULL if failed.
    fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        non_strict_eval_row!(sync, self, input)
    }

    fn eval_const(&self) -> Result<Datum> {
        self.inner.eval_const() // do not handle error
    }
}

impl<E, R> AsyncExpression for NonStrict<E, R>
where
    E: AsyncExpression,
    R: EvalErrorReport,
{
    async fn eval(&self, input: &DataChunk) -> Result<ArrayRef> {
        non_strict_eval_array!(async, self, input)
    }

    async fn eval_v2(&self, input: &DataChunk) -> Result<ValueImpl> {
        non_strict_eval_value!(async, self, input)
    }

    /// Evaluate expression on a single row, report error and return NULL if failed.
    async fn eval_row(&self, input: &OwnedRow) -> Result<Datum> {
        non_strict_eval_row!(async, self, input)
    }
}

#[cfg(test)]
mod tests {
    use risingwave_common::array::{Array, I32Array};

    use super::*;

    #[derive(Debug)]
    struct FailingExpression {
        fatal: bool,
        multiple: bool,
    }

    impl FailingExpression {
        fn error(&self) -> ExprError {
            let error = if self.fatal {
                ExprError::External("request failed".into())
            } else {
                ExprError::DivisionByZero
            };
            if self.multiple {
                ExprError::Multiple(
                    I32Array::from_iter([None]).into_ref(),
                    vec![ExprError::NumericOutOfRange, error].into(),
                )
            } else {
                error
            }
        }
    }

    impl ExpressionInfo for FailingExpression {
        fn return_type(&self) -> DataType {
            DataType::Int32
        }
    }

    impl SyncExpression for FailingExpression {
        fn eval(&self, _: &DataChunk) -> Result<ArrayRef> {
            Err(self.error())
        }

        fn eval_row(&self, _: &OwnedRow) -> Result<Datum> {
            Err(self.error())
        }
    }

    #[test]
    fn external_errors_survive_nested_non_strict_wrappers() {
        let input = DataChunk::new_dummy(1);
        for fatal in [false, true] {
            for multiple in [false, true] {
                let expr = NonStrict::new(
                    NonStrict::new(FailingExpression { fatal, multiple }, LogReport),
                    LogReport,
                );
                let array = expr.eval(&input);
                let value = expr.eval_v2(&input);
                let row = expr.eval_row(&OwnedRow::empty());
                if fatal {
                    assert!(array.unwrap_err().is_fatal());
                    assert!(value.unwrap_err().is_fatal());
                    assert!(row.unwrap_err().is_fatal());
                } else {
                    assert!(array.unwrap().value_at(0).is_none());
                    assert!(value.is_ok());
                    assert!(row.unwrap().is_none());
                }
            }
        }
    }

    #[test]
    fn external_errors_remain_fatal_with_function_context() {
        let error = ExprError::function("outer", [], ExprError::External("request failed".into()));
        assert!(error.is_fatal());
        assert!(ExprError::Internal(anyhow::Error::new(error).context("context")).is_fatal());
        assert!(!ExprError::function("outer", [], ExprError::DivisionByZero).is_fatal());
    }
}
