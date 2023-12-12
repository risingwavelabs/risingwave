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

use risingwave_common::array::ArrayError;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::PbFieldNotFound;
use thiserror::Error;
use thiserror_ext::AsReport;

/// A specialized Result type for expression operations.
pub type Result<T> = std::result::Result<T, ExprError>;

pub struct ContextUnavailable(&'static str);

impl ContextUnavailable {
    pub fn new(field: &'static str) -> Self {
        Self(field)
    }
}

impl From<ContextUnavailable> for ExprError {
    fn from(e: ContextUnavailable) -> Self {
        ExprError::Context(e.0)
    }
}

/// The error type for expression operations.
#[derive(Error, Debug)]
pub enum ExprError {
    // Ideally "Unsupported" errors are caught by frontend. But when the match arms between
    // frontend and backend are inconsistent, we do not panic with `unreachable!`.
    #[error("Unsupported function: {0}")]
    UnsupportedFunction(String),

    #[error("Unsupported cast: {0} to {1}")]
    UnsupportedCast(DataType, DataType),

    #[error("Casting to {0} out of range")]
    CastOutOfRange(&'static str),

    #[error("Numeric out of range")]
    NumericOutOfRange,

    #[error("Numeric out of range: underflow")]
    NumericUnderflow,

    #[error("Numeric out of range: overflow")]
    NumericOverflow,

    #[error("Division by zero")]
    DivisionByZero,

    #[error("Parse error: {0}")]
    // TODO(error-handling): should prefer use error types than strings.
    Parse(Box<str>),

    #[error("Invalid parameter {name}: {reason}")]
    // TODO(error-handling): should prefer use error types than strings.
    InvalidParam {
        name: &'static str,
        reason: Box<str>,
    },

    #[error("Array error: {0}")]
    Array(
        #[from]
        #[backtrace]
        ArrayError,
    ),

    #[error("More than one row returned by {0} used as an expression")]
    MaxOneRow(&'static str),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),

    #[error("UDF error: {0}")]
    Udf(
        #[from]
        #[backtrace]
        risingwave_udf::Error,
    ),

    #[error("not a constant")]
    NotConstant,

    #[error("Context {0} not found")]
    Context(&'static str),

    #[error("field name must not be null")]
    FieldNameNull,

    #[error("too few arguments for format()")]
    TooFewArguments,

    #[error("invalid state: {0}")]
    InvalidState(String),
}

static_assertions::const_assert_eq!(std::mem::size_of::<ExprError>(), 40);

impl From<ExprError> for RwError {
    fn from(s: ExprError) -> Self {
        ErrorCode::ExprError(Box::new(s)).into()
    }
}

impl From<chrono::ParseError> for ExprError {
    fn from(e: chrono::ParseError) -> Self {
        Self::Parse(e.to_report_string().into())
    }
}

impl From<PbFieldNotFound> for ExprError {
    fn from(err: PbFieldNotFound) -> Self {
        Self::Internal(anyhow::anyhow!(
            "Failed to decode prost: field not found `{}`",
            err.0
        ))
    }
}
