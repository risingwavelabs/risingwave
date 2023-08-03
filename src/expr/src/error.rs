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
use risingwave_common::error::{BoxedError, ErrorCode, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::PbFieldNotFound;
use snafu::{IntoError, Snafu};
use thiserror::Error;

/// A specialized Result type for expression operations.
pub type Result<T> = std::result::Result<T, ExprError>;

// #[derive(Snafu, Debug)]
// #[snafu(visibility(pub(crate)))]
// pub enum ExprError {
//     #[snafu(display("unsupported function `{name}`"))]
//     UnsupportedFunction { name: Box<str> },

//     #[snafu(display("cannot cast `{from}` to `{to}`"))]
//     UnsupportedCast { from: DataType, to: DataType },

//     #[snafu(display("out of range when casting to {to}"))]
//     CastOutOfRange { to: &'static str },

//     #[snafu(display("numeric out of range"))]
//     NumericOutOfRange,

//     #[snafu(display("numeric out of range: underflow"))]
//     NumericUnderflow,

//     #[snafu(display("numeric out of range: overflow"))]
//     NumericOverflow,

//     #[snafu(display("division by zero"))]
//     DivisionByZero,

//     #[snafu(display("parse error"))]
//     Parse { source: BoxedError },

//     #[snafu(display("invalid parameter `{name}`: {reason}"))]
//     InvalidParam {
//         name: &'static str,
//         reason: Box<str>,
//     },

//     #[snafu(display("array error"), context(false))]
//     Array { source: ArrayError },

//     #[snafu(display("udf error"), context(false))]
//     Udf { source: risingwave_udf::Error },

//     #[snafu(display("more than one row returned by {name} used as an expression"))]
//     MaxOneRow { name: &'static str },

//     #[snafu(display("not a constant"))]
//     NotConstant,

//     #[snafu(display("context not found"))]
//     Context,

//     #[snafu(display("field name must not be null"))]
//     FieldNameNull,

//     #[snafu(display("uncategorized error"), context(false))]
//     Uncategorized { source: anyhow::Error },
// }

// impl snafu::IntoError<ExprError> for ParseSnafu {
//     type Source = String;

//     fn into_error(self, source: Self::Source) -> ExprError {
//         ExprError::Parse {
//             source: source.into(),
//         }
//     }
// }

// impl ::snafu::IntoError<ExprError> for ParseSnafu
// where
//     ExprError: ::snafu::Error + ::snafu::ErrorCompat,
// {
//     type Source = BoxedError;

//     #[track_caller]
//     fn into_error(self, error: Self::Source) -> ExprError {
//         let error: BoxedError = (|v| v)(error);
//         ExprError::Parse { source: error }
//     }
// }

/// The error type for expression operations.
#[derive(Error, Debug)]
pub enum ExprError {
    // Ideally "Unsupported" errors are caught by frontend. But when the match arms between
    // frontend and backend are inconsistent, we do not panic with `unreachable!`.
    #[error("Unsupported function: {0}")]
    UnsupportedFunction(String),

    #[error("Unsupported cast: {0:?} to {1:?}")]
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
    Parse(Box<str>),

    #[error("Invalid parameter {name}: {reason}")]
    InvalidParam {
        name: &'static str,
        reason: Box<str>,
    },

    #[error("Array error: {0}")]
    Array(#[from] ArrayError),

    #[error("More than one row returned by {0} used as an expression")]
    MaxOneRow(&'static str),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),

    #[error("UDF error: {0}")]
    Udf(#[from] risingwave_udf::Error),

    #[error("not a constant")]
    NotConstant,

    #[error("Context not found")]
    Context,

    #[error("field name must not be null")]
    FieldNameNull,
}

static_assertions::const_assert_eq!(std::mem::size_of::<ExprError>(), 40);

impl From<ExprError> for RwError {
    fn from(s: ExprError) -> Self {
        ErrorCode::ExprError(Box::new(s)).into()
    }
}

impl From<regex::Error> for ExprError {
    fn from(re: regex::Error) -> Self {
        Self::InvalidParam {
            name: "pattern",
            reason: re.to_string().into(),
        }
    }
}

impl From<chrono::ParseError> for ExprError {
    fn from(e: chrono::ParseError) -> Self {
        // Self::Parse { source: e.into() }
        todo!()
    }
}

impl From<PbFieldNotFound> for ExprError {
    fn from(err: PbFieldNotFound) -> Self {
        anyhow::anyhow!("Failed to decode prost: field not found `{}`", err.0).into()
    }
}
