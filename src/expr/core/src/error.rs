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

use anyhow::anyhow;
use risingwave_common::array::ArrayError;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_pb::PbFieldNotFound;
use thiserror::Error;

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

    #[error("Array error: {0}")]
    Array(
        #[from]
        #[backtrace]
        ArrayError,
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

    #[error("invalid state: {0}")]
    InvalidState(String),

    /// Errors returned by functions.
    #[error(transparent)]
    Anyhow(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),
}

static_assertions::const_assert_eq!(std::mem::size_of::<ExprError>(), 32);

impl From<ExprError> for RwError {
    fn from(s: ExprError) -> Self {
        ErrorCode::ExprError(Box::new(s)).into()
    }
}

impl From<chrono::ParseError> for ExprError {
    fn from(e: chrono::ParseError) -> Self {
        anyhow!(e).context("failed to parse date/time").into()
    }
}

impl From<PbFieldNotFound> for ExprError {
    fn from(err: PbFieldNotFound) -> Self {
        anyhow!(err).into()
    }
}
