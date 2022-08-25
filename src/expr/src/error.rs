// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

pub use anyhow::anyhow;
use regex;
use risingwave_common::array::ArrayError;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::types::DataType;
use risingwave_pb::ProstFieldNotFound;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExprError {
    #[error("Unsupported function: {0}")]
    UnsupportedFunction(String),

    #[error("Can't cast {0} to {1}")]
    Cast(&'static str, &'static str),

    // TODO: Unify Cast and Cast2.
    #[error("Can't cast {0:?} to {1:?}")]
    Cast2(DataType, DataType),

    #[error("Out of range")]
    NumericOutOfRange,

    #[error("Division by zero")]
    DivisionByZero,

    #[error("Parse error: {0}")]
    Parse(&'static str),

    #[error("Invalid parameter {name}: {reason}")]
    InvalidParam { name: &'static str, reason: String },

    #[error("Array error: {0}")]
    Array(#[from] ArrayError),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<ExprError> for RwError {
    fn from(s: ExprError) -> Self {
        ErrorCode::ExprError(Box::new(s)).into()
    }
}

impl From<regex::Error> for ExprError {
    fn from(re: regex::Error) -> Self {
        Self::InvalidParam {
            name: "pattern",
            reason: re.to_string(),
        }
    }
}

impl From<ProstFieldNotFound> for ExprError {
    fn from(err: ProstFieldNotFound) -> Self {
        Self::Internal(anyhow::anyhow!(
            "Failed to decode prost: field not found `{}`",
            err.0
        ))
    }
}
