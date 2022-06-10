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
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::types::DataType;
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

    #[error("Parse error: {0}")]
    Parse(&'static str),

    #[error("Invalid parameter {name}: {reason}")]
    InvalidParam { name: &'static str, reason: String },

    #[error("Array error: {0}")]
    Array(
        #[backtrace]
        #[source]
        RwError,
    ),

    #[error("Struct error: {0}")]
    Struct(
        #[backtrace]
        #[source]
        RwError,
    ),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

#[macro_export]
macro_rules! ensure {
    ($cond:expr $(,)?) => {
        if !$cond {
            return Err($crate::ExprError::Internal($crate::error::anyhow!(
                stringify!($cond)
            )));
        }
    };
    ($cond:expr, $msg:literal $(,)?) => {
        if !$cond {
            return Err($crate::ExprError::Internal($crate::error::anyhow!($msg)));
        }
    };
    ($cond:expr, $err:expr $(,)?) => {
        if !$cond {
            return Err($crate::ExprError::Internal($crate::error::anyhow!$err));
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return Err($crate::ExprError::Internal($crate::error::anyhow!($fmt, $($arg)*)));
        }
    };
}

impl From<ExprError> for RwError {
    fn from(s: ExprError) -> Self {
        ErrorCode::ExprError(Box::new(s)).into()
    }
}

#[macro_export]
macro_rules! bail {
    ($msg:literal $(,)?) => {
        return Err($crate::ExprError::Internal($crate::error::anyhow!($msg)))
    };
    ($err:expr $(,)?) => {
        return Err($crate::ExprError::Internal($crate::error::anyhow!($err)))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err($crate::ExprError::Internal($crate::error::anyhow!($fmt, $($arg)*)))
    };
}
