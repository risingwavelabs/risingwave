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
use risingwave_common::array::ArrayError;
use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, BatchError>;

pub trait Error = std::error::Error + Send + Sync + 'static;

#[derive(Error, Debug)]
pub enum BatchError {
    #[error("Unsupported function: {0}")]
    UnsupportedFunction(String),

    #[error("Can't cast {0} to {1}")]
    Cast(&'static str, &'static str),

    #[error("Array error: {0}")]
    Array(#[from] ArrayError),

    #[error("Out of range")]
    NumericOutOfRange,

    #[error("Failed to send result to channel")]
    SenderError,

    #[error(transparent)]
    Internal(#[from] anyhow::Error),

    #[error("Prometheus error: {0}")]
    Prometheus(#[from] prometheus::Error),
}

impl From<BatchError> for RwError {
    fn from(s: BatchError) -> Self {
        ErrorCode::BatchError(Box::new(s)).into()
    }
}
