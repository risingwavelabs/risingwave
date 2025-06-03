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

use std::convert::Infallible;

pub use anyhow::anyhow;
use risingwave_pb::PbFieldNotFound;
use thiserror::Error;
use thiserror_ext::Construct;

use crate::error::BoxedError;

#[derive(Error, Debug, Construct)]
pub enum ArrayError {
    #[error("Pb decode error: {0}")]
    PbDecode(#[from] prost::DecodeError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        anyhow::Error,
    ),

    #[error("Convert from arrow error: {0}")]
    FromArrow(
        #[source]
        #[backtrace]
        BoxedError,
    ),

    #[error("Convert to arrow error: {0}")]
    ToArrow(
        #[source]
        #[backtrace]
        BoxedError,
    ),
}

impl From<PbFieldNotFound> for ArrayError {
    fn from(err: PbFieldNotFound) -> Self {
        anyhow!("Failed to decode prost: field not found `{}`", err.0).into()
    }
}

impl From<Infallible> for ArrayError {
    fn from(err: Infallible) -> Self {
        unreachable!("Infallible error: {:?}", err)
    }
}

impl ArrayError {
    pub fn internal(msg: impl ToString) -> Self {
        ArrayError::Internal(anyhow!(msg.to_string()))
    }
}
