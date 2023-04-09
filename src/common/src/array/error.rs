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

pub use anyhow::anyhow;
use risingwave_pb::PbFieldNotFound;
use thiserror::Error;

use crate::error::{ErrorCode, RwError};

#[derive(Error, Debug)]
pub enum ArrayError {
    #[error("Pb decode error: {0}")]
    PbDecode(#[from] prost::DecodeError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<ArrayError> for RwError {
    fn from(s: ArrayError) -> Self {
        ErrorCode::ArrayError(s).into()
    }
}

impl From<PbFieldNotFound> for ArrayError {
    fn from(err: PbFieldNotFound) -> Self {
        anyhow!("Failed to decode prost: field not found `{}`", err.0).into()
    }
}

impl ArrayError {
    pub fn internal(msg: impl ToString) -> Self {
        ArrayError::Internal(anyhow!(msg.to_string()))
    }
}
