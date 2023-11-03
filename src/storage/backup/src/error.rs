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

use bincode::Error;
use risingwave_common::error::BoxedError;
use thiserror::Error;

pub type BackupResult<T> = Result<T, BackupError>;

#[derive(Error, Debug)]
pub enum BackupError {
    #[error("BackupStorage error: {0}")]
    BackupStorage(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    #[error("MetaStorage error: {0}")]
    MetaStorage(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    #[error("StateStorage error: {0}")]
    StateStorage(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    #[error("Encoding error: {0}")]
    Encoding(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    #[error("Decoding error: {0}")]
    Decoding(
        #[backtrace]
        #[source]
        BoxedError,
    ),
    #[error("Checksum mismatch: expected {expected}, found: {found}")]
    ChecksumMismatch { expected: u64, found: u64 },
    #[error("Meta storage is not empty before being restored")]
    NonemptyMetaStorage,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl From<bincode::Error> for BackupError {
    fn from(value: Error) -> Self {
        // TODO: match error
        BackupError::Other(value.into())
    }
}
