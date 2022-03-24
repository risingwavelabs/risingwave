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
//
use std::backtrace::Backtrace;

use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HummockError {
    #[error("Checksum mismatch: expected {expected}, found: {found}.")]
    ChecksumMismatch { expected: u64, found: u64 },
    #[error("Invalid block.")]
    InvalidBlock,
    #[error("Encode error {0}.")]
    EncodeError(String),
    #[error("Decode error {0}.")]
    DecodeError(String),
    #[error("Mock error {0}.")]
    MockError(String),
    #[error("ObjectStore failed with IO error {0}.")]
    ObjectIoError(String),
    #[error("Meta error {0}.")]
    MetaError(String),
    #[error("Invalid WriteBatch.")]
    InvalidWriteBatch,
    #[error("SharedBuffer error {0}.")]
    SharedBufferError(String),
    #[error("Wait epoch error {0}.")]
    WaitEpoch(String),
    #[error("Expired Epoch: watermark {safe_epoch}, epoch {epoch}.")]
    ExpiredEpoch { safe_epoch: u64, epoch: u64 },
    #[error("Other error {0}.")]
    Other(String),
}

impl HummockError {
    pub fn object_io_error(error: impl ToString) -> TracedHummockError {
        Self::ObjectIoError(error.to_string()).into()
    }

    pub fn invalid_block() -> TracedHummockError {
        Self::InvalidBlock.into()
    }

    pub fn encode_error(error: impl ToString) -> TracedHummockError {
        Self::EncodeError(error.to_string()).into()
    }

    pub fn decode_error(error: impl ToString) -> TracedHummockError {
        Self::DecodeError(error.to_string()).into()
    }

    pub fn checksum_mismatch(expected: u64, found: u64) -> TracedHummockError {
        Self::ChecksumMismatch { expected, found }.into()
    }

    pub fn meta_error(error: impl ToString) -> TracedHummockError {
        Self::MetaError(error.to_string()).into()
    }

    pub fn invalid_write_batch() -> TracedHummockError {
        Self::InvalidWriteBatch.into()
    }

    pub fn shared_buffer_error(error: impl ToString) -> TracedHummockError {
        Self::SharedBufferError(error.to_string()).into()
    }

    pub fn wait_epoch(error: impl ToString) -> TracedHummockError {
        Self::WaitEpoch(error.to_string()).into()
    }

    pub fn expired_epoch(safe_epoch: u64, epoch: u64) -> TracedHummockError {
        Self::ExpiredEpoch { safe_epoch, epoch }.into()
    }
}

impl From<prost::DecodeError> for HummockError {
    fn from(error: prost::DecodeError) -> Self {
        Self::DecodeError(error.to_string())
    }
}

impl From<prost::DecodeError> for TracedHummockError {
    fn from(error: prost::DecodeError) -> Self {
        Self::from(HummockError::from(error))
    }
}

#[derive(Error)]
#[error("{source:?}\n{backtrace:#}")]
pub struct TracedHummockError {
    #[from]
    source: HummockError,
    backtrace: Backtrace,
}

impl std::fmt::Debug for TracedHummockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl From<TracedHummockError> for RwError {
    fn from(h: TracedHummockError) -> Self {
        ErrorCode::StorageError(Box::new(h)).into()
    }
}

pub type HummockResult<T> = std::result::Result<T, TracedHummockError>;
