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

#[derive(Error, Debug)]
#[error("{source}")]
pub struct TracedHummockError {
    #[from]
    source: HummockError,
    backtrace: Backtrace,
}

impl From<TracedHummockError> for RwError {
    fn from(h: TracedHummockError) -> Self {
        ErrorCode::StorageError(Box::new(h)).into()
    }
}

pub type HummockResult<T> = std::result::Result<T, TracedHummockError>;
