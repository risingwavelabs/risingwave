use std::backtrace::Backtrace;

use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HummockError {
    #[error("Checksum mismatch: expected {expected}, found: {found}.")]
    ChecksumMismatch { expected: u64, found: u64 },
    #[error("Invalid block.")]
    InvalidBlock,
    #[error("Decode error {0}.")]
    DecodeError(String),
    #[error("Mock error {0}.")]
    MockError(String),
    #[error("ObjectStore failed with IO error {0}.")]
    ObjectIoError(String),
    #[error("Invalid HummockContext {0}.")]
    InvalidHummockContext(i32),
    #[error("Failed to create RPC client.")]
    CreateRPCClientError,
    #[error("No matching pinned version with version_id={0} was found in context.")]
    NoMatchingPinVersion(u64),
    #[error("No matching pinned snapshot with snapshot_id={0} found in context.")]
    NoMatchingPinSnapshot(u64),
    #[error("No compact task.")]
    NoCompactTaskFound,
    #[error("Invalid WriteBatch.")]
    InvalidWriteBatch,
}

impl HummockError {
    pub fn object_io_error(error: impl ToString) -> TracedHummockError {
        Self::ObjectIoError(error.to_string()).into()
    }

    pub fn invalid_block() -> TracedHummockError {
        Self::InvalidBlock.into()
    }

    pub fn decode_error(error: impl ToString) -> TracedHummockError {
        Self::DecodeError(error.to_string()).into()
    }

    pub fn checksum_mismatch(expected: u64, found: u64) -> TracedHummockError {
        Self::ChecksumMismatch { expected, found }.into()
    }

    pub fn no_compact_task_found() -> TracedHummockError {
        Self::NoCompactTaskFound.into()
    }

    pub fn create_rpc_client_error() -> TracedHummockError {
        Self::CreateRPCClientError.into()
    }

    pub fn no_matching_pin_version(version: u64) -> TracedHummockError {
        Self::NoMatchingPinVersion(version).into()
    }

    pub fn no_matching_pin_snapshot(snapshot: u64) -> TracedHummockError {
        Self::NoMatchingPinSnapshot(snapshot).into()
    }

    pub fn invalid_hummock_context(context_id: i32) -> TracedHummockError {
        Self::InvalidHummockContext(context_id).into()
    }

    pub fn invalid_write_batch() -> TracedHummockError {
        Self::InvalidWriteBatch.into()
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
