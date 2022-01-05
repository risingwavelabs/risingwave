use prost::DecodeError;
use risingwave_common::error::{ErrorCode, RwError};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HummockError {
    #[error("Checksum Mismatch")]
    ChecksumMismatch,
    #[error("Invalid Block")]
    InvalidBlock,
    #[error("Decode Error {0}")]
    DecodeError(String),
    #[error("Mock Error {0}")]
    MockError(String),
    #[error("Object Store IO Error {0}")]
    ObjectIoError(String),
    #[error("Invalid Hummock Context {0}")]
    InvalidHummockContext(String),
    #[error("Failed to Create RPC Client")]
    CreateRPCClientError,
    #[error("No Pin Version Record Matching version_id={0} in context")]
    NoMatchingPinVersion(String),
    #[error("No Pin Snapshot Record Matching snapshot_id={0} in context")]
    NoMatchingPinSnapshot(String),
    #[error("No compact task")]
    NoCompactTaskFound,
}

impl From<prost::DecodeError> for HummockError {
    fn from(e: DecodeError) -> Self {
        Self::DecodeError(e.to_string())
    }
}

impl From<HummockError> for RwError {
    fn from(h: HummockError) -> Self {
        ErrorCode::StorageError(Box::new(h)).into()
    }
}

pub type HummockResult<T> = std::result::Result<T, HummockError>;
