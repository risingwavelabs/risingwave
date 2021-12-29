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
