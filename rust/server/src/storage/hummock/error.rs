use prost::DecodeError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HummockError {
    #[error("ok")]
    OK,
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

pub type HummockResult<T> = std::result::Result<T, HummockError>;
