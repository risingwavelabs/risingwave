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
    DecodeError(#[from] prost::DecodeError),
}

pub type HummockResult<T> = std::result::Result<T, HummockError>;
