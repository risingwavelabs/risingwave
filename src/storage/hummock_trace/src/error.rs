use bincode::error::{DecodeError, EncodeError};
use thiserror::Error;

pub(crate) type Result<T> = std::result::Result<T, TraceError>;

#[derive(Error, Debug)]
pub(crate) enum TraceError {
    #[error("failed to encode, {0}")]
    EncodeError(EncodeError),

    #[error("failed to decode, {0}")]
    DecodeError(DecodeError),

    #[error("failed to read or write {0}")]
    IOError(std::io::Error),

    #[error("Invalid magic bytes, expected {expected:?}, found {found:?}")]
    MagicBytesError { expected: u32, found: u32 },
}

impl From<EncodeError> for TraceError {
    fn from(err: EncodeError) -> Self {
        TraceError::EncodeError(err)
    }
}

impl From<DecodeError> for TraceError {
    fn from(err: DecodeError) -> Self {
        TraceError::DecodeError(err)
    }
}

impl From<std::io::Error> for TraceError {
    fn from(err: std::io::Error) -> Self {
        TraceError::IOError(err)
    }
}
