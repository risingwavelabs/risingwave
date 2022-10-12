use bincode::error::{DecodeError, EncodeError};
use thiserror::Error;

use crate::RecordID;

pub type Result<T> = std::result::Result<T, TraceError>;

#[derive(Error, Debug)]
pub enum TraceError {
    #[error("failed to encode, {0}")]
    EncodeError(EncodeError),

    #[error("failed to decode, {0}")]
    DecodeError(DecodeError),

    #[error("failed to read or write {0}")]
    IOError(std::io::Error),

    #[error("invalid magic bytes, expected {expected:?}, found {found:?}")]
    MagicBytesError { expected: u32, found: u32 },

    #[error("try to close a non-existing record {0}")]
    FinRecordError(RecordID),
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
