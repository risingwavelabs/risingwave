use bincode::error::EncodeError;
use thiserror::Error;

pub(crate) type Result<T> = std::result::Result<T, TraceError>;

#[derive(Error, Debug)]
pub(crate) enum TraceError {
    #[error("failed to encode, {0}")]
    EncodeError(EncodeError),

    #[error("failed to read or write {0}")]
    IOError(std::io::Error),
}

impl From<EncodeError> for TraceError {
    fn from(err: EncodeError) -> Self {
        TraceError::EncodeError(err)
    }
}

impl From<std::io::Error> for TraceError {
    fn from(err: std::io::Error) -> Self {
        TraceError::IOError(err)
    }
}
