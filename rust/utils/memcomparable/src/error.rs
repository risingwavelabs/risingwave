use std::fmt::Display;

use serde::{de, ser};
use thiserror::Error;

/// The result of a serialization or deserialization operation.
pub type Result<T> = std::result::Result<T, Error>;

/// An error that can be produced during (de)serializing.
#[allow(missing_docs)]
#[derive(Error, Clone, Debug, PartialEq)]
pub enum Error {
    #[error("{0}")]
    Message(String),
    #[error("unexpected end of input")]
    Eof,
    #[error("unsupported type: {0}")]
    NotSupported(&'static str),
    #[error("invalid bool encoding: {0}")]
    InvalidBoolEncoding(u8),
    #[error("invalid char encoding: {0}")]
    InvalidCharEncoding(u32),
    #[error("invalid tag encoding: {0}")]
    InvalidTagEncoding(usize),
    #[error("invalid sequence encoding: {0}")]
    InvalidSeqEncoding(u8),
    #[error("invalid UTF8: {0}")]
    InvalidUtf8(#[from] std::string::FromUtf8Error),
    #[error("invalid bytes encoding: {0}")]
    InvalidBytesEncoding(u8),
    #[error("trailing characters")]
    TrailingCharacters,
    #[error("invalid NaiveDate scope: days: {0}")]
    InvalidNaiveDateEncoding(i32),
    #[error("invalid NaiveDateTime scope: secs: {0} nsecs: {1}")]
    InvalidNaiveDateTimeEncoding(i64, u32),
    #[error("invalid NaiveTime scope: secs: {0} nano: {1}")]
    InvalidNaiveTimeEncoding(u32, u32),
}

impl ser::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}
