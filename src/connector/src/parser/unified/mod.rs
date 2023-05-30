use risingwave_common::types::{DataType, Datum};
use thiserror::Error;

pub mod avro;
pub mod json;
pub type AccessResult =  std::result::Result<Datum, AccessError>;
pub trait Access {
    fn access(&self, path: &[&str], shape: DataType) -> AccessResult;
}
#[derive(Error, Debug)]
pub enum AccessError {
    #[error("Undefined {name} at {path}")]
    Undefined { name: String, path: String },
    #[error("TypeError {expected} expected, got {got} {value}")]
    TypeError {
        expected: String,
        got: String,
        value: String,
    },
}
