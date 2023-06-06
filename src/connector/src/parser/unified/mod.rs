//! Unified parsers for both normal events or CDC events of multiple message formats

use risingwave_common::types::{DataType, Datum};
use thiserror::Error;

pub mod avro;
pub mod debezium;
pub mod json;
pub mod upsert;
pub mod util;

pub type AccessResult = std::result::Result<Datum, AccessError>;

/// Access a certain field in an object according to the path
pub trait Access {
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> AccessResult;
}

#[derive(Debug)]
pub enum ChangeEventOperation {
    Upsert, // Insert or Update
    Delete,
}

/// Methods to access a CDC event.
pub trait ChangeEvent {
    /// Access the operation type.
    fn op(&self) -> std::result::Result<ChangeEventOperation, AccessError>;
    /// Access the field after the operation.
    fn access_field(&self, name: &str, type_expected: &DataType) -> AccessResult;
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
