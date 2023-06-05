use risingwave_common::types::{DataType, Datum};
use thiserror::Error;

pub mod avro;
pub mod debezium;
pub mod json;
pub mod upsert;
pub mod util;
pub type AccessResult = std::result::Result<Datum, AccessError>;
pub trait Access {
    fn access(&self, path: &[&str], shape: Option<&DataType>) -> AccessResult;
}
#[derive(Debug)]
pub enum RowOperation {
    Insert,
    Update,
    Delete,
}
pub trait OperateRow {
    fn op(&self) -> std::result::Result<RowOperation, AccessError>;
    fn access_field(&self, name: &str, shape: &DataType) -> AccessResult;
    fn access_before(&self, name: &str, shape: &DataType) -> AccessResult;
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
