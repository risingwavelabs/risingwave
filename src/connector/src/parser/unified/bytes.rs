use std::fmt::format;

use risingwave_common::cast::str_to_bytea;
use risingwave_common::types::{DataType, ScalarImpl};

use super::{Access, AccessError, AccessResult, ChangeEvent, ChangeEventOperation};

// where do we put data

pub struct BytesAccess {
    bytes: Vec<u8>,
}

impl BytesAccess {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

pub struct BytesChangeEvent {
    value_accessor: BytesAccess,
    key_accessor: Option<BytesAccess>,
}

impl BytesChangeEvent {
    pub fn with_value(value_accessor: BytesAccess) -> Self {
        Self::new(None, value_accessor)
    }

    pub fn new(key_accessor: Option<BytesAccess>, value_accessor: BytesAccess) -> Self {
        Self {
            value_accessor,
            key_accessor,
        }
    }
}

impl ChangeEvent for BytesChangeEvent {
    fn op(&self) -> std::result::Result<ChangeEventOperation, super::AccessError> {
        Ok(ChangeEventOperation::Upsert)
    }

    fn access_field(&self, name: &str, type_expected: &DataType) -> super::AccessResult {
        self.value_accessor.access(&[name], Some(type_expected))
    }
}

impl Access for BytesAccess {
    /// path is empty currently, type_expected should be `Bytea`
    fn access(&self, _path: &[&str], type_expected: Option<&DataType>) -> AccessResult {
        if let Some(DataType::Bytea) = type_expected {
            return Ok(Some(ScalarImpl::Bytea(Box::from(self.bytes.as_slice()))));
        }
        return Err(AccessError::TypeError {
            expected: "Bytea".to_string(),
            got: format!("{:?}", type_expected),
            value: "".to_string(),
        });
    }
}
