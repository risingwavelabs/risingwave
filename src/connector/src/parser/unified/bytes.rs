// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    /// path is empty currently, `type_expected` should be `Bytea`
    fn access(&self, _path: &[&str], type_expected: Option<&DataType>) -> AccessResult {
        if let Some(DataType::Bytea) = type_expected {
            return Ok(Some(ScalarImpl::Bytea(Box::from(self.bytes.as_slice()))));
        }
        Err(AccessError::TypeError {
            expected: "Bytea".to_string(),
            got: format!("{:?}", type_expected),
            value: "".to_string(),
        })
    }
}
