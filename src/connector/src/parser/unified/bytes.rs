// Copyright 2025 RisingWave Labs
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

use risingwave_common::types::{DataType, DatumCow, ScalarRefImpl};

use super::{Access, AccessError, AccessResult};

// where do we put data

pub struct BytesAccess<'a> {
    column_name: &'a Option<String>,
    bytes: Vec<u8>,
}

impl<'a> BytesAccess<'a> {
    pub fn new(column_name: &'a Option<String>, bytes: Vec<u8>) -> Self {
        Self { column_name, bytes }
    }
}

impl Access for BytesAccess<'_> {
    /// path is empty currently, `type_expected` should be `Bytea`
    fn access<'a>(&'a self, path: &[&str], type_expected: &DataType) -> AccessResult<DatumCow<'a>> {
        if let DataType::Bytea = type_expected {
            if self.column_name.is_none()
                || (path.len() == 1 && self.column_name.as_ref().unwrap() == path[0])
            {
                return Ok(DatumCow::Borrowed(Some(ScalarRefImpl::Bytea(
                    self.bytes.as_slice(),
                ))));
            }
            return Err(AccessError::Undefined {
                name: path[0].to_owned(),
                path: self.column_name.as_ref().unwrap().clone(),
            });
        }
        Err(AccessError::TypeError {
            expected: "Bytea".to_owned(),
            got: format!("{:?}", type_expected),
            value: "".to_owned(),
        })
    }
}
