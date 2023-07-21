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

use std::str::from_utf8;

use anyhow::anyhow;
use risingwave_common::types::{DataType, ScalarImpl};

use super::{Access, AccessError, AccessResult};

// where do we put data

pub struct BytesAccess<'a> {
    // whether to treat the bytes as bytea or string
    as_str: bool,
    column_name: &'a Option<String>,
    bytes: Vec<u8>,
}

impl<'a> BytesAccess<'a> {
    pub fn new(as_str: bool, column_name: &'a Option<String>, bytes: Vec<u8>) -> Self {
        Self {
            as_str,
            column_name,
            bytes,
        }
    }
}

impl<'a> Access for BytesAccess<'a> {
    /// path is empty currently, `type_expected` should be `Bytea`
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> AccessResult {
        match (self.as_str, type_expected) {
            (true, Some(DataType::Varchar)) => {
                if path.len() == 1 && self.column_name.as_ref().unwrap() == path[0] {
                    let str = from_utf8(&self.bytes).map_err(|e| AccessError::Other(anyhow!(e)))?;
                    Ok(Some(str.into()))
                } else {
                    Err(AccessError::Undefined {
                        name: path[0].into(),
                        path: self.column_name.clone().unwrap(),
                    })
                }
            }
            (false, Some(DataType::Bytea)) => {
                Ok(Some(ScalarImpl::Bytea(Box::from(self.bytes.as_slice()))))
            }
            (_, _) => Err(AccessError::TypeError {
                expected: if self.as_str {
                    "Varchar".to_string()
                } else {
                    "Bytea".to_string()
                },
                got: format!("{:?}", type_expected),
                value: "".to_string(),
            }),
        }
    }
}
