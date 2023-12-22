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

use risingwave_common::types::DataType;
use risingwave_pb::plan_common::AdditionalColumnType;

use super::{Access, ChangeEvent, ChangeEventOperation};
use crate::parser::unified::AccessError;
use crate::source::SourceColumnDesc;

/// `UpsertAccess` wraps a key-value message format into an upsert source.
/// A key accessor and a value accessor are required.
pub struct UpsertChangeEvent<K, V> {
    key_accessor: Option<K>,
    value_accessor: Option<V>,
    key_column_name: Option<String>,
}

impl<K, V> Default for UpsertChangeEvent<K, V> {
    fn default() -> Self {
        Self {
            key_accessor: None,
            value_accessor: None,
            key_column_name: None,
        }
    }
}

impl<K, V> UpsertChangeEvent<K, V> {
    pub fn with_key(mut self, key: K) -> Self
    where
        K: Access,
    {
        self.key_accessor = Some(key);
        self
    }

    pub fn with_value(mut self, value: V) -> Self
    where
        V: Access,
    {
        self.value_accessor = Some(value);
        self
    }

    pub fn with_key_column_name(mut self, name: impl ToString) -> Self {
        self.key_column_name = Some(name.to_string());
        self
    }
}

impl<K, V> Access for UpsertChangeEvent<K, V>
where
    K: Access,
    V: Access,
{
    fn access(&self, path: &[&str], type_expected: Option<&DataType>) -> super::AccessResult {
        let create_error = |name: String| AccessError::Undefined {
            name,
            path: String::new(),
        };
        match path.first() {
            Some(&"key") => {
                if let Some(ka) = &self.key_accessor {
                    ka.access(&path[1..], type_expected)
                } else {
                    Err(create_error("key".to_string()))
                }
            }
            Some(&"value") => {
                if let Some(va) = &self.value_accessor {
                    va.access(&path[1..], type_expected)
                } else {
                    Err(create_error("value".to_string()))
                }
            }
            None => Ok(None),
            Some(other) => Err(create_error(other.to_string())),
        }
    }
}

impl<K, V> ChangeEvent for UpsertChangeEvent<K, V>
where
    K: Access,
    V: Access,
{
    fn op(&self) -> std::result::Result<ChangeEventOperation, AccessError> {
        if let Ok(Some(_)) = self.access(&["value"], None) {
            Ok(ChangeEventOperation::Upsert)
        } else {
            Ok(ChangeEventOperation::Delete)
        }
    }

    fn access_field(&self, desc: &SourceColumnDesc) -> super::AccessResult {
        match desc.additional_column_type {
            AdditionalColumnType::Key => {
                if let Some(key_as_column_name) = &self.key_column_name
                    && &desc.name == key_as_column_name
                {
                    self.access(&["key"], Some(&desc.data_type))
                } else {
                    self.access(&["key", &desc.name], Some(&desc.data_type))
                }
            }
            AdditionalColumnType::Unspecified | AdditionalColumnType::Normal => {
                self.access(&["value", &desc.name], Some(&desc.data_type))
            }
            _ => unreachable!(),
        }
    }
}
