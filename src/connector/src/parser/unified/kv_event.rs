// Copyright 2024 RisingWave Labs
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
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;

use super::Access;
use crate::parser::unified::AccessError;
use crate::source::SourceColumnDesc;

pub struct KvEvent<K, V> {
    key_accessor: Option<K>,
    value_accessor: Option<V>,
}

impl<K, V> Default for KvEvent<K, V> {
    fn default() -> Self {
        Self {
            key_accessor: None,
            value_accessor: None,
        }
    }
}

impl<K, V> KvEvent<K, V> {
    pub fn with_key(&mut self, key: K)
    where
        K: Access,
    {
        self.key_accessor = Some(key);
    }

    pub fn with_value(&mut self, value: V)
    where
        V: Access,
    {
        self.value_accessor = Some(value);
    }
}

impl<K, V> KvEvent<K, V>
where
    K: Access,
    V: Access,
{
    fn access_key(&self, path: &[&str], type_expected: &DataType) -> super::AccessResult {
        if let Some(ka) = &self.key_accessor {
            ka.access(path, type_expected)
        } else {
            Err(AccessError::Undefined {
                name: "key".to_string(),
                path: String::new(),
            })
        }
    }

    fn access_value(&self, path: &[&str], type_expected: &DataType) -> super::AccessResult {
        if let Some(va) = &self.value_accessor {
            va.access(path, type_expected)
        } else {
            Err(AccessError::Undefined {
                name: "value".to_string(),
                path: String::new(),
            })
        }
    }

    pub fn access_field(&self, desc: &SourceColumnDesc) -> super::AccessResult {
        match desc.additional_column.column_type {
            Some(AdditionalColumnType::Key(_)) => self.access_key(&[&desc.name], &desc.data_type),
            None => self.access_value(&[&desc.name], &desc.data_type),
            _ => unreachable!(),
        }
    }
}
