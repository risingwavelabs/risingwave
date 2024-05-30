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

use std::ops::{Deref, DerefMut};

use risingwave_common::types::DataType;
use risingwave_pb::plan_common::additional_column::ColumnType as AdditionalColumnType;

use super::{Access, ChangeEvent, ChangeEventOperation, NullableAccess};
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

    pub fn access_field_impl(&self, desc: &SourceColumnDesc) -> super::AccessResult {
        match desc.additional_column.column_type {
            Some(AdditionalColumnType::Key(_)) => self.access_key(&[&desc.name], &desc.data_type),
            None => self.access_value(&[&desc.name], &desc.data_type),
            _ => unreachable!(),
        }
    }
}

/// Wraps a key-value message into an upsert event, which uses `null` value to represent `DELETE`s.
pub struct UpsertChangeEvent<K, V>(KvEvent<K, V>);

impl<K, V> Default for UpsertChangeEvent<K, V> {
    fn default() -> Self {
        Self(KvEvent::default())
    }
}

impl<K, V> Deref for UpsertChangeEvent<K, V> {
    type Target = KvEvent<K, V>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K, V> DerefMut for UpsertChangeEvent<K, V> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<K, V> ChangeEvent for UpsertChangeEvent<K, V>
where
    K: Access,
    V: NullableAccess,
{
    fn op(&self) -> std::result::Result<ChangeEventOperation, AccessError> {
        if let Some(va) = &self.0.value_accessor {
            if va.is_null() {
                Ok(ChangeEventOperation::Delete)
            } else {
                Ok(ChangeEventOperation::Upsert)
            }
        } else {
            Err(AccessError::Undefined {
                name: "value".to_string(),
                path: String::new(),
            })
        }
    }

    fn access_field(&self, desc: &SourceColumnDesc) -> super::AccessResult {
        self.0.access_field_impl(desc)
    }
}

pub type PlainEvent<K, V> = KvEvent<K, V>;
