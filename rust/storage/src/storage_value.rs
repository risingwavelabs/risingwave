// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use bytes::Bytes;

/// Size of value meta in bytes.
pub const VALUE_META_SIZE: usize = 2;

/// Value meta stores some metadata for a kv pair. When writing to storage, it is located right
/// after user key and before user value. Currently, value meta consists of a 2-byte consistent hash
/// value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValueMeta(Bytes);

impl Default for ValueMeta {
    fn default() -> Self {
        Self(vec![0u8; VALUE_META_SIZE].into())
    }
}

impl From<ValueMeta> for Vec<u8> {
    fn from(value_meta: ValueMeta) -> Self {
        value_meta.0.to_vec()
    }
}

impl From<ValueMeta> for Bytes {
    fn from(value_meta: ValueMeta) -> Self {
        value_meta.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageValue {
    value_meta: ValueMeta,
    user_value: Option<Bytes>,
}

impl StorageValue {
    pub fn new(value_meta: Bytes, user_value: Option<Bytes>) -> Self {
        Self {
            value_meta: ValueMeta(value_meta),
            user_value,
        }
    }

    pub fn new_put(value_meta: Bytes, user_value: impl Into<Bytes>) -> Self {
        Self {
            value_meta: ValueMeta(value_meta),
            user_value: Some(user_value.into()),
        }
    }

    pub fn new_default_put(user_value: impl Into<Bytes>) -> Self {
        Self {
            value_meta: Default::default(),
            user_value: Some(user_value.into()),
        }
    }

    pub fn new_delete(value_meta: Bytes) -> Self {
        Self {
            value_meta: ValueMeta(value_meta),
            user_value: None,
        }
    }

    pub fn new_default_delete() -> Self {
        Self {
            value_meta: Default::default(),
            user_value: None,
        }
    }

    /// Returns the length of the sum of value meta and user value
    pub fn size(&self) -> usize {
        self.value_meta.0.len()
            + self
                .user_value
                .as_ref()
                .map(|v| v.len())
                .unwrap_or_default()
    }

    /// Consumes itself and returns a `Bytes` instance containing both value meta and user value
    pub fn to_bytes(self) -> Bytes {
        [self.value_meta.0, self.user_value.unwrap_or_default()]
            .concat()
            .into()
    }

    /// Consumes itself and returns user value
    pub fn user_value(self) -> Option<Bytes> {
        self.user_value
    }

    pub fn is_some(&self) -> bool {
        self.user_value.is_some()
    }

    pub fn is_none(&self) -> bool {
        self.user_value.is_none()
    }
}

/// Consumes `value`, which contains all bytes including value meta and user value. Returns user
/// value.
pub fn value_to_user_value(value: impl Into<Bytes>) -> Bytes {
    value.into().split_off(VALUE_META_SIZE)
}
