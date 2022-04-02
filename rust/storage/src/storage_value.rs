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

use bytes::{Buf, BufMut, Bytes};

/// Size of value meta in bytes. Since there might exist paddings between fields in `ValueMeta`, we
/// can't simply use `size_of` to retrieve its size.
pub const VALUE_META_SIZE: usize = 2;

/// Value meta stores some metadata for a kv pair. When writing to storage, it is located right
/// after user key and before user value. Currently, value meta consists of a 2-byte consistent hash
/// value.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ValueMeta {
    consistent_hash_value: u16,
}

impl From<ValueMeta> for Vec<u8> {
    fn from(value_meta: ValueMeta) -> Self {
        value_meta.consistent_hash_value.to_le_bytes().to_vec()
    }
}

impl From<ValueMeta> for Bytes {
    fn from(value_meta: ValueMeta) -> Self {
        Bytes::from_iter(value_meta.consistent_hash_value.to_le_bytes().into_iter())
    }
}

impl ValueMeta {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.put_u16_le(self.consistent_hash_value);
    }

    pub fn decode(buf: &mut &[u8]) -> Self {
        let consistent_hash_value = buf.get_u16_le();
        Self {
            consistent_hash_value,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageValue {
    value_meta: ValueMeta,
    user_value: Option<Bytes>,
}

impl StorageValue {
    pub fn new(value_meta: ValueMeta, user_value: Option<Bytes>) -> Self {
        Self {
            value_meta,
            user_value,
        }
    }

    pub fn new_put(value_meta: ValueMeta, user_value: impl Into<Bytes>) -> Self {
        Self {
            value_meta,
            user_value: Some(user_value.into()),
        }
    }

    pub fn new_default_put(user_value: impl Into<Bytes>) -> Self {
        Self {
            value_meta: Default::default(),
            user_value: Some(user_value.into()),
        }
    }

    pub fn new_delete(value_meta: ValueMeta) -> Self {
        Self {
            value_meta,
            user_value: None,
        }
    }

    pub fn new_default_delete() -> Self {
        Self {
            value_meta: Default::default(),
            user_value: None,
        }
    }

    /// Returns the length of the sum of value meta and user value in storage (not struct)
    pub fn size(&self) -> usize {
        VALUE_META_SIZE
            + self
                .user_value
                .as_ref()
                .map(|v| v.len())
                .unwrap_or_default()
    }

    /// Encode `StorageValue` into little-endian bytes
    pub fn encode_to_bytes(&self) -> Bytes {
        let mut buf = Vec::with_capacity(self.size());
        self.value_meta.encode(&mut buf);
        if let Some(user_value) = &self.user_value {
            buf.put_slice(&user_value[..]);
        }
        Bytes::from(buf)
    }

    /// Consumes `StorageValue` and returns user value
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

/// Consumes `value`, returns user value and discards value meta.
pub fn value_to_user_value(value: impl Into<Bytes>) -> Bytes {
    value.into().split_off(VALUE_META_SIZE)
}
