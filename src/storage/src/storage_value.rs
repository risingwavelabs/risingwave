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
use risingwave_common::hash::VirtualNode;

/// Size of value meta in bytes. Since there might exist paddings between fields in `ValueMeta`, we
/// can't simply use `size_of` to retrieve its size.
pub const VALUE_META_SIZE: usize = 2;

/// Value meta stores some metadata for a kv pair. When writing to storage, it is located right
/// after user key and before user value. Currently, value meta consists of a 2-byte consistent hash
/// value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ValueMeta {
    pub vnode: VirtualNode,
}

impl From<ValueMeta> for Vec<u8> {
    fn from(value_meta: ValueMeta) -> Self {
        value_meta.vnode.to_le_bytes().to_vec()
    }
}

impl From<ValueMeta> for Bytes {
    fn from(value_meta: ValueMeta) -> Self {
        Bytes::from_iter(value_meta.vnode.to_le_bytes().into_iter())
    }
}

impl ValueMeta {
    pub fn with_vnode(vnode: VirtualNode) -> Self {
        Self { vnode }
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u16_le(self.vnode);
    }

    pub fn decode(mut buf: impl Buf) -> Self {
        let vnode = buf.get_u16_le();
        Self { vnode }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageValue {
    pub value_meta: ValueMeta,
    pub user_value: Option<Bytes>,
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

    /// Returns the length of the sum of value meta and user value in storage
    pub fn size(&self) -> usize {
        VALUE_META_SIZE
            + self
                .user_value
                .as_ref()
                .map(|v| v.len())
                .unwrap_or_default()
    }

    pub fn is_some(&self) -> bool {
        self.user_value.is_some()
    }

    pub fn is_none(&self) -> bool {
        self.user_value.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_meta_decode_encode() {
        let mut result = vec![];
        let value_meta = ValueMeta { vnode: 5678 };
        value_meta.encode(&mut result);
        assert_eq!(ValueMeta::decode(&mut &result[..]), value_meta);
    }
}
