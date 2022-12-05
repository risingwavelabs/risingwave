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

use super::{HummockError, HummockResult};
use crate::storage_value::StorageValue;

pub const VALUE_DELETE: u8 = 1 << 0;
pub const VALUE_PUT: u8 = 0;

/// [`HummockValue`] can be created on either a `Vec<u8>` or a `&[u8]`.
///
/// Its encoding is a 1-byte flag + storage value. For `Put`, storage value contains both value meta
/// and user value. For `Delete`, storage value contains only value meta.
#[derive(Debug, Clone)]
pub enum HummockValue<T> {
    Put(T),
    Delete,
}

impl<T> Copy for HummockValue<T> where T: Copy {}

impl<T: PartialEq> PartialEq for HummockValue<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Put(l0), Self::Put(r0)) => l0.eq(r0),
            (Self::Delete, Self::Delete) => true,
            _ => false,
        }
    }
}

impl<T: Eq> Eq for HummockValue<T> {}

impl<T> HummockValue<T>
where
    T: PartialEq + Eq + AsRef<[u8]>,
{
    pub fn encoded_len(&self) -> usize {
        match self {
            HummockValue::Put(val) => 1 + val.as_ref().len(),
            HummockValue::Delete => 1,
        }
    }

    /// Encodes the object
    pub fn encode(&self, buffer: &mut impl BufMut) {
        match self {
            HummockValue::Put(val) => {
                // set flag
                buffer.put_u8(VALUE_PUT);
                buffer.put_slice(val.as_ref());
            }
            HummockValue::Delete => {
                // set flag
                buffer.put_u8(VALUE_DELETE);
            }
        }
    }

    /// Gets the user value out of the `HummockValue`. If the current value is `Delete`, `None` will
    /// be returned.
    pub fn into_user_value(self) -> Option<T> {
        match self {
            Self::Put(val) => Some(val),
            Self::Delete => None,
        }
    }

    pub fn is_delete(&self) -> bool {
        matches!(self, Self::Delete)
    }

    pub fn put(data: T) -> Self {
        Self::Put(data)
    }

    pub fn delete() -> Self {
        Self::Delete
    }
}

impl HummockValue<Vec<u8>> {
    /// Decodes the object from `Vec<u8>`.
    pub fn decode(buffer: &mut impl Buf) -> HummockResult<Self> {
        if buffer.remaining() == 0 {
            return Err(HummockError::decode_error("empty value"));
        }
        match buffer.get_u8() {
            VALUE_PUT => Ok(Self::Put(Vec::from(buffer.chunk()))),
            VALUE_DELETE => Ok(Self::Delete),
            _ => Err(HummockError::decode_error("non-empty but format error")),
        }
    }

    pub fn as_slice(&self) -> HummockValue<&[u8]> {
        match self {
            HummockValue::Put(data) => HummockValue::Put(data),
            HummockValue::Delete => HummockValue::Delete,
        }
    }
}

impl<'a> HummockValue<&'a [u8]> {
    /// Decodes the object from `&[u8]`.
    pub fn from_slice(mut buffer: &'a [u8]) -> HummockResult<Self> {
        if buffer.remaining() == 0 {
            return Err(HummockError::decode_error("empty value"));
        }
        match buffer.get_u8() {
            VALUE_PUT => Ok(Self::Put(buffer)),
            VALUE_DELETE => Ok(Self::Delete),
            _ => Err(HummockError::decode_error("non-empty but format error")),
        }
    }

    /// Copies `self` into [`HummockValue<Bytes>`].
    pub fn to_bytes(self) -> HummockValue<Bytes> {
        match self {
            HummockValue::Put(value) => HummockValue::Put(Bytes::copy_from_slice(value)),
            HummockValue::Delete => HummockValue::Delete,
        }
    }
}

impl HummockValue<Bytes> {
    pub fn as_slice(&self) -> HummockValue<&[u8]> {
        match self {
            HummockValue::Put(data) => HummockValue::Put(&data[..]),
            HummockValue::Delete => HummockValue::Delete,
        }
    }

    pub fn to_vec(&self) -> HummockValue<Vec<u8>> {
        match self {
            HummockValue::Put(data) => HummockValue::Put(data.to_vec()),
            HummockValue::Delete => HummockValue::Delete,
        }
    }
}

impl From<HummockValue<Vec<u8>>> for HummockValue<Bytes> {
    fn from(data: HummockValue<Vec<u8>>) -> Self {
        match data {
            HummockValue::Put(data) => HummockValue::Put(data.into()),
            HummockValue::Delete => HummockValue::Delete,
        }
    }
}

impl From<StorageValue> for HummockValue<Bytes> {
    fn from(data: StorageValue) -> Self {
        if data.is_some() {
            HummockValue::Put(data.user_value.unwrap_or_default())
        } else {
            HummockValue::Delete
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_vec_decode_encode() {
        let mut result = vec![];
        HummockValue::Put(b"233333".to_vec()).encode(&mut result);
        assert_eq!(
            HummockValue::Put(b"233333".to_vec()),
            HummockValue::decode(&mut &result[..]).unwrap()
        );
    }

    #[test]
    fn test_slice_decode_encode() {
        let mut result = vec![];
        HummockValue::Put(b"233333".to_vec()).encode(&mut result);
        assert_eq!(
            HummockValue::Put(b"233333".as_slice()),
            HummockValue::from_slice(&result).unwrap()
        );
    }
}
