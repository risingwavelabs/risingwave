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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageValue {
    user_value: Bytes,
}

impl From<Bytes> for StorageValue {
    fn from(bytes: Bytes) -> Self {
        Self { user_value: bytes }
    }
}

impl From<Vec<u8>> for StorageValue {
    fn from(data: Vec<u8>) -> Self {
        Self {
            user_value: data.into(),
        }
    }
}

impl StorageValue {
    /// Returns the length of user value (value meta is excluded)
    pub fn len(&self) -> usize {
        self.user_value.len()
    }

    /// Returns whether user value is null
    pub fn is_empty(&self) -> bool {
        self.user_value.is_empty()
    }

    /// Consumes the value and returns a Bytes instance containing identical bytes
    pub fn to_bytes(self) -> Bytes {
        self.user_value
    }

    /// Returns a reference of all Bytes
    pub fn as_bytes(&self) -> &Bytes {
        &self.user_value
    }
}
