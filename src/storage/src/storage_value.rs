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
    pub user_value: Option<Bytes>,
}

impl StorageValue {
    pub fn new(user_value: Option<Bytes>) -> Self {
        Self { user_value }
    }

    pub fn new_put(user_value: impl Into<Bytes>) -> Self {
        Self {
            user_value: Some(user_value.into()),
        }
    }

    pub fn new_delete() -> Self {
        Self { user_value: None }
    }

    /// Returns the length of the sum of value meta and user value in storage
    pub fn size(&self) -> usize {
        self.user_value
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
