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

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SimError {
    #[error("NotFound error: {0}")]
    NotFound(String),
    #[error("Other error: {0}")]
    Other(String),
}

impl SimError {
    pub fn is_object_not_found_error(&self) -> bool {
        matches!(self, SimError::NotFound(_))
    }
}

impl SimError {
    pub(crate) fn not_found(msg: impl ToString) -> Self {
        SimError::NotFound(msg.to_string())
    }

    pub(crate) fn other(msg: impl ToString) -> Self {
        SimError::Other(msg.to_string())
    }
}
