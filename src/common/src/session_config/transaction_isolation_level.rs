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

use std::fmt::Formatter;

use crate::error::{ErrorCode, RwError};
use crate::session_config::{ConfigEntry, CONFIG_KEYS, TRANSACTION_ISOLATION_LEVEL};

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
// Some variants are never constructed so allow dead code here.
#[allow(dead_code)]
pub enum IsolationLevel {
    #[default]
    ReadCommitted,
    ReadUncommitted,
    RepeatableRead,
    Serializable,
}

impl ConfigEntry for IsolationLevel {
    fn entry_name() -> &'static str {
        CONFIG_KEYS[TRANSACTION_ISOLATION_LEVEL]
    }
}

impl TryFrom<&[&str]> for IsolationLevel {
    type Error = RwError;

    fn try_from(_value: &[&str]) -> Result<Self, Self::Error> {
        Err(
            ErrorCode::InternalError("Support set transaction isolation level first".to_string())
                .into(),
        )
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadCommitted => write!(f, "READ_COMMITTED"),
            Self::ReadUncommitted => write!(f, "READ_UNCOMMITTED"),
            Self::RepeatableRead => write!(f, "REPEATABLE_READ"),
            Self::Serializable => write!(f, "SERIALIZABLE"),
        }
    }
}
