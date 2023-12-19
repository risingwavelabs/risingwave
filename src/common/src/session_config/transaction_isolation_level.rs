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
use std::str::FromStr;

use crate::error::{bail_not_implemented, NotImplemented};

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

impl FromStr for IsolationLevel {
    type Err = NotImplemented;

    fn from_str(_s: &str) -> Result<Self, Self::Err> {
        bail_not_implemented!(issue = 10736, "isolation level");
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadCommitted => write!(f, "read committed"),
            Self::ReadUncommitted => write!(f, "read uncommitted"),
            Self::RepeatableRead => write!(f, "repeatable read"),
            Self::Serializable => write!(f, "serializable"),
        }
    }
}
