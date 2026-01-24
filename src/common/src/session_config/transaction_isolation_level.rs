// Copyright 2022 RisingWave Labs
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
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "read committed" => Ok(Self::ReadCommitted),
            "read uncommitted" => Ok(Self::ReadUncommitted),
            "repeatable read" => Ok(Self::RepeatableRead),
            "serializable" => Ok(Self::Serializable),
            _ => Err(
                "expect one of [read committed, read uncommitted, repeatable read, serializable]",
            ),
        }
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
