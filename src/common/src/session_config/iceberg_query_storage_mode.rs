// Copyright 2026 RisingWave Labs
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

//! Storage mode for batch SELECT on Iceberg engine tables.
//! Iceberg engine tables have both Hummock (row store) and Iceberg (columnar) storage.
//! This setting controls which storage is used for batch queries.

use std::fmt::Formatter;
use std::str::FromStr;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub enum IcebergQueryStorageMode {
    /// Decided by the optimizer.
    Auto,

    /// Force batch SELECT to use Iceberg (columnar) storage.
    #[default]
    Iceberg,

    /// Force batch SELECT to use Hummock (row) storage.
    Hummock,
}

impl FromStr for IcebergQueryStorageMode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("auto") {
            Ok(Self::Auto)
        } else if s.eq_ignore_ascii_case("iceberg") {
            Ok(Self::Iceberg)
        } else if s.eq_ignore_ascii_case("hummock") {
            Ok(Self::Hummock)
        } else {
            Err("expect one of [auto, iceberg, hummock]")
        }
    }
}

impl std::fmt::Display for IcebergQueryStorageMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
            Self::Iceberg => write!(f, "iceberg"),
            Self::Hummock => write!(f, "hummock"),
        }
    }
}
