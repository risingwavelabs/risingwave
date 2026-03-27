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

use std::fmt::{Display, Formatter};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::system_param::ParamValue;

#[derive(PartialEq, Copy, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum SstableFilterKind {
    #[default]
    Xor16,
    Xor8,
}

impl Display for SstableFilterKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SstableFilterKind::Xor16 => write!(f, "xor16"),
            SstableFilterKind::Xor8 => write!(f, "xor8"),
        }
    }
}

impl From<SstableFilterKind> for String {
    fn from(value: SstableFilterKind) -> Self {
        value.to_string()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SstableFilterKindParseError {
    #[error("unsupported sstable filter kind: {0}")]
    Unsupported(String),
}

impl FromStr for SstableFilterKind {
    type Err = SstableFilterKindParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "xor16" => Ok(SstableFilterKind::Xor16),
            "xor8" => Ok(SstableFilterKind::Xor8),
            _ => Err(SstableFilterKindParseError::Unsupported(s.to_owned())),
        }
    }
}

impl TryFrom<String> for SstableFilterKind {
    type Error = SstableFilterKindParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl ParamValue for SstableFilterKind {
    type Borrowed<'a> = SstableFilterKind;
}

#[cfg(test)]
mod tests {
    use super::SstableFilterKind;

    #[test]
    fn test_parse_sstable_filter_kind() {
        assert_eq!(
            "xor16".parse::<SstableFilterKind>().unwrap(),
            SstableFilterKind::Xor16
        );
        assert_eq!(
            "XOR8".parse::<SstableFilterKind>().unwrap(),
            SstableFilterKind::Xor8
        );
        assert!("bfuse8".parse::<SstableFilterKind>().is_err());
    }
}
