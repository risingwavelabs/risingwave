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

//! Contains configurations that could be accessed via "set" command.

use std::fmt::Formatter;

use super::{ConfigEntry, CONFIG_KEYS, QUERY_MODE};
use crate::error::ErrorCode::{self, InvalidConfigValue};
use crate::error::RwError;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub enum QueryMode {
    #[default]
    Local,

    Distributed,
}

impl ConfigEntry for QueryMode {
    fn entry_name() -> &'static str {
        CONFIG_KEYS[QUERY_MODE]
    }
}

impl TryFrom<&[&str]> for QueryMode {
    type Error = RwError;

    fn try_from(value: &[&str]) -> Result<Self, Self::Error> {
        if value.len() != 1 {
            return Err(ErrorCode::InternalError(format!(
                "SET {} takes only one argument",
                Self::entry_name()
            ))
            .into());
        }

        let s = value[0];
        if s.eq_ignore_ascii_case("local") {
            Ok(Self::Local)
        } else if s.eq_ignore_ascii_case("distributed") {
            Ok(Self::Distributed)
        } else {
            Err(InvalidConfigValue {
                config_entry: Self::entry_name().to_string(),
                config_value: s.to_string(),
            })?
        }
    }
}

impl std::fmt::Display for QueryMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local => write!(f, "local"),
            Self::Distributed => write!(f, "distributed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_query_mode() {
        assert_eq!(
            QueryMode::try_from(["local"].as_slice()).unwrap(),
            QueryMode::Local
        );
        assert_eq!(
            QueryMode::try_from(["Local"].as_slice()).unwrap(),
            QueryMode::Local
        );
        assert_eq!(
            QueryMode::try_from(["distributed"].as_slice()).unwrap(),
            QueryMode::Distributed
        );
        assert_eq!(
            QueryMode::try_from(["diStributed"].as_slice()).unwrap(),
            QueryMode::Distributed
        );
        assert!(QueryMode::try_from(["ab"].as_slice()).is_err());
    }
}
