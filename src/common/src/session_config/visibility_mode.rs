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

use super::{ConfigEntry, CONFIG_KEYS};
use crate::error::ErrorCode::{self, InvalidConfigValue};
use crate::error::RwError;
use crate::session_config::VISIBILITY_MODE;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub enum VisibilityMode {
    #[default]
    Checkpoint,

    All,
}

impl ConfigEntry for VisibilityMode {
    fn entry_name() -> &'static str {
        CONFIG_KEYS[VISIBILITY_MODE]
    }
}

impl TryFrom<&[&str]> for VisibilityMode {
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
        if s.eq_ignore_ascii_case("all") {
            Ok(Self::All)
        } else if s.eq_ignore_ascii_case("checkpoint") {
            Ok(Self::Checkpoint)
        } else {
            Err(InvalidConfigValue {
                config_entry: Self::entry_name().to_string(),
                config_value: s.to_string(),
            })?
        }
    }
}

impl std::fmt::Display for VisibilityMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Checkpoint => write!(f, "checkpoint"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_query_mode() {
        assert_eq!(
            VisibilityMode::try_from(["all"].as_slice()).unwrap(),
            VisibilityMode::All
        );
        assert_eq!(
            VisibilityMode::try_from(["All"].as_slice()).unwrap(),
            VisibilityMode::All
        );
        assert_eq!(
            VisibilityMode::try_from(["checkpoint"].as_slice()).unwrap(),
            VisibilityMode::Checkpoint
        );
        assert_eq!(
            VisibilityMode::try_from(["checkPoint"].as_slice()).unwrap(),
            VisibilityMode::Checkpoint
        );
        assert!(VisibilityMode::try_from(["ab"].as_slice()).is_err());
    }
}
