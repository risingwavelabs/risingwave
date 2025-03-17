// Copyright 2025 RisingWave Labs
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

//! Contains configurations that could be accessed via "set" command.

use std::fmt::Formatter;
use std::str::FromStr;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub enum VisibilityMode {
    // apply frontend config.
    #[default]
    Default,
    // read barrier from streaming compute node.
    All,
    // read checkpoint from serving compute node.
    Checkpoint,
}

impl FromStr for VisibilityMode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("all") {
            Ok(Self::All)
        } else if s.eq_ignore_ascii_case("checkpoint") {
            Ok(Self::Checkpoint)
        } else if s.eq_ignore_ascii_case("default") {
            Ok(Self::Default)
        } else {
            Err("expect one of [all, checkpoint, default]")
        }
    }
}

impl std::fmt::Display for VisibilityMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Default => write!(f, "default"),
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
            VisibilityMode::from_str("all").unwrap(),
            VisibilityMode::All
        );
        assert_eq!(
            VisibilityMode::from_str("All").unwrap(),
            VisibilityMode::All
        );
        assert_eq!(
            VisibilityMode::from_str("checkpoint").unwrap(),
            VisibilityMode::Checkpoint
        );
        assert_eq!(
            VisibilityMode::from_str("checkPoint").unwrap(),
            VisibilityMode::Checkpoint
        );
        assert_eq!(
            VisibilityMode::from_str("default").unwrap(),
            VisibilityMode::Default
        );
        assert!(VisibilityMode::from_str("ab").is_err());
    }
}
