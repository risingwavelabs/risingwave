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

//! Contains configurations that could be accessed via "set" command.

use std::fmt::Formatter;
use std::str::FromStr;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub enum QueryMode {
    Auto,
    #[default]
    Local,

    Distributed,
}

impl FromStr for QueryMode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("local") {
            Ok(Self::Local)
        } else if s.eq_ignore_ascii_case("distributed") {
            Ok(Self::Distributed)
        } else if s.eq_ignore_ascii_case("auto") {
            Ok(Self::Auto)
        } else {
            Err("expect one of [local, distributed, auto]")
        }
    }
}

impl std::fmt::Display for QueryMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Auto => write!(f, "auto"),
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
        assert_eq!(QueryMode::from_str("auto").unwrap(), QueryMode::Auto);
        assert_eq!(QueryMode::from_str("Auto").unwrap(), QueryMode::Auto);
        assert_eq!(QueryMode::from_str("local").unwrap(), QueryMode::Local);
        assert_eq!(QueryMode::from_str("Local").unwrap(), QueryMode::Local);
        assert_eq!(
            QueryMode::from_str("distributed").unwrap(),
            QueryMode::Distributed
        );
        assert_eq!(
            QueryMode::from_str("diStributed").unwrap(),
            QueryMode::Distributed
        );
        assert!(QueryMode::from_str("ab").is_err());
    }
}
