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

use risingwave_common::error::ErrorCode::InvalidConfigValue;
use risingwave_common::error::RwError;

use crate::config::QueryMode::{Distributed, Local};

pub static QUERY_MODE: &str = "query_mode";

#[derive(Debug, Clone)]
pub enum QueryMode {
    Local,
    Distributed,
}

impl Default for QueryMode {
    fn default() -> Self {
        Distributed
    }
}

/// Parse query mode from string.
impl<'a> TryFrom<&'a str> for QueryMode {
    type Error = RwError;

    fn try_from(s: &'a str) -> Result<Self, RwError> {
        if s.eq_ignore_ascii_case("local") {
            Ok(Local)
        } else if s.eq_ignore_ascii_case("distributed") {
            Ok(Distributed)
        } else {
            Err(InvalidConfigValue {
                config_entry: QUERY_MODE.to_string(),
                config_value: s.to_string(),
            })?
        }
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use crate::config::QueryMode;

    #[test]
    fn parse_query_mode() {
        assert_matches!("local".try_into().unwrap(), QueryMode::Local);
        assert_matches!("Local".try_into().unwrap(), QueryMode::Local);
        assert_matches!("distributed".try_into().unwrap(), QueryMode::Distributed);
        assert_matches!("diStributed".try_into().unwrap(), QueryMode::Distributed);
        assert!(QueryMode::try_from("ab").is_err());
    }
}
