//! Contains configurations that could be accessed via "set" command.

use risingwave_common::error::ErrorCode::InvalidConfigValue;
use risingwave_common::error::RwError;

use crate::config::QueryMode::{DISTRIBUTED, LOCAL};

pub static QUERY_MODE: &str = "query_mode";

#[derive(Debug, Clone)]
pub enum QueryMode {
    LOCAL,
    DISTRIBUTED,
}

impl Default for QueryMode {
    fn default() -> Self {
        DISTRIBUTED
    }
}

/// Parse query mode from string.
impl<'a> TryFrom<&'a str> for QueryMode {
    type Error = RwError;

    fn try_from(s: &'a str) -> Result<Self, RwError> {
        if s.eq_ignore_ascii_case("local") {
            Ok(LOCAL)
        } else if s.eq_ignore_ascii_case("distributed") {
            Ok(DISTRIBUTED)
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
        assert_matches!("local".try_into().unwrap(), QueryMode::LOCAL);
        assert_matches!("Local".try_into().unwrap(), QueryMode::LOCAL);
        assert_matches!("distributed".try_into().unwrap(), QueryMode::DISTRIBUTED);
        assert_matches!("diStributed".try_into().unwrap(), QueryMode::DISTRIBUTED);
        assert!(QueryMode::try_from("ab").is_err());
    }
}
