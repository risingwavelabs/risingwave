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

//! Locality backfill mode for streaming queries.

use std::fmt::Formatter;
use std::str::FromStr;

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub enum LocalityBackfillMode {
    Off,

    On,

    #[default]
    Auto,
}

impl FromStr for LocalityBackfillMode {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("off") || s.eq_ignore_ascii_case("false") {
            Ok(Self::Off)
        } else if s.eq_ignore_ascii_case("on") || s.eq_ignore_ascii_case("true") {
            Ok(Self::On)
        } else if s.eq_ignore_ascii_case("auto") {
            Ok(Self::Auto)
        } else {
            Err("expect one of [off, on, auto]")
        }
    }
}

impl std::fmt::Display for LocalityBackfillMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Off => write!(f, "off"),
            Self::On => write!(f, "on"),
            Self::Auto => write!(f, "auto"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_locality_backfill_mode() {
        assert_eq!(
            LocalityBackfillMode::from_str("auto").unwrap(),
            LocalityBackfillMode::Auto
        );
        assert_eq!(
            LocalityBackfillMode::from_str("on").unwrap(),
            LocalityBackfillMode::On
        );
        assert_eq!(
            LocalityBackfillMode::from_str("true").unwrap(),
            LocalityBackfillMode::On
        );
        assert_eq!(
            LocalityBackfillMode::from_str("off").unwrap(),
            LocalityBackfillMode::Off
        );
        assert_eq!(
            LocalityBackfillMode::from_str("false").unwrap(),
            LocalityBackfillMode::Off
        );
        assert!(LocalityBackfillMode::from_str("invalid").is_err());
    }
}
