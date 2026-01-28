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

use std::fmt::Formatter;
use std::str::FromStr;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct StatementTimeout(pub u32);

impl FromStr for StatementTimeout {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s == "0" {
            return Ok(Self(0));
        }

        let (val_str, unit) = s
            .find(|c: char| !c.is_numeric())
            .map(|i| s.split_at(i))
            .ok_or_else(|| {
                format!(
                    "invalid value for parameter \"statement_timeout\": \"{}\"",
                    s
                )
            })?;

        let val = val_str.parse::<u64>().map_err(|_| {
            format!(
                "invalid value for parameter \"statement_timeout\": \"{}\"",
                s
            )
        })?;

        let mul = match unit.trim() {
            "ms" => 1,
            "s" => 1000,
            "min" => 60 * 1000,
            "h" => 60 * 60 * 1000,
            "d" => 24 * 60 * 60 * 1000,
            _ => {
                return Err(format!(
                    "invalid value for parameter \"statement_timeout\": \"{}\"",
                    s
                ));
            }
        };

        Ok(Self((val * mul) as u32))
    }
}

impl std::fmt::Display for StatementTimeout {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}ms", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_statement_timeout() {
        assert_eq!(
            "0".parse::<StatementTimeout>().unwrap(),
            StatementTimeout(0)
        );
        assert_eq!(
            "100ms".parse::<StatementTimeout>().unwrap(),
            StatementTimeout(100)
        );
        assert_eq!(
            "1s".parse::<StatementTimeout>().unwrap(),
            StatementTimeout(1000)
        );
        assert_eq!(
            "1min".parse::<StatementTimeout>().unwrap(),
            StatementTimeout(60000)
        );
        assert_eq!(
            "1h".parse::<StatementTimeout>().unwrap(),
            StatementTimeout(3600000)
        );
        assert_eq!(
            "1d".parse::<StatementTimeout>().unwrap(),
            StatementTimeout(86400000)
        );
        assert_eq!(
            " 100 ms ".parse::<StatementTimeout>().unwrap(),
            StatementTimeout(100)
        );

        assert!(
            "100".parse::<StatementTimeout>().is_err(),
            "should fail without unit"
        );
        assert!(
            "100x".parse::<StatementTimeout>().is_err(),
            "should fail with invalid unit"
        );
    }
}
