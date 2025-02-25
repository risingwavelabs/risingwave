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

use std::cmp::{max, min};
use std::fmt::{Display, Formatter};
use std::num::NonZeroUsize;
use std::str::FromStr;

use regex::Regex;
use risingwave_common::system_param::ParamValue;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(PartialEq, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum AdaptiveParallelismStrategy {
    Auto,
    Full,
    Bounded(NonZeroUsize),
    Ratio(f32),
}

impl Default for AdaptiveParallelismStrategy {
    fn default() -> Self {
        AdaptiveParallelismStrategy::Auto
    }
}

impl Display for AdaptiveParallelismStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AdaptiveParallelismStrategy::Auto => write!(f, "AUTO"),
            AdaptiveParallelismStrategy::Full => write!(f, "FULL"),
            AdaptiveParallelismStrategy::Bounded(n) => write!(f, "BOUNDED({})", n),
            AdaptiveParallelismStrategy::Ratio(r) => write!(f, "RATIO({})", r),
        }
    }
}

impl Into<String> for AdaptiveParallelismStrategy {
    fn into(self) -> String {
        self.to_string()
    }
}

#[derive(Error, Debug)]
pub enum ParallelismStrategyParseError {
    #[error("Unsupported strategy: {0}")]
    UnsupportedStrategy(String),

    #[error("Parse error: {0}")]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error("Parse error: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),

    #[error("Invalid value for Bounded strategy: must be positive integer")]
    InvalidBoundedValue,

    #[error("Invalid value for Ratio strategy: must be between 0.0 and 1.0")]
    InvalidRatioValue,
}

impl AdaptiveParallelismStrategy {
    pub fn compute_target_parallelism(&self, current_parallelism: usize) -> usize {
        match self {
            AdaptiveParallelismStrategy::Auto | AdaptiveParallelismStrategy::Full => {
                current_parallelism
            }
            AdaptiveParallelismStrategy::Bounded(n) => min(n.get(), current_parallelism),
            AdaptiveParallelismStrategy::Ratio(r) => {
                max((current_parallelism as f32 * r).floor() as usize, 1)
            }
        }
    }
}

pub fn parse_strategy(
    input: &str,
) -> Result<AdaptiveParallelismStrategy, ParallelismStrategyParseError> {
    lazy_static::lazy_static! {
        static ref RE: Regex = Regex::new(r"(?xi)
            ^
            (?P<auto>auto)                       # Auto strategy
            | (?P<full>full)                     # Full strategy
            | bounded\((?P<bounded_value>\d+)\)  # Bounded with value
            | ratio\((?P<ratio_value>[0-9.]+)\)  # Ratio with value
            $
        ").unwrap();
    }

    let input = input.trim();

    let caps = RE
        .captures(input)
        .ok_or_else(|| ParallelismStrategyParseError::UnsupportedStrategy(input.to_string()))?;

    if caps.name("auto").is_some() {
        Ok(AdaptiveParallelismStrategy::Auto)
    } else if caps.name("full").is_some() {
        Ok(AdaptiveParallelismStrategy::Full)
    } else if let Some(m) = caps.name("bounded_value") {
        let value = m.as_str().parse::<u64>()?;
        let non_zero = NonZeroUsize::new(value as usize)
            .ok_or(ParallelismStrategyParseError::InvalidBoundedValue)?;
        Ok(AdaptiveParallelismStrategy::Bounded(non_zero))
    } else if let Some(m) = caps.name("ratio_value") {
        let value = m.as_str().parse::<f32>()?;
        if !(0.0..=1.0).contains(&value) {
            return Err(ParallelismStrategyParseError::InvalidRatioValue);
        }
        Ok(AdaptiveParallelismStrategy::Ratio(value))
    } else {
        Err(ParallelismStrategyParseError::UnsupportedStrategy(
            input.to_string(),
        ))
    }
}

impl FromStr for AdaptiveParallelismStrategy {
    type Err = ParallelismStrategyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_strategy(s)
    }
}

impl ParamValue for AdaptiveParallelismStrategy {
    type Borrowed<'a> = AdaptiveParallelismStrategy;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_strategies() {
        assert_eq!(
            parse_strategy("Auto").unwrap(),
            AdaptiveParallelismStrategy::Auto
        );
        assert_eq!(
            parse_strategy("FULL").unwrap(),
            AdaptiveParallelismStrategy::Full
        );

        let bounded = parse_strategy("Bounded(42)").unwrap();
        assert!(matches!(bounded, AdaptiveParallelismStrategy::Bounded(n) if n.get() == 42));

        let ratio = parse_strategy("Ratio(0.75)").unwrap();
        assert!(matches!(ratio, AdaptiveParallelismStrategy::Ratio(0.75)));
    }

    #[test]
    fn test_invalid_values() {
        assert!(matches!(
            parse_strategy("Bounded(0)"),
            Err(ParallelismStrategyParseError::InvalidBoundedValue)
        ));

        assert!(matches!(
            parse_strategy("Ratio(1.1)"),
            Err(ParallelismStrategyParseError::InvalidRatioValue)
        ));

        assert!(matches!(
            parse_strategy("Ratio(-0.5)"),
            Err(ParallelismStrategyParseError::ParseFloatError(_))
        ));
    }

    #[test]
    fn test_unsupported_formats() {
        assert!(matches!(
            parse_strategy("Invalid"),
            Err(ParallelismStrategyParseError::UnsupportedStrategy(_))
        ));

        assert!(matches!(
            parse_strategy("Auto(5)"),
            Err(ParallelismStrategyParseError::UnsupportedStrategy(_))
        ));
    }
}
