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

#[derive(PartialEq, Copy, Clone, Debug, Serialize, Deserialize, Default)]
pub enum AdaptiveParallelismStrategy {
    #[default]
    Auto,
    Full,
    Bounded(NonZeroUsize),
    Ratio(f32),
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

impl From<AdaptiveParallelismStrategy> for String {
    fn from(val: AdaptiveParallelismStrategy) -> Self {
        val.to_string()
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
    let lower_input = input.to_lowercase();

    // Handle Auto/Full case-insensitively without regex
    match lower_input.as_str() {
        "auto" => return Ok(AdaptiveParallelismStrategy::Auto),
        "full" => return Ok(AdaptiveParallelismStrategy::Full),
        _ => (),
    }

    // Compile regex patterns once using OnceLock
    fn bounded_re() -> &'static Regex {
        static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
        RE.get_or_init(|| Regex::new(r"(?i)^bounded\((?<value>\d+)\)$").unwrap())
    }

    fn ratio_re() -> &'static Regex {
        static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
        RE.get_or_init(|| Regex::new(r"(?i)^ratio\((?<value>[+-]?\d+(?:\.\d+)?)\)$").unwrap())
    }

    // Try to match Bounded pattern
    if let Some(caps) = bounded_re().captures(&lower_input) {
        let value_str = caps.name("value").unwrap().as_str();
        let value: usize = value_str.parse()?;

        let value =
            NonZeroUsize::new(value).ok_or(ParallelismStrategyParseError::InvalidBoundedValue)?;

        return Ok(AdaptiveParallelismStrategy::Bounded(value));
    }

    // Try to match Ratio pattern
    if let Some(caps) = ratio_re().captures(&lower_input) {
        let value_str = caps.name("value").unwrap().as_str();
        let value: f32 = value_str.parse()?;

        if !(0.0..=1.0).contains(&value) {
            return Err(ParallelismStrategyParseError::InvalidRatioValue);
        }

        return Ok(AdaptiveParallelismStrategy::Ratio(value));
    }

    // If no patterns matched
    Err(ParallelismStrategyParseError::UnsupportedStrategy(
        input.to_owned(),
    ))
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
            Err(ParallelismStrategyParseError::InvalidRatioValue)
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

    #[test]
    fn test_auto_full_strategies() {
        let auto = AdaptiveParallelismStrategy::Auto;
        let full = AdaptiveParallelismStrategy::Full;

        // Basic cases
        assert_eq!(auto.compute_target_parallelism(1), 1);
        assert_eq!(auto.compute_target_parallelism(10), 10);
        assert_eq!(full.compute_target_parallelism(5), 5);
        assert_eq!(full.compute_target_parallelism(8), 8);

        // Edge cases
        assert_eq!(auto.compute_target_parallelism(usize::MAX), usize::MAX);
    }

    #[test]
    fn test_bounded_strategy() {
        let bounded_8 = AdaptiveParallelismStrategy::Bounded(NonZeroUsize::new(8).unwrap());
        let bounded_1 = AdaptiveParallelismStrategy::Bounded(NonZeroUsize::new(1).unwrap());

        // Below bound
        assert_eq!(bounded_8.compute_target_parallelism(5), 5);
        // Exactly at bound
        assert_eq!(bounded_8.compute_target_parallelism(8), 8);
        // Above bound
        assert_eq!(bounded_8.compute_target_parallelism(10), 8);
        // Minimum bound
        assert_eq!(bounded_1.compute_target_parallelism(1), 1);
        assert_eq!(bounded_1.compute_target_parallelism(2), 1);
    }

    #[test]
    fn test_ratio_strategy() {
        let ratio_half = AdaptiveParallelismStrategy::Ratio(0.5);
        let ratio_30pct = AdaptiveParallelismStrategy::Ratio(0.3);
        let ratio_full = AdaptiveParallelismStrategy::Ratio(1.0);

        // Normal calculations
        assert_eq!(ratio_half.compute_target_parallelism(4), 2);
        assert_eq!(ratio_half.compute_target_parallelism(5), 2);

        // Flooring behavior
        assert_eq!(ratio_30pct.compute_target_parallelism(3), 1);
        assert_eq!(ratio_30pct.compute_target_parallelism(4), 1);
        assert_eq!(ratio_30pct.compute_target_parallelism(5), 1);
        assert_eq!(ratio_30pct.compute_target_parallelism(7), 2);

        // Full ratio
        assert_eq!(ratio_full.compute_target_parallelism(5), 5);
    }

    #[test]
    fn test_edge_cases() {
        let ratio_overflow = AdaptiveParallelismStrategy::Ratio(2.5);
        assert_eq!(ratio_overflow.compute_target_parallelism(4), 10);

        let max_parallelism =
            AdaptiveParallelismStrategy::Bounded(NonZeroUsize::new(usize::MAX).unwrap());
        assert_eq!(
            max_parallelism.compute_target_parallelism(usize::MAX),
            usize::MAX
        );
    }
}
