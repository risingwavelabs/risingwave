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

use std::num::{NonZeroU64, NonZeroUsize, ParseIntError};
use std::str::FromStr;

use risingwave_common::system_param::adaptive_parallelism_strategy::{
    AdaptiveParallelismStrategy, ParallelismStrategyParseError, parse_strategy,
};

const KEYWORD_DEFAULT: &str = "default";
const KEYWORD_ADAPTIVE: &str = "adaptive";
const KEYWORD_AUTO: &str = "auto";
const KEYWORD_DEFAULT_STRATEGY: &str = "default";
const KEYWORD_SYSTEM_STRATEGY: &str = "system";

#[derive(Copy, Default, Debug, Clone, PartialEq, Eq)]
pub enum ConfigParallelism {
    #[default]
    Default,
    Fixed(NonZeroU64),
    Adaptive,
}

impl FromStr for ConfigParallelism {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            KEYWORD_DEFAULT => Ok(ConfigParallelism::Default),
            KEYWORD_ADAPTIVE | KEYWORD_AUTO => Ok(ConfigParallelism::Adaptive),
            s => {
                let parsed = s.parse::<u64>()?;
                if parsed == 0 {
                    Ok(ConfigParallelism::Adaptive)
                } else {
                    Ok(ConfigParallelism::Fixed(NonZeroU64::new(parsed).unwrap()))
                }
            }
        }
    }
}

impl std::fmt::Display for ConfigParallelism {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            ConfigParallelism::Adaptive => {
                write!(f, "{}", KEYWORD_ADAPTIVE)
            }
            ConfigParallelism::Default => {
                write!(f, "{}", KEYWORD_DEFAULT)
            }
            ConfigParallelism::Fixed(n) => {
                write!(f, "{}", n)
            }
        }
    }
}

#[derive(Copy, Default, Debug, Clone, PartialEq)]
pub enum ConfigAdaptiveParallelismStrategy {
    #[default]
    Default,
    Auto,
    Full,
    Bounded(NonZeroU64),
    Ratio(f32),
}

impl FromStr for ConfigAdaptiveParallelismStrategy {
    type Err = ParallelismStrategyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case(KEYWORD_SYSTEM_STRATEGY)
            || s.eq_ignore_ascii_case(KEYWORD_DEFAULT_STRATEGY)
        {
            return Ok(Self::Default);
        }

        let strategy = parse_strategy(s)?;
        Ok(strategy.into())
    }
}

impl From<AdaptiveParallelismStrategy> for ConfigAdaptiveParallelismStrategy {
    fn from(value: AdaptiveParallelismStrategy) -> Self {
        match value {
            AdaptiveParallelismStrategy::Auto => Self::Auto,
            AdaptiveParallelismStrategy::Full => Self::Full,
            AdaptiveParallelismStrategy::Bounded(n) => {
                // Safe to unwrap since `n` is non-zero.
                Self::Bounded(NonZeroU64::new(n.get() as u64).unwrap())
            }
            AdaptiveParallelismStrategy::Ratio(r) => Self::Ratio(r),
        }
    }
}

impl From<ConfigAdaptiveParallelismStrategy> for Option<AdaptiveParallelismStrategy> {
    fn from(value: ConfigAdaptiveParallelismStrategy) -> Self {
        match value {
            ConfigAdaptiveParallelismStrategy::Default => None,
            ConfigAdaptiveParallelismStrategy::Auto => Some(AdaptiveParallelismStrategy::Auto),
            ConfigAdaptiveParallelismStrategy::Full => Some(AdaptiveParallelismStrategy::Full),
            ConfigAdaptiveParallelismStrategy::Bounded(n) => {
                Some(AdaptiveParallelismStrategy::Bounded(
                    NonZeroUsize::new(n.get() as usize)
                        // Bounded strategy requires non-zero; `NonZeroU64` guarantees this.
                        .unwrap(),
                ))
            }
            ConfigAdaptiveParallelismStrategy::Ratio(r) => {
                Some(AdaptiveParallelismStrategy::Ratio(r))
            }
        }
    }
}

impl std::fmt::Display for ConfigAdaptiveParallelismStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigAdaptiveParallelismStrategy::Default => {
                write!(f, "{}", KEYWORD_SYSTEM_STRATEGY)
            }
            ConfigAdaptiveParallelismStrategy::Auto => AdaptiveParallelismStrategy::Auto.fmt(f),
            ConfigAdaptiveParallelismStrategy::Full => AdaptiveParallelismStrategy::Full.fmt(f),
            ConfigAdaptiveParallelismStrategy::Bounded(n) => {
                AdaptiveParallelismStrategy::Bounded(NonZeroUsize::new(n.get() as usize).unwrap())
                    .fmt(f)
            }
            ConfigAdaptiveParallelismStrategy::Ratio(r) => {
                AdaptiveParallelismStrategy::Ratio(*r).fmt(f)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_parse_default() {
        assert_eq!(
            "default"
                .parse::<ConfigAdaptiveParallelismStrategy>()
                .unwrap(),
            ConfigAdaptiveParallelismStrategy::Default
        );
    }

    #[test]
    fn test_strategy_parse_system() {
        assert_eq!(
            "system"
                .parse::<ConfigAdaptiveParallelismStrategy>()
                .unwrap(),
            ConfigAdaptiveParallelismStrategy::Default
        );
    }

    #[test]
    fn test_strategy_parse_ratio() {
        let strategy: ConfigAdaptiveParallelismStrategy = "Ratio(0.5)".parse().unwrap();
        assert_eq!(strategy, ConfigAdaptiveParallelismStrategy::Ratio(0.5));
    }

    #[test]
    fn test_strategy_into_option() {
        let opt: Option<AdaptiveParallelismStrategy> =
            ConfigAdaptiveParallelismStrategy::Full.into();
        assert_eq!(opt, Some(AdaptiveParallelismStrategy::Full));
    }
}
