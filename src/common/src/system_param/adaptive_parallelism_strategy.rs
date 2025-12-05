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
use smallvec::SmallVec;
use thiserror::Error;

/// Use `#[serde(try_from, into)]` to serialize/deserialize as string format (e.g., "Bounded(64)"),
/// which is consistent with `ALTER SYSTEM SET` command.
#[derive(PartialEq, Clone, Debug, Default, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum AdaptiveParallelismStrategy {
    #[default]
    Auto,
    Full,
    Bounded(NonZeroUsize),
    Ratio(f32),
    LinearCurve(LinearCurve),
}

impl TryFrom<String> for AdaptiveParallelismStrategy {
    type Error = ParallelismStrategyParseError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl Display for AdaptiveParallelismStrategy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AdaptiveParallelismStrategy::Auto => write!(f, "AUTO"),
            AdaptiveParallelismStrategy::Full => write!(f, "FULL"),
            AdaptiveParallelismStrategy::Bounded(n) => write!(f, "BOUNDED({})", n),
            AdaptiveParallelismStrategy::Ratio(r) => write!(f, "RATIO({})", r),
            AdaptiveParallelismStrategy::LinearCurve(curve) => {
                write!(f, "LINEAR_CURVE({})", curve)
            }
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

    #[error("Invalid value for LinearCurve strategy: {0}")]
    InvalidLinearCurveValue(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinearCurvePoint {
    pub available_parallelism: usize,
    pub target_ratio: f32,
}

impl LinearCurvePoint {
    fn new(available_parallelism: usize, target_ratio: f32) -> Result<Self, String> {
        if !(0.0..=1.0).contains(&target_ratio) {
            return Err(format!("target ratio out of range [0,1]: {}", target_ratio));
        }
        Ok(Self {
            available_parallelism,
            target_ratio,
        })
    }
}

/// A simple piecewise-linear curve defined on available parallelism (integer) -> target ratio [0,1].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LinearCurve {
    pub points: SmallVec<[LinearCurvePoint; 8]>,
}

impl LinearCurve {
    fn interpolate(&self, available_parallelism: usize) -> f32 {
        let points = &self.points;
        // Guard: points len >= 2 ensured at construction.
        if available_parallelism <= points[0].available_parallelism {
            return points[0].target_ratio;
        }
        if let Some(last) = points.last()
            && available_parallelism >= last.available_parallelism
        {
            return last.target_ratio;
        }

        // Find segment.
        for window in points.windows(2) {
            let left = window[0];
            let right = window[1];
            if (left.available_parallelism..=right.available_parallelism)
                .contains(&available_parallelism)
            {
                if right.available_parallelism == left.available_parallelism {
                    return left.target_ratio;
                }
                let span = (right.available_parallelism - left.available_parallelism) as f32;
                let delta = (available_parallelism - left.available_parallelism) as f32;
                let t = delta / span;
                return left.target_ratio + t * (right.target_ratio - left.target_ratio);
            }
        }

        // Should not reach here; return last safely.
        points.last().map(|p| p.target_ratio).unwrap_or(1.0)
    }
}

impl std::fmt::Display for LinearCurve {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut first = true;
        for p in &self.points {
            if !first {
                write!(f, ",")?;
            }
            write!(
                f,
                "{}:{}",
                p.available_parallelism,
                trim_trailing_zero(p.target_ratio)
            )?;
            first = false;
        }
        Ok(())
    }
}

fn trim_trailing_zero(value: f32) -> String {
    let mut s = format!("{}", value);
    if s.contains('.') {
        while s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
    }
    if s.is_empty() {
        s.push('0');
    }
    s
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
            AdaptiveParallelismStrategy::LinearCurve(curve) => {
                let ratio = curve.interpolate(current_parallelism);
                let target = (current_parallelism as f32 * ratio).floor() as usize;
                max(target, 1)
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

    fn linear_curve_re() -> &'static Regex {
        static RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
        RE.get_or_init(|| Regex::new(r"(?i)^linear_curve\((?<body>.+)\)$").unwrap())
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

    // Try to match LinearCurve pattern
    if let Some(caps) = linear_curve_re().captures(input) {
        let body = caps.name("body").unwrap().as_str();
        let curve = parse_linear_curve(body)
            .map_err(ParallelismStrategyParseError::InvalidLinearCurveValue)?;
        return Ok(AdaptiveParallelismStrategy::LinearCurve(curve));
    }

    // If no patterns matched
    Err(ParallelismStrategyParseError::UnsupportedStrategy(
        input.to_owned(),
    ))
}

fn parse_linear_curve(body: &str) -> Result<LinearCurve, String> {
    let mut points: SmallVec<[LinearCurvePoint; 8]> = SmallVec::new();

    for pair in body.split(',') {
        let trimmed = pair.trim();
        if trimmed.is_empty() {
            return Err("empty pair".to_owned());
        }
        let (a_str, b_str) = trimmed
            .split_once(':')
            .ok_or_else(|| format!("invalid pair '{}', expected a:b", trimmed))?;
        let available: usize = a_str
            .parse()
            .map_err(|e| format!("invalid available '{}': {e}", a_str))?;
        let target: f32 = b_str
            .parse()
            .map_err(|e| format!("invalid target '{}': {e}", b_str))?;
        points.push(LinearCurvePoint::new(available, target)?);
    }

    if points.len() < 2 {
        return Err("requires at least two points".to_owned());
    }

    // Sort and dedup check.
    points.sort_by_key(|p| p.available_parallelism);

    // Reject duplicate available.
    for w in points.windows(2) {
        if w[0].available_parallelism == w[1].available_parallelism {
            return Err(format!(
                "duplicate available parallelism {}",
                w[0].available_parallelism
            ));
        }
    }

    // Auto pad with 0 if needed.
    if let Some(first) = points.first().copied()
        && first.available_parallelism > 0
    {
        points.insert(0, LinearCurvePoint::new(0, first.target_ratio)?);
    }

    // Auto pad with 1 if missing.
    let has_one = points.iter().any(|p| p.available_parallelism == 1usize);
    if !has_one {
        let ratio_at_one = interpolate_for_padding(&points, 1);
        points.insert(
            find_insert_pos(&points, 1),
            LinearCurvePoint::new(1, ratio_at_one)?,
        );
    }

    // Final validation len>=2 and monotonic increasing already guaranteed.
    if points.len() < 2 {
        return Err("requires at least two points".to_owned());
    }

    Ok(LinearCurve { points })
}

fn interpolate_for_padding(points: &[LinearCurvePoint], target_available: usize) -> f32 {
    // points assumed sorted, len >=1. Used only for auto-padding missing anchors (e.g., insert 1).
    // Behavior:
    // - If target is before the first point: clamp to first.
    // - If after the last point: clamp to last.
    // - Otherwise: linear interpolate within the containing segment.
    if points.is_empty() {
        return 0.0;
    }
    if target_available <= points[0].available_parallelism {
        return points[0].target_ratio;
    }
    if let Some(last) = points.last()
        && target_available >= last.available_parallelism
    {
        return last.target_ratio;
    }
    for w in points.windows(2) {
        let left = w[0];
        let right = w[1];
        if (left.available_parallelism..=right.available_parallelism).contains(&target_available) {
            let span = (right.available_parallelism - left.available_parallelism) as f32;
            if span == 0.0 {
                return left.target_ratio;
            }
            let t = (target_available - left.available_parallelism) as f32 / span;
            return left.target_ratio + t * (right.target_ratio - left.target_ratio);
        }
    }
    points.last().map(|p| p.target_ratio).unwrap_or(0.0)
}

fn find_insert_pos(points: &[LinearCurvePoint], available: usize) -> usize {
    points
        .binary_search_by_key(&available, |p| p.available_parallelism)
        .unwrap_or_else(|pos| pos)
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
        // Bounded(0) is invalid - must be positive
        assert!(matches!(
            parse_strategy("Bounded(0)"),
            Err(ParallelismStrategyParseError::InvalidBoundedValue)
        ));

        // Ratio out of range [0.0, 1.0]
        assert!(matches!(
            parse_strategy("Ratio(1.1)"),
            Err(ParallelismStrategyParseError::InvalidRatioValue)
        ));
        assert!(matches!(
            parse_strategy("Ratio(-0.5)"),
            Err(ParallelismStrategyParseError::InvalidRatioValue)
        ));
        assert!(matches!(
            parse_strategy("Ratio(-1)"),
            Err(ParallelismStrategyParseError::InvalidRatioValue)
        ));

        // Invalid number format - regex won't match
        assert!(matches!(
            parse_strategy("Ratio(-0.a)"),
            Err(ParallelismStrategyParseError::UnsupportedStrategy(_))
        ));

        // Negative bounded - regex won't match (only \d+ allowed)
        assert!(matches!(
            parse_strategy("Bounded(-5)"),
            Err(ParallelismStrategyParseError::UnsupportedStrategy(_))
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

    #[test]
    fn test_linear_curve_parse_and_compute() {
        let curve = parse_strategy("linear_curve(0:0,10:0.5,20:0.8)").unwrap();
        match curve {
            AdaptiveParallelismStrategy::LinearCurve(curve) => {
                // auto pad 1 with interpolation between 0 and 10 => 0.05
                assert_eq!(curve.points[0].available_parallelism, 0);
                assert_eq!(curve.points[1].available_parallelism, 1);
                assert!((curve.points[1].target_ratio - 0.05).abs() < 1e-6);

                // interpolate
                let ratio = curve.interpolate(15);
                assert!((ratio - 0.65).abs() < 1e-6);
                // compute target with current_parallelism=15, ratio=0.65 => 9.75 -> 9 after floor, min 1
                assert_eq!(
                    AdaptiveParallelismStrategy::LinearCurve(curve).compute_target_parallelism(15),
                    9
                );
            }
            _ => panic!("expected linear curve"),
        }
    }

    #[test]
    fn test_linear_curve_auto_padding_and_ordering() {
        // Missing 0 and 1, unordered input should be sorted.
        let curve = parse_strategy("LINEAR_CURVE(5:0.2,3:0.1)").unwrap();
        match curve {
            AdaptiveParallelismStrategy::LinearCurve(curve) => {
                assert_eq!(curve.points[0].available_parallelism, 0);
                assert_eq!(curve.points[1].available_parallelism, 1);
                // Sorted
                assert_eq!(curve.points[2].available_parallelism, 3);
                assert_eq!(curve.points[3].available_parallelism, 5);
                // ratio at 1 between 0->0.1 over 3 => 0.1 (flat segment)
                assert!((curve.points[1].target_ratio - 0.1).abs() < 1e-6);
            }
            _ => panic!("expected linear curve"),
        }
    }

    #[test]
    fn test_linear_curve_invalid_cases() {
        // Duplicate available
        assert!(matches!(
            parse_strategy("linear_curve(1:0.1,1:0.2)"),
            Err(ParallelismStrategyParseError::InvalidLinearCurveValue(_))
        ));

        // Target out of range
        assert!(matches!(
            parse_strategy("linear_curve(0:1.2,2:0.5)"),
            Err(ParallelismStrategyParseError::InvalidLinearCurveValue(_))
        ));

        // Not enough points
        assert!(matches!(
            parse_strategy("linear_curve(1:0.2)"),
            Err(ParallelismStrategyParseError::InvalidLinearCurveValue(_))
        ));
    }

    #[test]
    fn test_interpolate_for_padding_clamp_and_linear() {
        // points sorted, len>=1 guaranteed
        let points = vec![
            LinearCurvePoint::new(0, 0.2).unwrap(),
            LinearCurvePoint::new(4, 0.6).unwrap(),
            LinearCurvePoint::new(10, 1.0).unwrap(),
        ];

        // Before first point: clamp to first
        assert!((interpolate_for_padding(&points, 0) - 0.2).abs() < 1e-6);

        // Middle segment: between 0 and 4, expect linear interpolation
        // t = (2-0)/(4-0)=0.5, ratio = 0.2 + 0.5*(0.6-0.2)=0.4
        assert!((interpolate_for_padding(&points, 2) - 0.4).abs() < 1e-6);

        // Exact boundary should return right point
        assert!((interpolate_for_padding(&points, 4) - 0.6).abs() < 1e-6);

        // After last point: clamp to last
        assert!((interpolate_for_padding(&points, 20) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_linear_curve_interpolation_ratios() {
        let strategy = parse_strategy("linear_curve(0:0,5:0.5,10:1.0)").unwrap();
        let curve = match strategy {
            AdaptiveParallelismStrategy::LinearCurve(curve) => curve,
            _ => panic!("expected linear curve"),
        };

        // Exact points
        assert!((curve.interpolate(0) - 0.0).abs() < 1e-6);
        assert!((curve.interpolate(5) - 0.5).abs() < 1e-6);
        assert!((curve.interpolate(10) - 1.0).abs() < 1e-6);

        // Mid segments
        assert!((curve.interpolate(2) - 0.2).abs() < 1e-6); // (0->5): 2/5 * 0.5
        assert!((curve.interpolate(7) - 0.7).abs() < 1e-6); // (5->10): 2/5 * (1-0.5) + 0.5

        // Clamp beyond last
        assert!((curve.interpolate(20) - 1.0).abs() < 1e-6);
    }

    /// Test serde serialization/deserialization uses string format,
    /// which is consistent with `ALTER SYSTEM SET` command.
    #[test]
    fn test_serde_string_format() {
        // Test deserialization from string (as used in config files)
        let auto: AdaptiveParallelismStrategy = serde_json::from_str(r#""Auto""#).unwrap();
        assert_eq!(auto, AdaptiveParallelismStrategy::Auto);

        let full: AdaptiveParallelismStrategy = serde_json::from_str(r#""Full""#).unwrap();
        assert_eq!(full, AdaptiveParallelismStrategy::Full);

        let bounded: AdaptiveParallelismStrategy =
            serde_json::from_str(r#""Bounded(64)""#).unwrap();
        assert!(matches!(bounded, AdaptiveParallelismStrategy::Bounded(n) if n.get() == 64));

        let ratio: AdaptiveParallelismStrategy = serde_json::from_str(r#""Ratio(0.5)""#).unwrap();
        assert!(
            matches!(ratio, AdaptiveParallelismStrategy::Ratio(r) if (r - 0.5).abs() < f32::EPSILON)
        );

        // Test serialization to string
        let auto = AdaptiveParallelismStrategy::Auto;
        assert_eq!(serde_json::to_string(&auto).unwrap(), r#""AUTO""#);

        let bounded = AdaptiveParallelismStrategy::Bounded(NonZeroUsize::new(64).unwrap());
        assert_eq!(serde_json::to_string(&bounded).unwrap(), r#""BOUNDED(64)""#);

        // Test roundtrip
        let original = AdaptiveParallelismStrategy::Bounded(NonZeroUsize::new(128).unwrap());
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: AdaptiveParallelismStrategy = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original, deserialized);

        // Test deserialization errors (same validation as ALTER SYSTEM SET)
        assert!(serde_json::from_str::<AdaptiveParallelismStrategy>(r#""Ratio(-1)""#).is_err());
        assert!(serde_json::from_str::<AdaptiveParallelismStrategy>(r#""Ratio(-0.a)""#).is_err());
        assert!(serde_json::from_str::<AdaptiveParallelismStrategy>(r#""Bounded(0)""#).is_err());
        assert!(serde_json::from_str::<AdaptiveParallelismStrategy>(r#""Invalid""#).is_err());
    }
}
