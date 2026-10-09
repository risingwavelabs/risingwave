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

//! Finite decimals with up to 38 significant digits.
//!
//! [`super::Decimal`] uses this module only for values that [`super::LegacyDecimal`] cannot
//! represent, or when an operation of the legacy implementation fails. Intermediate results use
//! 256-bit integers, which hold any product of two coefficients exactly.

use std::cmp::Ordering;
use std::fmt;
use std::sync::LazyLock;

use ethnum::U256;

/// The maximum number of significant digits.
pub const MAX_DIGITS: u32 = 38;
/// The maximum number of digits after the decimal point.
pub const MAX_SCALE: u32 = 38;

/// `10^n` for `n` in `0..=77`. `10^77` is the largest power of ten below `2^256`.
static POW10: LazyLock<[U256; 78]> = LazyLock::new(|| {
    let mut table = [U256::ONE; 78];
    for i in 1..table.len() {
        table[i] = table[i - 1] * 10;
    }
    table
});

fn pow10(n: u32) -> U256 {
    POW10[n as usize]
}

/// The number of decimal digits of `value`, or 0 for zero.
fn digit_count(value: U256) -> u32 {
    POW10.partition_point(|p| *p <= value) as u32
}

/// How to round when digits are dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rounding {
    /// Round half to even, as `rust_decimal` does in arithmetic.
    HalfEven,
    /// Round half away from zero, as `round()` and text input do.
    HalfAwayFromZero,
    /// Round toward zero.
    Down,
    /// Round toward negative infinity.
    Floor,
    /// Round toward positive infinity.
    Ceiling,
}

/// Divides `value` by `divisor` and rounds the quotient. `sticky` tells that a non-zero part was
/// already discarded below the remainder, which matters for ties.
fn round_div(value: U256, divisor: U256, negative: bool, mode: Rounding, sticky: bool) -> U256 {
    let (quotient, remainder) = value.div_rem(divisor);
    if remainder == U256::ZERO && !sticky {
        return quotient;
    }
    // `remainder < divisor`, so comparing it with `divisor - remainder` avoids overflowing.
    let half = remainder.cmp(&(divisor - remainder));
    let round_up = match mode {
        Rounding::Down => false,
        Rounding::Floor => negative,
        Rounding::Ceiling => !negative,
        Rounding::HalfAwayFromZero => half != Ordering::Less,
        Rounding::HalfEven => match half {
            Ordering::Greater => true,
            Ordering::Equal => sticky || quotient.as_u8() & 1 == 1,
            Ordering::Less => false,
        },
    };
    if round_up { quotient + 1 } else { quotient }
}

/// A finite decimal `coefficient * 10^-scale` with at most [`MAX_DIGITS`] digits in the
/// coefficient and a scale of at most [`MAX_SCALE`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Finite {
    pub negative: bool,
    pub coefficient: u128,
    pub scale: u32,
}

impl Finite {
    pub const ZERO: Self = Self {
        negative: false,
        coefficient: 0,
        scale: 0,
    };

    pub fn is_zero(&self) -> bool {
        self.coefficient == 0
    }

    /// Builds a decimal from an exact value `value * 10^-scale`, dropping digits beyond the limits
    /// with the given rounding. `sticky` tells that the exact value has more non-zero digits below
    /// `value`; it requires that some digits are dropped. Returns `None` if the integer part has
    /// more than [`MAX_DIGITS`] digits.
    fn fit(negative: bool, value: U256, scale: u32, mode: Rounding, sticky: bool) -> Option<Self> {
        let drop = digit_count(value)
            .saturating_sub(MAX_DIGITS)
            .max(scale.saturating_sub(MAX_SCALE));
        if drop > scale {
            return None;
        }
        debug_assert!(drop > 0 || !sticky, "sticky digits must be dropped");
        // A product has at most 76 digits and a scale of at most 76. A quotient has at least 39
        // digits and a scale of at most 114. Either way, `drop` stays within the table.
        let (mut value, mut scale) = (value, scale);
        if drop > 0 {
            value = round_div(value, pow10(drop), negative, mode, sticky);
            scale -= drop;
        }
        if value >= pow10(MAX_DIGITS) {
            // Rounding carried into a new digit, as in `99.95 -> 100.0`.
            if scale == 0 {
                return None;
            }
            value /= 10;
            scale -= 1;
        }
        Some(Self {
            negative: negative && value != U256::ZERO,
            coefficient: value.as_u128(),
            scale,
        })
    }

    /// The coefficient scaled to `scale`, which must not be smaller than `self.scale`.
    fn scaled(&self, scale: u32) -> U256 {
        U256::new(self.coefficient) * pow10(scale - self.scale)
    }

    /// Strips trailing zeros after the decimal point. Zero becomes positive with scale 0.
    #[must_use]
    pub fn normalize(self) -> Self {
        if self.is_zero() {
            return Self::ZERO;
        }
        let (mut coefficient, mut scale) = (self.coefficient, self.scale);
        while scale > 0 && coefficient % 10 == 0 {
            coefficient /= 10;
            scale -= 1;
        }
        Self {
            negative: self.negative,
            coefficient,
            scale,
        }
    }

    /// Compares the values, so `-0 == 0` and `1.0 == 1`.
    pub fn cmp_value(&self, other: &Self) -> Ordering {
        match (self.is_zero(), other.is_zero()) {
            (true, true) => return Ordering::Equal,
            (true, false) => {
                return if other.negative {
                    Ordering::Greater
                } else {
                    Ordering::Less
                };
            }
            (false, true) => {
                return if self.negative {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
            }
            (false, false) => {}
        }
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (negative, _) => {
                let scale = self.scale.max(other.scale);
                let magnitude = self.scaled(scale).cmp(&other.scaled(scale));
                if negative {
                    magnitude.reverse()
                } else {
                    magnitude
                }
            }
        }
    }

    #[must_use]
    pub fn neg(self) -> Self {
        Self {
            negative: !self.negative && !self.is_zero(),
            ..self
        }
    }

    #[must_use]
    pub fn abs(self) -> Self {
        Self {
            negative: false,
            ..self
        }
    }

    pub fn checked_add(self, other: Self) -> Option<Self> {
        let scale = self.scale.max(other.scale);
        let (lhs, rhs) = (self.scaled(scale), other.scaled(scale));
        let (negative, magnitude) = if self.negative == other.negative {
            (self.negative, lhs + rhs)
        } else if lhs >= rhs {
            (self.negative, lhs - rhs)
        } else {
            (other.negative, rhs - lhs)
        };
        Self::fit(negative, magnitude, scale, Rounding::HalfEven, false)
    }

    pub fn checked_sub(self, other: Self) -> Option<Self> {
        self.checked_add(other.neg())
    }

    pub fn checked_mul(self, other: Self) -> Option<Self> {
        let product = U256::new(self.coefficient) * U256::new(other.coefficient);
        Self::fit(
            self.negative != other.negative,
            product,
            self.scale + other.scale,
            Rounding::HalfEven,
            false,
        )
    }

    /// Returns `None` on division by zero or overflow. The quotient keeps up to [`MAX_DIGITS`]
    /// significant digits and has no trailing zeros after the decimal point.
    pub fn checked_div(self, other: Self) -> Option<Self> {
        if other.is_zero() {
            return None;
        }
        if self.is_zero() {
            return Some(Self::ZERO);
        }
        // Widen the dividend to 77 digits, so the quotient has at least 39 digits and every
        // dropped digit is accounted for by the rounding below.
        let dividend = U256::new(self.coefficient);
        let shift = 77 - digit_count(dividend);
        let (quotient, remainder) = (dividend * pow10(shift)).div_rem(U256::new(other.coefficient));
        // `self.scale + shift >= 39 > other.scale`.
        let scale = self.scale + shift - other.scale;
        Self::fit(
            self.negative != other.negative,
            quotient,
            scale,
            Rounding::HalfEven,
            remainder != U256::ZERO,
        )
        .map(Self::normalize)
    }

    /// The remainder of a division truncated toward zero, with the sign of the dividend. Returns
    /// `None` on division by zero.
    pub fn checked_rem(self, other: Self) -> Option<Self> {
        if other.is_zero() {
            return None;
        }
        let scale = self.scale.max(other.scale);
        let remainder = self.scaled(scale) % other.scaled(scale);
        // The remainder is below both scaled operands, one of which is an unscaled coefficient.
        Some(Self {
            negative: self.negative && remainder != U256::ZERO,
            coefficient: remainder.as_u128(),
            scale,
        })
    }

    /// Rounds to `dp` digits after the decimal point. Keeps the value if it has no more digits.
    #[must_use]
    pub fn round_dp(self, dp: u32, mode: Rounding) -> Self {
        if dp >= self.scale {
            return self;
        }
        let coefficient = round_div(
            U256::new(self.coefficient),
            pow10(self.scale - dp),
            self.negative,
            mode,
            false,
        );
        // Dropping at least one digit leaves room for a carry.
        Self {
            negative: self.negative && coefficient != U256::ZERO,
            coefficient: coefficient.as_u128(),
            scale: dp,
        }
    }

    /// Rounds to a multiple of `10^left` with ties away from zero, as in `round(31.5, -1) = 30`.
    /// Returns `None` if the result has more than [`MAX_DIGITS`] digits.
    pub fn round_left(self, left: u32) -> Option<Self> {
        let shift = self.scale + left;
        let multiple = if shift > 77 {
            U256::ZERO
        } else {
            round_div(
                U256::new(self.coefficient),
                pow10(shift),
                self.negative,
                Rounding::HalfAwayFromZero,
                false,
            )
        };
        if multiple == U256::ZERO {
            return Some(Self::ZERO);
        }
        if digit_count(multiple) + left > MAX_DIGITS {
            return None;
        }
        Some(Self {
            negative: self.negative,
            coefficient: (multiple * pow10(left)).as_u128(),
            scale: 0,
        })
    }

    /// The value rounded half away from zero to an integer, if it fits in `i128`.
    pub fn to_i128(self) -> i128 {
        let integer = self.round_dp(0, Rounding::HalfAwayFromZero);
        // At most 38 digits always fit.
        let magnitude = integer.coefficient as i128;
        if integer.negative {
            -magnitude
        } else {
            magnitude
        }
    }
}

impl fmt::Display for Finite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let digits = self.coefficient.to_string();
        if self.negative {
            f.write_str("-")?;
        }
        let scale = self.scale as usize;
        if scale == 0 {
            return f.write_str(&digits);
        }
        if digits.len() > scale {
            let (int, frac) = digits.split_at(digits.len() - scale);
            write!(f, "{int}.{frac}")
        } else {
            write!(f, "0.{}{digits}", "0".repeat(scale - digits.len()))
        }
    }
}

/// An exactly parsed decimal literal: `digits * 10^exponent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedNumber {
    negative: bool,
    /// Decimal digits without leading zeros. Trailing zeros are kept because they set the scale.
    digits: Vec<u8>,
    exponent: i64,
}

impl ParsedNumber {
    /// Parses `[+-]digits[.digits][e[+-]digits]`, with at least one digit in the mantissa and
    /// optional underscores between mantissa digits. Special values are not accepted.
    pub fn parse(s: &str) -> Option<Self> {
        let bytes = s.as_bytes();
        let mut pos = 0;
        let negative = match bytes.first() {
            Some(b'-') => {
                pos += 1;
                true
            }
            Some(b'+') => {
                pos += 1;
                false
            }
            _ => false,
        };

        let mut digits = Vec::with_capacity(bytes.len());
        let mut fraction_digits: i64 = 0;
        let mut seen_point = false;
        let mut seen_digit = false;
        while let Some(&b) = bytes.get(pos) {
            match b {
                b'0'..=b'9' => {
                    if !(digits.is_empty() && b == b'0') {
                        digits.push(b - b'0');
                    }
                    if seen_point {
                        fraction_digits += 1;
                    }
                    seen_digit = true;
                }
                b'_' if seen_digit && bytes.get(pos + 1).is_some_and(u8::is_ascii_digit) => {}
                b'.' if !seen_point => seen_point = true,
                _ => break,
            }
            pos += 1;
        }
        if !seen_digit {
            return None;
        }

        let mut exponent: i64 = 0;
        if let Some(b'e' | b'E') = bytes.get(pos) {
            pos += 1;
            let exponent_negative = match bytes.get(pos) {
                Some(b'-') => {
                    pos += 1;
                    true
                }
                Some(b'+') => {
                    pos += 1;
                    false
                }
                _ => false,
            };
            let start = pos;
            while let Some(b) = bytes.get(pos).filter(|b| b.is_ascii_digit()) {
                exponent = exponent.checked_mul(10)?.checked_add((b - b'0') as i64)?;
                pos += 1;
            }
            if pos == start {
                return None;
            }
            if exponent_negative {
                exponent = -exponent;
            }
        }
        if pos != bytes.len() {
            return None;
        }

        Some(Self {
            negative,
            digits,
            exponent: exponent.checked_sub(fraction_digits)?,
        })
    }

    /// Rounds half away from zero to at most [`MAX_DIGITS`] significant digits and a scale of at
    /// most [`MAX_SCALE`]. Returns `None` if the integer part has more than [`MAX_DIGITS`] digits.
    pub fn to_finite(&self) -> Option<Finite> {
        let len = self.digits.len() as i64;
        if self.exponent >= 0 {
            // An integer: the digits followed by `exponent` zeros.
            if len == 0 {
                return Some(Finite::ZERO);
            }
            if len + self.exponent > MAX_DIGITS as i64 {
                return None;
            }
            let coefficient =
                self.digits_value(self.digits.len()) * 10u128.pow(self.exponent as u32);
            return Some(Finite {
                negative: self.negative,
                coefficient,
                scale: 0,
            });
        }

        let scale = -self.exponent;
        if len - scale > MAX_DIGITS as i64 {
            return None;
        }
        let drop = (len - MAX_DIGITS as i64)
            .max(scale - MAX_SCALE as i64)
            .max(0);
        let keep = (len - drop).max(0) as usize;
        let mut coefficient = U256::new(self.digits_value(keep));
        // Ties go away from zero, so the first dropped digit decides.
        let next = self.digits.get(keep).copied().unwrap_or(0);
        let dropped_all = drop > len;
        if !dropped_all && next >= 5 {
            coefficient += 1;
        }
        Finite::fit(
            self.negative,
            coefficient,
            (scale - drop) as u32,
            Rounding::Down,
            false,
        )
    }

    /// The value of the first `n` digits, where `n <= MAX_DIGITS`.
    fn digits_value(&self, n: usize) -> u128 {
        self.digits[..n]
            .iter()
            .fold(0u128, |acc, &d| acc * 10 + d as u128)
    }
}

/// The parts of the PostgreSQL binary `numeric` format of a finite value: base-10000 digits,
/// the weight of the first digit, and the display scale.
pub struct PgNumeric {
    pub negative: bool,
    pub weight: i16,
    pub dscale: u16,
    pub digits: Vec<i16>,
}

impl Finite {
    pub fn to_pg_numeric(self) -> PgNumeric {
        let decimal_digits = self.coefficient.to_string();
        let scale = self.scale as usize;
        // Split at the decimal point, then pad both sides to whole base-10000 digits.
        let (int_part, frac_part) = if decimal_digits.len() > scale {
            let (int, frac) = decimal_digits.split_at(decimal_digits.len() - scale);
            (int.to_owned(), frac.to_owned())
        } else {
            (
                String::new(),
                "0".repeat(scale - decimal_digits.len()) + &decimal_digits,
            )
        };
        let int_part = "0".repeat((4 - int_part.len() % 4) % 4) + &int_part;
        let frac_part = frac_part.clone() + &"0".repeat((4 - frac_part.len() % 4) % 4);
        let mut weight = (int_part.len() / 4) as i16 - 1;
        let mut digits: Vec<i16> = (int_part + &frac_part)
            .as_bytes()
            .chunks(4)
            .map(|group| std::str::from_utf8(group).unwrap().parse().unwrap())
            .collect();
        let leading_zeros = digits.iter().take_while(|&&d| d == 0).count();
        digits.drain(..leading_zeros);
        weight -= leading_zeros as i16;
        while digits.last() == Some(&0) {
            digits.pop();
        }
        if digits.is_empty() {
            weight = 0;
        }
        PgNumeric {
            negative: self.negative,
            weight,
            dscale: self.scale as u16,
            digits,
        }
    }

    /// Converts from the PostgreSQL format, rounding half away from zero to the limits. The
    /// display scale is kept where possible. Returns `None` if the integer part is too large.
    pub fn from_pg_numeric(numeric: &PgNumeric) -> Option<Self> {
        let mut digits = Vec::with_capacity(numeric.digits.len() * 4);
        for &group in &numeric.digits {
            for d in format!("{:04}", group.clamp(0, 9999)).bytes() {
                if !(digits.is_empty() && d == b'0') {
                    digits.push(d - b'0');
                }
            }
        }
        let parsed = ParsedNumber {
            negative: numeric.negative,
            digits,
            exponent: 4 * (numeric.weight as i64 - numeric.digits.len() as i64 + 1),
        };
        let value = parsed.to_finite()?;
        let dscale = (numeric.dscale as u32).min(MAX_SCALE);
        if value.scale > dscale {
            return Some(value.round_dp(dscale, Rounding::HalfAwayFromZero));
        }
        // Restore trailing zeros of the display scale, as far as the digits allow.
        let room = MAX_DIGITS.saturating_sub(digit_count(U256::new(value.coefficient)));
        let extra = (dscale - value.scale).min(room);
        Some(Self {
            coefficient: value.coefficient * 10u128.pow(extra),
            scale: value.scale + extra,
            ..value
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn finite(s: &str) -> Finite {
        ParsedNumber::parse(s).unwrap().to_finite().unwrap()
    }

    #[test]
    fn test_parse_and_display() {
        for (input, output) in [
            ("0", "0"),
            ("00012.3400", "12.3400"),
            ("-1.5", "-1.5"),
            ("+.5", "0.5"),
            ("5.", "5"),
            ("1_000.000_1", "1000.0001"),
            ("1e3", "1000"),
            ("1.5E-3", "0.0015"),
            ("-0.00", "0.00"),
            (
                "12345678901234567890123456789012345678",
                "12345678901234567890123456789012345678",
            ),
            (
                "0.12345678901234567890123456789012345678",
                "0.12345678901234567890123456789012345678",
            ),
            // Rounded half away from zero to 38 significant digits.
            (
                "1.234567890123456789012345678901234567850",
                "1.2345678901234567890123456789012345679",
            ),
            (
                "-9.9999999999999999999999999999999999999999",
                "-10.000000000000000000000000000000000000",
            ),
            // ... and to a scale of 38.
            ("1e-38", "0.00000000000000000000000000000000000001"),
            ("5e-39", "0.00000000000000000000000000000000000001"),
            ("4e-39", "0.00000000000000000000000000000000000000"),
            ("1e-100", "0.00000000000000000000000000000000000000"),
        ] {
            assert_eq!(finite(input).to_string(), output, "{input}");
        }

        for input in [
            "", "-", ".", "e3", "1e", "1e+", "1.2.3", "1_", "_1", "1__0", "1x", "nan", " 1",
            "1e3.5",
        ] {
            assert_eq!(ParsedNumber::parse(input), None, "{input}");
        }
        for input in [
            "1e38",
            "123456789012345678901234567890123456789",
            "99999999999999999999999999999999999999.5",
        ] {
            assert_eq!(
                ParsedNumber::parse(input).unwrap().to_finite(),
                None,
                "{input}"
            );
        }
    }

    #[test]
    fn test_arithmetic() {
        let max = finite("99999999999999999999999999999999999999");
        let one = finite("1");
        assert_eq!(max.checked_add(one), None);
        assert_eq!(max.neg().checked_sub(one), None);
        assert_eq!(
            max.checked_sub(one).unwrap().to_string(),
            "99999999999999999999999999999999999998"
        );
        // Adding a tiny value rounds half to even.
        let half = finite("0.5");
        assert_eq!(
            finite("1000000000000000000000000000000000000.5")
                .checked_add(half)
                .unwrap()
                .to_string(),
            "1000000000000000000000000000000000001.0"
        );
        assert_eq!(
            finite("10000000000000000000000000000000000000")
                .checked_add(half)
                .unwrap()
                .to_string(),
            "10000000000000000000000000000000000000"
        );
        assert_eq!(
            finite("10000000000000000000000000000000000001")
                .checked_add(half)
                .unwrap()
                .to_string(),
            "10000000000000000000000000000000000002"
        );

        let a = finite("12345678901234567890.123");
        let b = finite("98765432109876543210.98");
        assert_eq!(a.checked_mul(b), None);
        assert_eq!(
            a.checked_mul(finite("-0.001")).unwrap().to_string(),
            "-12345678901234567.890123"
        );
        assert_eq!(
            finite("1234567890123456789.0123456789")
                .checked_mul(finite("1234567890123456789.0123456789"))
                .unwrap()
                .to_string(),
            "1524157875323883675049535156253619878.8"
        );

        assert_eq!(one.checked_div(Finite::ZERO), None);
        assert_eq!(
            one.checked_div(finite("3")).unwrap().to_string(),
            "0.33333333333333333333333333333333333333"
        );
        assert_eq!(
            finite("2").checked_div(finite("3")).unwrap().to_string(),
            "0.66666666666666666666666666666666666667"
        );
        assert_eq!(one.checked_div(finite("4")).unwrap().to_string(), "0.25");
        assert_eq!(max.checked_div(finite("0.5")), None,);
        assert_eq!(
            max.checked_div(finite("-2")).unwrap().to_string(),
            "-50000000000000000000000000000000000000"
        );
        assert_eq!(
            finite("1e-38")
                .checked_div(finite("3"))
                .unwrap()
                .to_string(),
            "0"
        );

        assert_eq!(one.checked_rem(Finite::ZERO), None);
        assert_eq!(
            finite("-12345678901234567890123456789012345678")
                .checked_rem(finite("0.007"))
                .unwrap()
                .to_string(),
            "-0.005"
        );
        assert_eq!(
            finite("0.00000000000000000000000000000000000001")
                .checked_rem(finite("12345678901234567890123456789012345678"))
                .unwrap()
                .to_string(),
            "0.00000000000000000000000000000000000001"
        );
    }

    #[test]
    fn test_pg_numeric() {
        for input in [
            "0",
            "0.000",
            "1",
            "-1.5",
            "10000",
            "12345.6789",
            "0.00012",
            "-12345678901234567890123456789012345678",
            "0.12345678901234567890123456789012345678",
            "1234567890123456789.0123456789012345678",
            "0.00000000000000000000000000000000000001",
        ] {
            let value = finite(input);
            let numeric = value.to_pg_numeric();
            assert_eq!(Finite::from_pg_numeric(&numeric), Some(value), "{input}");
        }
        let numeric = finite("-12345.6789").to_pg_numeric();
        assert_eq!(
            (
                numeric.negative,
                numeric.weight,
                numeric.dscale,
                numeric.digits
            ),
            (true, 1, 4, vec![1, 2345, 6789])
        );
        let numeric = finite("0.00012").to_pg_numeric();
        assert_eq!(
            (numeric.weight, numeric.dscale, numeric.digits),
            (-1, 5, vec![1, 2000])
        );

        // The display scale is kept, and excess digits are rounded.
        let numeric = PgNumeric {
            negative: false,
            weight: 0,
            dscale: 10,
            digits: vec![1, 5000],
        };
        assert_eq!(
            Finite::from_pg_numeric(&numeric).unwrap().to_string(),
            "1.5000000000"
        );
        let numeric = PgNumeric {
            negative: false,
            weight: -10,
            dscale: 40,
            digits: vec![50],
        };
        assert_eq!(
            Finite::from_pg_numeric(&numeric).unwrap().to_string(),
            "0.00000000000000000000000000000000000001"
        );
        let numeric = PgNumeric {
            negative: false,
            weight: 9,
            dscale: 0,
            digits: vec![9999],
        };
        assert_eq!(Finite::from_pg_numeric(&numeric), None);
    }

    #[test]
    fn test_compare_and_round() {
        let ordered = [
            "-99999999999999999999999999999999999999",
            "-1.0000000000000000000000000000000000001",
            "-1",
            "0",
            "0.00000000000000000000000000000000000001",
            "1",
            "1.0000000000000000000000000000000000001",
            "99999999999999999999999999999999999999",
        ]
        .map(finite);
        for pair in ordered.windows(2) {
            assert_eq!(pair[0].cmp_value(&pair[1]), Ordering::Less);
            assert_eq!(pair[1].cmp_value(&pair[0]), Ordering::Greater);
        }
        assert_eq!(finite("1.000").cmp_value(&finite("1")), Ordering::Equal);
        assert_eq!(finite("-0.0").cmp_value(&Finite::ZERO), Ordering::Equal);
        assert_eq!(finite("1.000").normalize(), finite("1"));

        let x = finite("-2.5000000000000000000000000000000000001");
        assert_eq!(x.round_dp(0, Rounding::HalfEven).to_string(), "-3");
        assert_eq!(
            finite("-2.5").round_dp(0, Rounding::HalfEven).to_string(),
            "-2"
        );
        assert_eq!(
            finite("-2.5")
                .round_dp(0, Rounding::HalfAwayFromZero)
                .to_string(),
            "-3"
        );
        assert_eq!(x.round_dp(0, Rounding::Down).to_string(), "-2");
        assert_eq!(x.round_dp(0, Rounding::Floor).to_string(), "-3");
        assert_eq!(x.round_dp(0, Rounding::Ceiling).to_string(), "-2");
        assert_eq!(
            finite("-0.4").round_dp(0, Rounding::Ceiling).to_string(),
            "0"
        );
        assert_eq!(x.round_dp(40, Rounding::Down), x);

        let big = finite("54999999999999999999999999999999999999");
        assert_eq!(
            big.round_left(37).unwrap().to_string(),
            "50000000000000000000000000000000000000"
        );
        assert_eq!(
            big.round_left(36).unwrap().to_string(),
            "55000000000000000000000000000000000000"
        );
        assert_eq!(
            finite("99999999999999999999999999999999999999").round_left(1),
            None
        );
        assert_eq!(big.round_left(39).unwrap(), Finite::ZERO);
        assert_eq!(big.to_i128(), 54999999999999999999999999999999999999);
        assert_eq!(finite("-0.5").to_i128(), -1);
    }
}
