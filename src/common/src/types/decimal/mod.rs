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

mod legacy;
mod memcmp;
mod wide;

use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;

use bytes::{Buf, BufMut, BytesMut};
use num_traits::{
    CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Num, One, Zero,
};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use risingwave_common_estimate_size::ZeroHeapSize;
use rust_decimal::{Decimal as RustDecimal, Error};

use self::legacy::LegacyDecimal;
pub use self::legacy::PowError;
use self::wide::{Finite, ParsedNumber, PgNumeric, Rounding};
use super::DataType;
use super::to_text::ToText;
use crate::array::ArrayResult;
use crate::types::ordered_float::OrderedFloat;

const KIND_MASK: u32 = 0xff;
const KIND_FINITE: u32 = 0;
const KIND_NAN: u32 = 1;
const KIND_POSITIVE_INF: u32 = 2;
const KIND_NEGATIVE_INF: u32 = 3;
const SCALE_SHIFT: u32 = 16;
const SCALE_MASK: u32 = 0xff << SCALE_SHIFT;
const SIGN_MASK: u32 = 1 << 31;

/// The first byte of the 20-byte encoding of finite values outside the legacy range. Bytes 0 to 3
/// hold the legacy kinds.
const TAG_WIDE: u8 = 4;
/// The largest scale of the legacy range.
const LEGACY_MAX_SCALE: u32 = 28;

/// A decimal number with up to 38 significant digits and up to 38 digits after the decimal point,
/// or one of `NaN`, `Infinity` and `-Infinity`.
///
/// Values with a coefficient below `2^96` and a scale of at most 28 form the legacy range, which
/// was the whole range of the type before. Existing streaming jobs recompute old rows on updates
/// and deletes, so for such values the results, text forms and encodings stay exactly the same:
/// operations on them are carried out by [`LegacyDecimal`], and only fall back to the wider
/// implementation when it fails, for example on overflow. Results computed from wider values have
/// up to 38 significant digits, while results from legacy values keep the legacy precision, so
/// `1 / 3` still has 28 digits.
#[derive(Clone, Copy)]
pub struct Decimal {
    /// Bits 0..8 hold the kind (`KIND_*`). For finite values, bits 16..24 hold the scale and bit
    /// 31 the sign. Together with the low three words of `coefficient`, this is the legacy 16-byte
    /// encoding.
    flags: u32,
    /// The coefficient of a finite value, as little-endian 32-bit words.
    coefficient: [u32; 4],
}

/// The parts of a [`Decimal`], for conversions to other representations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalParts {
    /// The value `mantissa * 10^-scale`.
    Finite {
        mantissa: i128,
        scale: u32,
    },
    NaN,
    PositiveInf,
    NegativeInf,
}

/// The bytes are not a valid decimal encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid decimal encoding")]
pub struct InvalidDecimalEncoding;

impl Decimal {
    /// The maximum number of significant digits.
    pub const MAX_PRECISION: u8 = wide::MAX_DIGITS as u8;
    /// The maximum number of digits after the decimal point.
    pub const MAX_SCALE: u8 = wide::MAX_SCALE as u8;
    pub const NAN: Self = Self::special(KIND_NAN);
    pub const NEGATIVE_INF: Self = Self::special(KIND_NEGATIVE_INF);
    pub const POSITIVE_INF: Self = Self::special(KIND_POSITIVE_INF);

    const fn special(kind: u32) -> Self {
        Self {
            flags: kind,
            coefficient: [0; 4],
        }
    }

    fn kind(&self) -> u32 {
        self.flags & KIND_MASK
    }

    fn scale_bits(&self) -> u32 {
        (self.flags & SCALE_MASK) >> SCALE_SHIFT
    }

    /// Whether [`LegacyDecimal`] can represent the finite value as it is.
    fn fits_legacy(value: &Finite) -> bool {
        value.coefficient < 1 << 96 && value.scale <= LEGACY_MAX_SCALE
    }

    /// Whether [`LegacyDecimal`] can represent the value as it is.
    fn is_legacy(&self) -> bool {
        self.kind() != KIND_FINITE
            || (self.coefficient[3] == 0 && self.scale_bits() <= LEGACY_MAX_SCALE)
    }

    fn finite(&self) -> Option<Finite> {
        if self.kind() != KIND_FINITE {
            return None;
        }
        let [lo, mid, hi, top] = self.coefficient.map(u128::from);
        Some(Finite {
            negative: self.flags & SIGN_MASK != 0,
            coefficient: lo | mid << 32 | hi << 64 | top << 96,
            scale: self.scale_bits(),
        })
    }

    fn from_finite(value: Finite) -> Self {
        debug_assert!(value.coefficient < 10u128.pow(wide::MAX_DIGITS));
        debug_assert!(value.scale <= wide::MAX_SCALE);
        let sign = if value.negative { SIGN_MASK } else { 0 };
        let c = value.coefficient;
        Self {
            flags: sign | (value.scale << SCALE_SHIFT),
            coefficient: [
                c as u32,
                (c >> 32) as u32,
                (c >> 64) as u32,
                (c >> 96) as u32,
            ],
        }
    }

    fn to_legacy(self) -> LegacyDecimal {
        debug_assert!(self.is_legacy(), "the decimal is out of the legacy range");
        match self.kind() {
            KIND_FINITE => {
                let [lo, mid, hi, _] = self.coefficient;
                let mut d = RustDecimal::from_parts(lo, mid, hi, false, self.scale_bits());
                // Unlike `from_parts`, this keeps the sign of negative zero.
                d.set_sign_negative(self.flags & SIGN_MASK != 0);
                LegacyDecimal::Normalized(d)
            }
            KIND_NAN => LegacyDecimal::NaN,
            KIND_POSITIVE_INF => LegacyDecimal::PositiveInf,
            KIND_NEGATIVE_INF => LegacyDecimal::NegativeInf,
            _ => unreachable!("invalid decimal kind"),
        }
    }

    fn from_legacy(decimal: LegacyDecimal) -> Self {
        match decimal {
            LegacyDecimal::Normalized(d) => {
                let parts = d.unpack();
                let sign = if parts.negative { SIGN_MASK } else { 0 };
                Self {
                    flags: sign | (parts.scale << SCALE_SHIFT),
                    coefficient: [parts.lo, parts.mid, parts.hi, 0],
                }
            }
            LegacyDecimal::NaN => Self::NAN,
            LegacyDecimal::PositiveInf => Self::POSITIVE_INF,
            LegacyDecimal::NegativeInf => Self::NEGATIVE_INF,
        }
    }

    /// A legacy value with the same kind, sign and zeroness. For an operation with a non-finite
    /// operand, the result depends on nothing else of a finite operand.
    fn legacy_proxy(self) -> LegacyDecimal {
        match self.finite() {
            Some(value) if !self.is_legacy() => LegacyDecimal::from(match value {
                _ if value.is_zero() => 0,
                _ if value.negative => -1,
                _ => 1,
            }),
            _ => self.to_legacy(),
        }
    }

    /// A legacy approximation of the value, rounded to 28 digits after the decimal point. Returns
    /// `None` if the integer part is too large.
    fn to_legacy_rounded(self) -> Option<LegacyDecimal> {
        if self.is_legacy() {
            return Some(self.to_legacy());
        }
        let value = self
            .finite()?
            .round_dp(LEGACY_MAX_SCALE, Rounding::HalfEven);
        let rounded = Self::from_finite(value);
        rounded.is_legacy().then(|| rounded.to_legacy())
    }

    /// Parses with the given legacy result. The result only differs from the legacy one when it
    /// keeps more precision, or when the legacy parser fails because the value is too large.
    fn parse_with(s: &str, legacy: Result<LegacyDecimal, Error>) -> Result<Self, Error> {
        let has_exponent = s.bytes().any(|b| b == b'e' || b == b'E');
        let wide = || ParsedNumber::parse(s).and_then(|parsed| parsed.to_finite_strict());
        match legacy {
            Ok(legacy) => {
                let legacy = Self::from_legacy(legacy);
                // Inputs without an exponent and up to 29 bytes have at most 28 digits after the
                // decimal point and at most 29 digits, which the legacy parser either keeps
                // exactly or rejects.
                if s.len() <= 29 && !has_exponent {
                    return Ok(legacy);
                }
                match (legacy.finite(), wide()) {
                    (Some(old), Some(new)) if old.cmp_value(&new).is_ne() => {
                        Ok(Self::from_finite(new))
                    }
                    // After more digits than it can keep, `rust_decimal` ignores the rest of the
                    // input, so its result misses an exponent that puts the value out of range.
                    (Some(_), None) if has_exponent => Err(Error::from("Failed to parse")),
                    _ => Ok(legacy),
                }
            }
            Err(e) => wide().map(Self::from_finite).ok_or(e),
        }
    }

    pub fn is_finite(&self) -> bool {
        self.kind() == KIND_FINITE
    }

    pub fn is_nan(&self) -> bool {
        self.kind() == KIND_NAN
    }

    pub fn to_parts(self) -> DecimalParts {
        match self.kind() {
            KIND_FINITE => {
                let value = self.finite().unwrap();
                let mantissa = value.coefficient as i128;
                DecimalParts::Finite {
                    mantissa: if value.negative { -mantissa } else { mantissa },
                    scale: value.scale,
                }
            }
            KIND_NAN => DecimalParts::NaN,
            KIND_POSITIVE_INF => DecimalParts::PositiveInf,
            KIND_NEGATIVE_INF => DecimalParts::NegativeInf,
            _ => unreachable!("invalid decimal kind"),
        }
    }

    /// The length of [`Self::encode_unordered`].
    pub fn encoded_len(&self) -> usize {
        if self.is_legacy() { 16 } else { 20 }
    }

    /// Writes the encoding used by the value encoding and protobuf arrays. Values in the legacy
    /// range keep the legacy 16 bytes: the flags word followed by the low three words of the
    /// coefficient, all little-endian. Other values write [`TAG_WIDE`] instead of the kind and add
    /// the top word, for 20 bytes in total.
    pub fn encode_unordered(&self, buf: &mut impl BufMut) {
        let [lo, mid, hi, top] = self.coefficient;
        if self.is_legacy() {
            buf.put_u32_le(self.flags);
        } else {
            buf.put_u32_le(self.flags | TAG_WIDE as u32);
        }
        buf.put_u32_le(lo);
        buf.put_u32_le(mid);
        buf.put_u32_le(hi);
        if !self.is_legacy() {
            buf.put_u32_le(top);
        }
    }

    pub fn decode_unordered(buf: &mut impl Buf) -> Result<Self, InvalidDecimalEncoding> {
        if buf.remaining() < 16 {
            return Err(InvalidDecimalEncoding);
        }
        let flags = buf.get_u32_le();
        let (lo, mid, hi) = (buf.get_u32_le(), buf.get_u32_le(), buf.get_u32_le());
        let tag = (flags & KIND_MASK) as u8;
        match tag {
            0..=3 => {
                let mut bytes = [0; 16];
                bytes[0..4].copy_from_slice(&flags.to_le_bytes());
                bytes[4..8].copy_from_slice(&lo.to_le_bytes());
                bytes[8..12].copy_from_slice(&mid.to_le_bytes());
                bytes[12..16].copy_from_slice(&hi.to_le_bytes());
                Ok(Self::from_legacy(LegacyDecimal::unordered_deserialize(
                    bytes,
                )))
            }
            TAG_WIDE if buf.remaining() >= 4 => {
                let decimal = Self {
                    flags: flags & !KIND_MASK,
                    coefficient: [lo, mid, hi, buf.get_u32_le()],
                };
                let value = decimal.finite().unwrap();
                if value.coefficient >= 10u128.pow(wide::MAX_DIGITS)
                    || value.scale > wide::MAX_SCALE
                {
                    return Err(InvalidDecimalEncoding);
                }
                Ok(decimal)
            }
            _ => Err(InvalidDecimalEncoding),
        }
    }

    /// A fixed-size form of the exact representation, including the scale. Use it on
    /// [`Self::normalize`]d values to compare values.
    pub fn to_fixed_bytes(self) -> [u8; 20] {
        let mut bytes = [0; 20];
        bytes[0..4].copy_from_slice(&self.flags.to_le_bytes());
        for (i, word) in self.coefficient.iter().enumerate() {
            bytes[4 + i * 4..8 + i * 4].copy_from_slice(&word.to_le_bytes());
        }
        bytes
    }

    pub fn from_fixed_bytes(bytes: [u8; 20]) -> Self {
        let word = |i: usize| u32::from_le_bytes(bytes[i * 4..i * 4 + 4].try_into().unwrap());
        Self {
            flags: word(0),
            coefficient: [word(1), word(2), word(3), word(4)],
        }
    }

    /// Used by `PrimitiveArray` to serialize the array to protobuf.
    pub fn to_protobuf(self, output: &mut impl Write) -> ArrayResult<usize> {
        let mut buf = Vec::with_capacity(20);
        self.encode_unordered(&mut buf);
        output.write_all(&buf)?;
        Ok(buf.len())
    }

    /// Used by `PrimitiveArray` to deserialize the array from protobuf.
    pub fn from_protobuf(input: &mut impl Read) -> ArrayResult<Self> {
        let mut buf = [0u8; 20];
        input.read_exact(&mut buf[..16])?;
        if buf[0] == TAG_WIDE {
            input.read_exact(&mut buf[16..])?;
        }
        let len = if buf[0] == TAG_WIDE { 20 } else { 16 };
        Ok(Self::decode_unordered(&mut &buf[..len]).map_err(anyhow::Error::from)?)
    }

    pub fn from_scientific(value: &str) -> Option<Self> {
        let legacy =
            LegacyDecimal::from_scientific(value).ok_or_else(|| Error::from("Failed to parse"));
        Self::parse_with(value, legacy).ok()
    }

    pub fn from_str_radix(s: &str, radix: u32) -> rust_decimal::Result<Self> {
        let legacy = LegacyDecimal::from_str_radix(s, radix);
        if radix == 10 {
            Self::parse_with(s, legacy)
        } else {
            legacy.map(Self::from_legacy)
        }
    }

    /// Panics if the value cannot be represented.
    pub fn from_i128_with_scale(num: i128, scale: u32) -> Self {
        let value = Finite {
            negative: num < 0,
            coefficient: num.unsigned_abs(),
            scale,
        };
        if Self::fits_legacy(&value) {
            return Self::from_legacy(LegacyDecimal::from_i128_with_scale(num, scale));
        }
        assert!(
            value.coefficient < 10u128.pow(wide::MAX_DIGITS) && scale <= wide::MAX_SCALE,
            "decimal {num}e-{scale} is out of range"
        );
        Self::from_finite(value)
    }

    /// Truncate the given `num` and `scale` to fit into `Decimal`, return `None` if it cannot be
    /// represented even after truncation.
    pub fn truncated_i128_and_scale(num: i128, scale: u32) -> Option<Self> {
        let value = Finite {
            negative: num < 0,
            coefficient: num.unsigned_abs(),
            scale,
        };
        let legacy = LegacyDecimal::truncated_i128_and_scale(num, scale).map(Self::from_legacy);
        if Self::fits_legacy(&value) {
            return legacy;
        }
        // Drop digits beyond the limits toward zero.
        let digits = value.coefficient.checked_ilog10().map_or(0, |d| d + 1);
        let drop = digits
            .saturating_sub(wide::MAX_DIGITS)
            .max(scale.saturating_sub(wide::MAX_SCALE));
        if drop > scale {
            return None;
        }
        let wide = value.round_dp(scale - drop, Rounding::Down);
        // The legacy implementation drops digits to fit, which only loses trailing zeros for
        // values such as `Decimal128(38, 10)` integers. Keep its result then, as for parsing.
        match legacy.and_then(|legacy| legacy.finite()) {
            Some(old) if old.cmp_value(&wide).is_eq() => legacy,
            _ => Some(Self::from_finite(wide)),
        }
    }

    pub fn scale(&self) -> Option<i32> {
        self.finite().map(|value| value.scale as i32)
    }

    pub fn rescale(&mut self, scale: u32) {
        if self.is_legacy() {
            let mut decimal = self.to_legacy();
            decimal.rescale(scale);
            *self = Self::from_legacy(decimal);
            return;
        }
        let value = self.finite().unwrap();
        let scale = scale.min(wide::MAX_SCALE);
        if scale <= value.scale {
            *self = Self::from_finite(value.round_dp(scale, Rounding::HalfAwayFromZero));
            return;
        }
        // Add as many zeros as fit, like the legacy implementation.
        let digits = value.coefficient.checked_ilog10().map_or(1, |d| d + 1);
        let extra = (scale - value.scale).min(wide::MAX_DIGITS.saturating_sub(digits));
        *self = Self::from_finite(Finite {
            coefficient: value.coefficient * 10u128.pow(extra),
            scale: value.scale + extra,
            ..value
        });
    }

    /// Applies `legacy` to values in the legacy range, and `wide` to other finite values.
    fn apply(
        self,
        legacy: impl FnOnce(LegacyDecimal) -> LegacyDecimal,
        wide: impl FnOnce(Finite) -> Finite,
    ) -> Self {
        if self.is_legacy() {
            Self::from_legacy(legacy(self.to_legacy()))
        } else {
            Self::from_finite(wide(self.finite().unwrap()))
        }
    }

    #[must_use]
    pub fn round_dp_ties_away(&self, dp: u32) -> Self {
        self.apply(
            |d| d.round_dp_ties_away(dp),
            |d| d.round_dp(dp, Rounding::HalfAwayFromZero),
        )
    }

    /// Round to the left of the decimal point, for example `31.5` -> `30`.
    #[must_use]
    pub fn round_left_ties_away(&self, left: u32) -> Option<Self> {
        if self.is_legacy()
            && let Some(result) = self.to_legacy().round_left_ties_away(left)
        {
            return Some(Self::from_legacy(result));
        }
        self.finite()?.round_left(left).map(Self::from_finite)
    }

    #[must_use]
    pub fn ceil(&self) -> Self {
        self.apply(|d| d.ceil(), |d| d.round_dp(0, Rounding::Ceiling))
    }

    #[must_use]
    pub fn floor(&self) -> Self {
        self.apply(|d| d.floor(), |d| d.round_dp(0, Rounding::Floor))
    }

    #[must_use]
    pub fn trunc(&self) -> Self {
        self.apply(|d| d.trunc(), |d| d.round_dp(0, Rounding::Down))
    }

    #[must_use]
    pub fn round_ties_even(&self) -> Self {
        self.apply(
            |d| d.round_ties_even(),
            |d| d.round_dp(0, Rounding::HalfEven),
        )
    }

    /// Strips trailing zeros after the decimal point and turns negative zero into zero, so equal
    /// values have the same representation.
    #[must_use]
    pub fn normalize(&self) -> Self {
        self.apply(|d| d.normalize(), Finite::normalize)
    }

    pub fn abs(&self) -> Self {
        self.apply(|d| d.abs(), Finite::abs)
    }

    pub fn sign(&self) -> Self {
        Self::from_legacy(self.legacy_proxy().sign())
    }

    /// Splits a positive value into `mantissa * 10^exponent`, with a mantissa in the legacy range.
    fn split_positive(self) -> Option<(LegacyDecimal, i64)> {
        let value = self.finite().filter(|v| !v.negative && !v.is_zero())?;
        let digits = value.coefficient.ilog10() + 1;
        let drop = digits.saturating_sub(LEGACY_MAX_SCALE);
        let mantissa = Finite {
            negative: false,
            coefficient: value.coefficient,
            scale: drop,
        }
        .round_dp(0, Rounding::HalfEven);
        let mantissa = Self::from_finite(mantissa).to_legacy_rounded()?;
        Some((mantissa, drop as i64 - value.scale as i64))
    }

    fn legacy_ln10() -> LegacyDecimal {
        LegacyDecimal::from(10).checked_ln().unwrap()
    }

    pub fn checked_exp(&self) -> Option<Self> {
        match self.to_legacy_rounded() {
            Some(d) => d.checked_exp().map(Self::from_legacy),
            // Too large for the legacy range: `exp` underflows to zero or overflows.
            None => self.finite().filter(|v| v.negative).map(|_| Self::zero()),
        }
    }

    pub fn checked_ln(&self) -> Option<Self> {
        if self.is_legacy() {
            return self.to_legacy().checked_ln().map(Self::from_legacy);
        }
        // ln(m * 10^e) = ln(m) + e * ln(10)
        let (mantissa, exponent) = self.split_positive()?;
        let shift = LegacyDecimal::from(exponent).checked_mul(&Self::legacy_ln10())?;
        mantissa
            .checked_ln()?
            .checked_add(&shift)
            .map(Self::from_legacy)
    }

    pub fn checked_log10(&self) -> Option<Self> {
        if self.is_legacy() {
            return self.to_legacy().checked_log10().map(Self::from_legacy);
        }
        let (mantissa, exponent) = self.split_positive()?;
        mantissa
            .checked_log10()?
            .checked_add(&LegacyDecimal::from(exponent))
            .map(Self::from_legacy)
    }

    /// Returns `None` for negative values, including `-Infinity`.
    pub fn checked_sqrt(&self) -> Option<Self> {
        if self.is_legacy() {
            return self.to_legacy().checked_sqrt().map(Self::from_legacy);
        }
        let value = self.finite().unwrap();
        if value.negative {
            return None;
        }
        if value.is_zero() {
            return Some(Self::zero());
        }
        // sqrt(m * 10^e) = sqrt(m) * 10^(e / 2) for an even `e`.
        let (mantissa, exponent) = self.split_positive()?;
        let (mantissa, exponent) = if exponent % 2 == 0 {
            (mantissa, exponent)
        } else {
            (
                mantissa.checked_div(&LegacyDecimal::from(10))?,
                exponent + 1,
            )
        };
        let half = exponent / 2;
        let power = Finite {
            negative: false,
            coefficient: 10u128.pow(half.max(0) as u32),
            scale: (-half).max(0) as u32,
        };
        Self::from_legacy(mantissa.checked_sqrt()?).checked_mul(&Self::from_finite(power))
    }

    pub fn checked_powd(&self, rhs: &Self) -> Result<Self, PowError> {
        if self.is_legacy() && rhs.is_legacy() {
            match self.to_legacy().checked_powd(&rhs.to_legacy()) {
                Err(PowError::Overflow) => {}
                result => return result.map(Self::from_legacy),
            }
        }
        let (Some(base), Some(exponent)) = (self.finite(), rhs.finite()) else {
            return self
                .pow_proxy()
                .checked_powd(&rhs.pow_proxy())
                .map(Self::from_legacy);
        };
        if base.is_zero() && exponent.negative && !exponent.is_zero() {
            return Err(PowError::ZeroNegative);
        }
        let integral = exponent
            .round_dp(0, Rounding::Down)
            .cmp_value(&exponent)
            .is_eq();
        if base.negative && !integral {
            return Err(PowError::NegativeFract);
        }
        if integral {
            return Self::powi(base, exponent).ok_or(PowError::Overflow);
        }
        // x^y = exp(y * ln(x)) with the legacy precision.
        let ln = self.checked_ln().ok_or(PowError::Overflow)?;
        ln.checked_mul(rhs)
            .and_then(|product| product.checked_exp())
            .ok_or(PowError::Overflow)
    }

    /// A legacy value that a power with a non-finite operand treats alike: it has the same sign,
    /// the same comparison with 1, and is integral and even exactly when the value is.
    fn pow_proxy(self) -> LegacyDecimal {
        let Some(value) = self.finite().filter(|_| !self.is_legacy()) else {
            return self.to_legacy();
        };
        let one = Finite {
            negative: false,
            coefficient: 1,
            scale: 0,
        };
        let integral = value.round_dp(0, Rounding::Down);
        let proxy = if value.is_zero() {
            "0"
        } else if integral.cmp_value(&value).is_eq() {
            match value.abs().cmp_value(&one) {
                std::cmp::Ordering::Equal => "1",
                _ if integral.coefficient % 2 == 0 => "2",
                _ => "3",
            }
        } else if value.abs().cmp_value(&one).is_gt() {
            "1.5"
        } else {
            "0.5"
        };
        let proxy = LegacyDecimal::from_str(proxy).unwrap();
        if value.negative { -proxy } else { proxy }
    }

    /// Raises to an integral power by repeated squaring, and normalizes the result.
    fn powi(base: Finite, exponent: Finite) -> Option<Self> {
        fn power(mut base: Finite, mut n: u128) -> Option<Finite> {
            let mut result = Finite {
                negative: false,
                coefficient: 1,
                scale: 0,
            };
            while n > 0 {
                if n & 1 == 1 {
                    result = result.checked_mul(base)?;
                }
                n >>= 1;
                if n > 0 {
                    base = base.checked_mul(base)?;
                }
            }
            Some(result)
        }

        let one = Finite {
            negative: false,
            coefficient: 1,
            scale: 0,
        };
        let n = exponent.to_i128().unsigned_abs();
        let result = if exponent.negative {
            // Prefer the exact reciprocal of the power. If the power is too large, the result is
            // tiny and the reciprocal of the base is precise enough.
            match power(base, n) {
                Some(power) => one.checked_div(power)?,
                None => power(one.checked_div(base)?, n)?,
            }
        } else {
            power(base, n)?
        };
        Some(Self::from_finite(result.normalize()))
    }

    /// Applies a checked binary operation. Values in the legacy range use `legacy`, and only fall
    /// back to `wide` when it fails. A non-finite operand makes the result depend only on the
    /// sign and zeroness of the other one, which the legacy implementation handles.
    fn checked_binary(
        self,
        other: Self,
        legacy: impl FnOnce(&LegacyDecimal, &LegacyDecimal) -> Option<LegacyDecimal>,
        wide: impl FnOnce(Finite, Finite) -> Option<Finite>,
    ) -> Option<Self> {
        let finite = self.is_finite() && other.is_finite();
        if self.is_legacy() && other.is_legacy() {
            let result = legacy(&self.to_legacy(), &other.to_legacy());
            if result.is_some() || !finite {
                return result.map(Self::from_legacy);
            }
        } else if !finite {
            return legacy(&self.legacy_proxy(), &other.legacy_proxy()).map(Self::from_legacy);
        }
        wide(self.finite().unwrap(), other.finite().unwrap()).map(Self::from_finite)
    }

    /// Whether the value is finite and zero, which division handles like the legacy one.
    fn is_finite_zero(&self) -> bool {
        self.finite().is_some_and(|value| value.is_zero())
    }
}

/// The hash decides the vnode of rows distributed by a decimal column and is folded into
/// persisted aggregation states, so it must stay byte-for-byte stable across versions,
/// independent of the internal representation and of the `rust_decimal` version.
///
/// For values in the legacy range, it feeds the hasher exactly what the former `#[derive(Hash)]`
/// did: the variant index as `isize`; for finite values, also the low, middle and high 32-bit
/// words of the normalized coefficient, then a flags word with the sign at bit 31 and the scale
/// at bits 16..24. Values outside the legacy range add the top word of the coefficient before the
/// flags. Normalization strips trailing zeros and turns negative zero into zero, so equal values
/// hash equally.
impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.kind() {
            KIND_NEGATIVE_INF => state.write_isize(0),
            KIND_FINITE => {
                let d = self.normalize();
                let [lo, mid, hi, top] = d.coefficient;
                state.write_isize(1);
                state.write_u32(lo);
                state.write_u32(mid);
                state.write_u32(hi);
                if !d.is_legacy() {
                    state.write_u32(top);
                }
                state.write_u32(d.flags);
            }
            KIND_POSITIVE_INF => state.write_isize(2),
            KIND_NAN => state.write_isize(3),
            _ => unreachable!("invalid decimal kind"),
        }
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Decimal {}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Orders `-Infinity` < finite values < `Infinity` < `NaN`, with `NaN` equal to itself.
impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.is_legacy() && other.is_legacy() {
            return self.to_legacy().cmp(&other.to_legacy());
        }
        let rank = |d: &Self| match d.kind() {
            KIND_NEGATIVE_INF => 0,
            KIND_FINITE => 1,
            KIND_POSITIVE_INF => 2,
            _ => 3,
        };
        match (self.finite(), other.finite()) {
            (Some(lhs), Some(rhs)) => lhs.cmp_value(&rhs),
            _ => rank(self).cmp(&rank(other)),
        }
    }
}

impl fmt::Debug for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_legacy() {
            fmt::Debug::fmt(&self.to_legacy(), f)
        } else {
            write!(f, "Normalized({})", self.finite().unwrap())
        }
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_legacy() {
            fmt::Display::fmt(&self.to_legacy(), f)
        } else {
            fmt::Display::fmt(&self.finite().unwrap(), f)
        }
    }
}

impl ZeroHeapSize for Decimal {}

impl ToText for Decimal {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{self}")
    }

    fn write_with_type<W: std::fmt::Write>(&self, ty: &DataType, f: &mut W) -> std::fmt::Result {
        match ty {
            DataType::Decimal => self.write(f),
            _ => unreachable!(),
        }
    }
}

impl ToSql for Decimal {
    accepts!(NUMERIC);

    to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        if self.is_legacy() {
            return self.to_legacy().to_sql(ty, out);
        }
        let numeric = self.finite().unwrap().to_pg_numeric();
        out.reserve(8 + numeric.digits.len() * 2);
        out.put_u16(numeric.digits.len() as u16);
        out.put_i16(numeric.weight);
        out.put_u16(if numeric.negative { 0x4000 } else { 0 });
        out.put_u16(numeric.dscale);
        for digit in numeric.digits {
            out.put_i16(digit);
        }
        Ok(IsNull::No)
    }
}

impl<'a> FromSql<'a> for Decimal {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        let legacy = LegacyDecimal::from_sql(ty, raw).map(Self::from_legacy);
        let mut buf = raw;
        if buf.len() < 8 {
            return legacy;
        }
        let ndigits = buf.get_u16() as usize;
        let weight = buf.get_i16();
        let sign = buf.get_u16();
        let dscale = buf.get_u16();
        if !matches!(sign, 0 | 0x4000) || buf.len() < ndigits * 2 {
            return legacy;
        }
        // Up to 24 digits with a scale of at most 28 are exact in the legacy range.
        if let Ok(legacy) = legacy
            && ndigits <= 6
            && dscale as u32 <= LEGACY_MAX_SCALE
        {
            return Ok(legacy);
        }
        let numeric = PgNumeric {
            negative: sign == 0x4000,
            weight,
            dscale,
            digits: (0..ndigits).map(|_| buf.get_i16()).collect(),
        };
        match (legacy, Finite::from_pg_numeric(&numeric)) {
            (Ok(legacy), Some(new))
                if legacy
                    .finite()
                    .is_some_and(|old| old.cmp_value(&new).is_eq()) =>
            {
                Ok(legacy)
            }
            (_, Some(new)) => Ok(Self::from_finite(new)),
            (legacy, None) => {
                legacy.and_then(|_| Err("numeric value is out of the decimal range".into()))
            }
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

macro_rules! impl_convert_int {
    ($($T:ty),*) => {
        $(
            impl From<$T> for Decimal {
                fn from(t: $T) -> Self {
                    Self::from_legacy(t.into())
                }
            }

            impl TryFrom<Decimal> for $T {
                type Error = Error;

                fn try_from(d: Decimal) -> Result<Self, Self::Error> {
                    if d.is_legacy() {
                        return d.to_legacy().try_into();
                    }
                    d.finite()
                        .unwrap()
                        .to_i128()
                        .try_into()
                        .map_err(|_| Error::ConversionTo(std::any::type_name::<$T>().into()))
                }
            }
        )*
    };
}

impl_convert_int!(isize, i8, i16, i32, i64, usize, u8, u16, u32, u64);

macro_rules! impl_convert_float {
    ($($T:ty),*) => {
        $(
            impl TryFrom<$T> for Decimal {
                type Error = Error;

                fn try_from(num: $T) -> Result<Self, Self::Error> {
                    LegacyDecimal::try_from(num).map(Self::from_legacy).or_else(|e| {
                        // Beyond the legacy range, convert the shortest exact text form.
                        ParsedNumber::parse(&format!("{num:e}"))
                            .and_then(|parsed| parsed.to_finite())
                            .map(Self::from_finite)
                            .ok_or(e)
                    })
                }
            }

            impl TryFrom<OrderedFloat<$T>> for Decimal {
                type Error = Error;

                fn try_from(value: OrderedFloat<$T>) -> Result<Self, Self::Error> {
                    value.0.try_into()
                }
            }

            impl TryFrom<Decimal> for $T {
                type Error = Error;

                fn try_from(d: Decimal) -> Result<Self, Self::Error> {
                    if d.is_legacy() {
                        return d.to_legacy().try_into();
                    }
                    // Parsing the text form rounds correctly.
                    d.to_string()
                        .parse()
                        .map_err(|_| Error::ConversionTo(stringify!($T).into()))
                }
            }

            impl TryFrom<Decimal> for OrderedFloat<$T> {
                type Error = Error;

                fn try_from(d: Decimal) -> Result<Self, Self::Error> {
                    d.try_into().map(Self)
                }
            }
        )*
    };
}

impl_convert_float!(f32, f64);

impl CheckedAdd for Decimal {
    fn checked_add(&self, other: &Self) -> Option<Self> {
        self.checked_binary(*other, |a, b| a.checked_add(b), Finite::checked_add)
    }
}

impl CheckedSub for Decimal {
    fn checked_sub(&self, other: &Self) -> Option<Self> {
        self.checked_binary(*other, |a, b| a.checked_sub(b), Finite::checked_sub)
    }
}

impl CheckedMul for Decimal {
    fn checked_mul(&self, other: &Self) -> Option<Self> {
        self.checked_binary(*other, |a, b| a.checked_mul(b), Finite::checked_mul)
    }
}

/// Returns `None` on division by zero, like the legacy implementation.
impl CheckedDiv for Decimal {
    fn checked_div(&self, other: &Self) -> Option<Self> {
        self.checked_binary(*other, |a, b| a.checked_div(b), Finite::checked_div)
    }
}

/// Returns `None` on division by zero, like the legacy implementation.
impl CheckedRem for Decimal {
    fn checked_rem(&self, other: &Self) -> Option<Self> {
        // A finite value modulo an infinity is the value itself.
        if self.is_finite() && !other.is_finite() && !other.is_nan() {
            return Some(*self);
        }
        self.checked_binary(*other, |a, b| a.checked_rem(b), Finite::checked_rem)
    }
}

impl Add for Decimal {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        self.checked_add(&other)
            .expect("decimal addition overflowed")
    }
}

impl Sub for Decimal {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        self.checked_sub(&other)
            .expect("decimal subtraction overflowed")
    }
}

impl Mul for Decimal {
    type Output = Self;

    fn mul(self, other: Self) -> Self {
        self.checked_mul(&other)
            .expect("decimal multiplication overflowed")
    }
}

/// Division by zero gives an infinity, or `NaN` for `0 / 0`.
impl Div for Decimal {
    type Output = Self;

    fn div(self, other: Self) -> Self {
        if other.is_finite_zero() || !self.is_finite() || !other.is_finite() {
            return Self::from_legacy(self.legacy_proxy() / other.legacy_proxy());
        }
        self.checked_div(&other)
            .expect("decimal division overflowed")
    }
}

/// The remainder of division by zero is `NaN`.
impl Rem for Decimal {
    type Output = Self;

    fn rem(self, other: Self) -> Self {
        if other.is_finite_zero() {
            return Self::NAN;
        }
        self.checked_rem(&other).unwrap_or(Self::NAN)
    }
}

impl Neg for Decimal {
    type Output = Self;

    fn neg(self) -> Self {
        self.apply(|d| -d, Finite::neg)
    }
}

impl CheckedNeg for Decimal {
    fn checked_neg(&self) -> Option<Self> {
        Some(-*self)
    }
}

impl From<RustDecimal> for Decimal {
    fn from(d: RustDecimal) -> Self {
        Self::from_legacy(d.into())
    }
}

impl Default for Decimal {
    fn default() -> Self {
        Self::from_legacy(LegacyDecimal::default())
    }
}

impl FromStr for Decimal {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse_with(s, LegacyDecimal::from_str(s))
    }
}

impl Zero for Decimal {
    fn zero() -> Self {
        Self::from_legacy(LegacyDecimal::zero())
    }

    fn is_zero(&self) -> bool {
        self.is_finite_zero()
    }
}

impl One for Decimal {
    fn one() -> Self {
        Self::from_legacy(LegacyDecimal::one())
    }
}

impl Num for Decimal {
    type FromStrRadixErr = Error;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        let legacy = <LegacyDecimal as Num>::from_str_radix(str, radix);
        if radix == 10 {
            Self::parse_with(str, legacy)
        } else {
            legacy.map(Self::from_legacy)
        }
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools as _;
    use risingwave_common_estimate_size::EstimateSize;

    use super::*;
    use crate::util::iter_util::ZipEqFast;

    fn roundtrip(decimal: Decimal) -> Decimal {
        let mut buf = vec![];
        decimal.encode_unordered(&mut buf);
        assert_eq!(buf.len(), decimal.encoded_len());
        Decimal::decode_unordered(&mut &buf[..]).unwrap()
    }

    fn check(lhs: f32, rhs: f32) -> bool {
        if lhs.is_nan() && rhs.is_nan() {
            true
        } else if lhs.is_infinite() && rhs.is_infinite() {
            if lhs.is_sign_positive() && rhs.is_sign_positive() {
                true
            } else {
                lhs.is_sign_negative() && rhs.is_sign_negative()
            }
        } else if lhs.is_finite() && rhs.is_finite() {
            lhs == rhs
        } else {
            false
        }
    }

    #[test]
    fn check_op_with_float() {
        let decimals = [
            Decimal::NAN,
            Decimal::POSITIVE_INF,
            Decimal::NEGATIVE_INF,
            Decimal::try_from(1.0).unwrap(),
            Decimal::try_from(-1.0).unwrap(),
            Decimal::try_from(0.0).unwrap(),
        ];
        let floats = [
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            1.0f32,
            -1.0f32,
            0.0f32,
        ];
        for (d_lhs, f_lhs) in decimals.iter().zip_eq_fast(floats.iter()) {
            for (d_rhs, f_rhs) in decimals.iter().zip_eq_fast(floats.iter()) {
                assert!(check((*d_lhs + *d_rhs).try_into().unwrap(), f_lhs + f_rhs));
                assert!(check((*d_lhs - *d_rhs).try_into().unwrap(), f_lhs - f_rhs));
                assert!(check((*d_lhs * *d_rhs).try_into().unwrap(), f_lhs * f_rhs));
                assert!(check((*d_lhs / *d_rhs).try_into().unwrap(), f_lhs / f_rhs));
                assert!(check((*d_lhs % *d_rhs).try_into().unwrap(), f_lhs % f_rhs));
            }
        }
    }

    #[test]
    fn basic_test() {
        assert_eq!(Decimal::from_str("nan").unwrap(), Decimal::NAN,);
        assert_eq!(Decimal::from_str("NaN").unwrap(), Decimal::NAN,);
        assert_eq!(Decimal::from_str("NAN").unwrap(), Decimal::NAN,);
        assert_eq!(Decimal::from_str("nAn").unwrap(), Decimal::NAN,);
        assert_eq!(Decimal::from_str("nAN").unwrap(), Decimal::NAN,);
        assert_eq!(Decimal::from_str("Nan").unwrap(), Decimal::NAN,);
        assert_eq!(Decimal::from_str("NAn").unwrap(), Decimal::NAN,);

        assert_eq!(Decimal::from_str("inf").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("INF").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("iNF").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("inF").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("InF").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("INf").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("+inf").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("+INF").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("+Inf").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("+iNF").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("+inF").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("+InF").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(Decimal::from_str("+INf").unwrap(), Decimal::POSITIVE_INF,);
        assert_eq!(
            Decimal::from_str("inFINity").unwrap(),
            Decimal::POSITIVE_INF,
        );
        assert_eq!(
            Decimal::from_str("+infiNIty").unwrap(),
            Decimal::POSITIVE_INF,
        );

        assert_eq!(Decimal::from_str("-inf").unwrap(), Decimal::NEGATIVE_INF,);
        assert_eq!(Decimal::from_str("-INF").unwrap(), Decimal::NEGATIVE_INF,);
        assert_eq!(Decimal::from_str("-Inf").unwrap(), Decimal::NEGATIVE_INF,);
        assert_eq!(Decimal::from_str("-iNF").unwrap(), Decimal::NEGATIVE_INF,);
        assert_eq!(Decimal::from_str("-inF").unwrap(), Decimal::NEGATIVE_INF,);
        assert_eq!(Decimal::from_str("-InF").unwrap(), Decimal::NEGATIVE_INF,);
        assert_eq!(Decimal::from_str("-INf").unwrap(), Decimal::NEGATIVE_INF,);
        assert_eq!(
            Decimal::from_str("-INfinity").unwrap(),
            Decimal::NEGATIVE_INF,
        );

        assert_eq!(
            Decimal::try_from(10.0).unwrap() / Decimal::POSITIVE_INF,
            Decimal::try_from(0.0).unwrap(),
        );
        assert_eq!(
            Decimal::try_from(f32::INFINITY).unwrap(),
            Decimal::POSITIVE_INF
        );
        assert_eq!(Decimal::try_from(f64::NAN).unwrap(), Decimal::NAN);
        assert_eq!(
            Decimal::try_from(f64::INFINITY).unwrap(),
            Decimal::POSITIVE_INF
        );
        assert_eq!(
            roundtrip(Decimal::try_from(1.234).unwrap()),
            Decimal::try_from(1.234).unwrap(),
        );
        assert_eq!(roundtrip(Decimal::from(1u8)), Decimal::from(1u8),);
        assert_eq!(roundtrip(Decimal::from(1i8)), Decimal::from(1i8),);
        assert_eq!(roundtrip(Decimal::from(1u16)), Decimal::from(1u16),);
        assert_eq!(roundtrip(Decimal::from(1i16)), Decimal::from(1i16),);
        assert_eq!(roundtrip(Decimal::from(1u32)), Decimal::from(1u32),);
        assert_eq!(roundtrip(Decimal::from(1i32)), Decimal::from(1i32),);
        assert_eq!(
            roundtrip(Decimal::try_from(f64::NAN).unwrap()),
            Decimal::try_from(f64::NAN).unwrap(),
        );
        assert_eq!(
            roundtrip(Decimal::try_from(f64::INFINITY).unwrap()),
            Decimal::try_from(f64::INFINITY).unwrap(),
        );
        assert_eq!(u8::try_from(Decimal::from(1u8)).unwrap(), 1,);
        assert_eq!(i8::try_from(Decimal::from(1i8)).unwrap(), 1,);
        assert_eq!(u16::try_from(Decimal::from(1u16)).unwrap(), 1,);
        assert_eq!(i16::try_from(Decimal::from(1i16)).unwrap(), 1,);
        assert_eq!(u32::try_from(Decimal::from(1u32)).unwrap(), 1,);
        assert_eq!(i32::try_from(Decimal::from(1i32)).unwrap(), 1,);
        assert_eq!(u64::try_from(Decimal::from(1u64)).unwrap(), 1,);
        assert_eq!(i64::try_from(Decimal::from(1i64)).unwrap(), 1,);
    }

    #[test]
    fn test_order() {
        use crate::types::ScalarImpl;
        use crate::util::memcmp_encoding;
        use crate::util::sort_util::OrderType;

        let ordered = ["-inf", "-1", "0.00", "0.5", "2", "10", "inf", "nan"]
            .iter()
            .map(|s| Decimal::from_str(s).unwrap())
            .collect_vec();
        let memcmp = |d: Decimal| {
            memcmp_encoding::encode_value(Some(ScalarImpl::Decimal(d)), OrderType::ascending())
                .unwrap()
        };
        for i in 1..ordered.len() {
            assert!(ordered[i - 1] < ordered[i]);
            assert!(memcmp(ordered[i - 1]) < memcmp(ordered[i]));
        }
    }

    /// Locks the formats through which a decimal reaches persisted state, data placement, or
    /// other nodes:
    /// - the hash input decides the vnode of rows distributed by a decimal column, and is folded
    ///   into persisted aggregation states such as `approx_count_distinct` registers;
    /// - the memcomparable and value encodings are stored in state tables, and the value encoding
    ///   also holds constants in the catalog;
    /// - the protobuf array encoding is the wire format between nodes.
    ///
    /// Existing clusters depend on these bytes. Changing an existing entry is a breaking change
    /// that silently misplaces or misreads data, as happened to `jsonb` in #25336. A future
    /// representation may only add entries for values that could not be represented before.
    #[test]
    fn test_encoding_backward_compatible() {
        use std::fmt::Write as _;
        use std::hash::Hasher;

        use crate::array::{Array as _, DataChunk, DecimalArray};
        use crate::hash::VirtualNode;
        use crate::row::OwnedRow;
        use crate::types::{ScalarImpl, hash_datum};
        use crate::util::sort_util::OrderType;
        use crate::util::{memcmp_encoding, value_encoding};

        /// Records the bytes fed into a hasher, one entry per write, so that the expectation
        /// does not depend on the hash function.
        #[derive(Default)]
        struct RecordingHasher(Vec<Vec<u8>>);

        impl Hasher for RecordingHasher {
            fn write(&mut self, bytes: &[u8]) {
                self.0.push(bytes.to_vec());
            }

            fn finish(&self) -> u64 {
                unreachable!()
            }
        }

        let values = [
            "0",
            "-0.00",
            "1",
            "1.000",
            "-1.5",
            "1000.00",
            "123456789.987654321",
            "0.0000000000000000000000000001",
            "7.9228162514264337593543950335",
            "79228162514264337593543950335",
            "-79228162514264337593543950335",
            "NaN",
            "Infinity",
            "-Infinity",
            // Values beyond the legacy range.
            "79228162514264337593543950336",
            "-12345678901234567890123456789012345678",
            "99999999999999999999999999999999999999",
            "0.00000000000000000000000000001",
            "-1.0000000000000000000000000000000000001",
        ];

        let mut actual = String::new();
        for s in values {
            let decimal = Decimal::from_str(s).unwrap();
            let datum = Some(ScalarImpl::Decimal(decimal));

            let mut hasher = RecordingHasher::default();
            hash_datum(&datum, &mut hasher);
            let hash_input = hasher.0.iter().map(hex::encode).join(" ");

            let vnode = VirtualNode::compute_row(
                OwnedRow::new(vec![datum.clone()]),
                &[0],
                VirtualNode::COUNT_FOR_COMPAT,
            );
            let chunk = DataChunk::new(vec![DecimalArray::from_iter([decimal]).into_ref()], 1);
            let chunk_vnodes =
                VirtualNode::compute_chunk(&chunk, &[0], VirtualNode::COUNT_FOR_COMPAT);
            assert_eq!(chunk_vnodes, [vnode]);

            let memcmp = memcmp_encoding::encode_value(&datum, OrderType::ascending()).unwrap();
            let decoded =
                memcmp_encoding::decode_value(&DataType::Decimal, &memcmp, OrderType::ascending())
                    .unwrap();
            assert_eq!(decoded, datum);

            let value = value_encoding::serialize_datum(&datum);
            let decoded =
                value_encoding::deserialize_datum(&value[..], &DataType::Decimal).unwrap();
            assert_eq!(decoded, datum);

            let mut protobuf = vec![];
            decimal.to_protobuf(&mut protobuf).unwrap();
            let decoded = Decimal::from_protobuf(&mut &protobuf[..]).unwrap();
            assert_eq!(decoded, decimal);

            writeln!(actual, "{s}").unwrap();
            writeln!(actual, "  hash input: {hash_input}").unwrap();
            writeln!(actual, "  vnode:      {}", vnode.to_index()).unwrap();
            writeln!(actual, "  memcmp:     {}", hex::encode(&memcmp)).unwrap();
            writeln!(actual, "  value:      {}", hex::encode(&value)).unwrap();
            writeln!(actual, "  protobuf:   {}", hex::encode(&protobuf)).unwrap();
        }

        expect_test::expect![[r#"
            0
              hash input: 0100000000000000 00000000 00000000 00000000 00000000
              vnode:      7
              memcmp:     0015
              value:      0100000000000000000000000000000000
              protobuf:   00000000000000000000000000000000
            -0.00
              hash input: 0100000000000000 00000000 00000000 00000000 00000000
              vnode:      7
              memcmp:     0015
              value:      0100000200000000000000000000000000
              protobuf:   00000200000000000000000000000000
            1
              hash input: 0100000000000000 01000000 00000000 00000000 00000000
              vnode:      150
              memcmp:     001802
              value:      0100000000010000000000000000000000
              protobuf:   00000000010000000000000000000000
            1.000
              hash input: 0100000000000000 01000000 00000000 00000000 00000000
              vnode:      150
              memcmp:     001802
              value:      0100000300e80300000000000000000000
              protobuf:   00000300e80300000000000000000000
            -1.5
              hash input: 0100000000000000 0f000000 00000000 00000000 00000180
              vnode:      92
              memcmp:     0012fc9b
              value:      01000001800f0000000000000000000000
              protobuf:   000001800f0000000000000000000000
            1000.00
              hash input: 0100000000000000 e8030000 00000000 00000000 00000000
              vnode:      66
              memcmp:     001914
              value:      0100000200a08601000000000000000000
              protobuf:   00000200a08601000000000000000000
            123456789.987654321
              hash input: 0100000000000000 b1fa52e0 4b9bb601 00000000 00000900
              vnode:      99
              memcmp:     001c032f5b87b3c5996d4114
              value:      0100000900b1fa52e04b9bb60100000000
              protobuf:   00000900b1fa52e04b9bb60100000000
            0.0000000000000000000000000001
              hash input: 0100000000000000 01000000 00000000 00000000 00001c00
              vnode:      203
              memcmp:     0016f202
              value:      0100001c00010000000000000000000000
              protobuf:   00001c00010000000000000000000000
            7.9228162514264337593543950335
              hash input: 0100000000000000 ffffffff ffffffff ffffffff 00001c00
              vnode:      246
              memcmp:     00180fb93921331d35574b774757bf0746
              value:      0100001c00ffffffffffffffffffffffff
              protobuf:   00001c00ffffffffffffffffffffffff
            79228162514264337593543950335
              hash input: 0100000000000000 ffffffff ffffffff ffffffff 00000000
              vnode:      171
              memcmp:     00220f0fb93921331d35574b774757bf0746
              value:      0100000000ffffffffffffffffffffffff
              protobuf:   00000000ffffffffffffffffffffffff
            -79228162514264337593543950335
              hash input: 0100000000000000 ffffffff ffffffff ffffffff 00000080
              vnode:      139
              memcmp:     0008f0f046c6decce2caa8b488b8a840f8b9
              value:      0100000080ffffffffffffffffffffffff
              protobuf:   00000080ffffffffffffffffffffffff
            NaN
              hash input: 0300000000000000
              vnode:      138
              memcmp:     0024
              value:      0101000000000000000000000000000000
              protobuf:   01000000000000000000000000000000
            Infinity
              hash input: 0200000000000000
              vnode:      20
              memcmp:     0023
              value:      0102000000000000000000000000000000
              protobuf:   02000000000000000000000000000000
            -Infinity
              hash input: 0000000000000000
              vnode:      105
              memcmp:     0007
              value:      0103000000000000000000000000000000
              protobuf:   03000000000000000000000000000000
            79228162514264337593543950336
              hash input: 0100000000000000 00000000 00000000 00000000 01000000 00000000
              vnode:      106
              memcmp:     00220f0fb93921331d35574b774757bf0748
              value:      010400000000000000000000000000000001000000
              protobuf:   0400000000000000000000000000000001000000
            -12345678901234567890123456789012345678
              hash input: 0100000000000000 4ef338de 509049c4 133302f0 f6b04909 00000080
              vnode:      188
              memcmp:     0008ece6ba8e624ae6ba8e624ae6ba8e624ae6ba8e63
              value:      01040000804ef338de509049c4133302f0f6b04909
              protobuf:   040000804ef338de509049c4133302f0f6b04909
            99999999999999999999999999999999999999
              hash input: 0100000000000000 ffffffff 3f228a09 7ac4865a a84c3b4b 00000000
              vnode:      10
              memcmp:     002213c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c6
              value:      0104000000ffffffff3f228a097ac4865aa84c3b4b
              protobuf:   04000000ffffffff3f228a097ac4865aa84c3b4b
            0.00000000000000000000000000001
              hash input: 0100000000000000 01000000 00000000 00000000 00000000 00001d00
              vnode:      174
              memcmp:     0016f114
              value:      0104001d0001000000000000000000000000000000
              protobuf:   04001d0001000000000000000000000000000000
            -1.0000000000000000000000000000000000001
              hash input: 0100000000000000 01000000 a036f400 d946dad5 10ee8507 00002580
              vnode:      94
              memcmp:     0012fcfefefefefefefefefefefefefefefefefefeeb
              value:      010400258001000000a036f400d946dad510ee8507
              protobuf:   0400258001000000a036f400d946dad510ee8507
        "#]]
        .assert_eq(&actual);
    }

    #[test]
    fn test_decimal_estimate_size() {
        let decimal = Decimal::NEGATIVE_INF;
        assert_eq!(decimal.estimated_size(), 20);

        let decimal = Decimal::try_from(1.0).unwrap();
        assert_eq!(decimal.estimated_size(), 20);
    }

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    /// A random value in the legacy range.
    fn random_legacy(rng: &mut impl rand::Rng) -> Decimal {
        let coefficient = match rng.random_range(0..4) {
            0 => rng.random_range(0..1000u128),
            1 => rng.random_range(0..1u128 << 64),
            _ => rng.random_range(0..1u128 << 96),
        };
        let value = Finite {
            negative: rng.random(),
            coefficient,
            scale: rng.random_range(0..=28),
        };
        let decimal = Decimal::from_finite(value);
        assert!(decimal.is_legacy());
        decimal
    }

    /// For values in the legacy range, everything must stay exactly as it was, unless the legacy
    /// implementation fails.
    #[test]
    fn test_legacy_range_unchanged() {
        use rand::SeedableRng;

        fn same(new: Decimal, old: LegacyDecimal) {
            assert!(new.is_legacy(), "{new:?} vs {old:?}");
            assert_eq!(
                new.to_fixed_bytes(),
                Decimal::from_legacy(old).to_fixed_bytes()
            );
        }

        let mut rng = rand::rngs::StdRng::seed_from_u64(38);
        for _ in 0..20000 {
            let (a, b) = (random_legacy(&mut rng), random_legacy(&mut rng));
            let (la, lb) = (a.to_legacy(), b.to_legacy());
            for (new, old) in [
                (a.checked_add(&b), la.checked_add(&lb)),
                (a.checked_sub(&b), la.checked_sub(&lb)),
                (a.checked_mul(&b), la.checked_mul(&lb)),
                (a.checked_div(&b), la.checked_div(&lb)),
                (a.checked_rem(&b), la.checked_rem(&lb)),
            ] {
                if let Some(old) = old {
                    same(new.unwrap(), old);
                }
            }
            assert_eq!(a.cmp(&b), la.cmp(&lb));
            assert_eq!(a.to_string(), la.to_string());
            let text = a.to_string();
            same(
                Decimal::from_str(&text).unwrap(),
                LegacyDecimal::from_str(&text).unwrap(),
            );
            same(-a, -la);
            same(a.normalize(), la.normalize());
            same(a.round_dp_ties_away(3), la.round_dp_ties_away(3));
            same(a.ceil(), la.ceil());
            same(a.floor(), la.floor());
            same(a.trunc(), la.trunc());
            same(a.round_ties_even(), la.round_ties_even());
            same(roundtrip(a), la);
            let float: f64 = a.try_into().unwrap();
            assert_eq!(float, f64::try_from(la).unwrap());
            same(
                Decimal::try_from(float).unwrap(),
                LegacyDecimal::try_from(float).unwrap(),
            );
        }
    }

    #[test]
    fn test_wide_values() {
        // Inputs beyond the legacy range keep up to 38 digits.
        for (input, output) in [
            (
                "79228162514264337593543950336",
                "79228162514264337593543950336",
            ),
            (
                "-12345678901234567890123456789012345678",
                "-12345678901234567890123456789012345678",
            ),
            (
                "0.00000000000000000000000000001",
                "0.00000000000000000000000000001",
            ),
            (
                "1.0000000000000000000000000000000000001",
                "1.0000000000000000000000000000000000001",
            ),
            ("1e37", "10000000000000000000000000000000000000"),
            // Legacy results are kept when no precision is gained.
            (
                "1.0000000000000000000000000000000000000",
                "1.0000000000000000000000000000",
            ),
            // The legacy parser rejects exponents and scales beyond 28.
            ("1e-30", "0.000000000000000000000000000001"),
            ("1.5e-37", "0.00000000000000000000000000000000000015"),
            // The legacy parser ignores the exponent after its 29th fractional digit.
            (
                "1.00000000000000000000000000001e-5",
                "0.0000100000000000000000000000000001",
            ),
        ] {
            assert_eq!(dec(input).to_string(), output, "{input}");
        }
        // Scientific notation whose scale exceeds 38 is rejected, not rounded to zero.
        for input in [
            "1e-39",
            "1.5e-38",
            "1e-1000",
            "1.00000000000000000000000000001e-28",
        ] {
            assert!(Decimal::from_str(input).is_err(), "{input}");
            assert!(Decimal::from_scientific(input).is_none(), "{input}");
            assert!(Decimal::from_str_radix(input, 10).is_err(), "{input}");
        }
        assert!(Decimal::from_str("1e38").is_err());
        assert!(Decimal::from_str("123456789012345678901234567890123456789").is_err());
        assert_eq!(
            Decimal::from_str_radix("12345678901234567890123456789012345678", 10)
                .unwrap()
                .to_string(),
            "12345678901234567890123456789012345678"
        );
        assert_eq!(
            Decimal::from_scientific("1.5e30").unwrap().to_string(),
            "1500000000000000000000000000000"
        );

        // Legacy results stay legacy, overflows continue with 38 digits.
        let third = dec("1") / dec("3");
        assert_eq!(third.to_string(), "0.3333333333333333333333333333");
        let max_legacy = dec("79228162514264337593543950335");
        assert_eq!(
            (max_legacy + dec("1")).to_string(),
            "79228162514264337593543950336"
        );
        assert_eq!(
            (max_legacy * dec("1000")).to_string(),
            "79228162514264337593543950335000"
        );
        let wide = dec("12345678901234567890123456789012345678");
        assert_eq!(
            (wide / dec("3")).to_string(),
            "4115226300411522630041152263004115226"
        );
        assert_eq!(
            (dec("1e31") / dec("3")).to_string(),
            "3333333333333333333333333333333.3333333"
        );
        // An input that only adds trailing zeros stays in the legacy range.
        assert_eq!(
            (dec("1.00000000000000000000000000000000000") / dec("3")).to_string(),
            "0.3333333333333333333333333333"
        );
        let max = dec("99999999999999999999999999999999999999");
        assert_eq!(max.checked_add(&dec("1")), None);
        assert_eq!(max.checked_mul(&dec("1.5")), None);
        assert_eq!(
            (max * dec("0.5")).to_string(),
            "50000000000000000000000000000000000000"
        );
        assert_eq!((wide - wide).to_string(), "0");
        assert_eq!((wide % dec("10")).to_string(), "8");
        assert_eq!(
            (-wide).to_string(),
            "-12345678901234567890123456789012345678"
        );

        // Equal values compare and hash equally across representations.
        let two_wide = dec("2.0000000000000000000000000000000000001")
            - dec("0.0000000000000000000000000000000000001");
        assert!(!two_wide.is_legacy());
        assert_eq!(two_wide, dec("2"));
        let state = std::hash::RandomState::new();
        {
            use std::hash::BuildHasher;
            assert_eq!(state.hash_one(two_wide), state.hash_one(dec("2.00")));
        }
        assert_eq!(
            two_wide.normalize().to_fixed_bytes(),
            dec("2.00").normalize().to_fixed_bytes()
        );
        assert!(dec("-1e30") < dec("-1") && dec("-1") < dec("1e-30") && dec("1e-30") < dec("1e30"));
        assert!(Decimal::NEGATIVE_INF < dec("-1e30") && dec("1e30") < Decimal::POSITIVE_INF);
        assert!(Decimal::POSITIVE_INF < Decimal::NAN);

        // Special values behave as before.
        assert_eq!(wide + Decimal::POSITIVE_INF, Decimal::POSITIVE_INF);
        assert_eq!(-wide * Decimal::POSITIVE_INF, Decimal::NEGATIVE_INF);
        assert_eq!(wide / Decimal::POSITIVE_INF, Decimal::zero());
        assert_eq!(wide % Decimal::NEGATIVE_INF, wide);
        assert_eq!(wide / Decimal::zero(), Decimal::POSITIVE_INF);
        assert_eq!(wide.checked_div(&Decimal::zero()), None);
        assert!((wide % Decimal::zero()).is_nan());
        assert!((Decimal::POSITIVE_INF % wide).is_nan());
        assert_eq!(wide.sign(), dec("1"));

        // Conversions.
        assert_eq!(f64::try_from(wide).unwrap(), 1.2345678901234568e37);
        assert_eq!(
            Decimal::try_from(1e30f64).unwrap().to_string(),
            "1000000000000000000000000000000"
        );
        assert!(Decimal::try_from(1e38f64).is_err());
        assert!(i64::try_from(wide).is_err());
        assert_eq!(
            i64::try_from(dec("1.00000000000000000000000000000000000005")).unwrap(),
            1
        );
        assert_eq!(
            wide.to_parts(),
            DecimalParts::Finite {
                mantissa: 12345678901234567890123456789012345678,
                scale: 0
            }
        );
        assert_eq!(
            Decimal::from_i128_with_scale(-12345678901234567890123456789012345678, 38).to_string(),
            "-0.12345678901234567890123456789012345678"
        );
        assert_eq!(
            Decimal::truncated_i128_and_scale(i128::MAX, 2)
                .unwrap()
                .to_string(),
            "1701411834604692317316873037158841057.2"
        );
        // The legacy result is kept when only trailing zeros are dropped.
        assert_eq!(
            Decimal::truncated_i128_and_scale(9999999999999999999999999990000000000, 10)
                .unwrap()
                .to_string(),
            "999999999999999999999999999.0"
        );
        assert_eq!(
            Decimal::truncated_i128_and_scale(9999999999999999999999999990000000001, 10)
                .unwrap()
                .to_string(),
            "999999999999999999999999999.0000000001"
        );

        // Rounding.
        let x = dec("-1234567890123456789012345678.5678901234");
        assert_eq!(
            x.round_dp_ties_away(2).to_string(),
            "-1234567890123456789012345678.57"
        );
        assert_eq!(x.ceil().to_string(), "-1234567890123456789012345678");
        assert_eq!(x.floor().to_string(), "-1234567890123456789012345679");
        assert_eq!(x.trunc().to_string(), "-1234567890123456789012345678");
        assert_eq!(
            x.round_ties_even().to_string(),
            "-1234567890123456789012345679"
        );
        assert_eq!(
            x.round_left_ties_away(10).unwrap().to_string(),
            "-1234567890123456790000000000"
        );
        assert_eq!(x.scale(), Some(10));
        let mut y = x;
        y.rescale(4);
        assert_eq!(y.to_string(), "-1234567890123456789012345678.5679");
        y.rescale(12);
        assert_eq!(y.to_string(), "-1234567890123456789012345678.5679000000");

        // Functions on wide values have the legacy precision.
        assert_eq!(dec("1e30").checked_sqrt().unwrap(), dec("1e15"));
        assert_eq!(dec("1e-30").checked_sqrt().unwrap(), dec("1e-15"));
        assert_eq!(
            dec("2e30")
                .checked_sqrt()
                .unwrap()
                .round_dp_ties_away(10)
                .to_string(),
            "1414213562373095.0488016887"
        );
        assert_eq!(dec("1e30").checked_log10().unwrap(), dec("30"));
        assert_eq!(
            dec("1e30")
                .checked_ln()
                .unwrap()
                .round_dp_ties_away(20)
                .to_string(),
            "69.07755278982137052054"
        );
        assert!(dec("-1e30").checked_ln().is_none());
        assert_eq!(dec("-1e30").checked_exp(), Some(Decimal::zero()));
        assert_eq!(dec("1e30").checked_exp(), None);
        assert_eq!(
            dec("10").checked_powd(&dec("30")).ok().unwrap().to_string(),
            "1000000000000000000000000000000"
        );
        assert_eq!(
            dec("10")
                .checked_powd(&dec("-30"))
                .ok()
                .unwrap()
                .to_string(),
            "0.000000000000000000000000000001"
        );
        assert!(matches!(
            dec("10").checked_powd(&dec("38")),
            Err(PowError::Overflow)
        ));
        let pow = |b: &str, e: &str| dec(b).checked_powd(&dec(e)).ok();
        assert_eq!(pow("1e30", "Infinity"), Some(Decimal::POSITIVE_INF));
        assert_eq!(pow("1e-30", "Infinity"), Some(Decimal::zero()));
        let wide_one = dec("2.0000000000000000000000000000000000001")
            - dec("1.0000000000000000000000000000000000001");
        assert!(!wide_one.is_legacy());
        assert_eq!(wide_one.checked_powd(&Decimal::NAN).ok(), Some(dec("1")));
        assert_eq!(
            wide_one.checked_powd(&Decimal::POSITIVE_INF).ok(),
            Some(dec("1"))
        );
        assert_eq!(pow("1e30", "-Infinity"), Some(Decimal::zero()));
        assert_eq!(pow("-Infinity", "1e30"), Some(Decimal::POSITIVE_INF));
        assert_eq!(
            pow("-Infinity", "1000000000000000000000000000001"),
            Some(Decimal::NEGATIVE_INF)
        );
        assert!(matches!(
            dec("-Infinity").checked_powd(&dec("1.0000000000000000000000000000000000001")),
            Err(PowError::NegativeFract)
        ));
        assert_eq!(
            dec("2").checked_powd(&dec("96")).ok().unwrap().to_string(),
            "79228162514264337593543950336"
        );
        assert_eq!(
            dec("1e28").checked_powd(&dec("-2")).ok(),
            Some(Decimal::zero())
        );
        // Legacy operands keep the legacy precision.
        assert_eq!(
            dec("-1.5")
                .checked_powd(&dec("-3"))
                .ok()
                .unwrap()
                .to_string(),
            "-0.2962962962962962962962962963"
        );
        assert_eq!(
            dec("-1.5e30")
                .checked_powd(&dec("-1"))
                .ok()
                .unwrap()
                .to_string(),
            "-0.00000000000000000000000000000066666667"
        );
    }

    #[test]
    fn test_wide_encodings() {
        use crate::types::ScalarImpl;
        use crate::util::sort_util::OrderType;
        use crate::util::{memcmp_encoding, value_encoding};

        let mut values = [
            "-Infinity",
            "-99999999999999999999999999999999999999",
            "-79228162514264337593543950336",
            "-79228162514264337593543950335",
            "-1.0000000000000000000000000000000000001",
            "-1",
            "-0.00000000000000000000000000000000000001",
            "0",
            "0.00000000000000000000000000000000000001",
            "0.00000000000000000000000000001",
            "0.0000000000000000000000000001",
            "1",
            "1.0000000000000000000000000000000000001",
            "79228162514264337593543950335",
            "79228162514264337593543950336",
            "99999999999999999999999999999999999999",
            "Infinity",
            "NaN",
        ]
        .map(dec);
        let mut memcmp_values = vec![];
        for decimal in values {
            let datum = Some(ScalarImpl::Decimal(decimal));
            assert_eq!(
                roundtrip(decimal).to_fixed_bytes(),
                decimal.to_fixed_bytes()
            );
            let mut protobuf = vec![];
            decimal.to_protobuf(&mut protobuf).unwrap();
            assert_eq!(protobuf.len(), decimal.encoded_len());
            let decoded = Decimal::from_protobuf(&mut &protobuf[..]).unwrap();
            assert_eq!(decoded.to_fixed_bytes(), decimal.to_fixed_bytes());
            let value = value_encoding::serialize_datum(&datum);
            assert_eq!(
                value_encoding::deserialize_datum(&value[..], &DataType::Decimal).unwrap(),
                datum
            );
            for order in [OrderType::ascending(), OrderType::descending()] {
                let memcmp = memcmp_encoding::encode_value(&datum, order).unwrap();
                let decoded =
                    memcmp_encoding::decode_value(&DataType::Decimal, &memcmp, order).unwrap();
                assert_eq!(decoded, datum);
                if order == OrderType::ascending() {
                    memcmp_values.push(memcmp);
                }
            }
        }
        // The list is sorted, and so must be its memcomparable encodings.
        assert!(memcmp_values.is_sorted());
        assert!(values.is_sorted());
        values.reverse();
        assert!(!values.is_sorted());

        // The PostgreSQL binary format, used by pgwire and PostgreSQL connectors.
        for decimal in values {
            let mut bytes = BytesMut::new();
            decimal.to_sql(&Type::NUMERIC, &mut bytes).unwrap();
            let decoded = Decimal::from_sql(&Type::NUMERIC, &bytes).unwrap();
            assert_eq!(decoded, decimal);
            assert_eq!(decoded.scale(), decimal.scale());
        }

        // Unknown tags are rejected rather than misread.
        let mut bytes = vec![];
        dec("1e30").encode_unordered(&mut bytes);
        bytes[0] = 5;
        assert!(Decimal::decode_unordered(&mut &bytes[..]).is_err());
        assert!(Decimal::decode_unordered(&mut &bytes[..16]).is_err());
    }
}
