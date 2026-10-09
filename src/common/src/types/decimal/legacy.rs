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

use std::fmt::Debug;
use std::io::Cursor;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use num_traits::{
    CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Num, One, Zero,
};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use rust_decimal::prelude::FromStr;
use rust_decimal::{Decimal as RustDecimal, Error, MathematicalOps as _, RoundingStrategy};

use self::LegacyDecimal::Normalized;
use crate::types::ordered_float::OrderedFloat;

/// The implementation of [`super::Decimal`] before it supported more than 28 significant digits,
/// backed by `rust_decimal`.
///
/// Existing streaming jobs recompute old rows when they are updated or deleted, so for the values
/// it can represent, its results, text forms and encodings must stay exactly the same.
#[derive(Debug, Copy, parse_display::Display, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum LegacyDecimal {
    #[display("-Infinity")]
    NegativeInf,
    #[display("{0}")]
    Normalized(RustDecimal),
    #[display("Infinity")]
    PositiveInf,
    #[display("NaN")]
    NaN,
}

impl LegacyDecimal {
    pub fn from_scientific(value: &str) -> Option<Self> {
        let decimal = RustDecimal::from_scientific(value).ok()?;
        Some(Normalized(decimal))
    }

    pub fn from_str_radix(s: &str, radix: u32) -> rust_decimal::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "nan" => Ok(LegacyDecimal::NaN),
            "inf" | "+inf" | "infinity" | "+infinity" => Ok(LegacyDecimal::PositiveInf),
            "-inf" | "-infinity" => Ok(LegacyDecimal::NegativeInf),
            s => RustDecimal::from_str_radix(s, radix).map(LegacyDecimal::Normalized),
        }
    }
}

impl ToSql for LegacyDecimal {
    accepts!(NUMERIC);

    to_sql_checked!();

    fn to_sql(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            LegacyDecimal::Normalized(d) => {
                return d.to_sql(ty, out);
            }
            LegacyDecimal::NaN => {
                out.reserve(8);
                out.put_u16(0);
                out.put_i16(0);
                out.put_u16(0xC000);
                out.put_i16(0);
            }
            LegacyDecimal::PositiveInf => {
                out.reserve(8);
                out.put_u16(0);
                out.put_i16(0);
                out.put_u16(0xD000);
                out.put_i16(0);
            }
            LegacyDecimal::NegativeInf => {
                out.reserve(8);
                out.put_u16(0);
                out.put_i16(0);
                out.put_u16(0xF000);
                out.put_i16(0);
            }
        }
        Ok(IsNull::No)
    }
}

impl<'a> FromSql<'a> for LegacyDecimal {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        let mut rdr = Cursor::new(raw);
        let _n_digits = rdr.read_u16::<BigEndian>()?;
        let _weight = rdr.read_i16::<BigEndian>()?;
        let sign = rdr.read_u16::<BigEndian>()?;
        match sign {
            0xC000 => Ok(Self::NaN),
            0xD000 => Ok(Self::PositiveInf),
            0xF000 => Ok(Self::NegativeInf),
            _ => RustDecimal::from_sql(ty, raw).map(Self::Normalized),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}

macro_rules! impl_convert_int {
    ($T:ty) => {
        impl core::convert::From<$T> for LegacyDecimal {
            #[inline]
            fn from(t: $T) -> Self {
                Self::Normalized(t.into())
            }
        }

        impl core::convert::TryFrom<LegacyDecimal> for $T {
            type Error = Error;

            #[inline]
            fn try_from(d: LegacyDecimal) -> Result<Self, Self::Error> {
                match d.round_dp_ties_away(0) {
                    LegacyDecimal::Normalized(d) => d.try_into(),
                    _ => Err(Error::ConversionTo(std::any::type_name::<$T>().into())),
                }
            }
        }
    };
}

macro_rules! impl_convert_float {
    ($T:ty) => {
        impl core::convert::TryFrom<$T> for LegacyDecimal {
            type Error = Error;

            fn try_from(num: $T) -> Result<Self, Self::Error> {
                match num {
                    num if num.is_nan() => Ok(LegacyDecimal::NaN),
                    num if num.is_infinite() && num.is_sign_positive() => {
                        Ok(LegacyDecimal::PositiveInf)
                    }
                    num if num.is_infinite() && num.is_sign_negative() => {
                        Ok(LegacyDecimal::NegativeInf)
                    }
                    num => num.try_into().map(LegacyDecimal::Normalized),
                }
            }
        }
        impl core::convert::TryFrom<OrderedFloat<$T>> for LegacyDecimal {
            type Error = Error;

            fn try_from(value: OrderedFloat<$T>) -> Result<Self, Self::Error> {
                value.0.try_into()
            }
        }

        impl core::convert::TryFrom<LegacyDecimal> for $T {
            type Error = Error;

            fn try_from(d: LegacyDecimal) -> Result<Self, Self::Error> {
                match d {
                    LegacyDecimal::Normalized(d) => d.try_into(),
                    LegacyDecimal::NaN => Ok(<$T>::NAN),
                    LegacyDecimal::PositiveInf => Ok(<$T>::INFINITY),
                    LegacyDecimal::NegativeInf => Ok(<$T>::NEG_INFINITY),
                }
            }
        }
        impl core::convert::TryFrom<LegacyDecimal> for OrderedFloat<$T> {
            type Error = Error;

            fn try_from(d: LegacyDecimal) -> Result<Self, Self::Error> {
                d.try_into().map(Self)
            }
        }
    };
}

macro_rules! checked_proxy {
    ($trait:ty, $func:ident, $op: tt) => {
        impl $trait for LegacyDecimal {
            fn $func(&self, other: &Self) -> Option<Self> {
                match (self, other) {
                    (Self::Normalized(lhs), Self::Normalized(rhs)) => {
                        lhs.$func(rhs).map(LegacyDecimal::Normalized)
                    }
                    (lhs, rhs) => Some(*lhs $op *rhs),
                }
            }
        }
    }
}

impl_convert_float!(f32);
impl_convert_float!(f64);

impl_convert_int!(isize);
impl_convert_int!(i8);
impl_convert_int!(i16);
impl_convert_int!(i32);
impl_convert_int!(i64);
impl_convert_int!(usize);
impl_convert_int!(u8);
impl_convert_int!(u16);
impl_convert_int!(u32);
impl_convert_int!(u64);

checked_proxy!(CheckedRem, checked_rem, %);
checked_proxy!(CheckedSub, checked_sub, -);
checked_proxy!(CheckedAdd, checked_add, +);
checked_proxy!(CheckedDiv, checked_div, /);
checked_proxy!(CheckedMul, checked_mul, *);

impl Add for LegacyDecimal {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Normalized(lhs), Self::Normalized(rhs)) => Self::Normalized(lhs + rhs),
            (Self::NaN, _) => Self::NaN,
            (_, Self::NaN) => Self::NaN,
            (Self::PositiveInf, Self::NegativeInf) => Self::NaN,
            (Self::NegativeInf, Self::PositiveInf) => Self::NaN,
            (Self::PositiveInf, _) => Self::PositiveInf,
            (_, Self::PositiveInf) => Self::PositiveInf,
            (Self::NegativeInf, _) => Self::NegativeInf,
            (_, Self::NegativeInf) => Self::NegativeInf,
        }
    }
}

impl Neg for LegacyDecimal {
    type Output = Self;

    fn neg(self) -> Self {
        match self {
            Self::Normalized(d) => Self::Normalized(-d),
            Self::NaN => Self::NaN,
            Self::PositiveInf => Self::NegativeInf,
            Self::NegativeInf => Self::PositiveInf,
        }
    }
}

impl CheckedNeg for LegacyDecimal {
    fn checked_neg(&self) -> Option<Self> {
        match self {
            Self::Normalized(d) => Some(Self::Normalized(-d)),
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::NegativeInf),
            Self::NegativeInf => Some(Self::PositiveInf),
        }
    }
}

impl Rem for LegacyDecimal {
    type Output = Self;

    fn rem(self, other: Self) -> Self {
        match (self, other) {
            (Self::Normalized(lhs), Self::Normalized(rhs)) if !rhs.is_zero() => {
                Self::Normalized(lhs % rhs)
            }
            (Self::Normalized(_), Self::Normalized(_)) => Self::NaN,
            (Self::Normalized(lhs), Self::PositiveInf)
                if lhs.is_sign_positive() || lhs.is_zero() =>
            {
                Self::Normalized(lhs)
            }
            (Self::Normalized(d), Self::PositiveInf) => Self::Normalized(d),
            (Self::Normalized(lhs), Self::NegativeInf)
                if lhs.is_sign_negative() || lhs.is_zero() =>
            {
                Self::Normalized(lhs)
            }
            (Self::Normalized(d), Self::NegativeInf) => Self::Normalized(d),
            _ => Self::NaN,
        }
    }
}

impl Div for LegacyDecimal {
    type Output = Self;

    fn div(self, other: Self) -> Self {
        match (self, other) {
            // nan
            (Self::NaN, _) => Self::NaN,
            (_, Self::NaN) => Self::NaN,
            // div by zero
            (lhs, Self::Normalized(rhs)) if rhs.is_zero() => match lhs {
                Self::Normalized(lhs) => {
                    if lhs.is_sign_positive() && !lhs.is_zero() {
                        Self::PositiveInf
                    } else if lhs.is_sign_negative() && !lhs.is_zero() {
                        Self::NegativeInf
                    } else {
                        Self::NaN
                    }
                }
                Self::PositiveInf => Self::PositiveInf,
                Self::NegativeInf => Self::NegativeInf,
                _ => unreachable!(),
            },
            // div by +/-inf
            (Self::Normalized(_), Self::PositiveInf) => Self::Normalized(RustDecimal::from(0)),
            (_, Self::PositiveInf) => Self::NaN,
            (Self::Normalized(_), Self::NegativeInf) => Self::Normalized(RustDecimal::from(0)),
            (_, Self::NegativeInf) => Self::NaN,
            // div inf
            (Self::PositiveInf, Self::Normalized(d)) if d.is_sign_positive() => Self::PositiveInf,
            (Self::PositiveInf, Self::Normalized(d)) if d.is_sign_negative() => Self::NegativeInf,
            (Self::NegativeInf, Self::Normalized(d)) if d.is_sign_positive() => Self::NegativeInf,
            (Self::NegativeInf, Self::Normalized(d)) if d.is_sign_negative() => Self::PositiveInf,
            // normal case
            (Self::Normalized(lhs), Self::Normalized(rhs)) => Self::Normalized(lhs / rhs),
            _ => unreachable!(),
        }
    }
}

impl Mul for LegacyDecimal {
    type Output = Self;

    fn mul(self, other: Self) -> Self {
        match (self, other) {
            (Self::Normalized(lhs), Self::Normalized(rhs)) => Self::Normalized(lhs * rhs),
            (Self::NaN, _) => Self::NaN,
            (_, Self::NaN) => Self::NaN,
            (Self::PositiveInf, Self::Normalized(rhs))
                if !rhs.is_zero() && rhs.is_sign_negative() =>
            {
                Self::NegativeInf
            }
            (Self::PositiveInf, Self::Normalized(rhs))
                if !rhs.is_zero() && rhs.is_sign_positive() =>
            {
                Self::PositiveInf
            }
            (Self::PositiveInf, Self::PositiveInf) => Self::PositiveInf,
            (Self::PositiveInf, Self::NegativeInf) => Self::NegativeInf,
            (Self::Normalized(lhs), Self::PositiveInf)
                if !lhs.is_zero() && lhs.is_sign_negative() =>
            {
                Self::NegativeInf
            }
            (Self::Normalized(lhs), Self::PositiveInf)
                if !lhs.is_zero() && lhs.is_sign_positive() =>
            {
                Self::PositiveInf
            }
            (Self::NegativeInf, Self::PositiveInf) => Self::NegativeInf,
            (Self::NegativeInf, Self::Normalized(rhs))
                if !rhs.is_zero() && rhs.is_sign_negative() =>
            {
                Self::PositiveInf
            }
            (Self::NegativeInf, Self::Normalized(rhs))
                if !rhs.is_zero() && rhs.is_sign_positive() =>
            {
                Self::NegativeInf
            }
            (Self::NegativeInf, Self::NegativeInf) => Self::PositiveInf,
            (Self::Normalized(lhs), Self::NegativeInf)
                if !lhs.is_zero() && lhs.is_sign_negative() =>
            {
                Self::PositiveInf
            }
            (Self::Normalized(lhs), Self::NegativeInf)
                if !lhs.is_zero() && lhs.is_sign_positive() =>
            {
                Self::NegativeInf
            }
            // 0 * {inf, nan} => nan
            _ => Self::NaN,
        }
    }
}

impl Sub for LegacyDecimal {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        match (self, other) {
            (Self::Normalized(lhs), Self::Normalized(rhs)) => Self::Normalized(lhs - rhs),
            (Self::NaN, _) => Self::NaN,
            (_, Self::NaN) => Self::NaN,
            (Self::PositiveInf, Self::PositiveInf) => Self::NaN,
            (Self::NegativeInf, Self::NegativeInf) => Self::NaN,
            (Self::PositiveInf, _) => Self::PositiveInf,
            (_, Self::PositiveInf) => Self::NegativeInf,
            (Self::NegativeInf, _) => Self::NegativeInf,
            (_, Self::NegativeInf) => Self::PositiveInf,
        }
    }
}

impl LegacyDecimal {
    const MAX_I128_REPR: i128 = 0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF;
    pub const MAX_PRECISION: u8 = 28;

    pub fn scale(&self) -> Option<i32> {
        let LegacyDecimal::Normalized(d) = self else {
            return None;
        };
        Some(d.scale() as _)
    }

    pub fn rescale(&mut self, scale: u32) {
        if let Normalized(a) = self {
            a.rescale(scale);
        }
    }

    #[must_use]
    pub fn round_dp_ties_away(&self, dp: u32) -> Self {
        match self {
            Self::Normalized(d) => {
                let new_d = d.round_dp_with_strategy(dp, RoundingStrategy::MidpointAwayFromZero);
                Self::Normalized(new_d)
            }
            d => *d,
        }
    }

    /// Round to the left of the decimal point, for example `31.5` -> `30`.
    #[must_use]
    pub fn round_left_ties_away(&self, left: u32) -> Option<Self> {
        let &Self::Normalized(mut d) = self else {
            return Some(*self);
        };

        // First, move the decimal point to the left so that we can reuse `round`. This is more
        // efficient than division.
        let old_scale = d.scale();
        let new_scale = old_scale.saturating_add(left);
        const MANTISSA_UP: i128 = 5 * 10i128.pow(LegacyDecimal::MAX_PRECISION as _);
        let d = match new_scale.cmp(&Self::MAX_PRECISION.add(1).into()) {
            // trivial within 28 digits
            std::cmp::Ordering::Less => {
                d.set_scale(new_scale).unwrap();
                d.round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
            }
            // Special case: scale cannot be 29, but it may or may not be >= 0.5e+29
            std::cmp::Ordering::Equal => (d.mantissa() / MANTISSA_UP).signum().into(),
            // always 0 for >= 30 digits
            std::cmp::Ordering::Greater => 0.into(),
        };

        // Then multiply back. Note that we cannot move decimal point to the right in order to get
        // more zeros.
        match left > LegacyDecimal::MAX_PRECISION.into() {
            true => d.is_zero().then(|| 0.into()),
            false => d
                .checked_mul(RustDecimal::from_i128_with_scale(10i128.pow(left), 0))
                .map(Self::Normalized),
        }
    }

    #[must_use]
    pub fn ceil(&self) -> Self {
        match self {
            Self::Normalized(d) => {
                let mut d = d.ceil();
                if d.is_zero() {
                    d.set_sign_positive(true);
                }
                Self::Normalized(d)
            }
            d => *d,
        }
    }

    #[must_use]
    pub fn floor(&self) -> Self {
        match self {
            Self::Normalized(d) => Self::Normalized(d.floor()),
            d => *d,
        }
    }

    #[must_use]
    pub fn trunc(&self) -> Self {
        match self {
            Self::Normalized(d) => {
                let mut d = d.trunc();
                if d.is_zero() {
                    d.set_sign_positive(true);
                }
                Self::Normalized(d)
            }
            d => *d,
        }
    }

    #[must_use]
    pub fn round_ties_even(&self) -> Self {
        match self {
            Self::Normalized(d) => Self::Normalized(d.round()),
            d => *d,
        }
    }

    pub fn from_i128_with_scale(num: i128, scale: u32) -> Self {
        LegacyDecimal::Normalized(RustDecimal::from_i128_with_scale(num, scale))
    }

    /// Truncate the given `num` and `scale` to fit into `LegacyDecimal`, return `None` if it cannot be
    /// represented even after truncation.
    pub fn truncated_i128_and_scale(mut num: i128, mut scale: u32) -> Option<Self> {
        if num.abs() > Self::MAX_I128_REPR {
            let digits = num.abs().ilog10() + 1;
            let diff_scale = digits.saturating_sub(Self::MAX_PRECISION as u32);
            if scale < diff_scale {
                return None;
            }
            num /= 10i128.pow(diff_scale);
            scale -= diff_scale;
        }
        if scale > Self::MAX_PRECISION as u32 {
            let diff_scale = scale - Self::MAX_PRECISION as u32;
            num /= 10i128.pow(diff_scale);
            scale = Self::MAX_PRECISION as u32;
        }
        Some(LegacyDecimal::Normalized(
            RustDecimal::try_from_i128_with_scale(num, scale).ok()?,
        ))
    }

    #[must_use]
    pub fn normalize(&self) -> Self {
        match self {
            Self::Normalized(d) => Self::Normalized(d.normalize()),
            d => *d,
        }
    }

    pub fn unordered_serialize(&self) -> [u8; 16] {
        // according to https://docs.rs/rust_decimal/1.18.0/src/rust_decimal/decimal.rs.html#665-684
        // the lower 15 bits is not used, so we can use first byte to distinguish nan and inf
        match self {
            Self::Normalized(d) => d.serialize(),
            Self::NaN => [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            Self::PositiveInf => [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            Self::NegativeInf => [3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        }
    }

    pub fn unordered_deserialize(bytes: [u8; 16]) -> Self {
        match bytes[0] {
            0u8 => Self::Normalized(RustDecimal::deserialize(bytes)),
            1u8 => Self::NaN,
            2u8 => Self::PositiveInf,
            3u8 => Self::NegativeInf,
            _ => unreachable!(),
        }
    }

    pub fn abs(&self) -> Self {
        match self {
            Self::Normalized(d) => {
                if d.is_sign_negative() {
                    Self::Normalized(-d)
                } else {
                    Self::Normalized(*d)
                }
            }
            Self::NaN => Self::NaN,
            Self::PositiveInf => Self::PositiveInf,
            Self::NegativeInf => Self::PositiveInf,
        }
    }

    pub fn sign(&self) -> Self {
        match self {
            Self::NaN => Self::NaN,
            _ => match self.cmp(&0.into()) {
                std::cmp::Ordering::Less => (-1).into(),
                std::cmp::Ordering::Equal => 0.into(),
                std::cmp::Ordering::Greater => 1.into(),
            },
        }
    }

    pub fn checked_exp(&self) -> Option<LegacyDecimal> {
        match self {
            Self::Normalized(d) => d.checked_exp().map(Self::Normalized),
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::PositiveInf),
            Self::NegativeInf => Some(Self::zero()),
        }
    }

    pub fn checked_ln(&self) -> Option<LegacyDecimal> {
        match self {
            Self::Normalized(d) => d.checked_ln().map(Self::Normalized),
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::PositiveInf),
            Self::NegativeInf => None,
        }
    }

    pub fn checked_log10(&self) -> Option<LegacyDecimal> {
        match self {
            Self::Normalized(d) => d.checked_log10().map(Self::Normalized),
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::PositiveInf),
            Self::NegativeInf => None,
        }
    }

    pub fn checked_sqrt(&self) -> Option<LegacyDecimal> {
        match self {
            Self::Normalized(d) => d.sqrt().map(Self::Normalized),
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::PositiveInf),
            Self::NegativeInf => None,
        }
    }

    pub fn checked_powd(&self, rhs: &Self) -> Result<Self, PowError> {
        use std::cmp::Ordering;

        match (self, rhs) {
            // A. Handle `nan`, where `1 ^ nan == 1` and `nan ^ 0 == 1`
            (LegacyDecimal::NaN, LegacyDecimal::NaN)
            | (LegacyDecimal::PositiveInf, LegacyDecimal::NaN)
            | (LegacyDecimal::NegativeInf, LegacyDecimal::NaN)
            | (LegacyDecimal::NaN, LegacyDecimal::PositiveInf)
            | (LegacyDecimal::NaN, LegacyDecimal::NegativeInf) => Ok(Self::NaN),
            (Normalized(lhs), LegacyDecimal::NaN) => match lhs.is_one() {
                true => Ok(1.into()),
                false => Ok(Self::NaN),
            },
            (LegacyDecimal::NaN, Normalized(rhs)) => match rhs.is_zero() {
                true => Ok(1.into()),
                false => Ok(Self::NaN),
            },

            // B. Handle `b ^ inf`
            (Normalized(lhs), LegacyDecimal::PositiveInf) => match lhs.abs().cmp(&1.into()) {
                Ordering::Greater => Ok(Self::PositiveInf),
                Ordering::Equal => Ok(1.into()),
                Ordering::Less => Ok(0.into()),
            },
            // Simply special case of `abs(b) > 1`.
            // Also consistent with `inf ^ p` and `-inf ^ p` below where p is not fractional or odd.
            (LegacyDecimal::PositiveInf, LegacyDecimal::PositiveInf)
            | (LegacyDecimal::NegativeInf, LegacyDecimal::PositiveInf) => Ok(Self::PositiveInf),

            // C. Handle `b ^ -inf`, which is `(1/b) ^ inf`
            (Normalized(lhs), LegacyDecimal::NegativeInf) => match lhs.abs().cmp(&1.into()) {
                Ordering::Greater => Ok(0.into()),
                Ordering::Equal => Ok(1.into()),
                Ordering::Less => match lhs.is_zero() {
                    // Fun fact: ISO 9899 is removing this error to follow IEEE 754 2008.
                    true => Err(PowError::ZeroNegative),
                    false => Ok(Self::PositiveInf),
                },
            },
            (LegacyDecimal::PositiveInf, LegacyDecimal::NegativeInf)
            | (LegacyDecimal::NegativeInf, LegacyDecimal::NegativeInf) => Ok(0.into()),

            // D. Handle `inf ^ p`
            (LegacyDecimal::PositiveInf, Normalized(rhs)) => match rhs.cmp(&0.into()) {
                Ordering::Greater => Ok(Self::PositiveInf),
                Ordering::Equal => Ok(1.into()),
                Ordering::Less => Ok(0.into()),
            },

            // E. Handle `-inf ^ p`. Finite `p` can be fractional, odd, or even.
            (LegacyDecimal::NegativeInf, Normalized(rhs)) => match !rhs.fract().is_zero() {
                // Err in PostgreSQL. No err in ISO 9899 which treats fractional as non-odd below.
                true => Err(PowError::NegativeFract),
                false => match (rhs.cmp(&0.into()), rhs.rem(&2.into()).abs().is_one()) {
                    (Ordering::Greater, true) => Ok(Self::NegativeInf),
                    (Ordering::Greater, false) => Ok(Self::PositiveInf),
                    (Ordering::Equal, true) => unreachable!(),
                    (Ordering::Equal, false) => Ok(1.into()),
                    (Ordering::Less, true) => Ok(0.into()), // no `-0` in PostgreSQL decimal
                    (Ordering::Less, false) => Ok(0.into()),
                },
            },

            // F. Finite numbers
            (Normalized(lhs), Normalized(rhs)) => {
                if lhs.is_zero() && rhs < &0.into() {
                    return Err(PowError::ZeroNegative);
                }
                if lhs < &0.into() && !rhs.fract().is_zero() {
                    return Err(PowError::NegativeFract);
                }
                match lhs.checked_powd(*rhs) {
                    Some(d) => Ok(Self::Normalized(d)),
                    None => Err(PowError::Overflow),
                }
            }
        }
    }
}

pub enum PowError {
    ZeroNegative,
    NegativeFract,
    Overflow,
}

impl From<LegacyDecimal> for memcomparable::Decimal {
    fn from(d: LegacyDecimal) -> Self {
        match d {
            LegacyDecimal::Normalized(d) => Self::Normalized(d),
            LegacyDecimal::PositiveInf => Self::Inf,
            LegacyDecimal::NegativeInf => Self::NegInf,
            LegacyDecimal::NaN => Self::NaN,
        }
    }
}

impl From<memcomparable::Decimal> for LegacyDecimal {
    fn from(d: memcomparable::Decimal) -> Self {
        match d {
            memcomparable::Decimal::Normalized(d) => Self::Normalized(d),
            memcomparable::Decimal::Inf => Self::PositiveInf,
            memcomparable::Decimal::NegInf => Self::NegativeInf,
            memcomparable::Decimal::NaN => Self::NaN,
        }
    }
}

impl Default for LegacyDecimal {
    fn default() -> Self {
        Self::Normalized(RustDecimal::default())
    }
}

impl FromStr for LegacyDecimal {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "nan" => Ok(LegacyDecimal::NaN),
            "inf" | "+inf" | "infinity" | "+infinity" => Ok(LegacyDecimal::PositiveInf),
            "-inf" | "-infinity" => Ok(LegacyDecimal::NegativeInf),
            s => RustDecimal::from_str(s)
                .or_else(|_| RustDecimal::from_scientific(s))
                .map(LegacyDecimal::Normalized),
        }
    }
}

impl Zero for LegacyDecimal {
    fn zero() -> Self {
        Self::Normalized(RustDecimal::zero())
    }

    fn is_zero(&self) -> bool {
        if let Self::Normalized(d) = self {
            d.is_zero()
        } else {
            false
        }
    }
}

impl One for LegacyDecimal {
    fn one() -> Self {
        Self::Normalized(RustDecimal::one())
    }
}

impl Num for LegacyDecimal {
    type FromStrRadixErr = Error;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        if str.eq_ignore_ascii_case("inf") || str.eq_ignore_ascii_case("infinity") {
            Ok(Self::PositiveInf)
        } else if str.eq_ignore_ascii_case("-inf") || str.eq_ignore_ascii_case("-infinity") {
            Ok(Self::NegativeInf)
        } else if str.eq_ignore_ascii_case("nan") {
            Ok(Self::NaN)
        } else {
            RustDecimal::from_str_radix(str, radix).map(LegacyDecimal::Normalized)
        }
    }
}

impl From<RustDecimal> for LegacyDecimal {
    fn from(d: RustDecimal) -> Self {
        Self::Normalized(d)
    }
}
