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

use std::fmt::Debug;
use std::io::{Cursor, Read, Write};
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use num_traits::{
    CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Num, One, Zero,
};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use risingwave_common_estimate_size::ZeroHeapSize;
use rust_decimal::prelude::FromStr;
use rust_decimal::{Decimal as RustDecimal, Error, MathematicalOps as _, RoundingStrategy};

use super::DataType;
use super::to_text::ToText;
use crate::array::ArrayResult;
use crate::types::Decimal::Normalized;
use crate::types::ordered_float::OrderedFloat;

#[derive(Debug, Copy, parse_display::Display, Clone, PartialEq, Hash, Eq, Ord, PartialOrd)]
pub enum Decimal {
    #[display("-Infinity")]
    NegativeInf,
    #[display("{0}")]
    Normalized(RustDecimal),
    #[display("Infinity")]
    PositiveInf,
    #[display("NaN")]
    NaN,
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

impl Decimal {
    /// Used by `PrimitiveArray` to serialize the array to protobuf.
    pub fn to_protobuf(self, output: &mut impl Write) -> ArrayResult<usize> {
        let buf = self.unordered_serialize();
        output.write_all(&buf)?;
        Ok(buf.len())
    }

    /// Used by `DecimalValueReader` to deserialize the array from protobuf.
    pub fn from_protobuf(input: &mut impl Read) -> ArrayResult<Self> {
        let mut buf = [0u8; 16];
        input.read_exact(&mut buf)?;
        Ok(Self::unordered_deserialize(buf))
    }

    pub fn from_scientific(value: &str) -> Option<Self> {
        let decimal = RustDecimal::from_scientific(value).ok()?;
        Some(Normalized(decimal))
    }

    pub fn from_str_radix(s: &str, radix: u32) -> rust_decimal::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "nan" => Ok(Decimal::NaN),
            "inf" | "+inf" | "infinity" | "+infinity" => Ok(Decimal::PositiveInf),
            "-inf" | "-infinity" => Ok(Decimal::NegativeInf),
            s => RustDecimal::from_str_radix(s, radix).map(Decimal::Normalized),
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
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        match self {
            Decimal::Normalized(d) => {
                return d.to_sql(ty, out);
            }
            Decimal::NaN => {
                out.reserve(8);
                out.put_u16(0);
                out.put_i16(0);
                out.put_u16(0xC000);
                out.put_i16(0);
            }
            Decimal::PositiveInf => {
                out.reserve(8);
                out.put_u16(0);
                out.put_i16(0);
                out.put_u16(0xD000);
                out.put_i16(0);
            }
            Decimal::NegativeInf => {
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

impl<'a> FromSql<'a> for Decimal {
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
        impl core::convert::From<$T> for Decimal {
            #[inline]
            fn from(t: $T) -> Self {
                Self::Normalized(t.into())
            }
        }

        impl core::convert::TryFrom<Decimal> for $T {
            type Error = Error;

            #[inline]
            fn try_from(d: Decimal) -> Result<Self, Self::Error> {
                match d.round_dp_ties_away(0) {
                    Decimal::Normalized(d) => d.try_into(),
                    _ => Err(Error::ConversionTo(std::any::type_name::<$T>().into())),
                }
            }
        }
    };
}

macro_rules! impl_convert_float {
    ($T:ty) => {
        impl core::convert::TryFrom<$T> for Decimal {
            type Error = Error;

            fn try_from(num: $T) -> Result<Self, Self::Error> {
                match num {
                    num if num.is_nan() => Ok(Decimal::NaN),
                    num if num.is_infinite() && num.is_sign_positive() => Ok(Decimal::PositiveInf),
                    num if num.is_infinite() && num.is_sign_negative() => Ok(Decimal::NegativeInf),
                    num => num.try_into().map(Decimal::Normalized),
                }
            }
        }
        impl core::convert::TryFrom<OrderedFloat<$T>> for Decimal {
            type Error = Error;

            fn try_from(value: OrderedFloat<$T>) -> Result<Self, Self::Error> {
                value.0.try_into()
            }
        }

        impl core::convert::TryFrom<Decimal> for $T {
            type Error = Error;

            fn try_from(d: Decimal) -> Result<Self, Self::Error> {
                match d {
                    Decimal::Normalized(d) => d.try_into(),
                    Decimal::NaN => Ok(<$T>::NAN),
                    Decimal::PositiveInf => Ok(<$T>::INFINITY),
                    Decimal::NegativeInf => Ok(<$T>::NEG_INFINITY),
                }
            }
        }
        impl core::convert::TryFrom<Decimal> for OrderedFloat<$T> {
            type Error = Error;

            fn try_from(d: Decimal) -> Result<Self, Self::Error> {
                d.try_into().map(Self)
            }
        }
    };
}

macro_rules! checked_proxy {
    ($trait:ty, $func:ident, $op: tt) => {
        impl $trait for Decimal {
            fn $func(&self, other: &Self) -> Option<Self> {
                match (self, other) {
                    (Self::Normalized(lhs), Self::Normalized(rhs)) => {
                        lhs.$func(rhs).map(Decimal::Normalized)
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

impl Add for Decimal {
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

impl Neg for Decimal {
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

impl CheckedNeg for Decimal {
    fn checked_neg(&self) -> Option<Self> {
        match self {
            Self::Normalized(d) => Some(Self::Normalized(-d)),
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::NegativeInf),
            Self::NegativeInf => Some(Self::PositiveInf),
        }
    }
}

impl Rem for Decimal {
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

impl Div for Decimal {
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

impl Mul for Decimal {
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

impl Sub for Decimal {
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

impl Decimal {
    pub const MAX_PRECISION: u8 = 28;

    pub fn scale(&self) -> Option<i32> {
        let Decimal::Normalized(d) = self else {
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
        const MANTISSA_UP: i128 = 5 * 10i128.pow(Decimal::MAX_PRECISION as _);
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
        match left > Decimal::MAX_PRECISION.into() {
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
        Decimal::Normalized(RustDecimal::from_i128_with_scale(num, scale))
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

    pub fn checked_exp(&self) -> Option<Decimal> {
        match self {
            Self::Normalized(d) => d.checked_exp().map(Self::Normalized),
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::PositiveInf),
            Self::NegativeInf => Some(Self::zero()),
        }
    }

    pub fn checked_ln(&self) -> Option<Decimal> {
        match self {
            Self::Normalized(d) => d.checked_ln().map(Self::Normalized),
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::PositiveInf),
            Self::NegativeInf => None,
        }
    }

    pub fn checked_log10(&self) -> Option<Decimal> {
        match self {
            Self::Normalized(d) => d.checked_log10().map(Self::Normalized),
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::PositiveInf),
            Self::NegativeInf => None,
        }
    }

    pub fn checked_powd(&self, rhs: &Self) -> Result<Self, PowError> {
        use std::cmp::Ordering;

        match (self, rhs) {
            // A. Handle `nan`, where `1 ^ nan == 1` and `nan ^ 0 == 1`
            (Decimal::NaN, Decimal::NaN)
            | (Decimal::PositiveInf, Decimal::NaN)
            | (Decimal::NegativeInf, Decimal::NaN)
            | (Decimal::NaN, Decimal::PositiveInf)
            | (Decimal::NaN, Decimal::NegativeInf) => Ok(Self::NaN),
            (Normalized(lhs), Decimal::NaN) => match lhs.is_one() {
                true => Ok(1.into()),
                false => Ok(Self::NaN),
            },
            (Decimal::NaN, Normalized(rhs)) => match rhs.is_zero() {
                true => Ok(1.into()),
                false => Ok(Self::NaN),
            },

            // B. Handle `b ^ inf`
            (Normalized(lhs), Decimal::PositiveInf) => match lhs.abs().cmp(&1.into()) {
                Ordering::Greater => Ok(Self::PositiveInf),
                Ordering::Equal => Ok(1.into()),
                Ordering::Less => Ok(0.into()),
            },
            // Simply special case of `abs(b) > 1`.
            // Also consistent with `inf ^ p` and `-inf ^ p` below where p is not fractional or odd.
            (Decimal::PositiveInf, Decimal::PositiveInf)
            | (Decimal::NegativeInf, Decimal::PositiveInf) => Ok(Self::PositiveInf),

            // C. Handle `b ^ -inf`, which is `(1/b) ^ inf`
            (Normalized(lhs), Decimal::NegativeInf) => match lhs.abs().cmp(&1.into()) {
                Ordering::Greater => Ok(0.into()),
                Ordering::Equal => Ok(1.into()),
                Ordering::Less => match lhs.is_zero() {
                    // Fun fact: ISO 9899 is removing this error to follow IEEE 754 2008.
                    true => Err(PowError::ZeroNegative),
                    false => Ok(Self::PositiveInf),
                },
            },
            (Decimal::PositiveInf, Decimal::NegativeInf)
            | (Decimal::NegativeInf, Decimal::NegativeInf) => Ok(0.into()),

            // D. Handle `inf ^ p`
            (Decimal::PositiveInf, Normalized(rhs)) => match rhs.cmp(&0.into()) {
                Ordering::Greater => Ok(Self::PositiveInf),
                Ordering::Equal => Ok(1.into()),
                Ordering::Less => Ok(0.into()),
            },

            // E. Handle `-inf ^ p`. Finite `p` can be fractional, odd, or even.
            (Decimal::NegativeInf, Normalized(rhs)) => match !rhs.fract().is_zero() {
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

impl From<Decimal> for memcomparable::Decimal {
    fn from(d: Decimal) -> Self {
        match d {
            Decimal::Normalized(d) => Self::Normalized(d),
            Decimal::PositiveInf => Self::Inf,
            Decimal::NegativeInf => Self::NegInf,
            Decimal::NaN => Self::NaN,
        }
    }
}

impl From<memcomparable::Decimal> for Decimal {
    fn from(d: memcomparable::Decimal) -> Self {
        match d {
            memcomparable::Decimal::Normalized(d) => Self::Normalized(d),
            memcomparable::Decimal::Inf => Self::PositiveInf,
            memcomparable::Decimal::NegInf => Self::NegativeInf,
            memcomparable::Decimal::NaN => Self::NaN,
        }
    }
}

impl Default for Decimal {
    fn default() -> Self {
        Self::Normalized(RustDecimal::default())
    }
}

impl FromStr for Decimal {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "nan" => Ok(Decimal::NaN),
            "inf" | "+inf" | "infinity" | "+infinity" => Ok(Decimal::PositiveInf),
            "-inf" | "-infinity" => Ok(Decimal::NegativeInf),
            s => RustDecimal::from_str(s).map(Decimal::Normalized),
        }
    }
}

impl Zero for Decimal {
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

impl One for Decimal {
    fn one() -> Self {
        Self::Normalized(RustDecimal::one())
    }
}

impl Num for Decimal {
    type FromStrRadixErr = Error;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        if str.eq_ignore_ascii_case("inf") || str.eq_ignore_ascii_case("infinity") {
            Ok(Self::PositiveInf)
        } else if str.eq_ignore_ascii_case("-inf") || str.eq_ignore_ascii_case("-infinity") {
            Ok(Self::NegativeInf)
        } else if str.eq_ignore_ascii_case("nan") {
            Ok(Self::NaN)
        } else {
            RustDecimal::from_str_radix(str, radix).map(Decimal::Normalized)
        }
    }
}

impl From<RustDecimal> for Decimal {
    fn from(d: RustDecimal) -> Self {
        Self::Normalized(d)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools as _;
    use risingwave_common_estimate_size::EstimateSize;

    use super::*;
    use crate::util::iter_util::ZipEqFast;

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
            Decimal::NaN,
            Decimal::PositiveInf,
            Decimal::NegativeInf,
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
        assert_eq!(Decimal::from_str("nan").unwrap(), Decimal::NaN,);
        assert_eq!(Decimal::from_str("NaN").unwrap(), Decimal::NaN,);
        assert_eq!(Decimal::from_str("NAN").unwrap(), Decimal::NaN,);
        assert_eq!(Decimal::from_str("nAn").unwrap(), Decimal::NaN,);
        assert_eq!(Decimal::from_str("nAN").unwrap(), Decimal::NaN,);
        assert_eq!(Decimal::from_str("Nan").unwrap(), Decimal::NaN,);
        assert_eq!(Decimal::from_str("NAn").unwrap(), Decimal::NaN,);

        assert_eq!(Decimal::from_str("inf").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("INF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("iNF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("inF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("InF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("INf").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+inf").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+INF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+Inf").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+iNF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+inF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+InF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+INf").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("inFINity").unwrap(), Decimal::PositiveInf,);
        assert_eq!(
            Decimal::from_str("+infiNIty").unwrap(),
            Decimal::PositiveInf,
        );

        assert_eq!(Decimal::from_str("-inf").unwrap(), Decimal::NegativeInf,);
        assert_eq!(Decimal::from_str("-INF").unwrap(), Decimal::NegativeInf,);
        assert_eq!(Decimal::from_str("-Inf").unwrap(), Decimal::NegativeInf,);
        assert_eq!(Decimal::from_str("-iNF").unwrap(), Decimal::NegativeInf,);
        assert_eq!(Decimal::from_str("-inF").unwrap(), Decimal::NegativeInf,);
        assert_eq!(Decimal::from_str("-InF").unwrap(), Decimal::NegativeInf,);
        assert_eq!(Decimal::from_str("-INf").unwrap(), Decimal::NegativeInf,);
        assert_eq!(
            Decimal::from_str("-INfinity").unwrap(),
            Decimal::NegativeInf,
        );

        assert_eq!(
            Decimal::try_from(10.0).unwrap() / Decimal::PositiveInf,
            Decimal::try_from(0.0).unwrap(),
        );
        assert_eq!(
            Decimal::try_from(f32::INFINITY).unwrap(),
            Decimal::PositiveInf
        );
        assert_eq!(Decimal::try_from(f64::NAN).unwrap(), Decimal::NaN);
        assert_eq!(
            Decimal::try_from(f64::INFINITY).unwrap(),
            Decimal::PositiveInf
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::try_from(1.234).unwrap().unordered_serialize()),
            Decimal::try_from(1.234).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from(1u8).unordered_serialize()),
            Decimal::from(1u8),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from(1i8).unordered_serialize()),
            Decimal::from(1i8),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from(1u16).unordered_serialize()),
            Decimal::from(1u16),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from(1i16).unordered_serialize()),
            Decimal::from(1i16),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from(1u32).unordered_serialize()),
            Decimal::from(1u32),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from(1i32).unordered_serialize()),
            Decimal::from(1i32),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::try_from(f64::NAN).unwrap().unordered_serialize()
            ),
            Decimal::try_from(f64::NAN).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::try_from(f64::INFINITY)
                    .unwrap()
                    .unordered_serialize()
            ),
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
        let ordered = ["-inf", "-1", "0.00", "0.5", "2", "10", "inf", "nan"]
            .iter()
            .map(|s| Decimal::from_str(s).unwrap())
            .collect_vec();
        for i in 1..ordered.len() {
            assert!(ordered[i - 1] < ordered[i]);
            assert!(
                memcomparable::Decimal::from(ordered[i - 1])
                    < memcomparable::Decimal::from(ordered[i])
            );
        }
    }

    #[test]
    fn test_decimal_estimate_size() {
        let decimal = Decimal::NegativeInf;
        assert_eq!(decimal.estimated_size(), 20);

        let decimal = Decimal::Normalized(RustDecimal::try_from(1.0).unwrap());
        assert_eq!(decimal.estimated_size(), 20);
    }
}
