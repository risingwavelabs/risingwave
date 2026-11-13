// Copyright 2022 RisingWave Labs
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
    const MAX_I128_REPR: i128 = 0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF;
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

    /// Truncate the given `num` and `scale` to fit into `Decimal`, return `None` if it cannot be
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
        Some(Decimal::Normalized(
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
            s => RustDecimal::from_str(s)
                .or_else(|_| RustDecimal::from_scientific(s))
                .map(Decimal::Normalized),
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
        "#]]
        .assert_eq(&actual);
    }

    #[test]
    fn test_decimal_estimate_size() {
        let decimal = Decimal::NegativeInf;
        assert_eq!(decimal.estimated_size(), 20);

        let decimal = Decimal::Normalized(RustDecimal::try_from(1.0).unwrap());
        assert_eq!(decimal.estimated_size(), 20);
    }
}
