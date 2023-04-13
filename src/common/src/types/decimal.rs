// Copyright 2023 RisingWave Labs
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
use std::io::{Read, Write};
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use bytes::{BufMut, Bytes, BytesMut};
use num_traits::{
    CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, One, Zero,
};
use postgres_types::{ToSql, Type};
use risingwave_common_proc_macro::EstimateSize;
use rust_decimal::prelude::FromStr;
use rust_decimal::{Decimal as RustDecimal, Error, RoundingStrategy};

use super::to_binary::ToBinary;
use super::to_text::ToText;
use super::DataType;
use crate::array::ArrayResult;
use crate::error::Result as RwResult;
use crate::estimate_size::EstimateSize;
use crate::types::ordered_float::OrderedFloat;
use crate::types::Decimal::Normalized;

#[derive(
    Debug, Copy, parse_display::Display, Clone, PartialEq, Hash, Eq, Ord, PartialOrd, EstimateSize,
)]
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
}

impl ToBinary for Decimal {
    fn to_binary_with_type(&self, ty: &DataType) -> RwResult<Option<Bytes>> {
        match ty {
            DataType::Decimal => {
                let mut output = BytesMut::new();
                match self {
                    Decimal::Normalized(d) => {
                        d.to_sql(&Type::ANY, &mut output).unwrap();
                        return Ok(Some(output.freeze()));
                    }
                    Decimal::NaN => {
                        output.reserve(8);
                        output.put_u16(0);
                        output.put_i16(0);
                        output.put_u16(0xC000);
                        output.put_i16(0);
                    }
                    Decimal::PositiveInf => {
                        output.reserve(8);
                        output.put_u16(0);
                        output.put_i16(0);
                        output.put_u16(0xD000);
                        output.put_i16(0);
                    }
                    Decimal::NegativeInf => {
                        output.reserve(8);
                        output.put_u16(0);
                        output.put_i16(0);
                        output.put_u16(0xF000);
                        output.put_i16(0);
                    }
                };
                Ok(Some(output.freeze()))
            }
            _ => unreachable!(),
        }
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
    /// TODO: handle nan and inf
    pub fn mantissa(&self) -> i128 {
        match self {
            Self::Normalized(d) => d.mantissa(),
            _ => 0,
        }
    }

    /// TODO: handle nan and inf
    pub fn scale(&self) -> i32 {
        match self {
            Self::Normalized(d) => d.scale() as i32,
            _ => 0,
        }
    }

    /// TODO: handle nan and inf
    /// There are maybe better solution here.
    pub fn precision(&self) -> u32 {
        match &self {
            Decimal::Normalized(decimal) => {
                let s = decimal.to_string();
                let mut res = s.len();
                if s.find('.').is_some() {
                    res -= 1;
                }
                if s.find('-').is_some() {
                    res -= 1;
                }
                res as u32
            }
            _ => 0,
        }
    }

    pub fn new(num: i64, scale: u32) -> Self {
        Self::Normalized(RustDecimal::new(num, scale))
    }

    pub fn zero() -> Self {
        Self::from(0)
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

impl From<RustDecimal> for Decimal {
    fn from(d: RustDecimal) -> Self {
        Self::Normalized(d)
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools as _;

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
