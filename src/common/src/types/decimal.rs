// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};

use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Zero};
pub use rust_decimal::prelude::{FromPrimitive, FromStr, ToPrimitive};
use rust_decimal::{Decimal as RustDecimal, Error, RoundingStrategy};

#[derive(Debug, parse_display::Display, Copy, Clone, PartialEq, Hash, Eq, Ord, PartialOrd)]
pub enum Decimal {
    #[display("{0}")]
    Normalized(RustDecimal),
    #[display("NaN")]
    NaN,
    #[display("+Inf")]
    PositiveInf,
    #[display("-Inf")]
    NegativeInf,
}

macro_rules! impl_from_integer {
    ([$(($T:ty, $from_int:ident)), *]) => {
        $(fn $from_int(num: $T) -> Option<Self> {
            RustDecimal::$from_int(num).map(Decimal::Normalized)
        })*
    }
}

macro_rules! impl_to_integer {
    ([$(($T:ty, $to_int:ident)), *]) => {
        $(fn $to_int(&self) -> Option<$T> {
            match self {
                Self::Normalized(d) => d.$to_int(),
                _ => None,
            }
        })*
    }
}

macro_rules! impl_to_float {
    ([$(($T:ty, $to_float:ident)), *]) => {
        $(fn $to_float(&self) -> Option<$T> {
            match self {
                Self::Normalized(d) => d.$to_float(),
                Self::NaN => Some(<$T>::NAN),
                Self::PositiveInf => Some(<$T>::INFINITY),
                Self::NegativeInf => Some(<$T>::NEG_INFINITY),
            }
        })*
    }
}

macro_rules! impl_from_float {
    ([$(($T:ty, $from_float:ident)), *]) => {
        $(fn $from_float(num: $T) -> Option<Self> {
            match num {
                num if num.is_nan() => Some(Decimal::NaN),
                num if num.is_infinite() && num.is_sign_positive() => Some(Decimal::PositiveInf),
                num if num.is_infinite() && num.is_sign_negative() => Some(Decimal::NegativeInf),
                num => {
                    RustDecimal::$from_float(num).map(Decimal::Normalized)
                },
            }
        })*
    }
}

macro_rules! impl_from {
    ($T:ty, $from_ty:path) => {
        impl core::convert::From<$T> for Decimal {
            #[inline]
            fn from(t: $T) -> Self {
                $from_ty(t).unwrap()
            }
        }
    };
}

macro_rules! impl_try_from_decimal {
    ($from_ty:ty, $to_ty:ty, $convert:path, $err:expr) => {
        impl core::convert::TryFrom<$from_ty> for $to_ty {
            type Error = Error;

            fn try_from(value: $from_ty) -> Result<Self, Self::Error> {
                $convert(&value).ok_or_else(|| Error::from($err))
            }
        }
    };
}

macro_rules! impl_try_from_float {
    ($from_ty:ty, $to_ty:ty, $convert:path, $err:expr) => {
        impl core::convert::TryFrom<$from_ty> for $to_ty {
            type Error = Error;

            fn try_from(value: $from_ty) -> Result<Self, Self::Error> {
                $convert(value).ok_or_else(|| Error::from($err))
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

impl_try_from_decimal!(Decimal, f32, Decimal::to_f32, "Failed to convert to f32");
impl_try_from_decimal!(Decimal, f64, Decimal::to_f64, "Failed to convert to f64");
impl_try_from_float!(
    f32,
    Decimal,
    Decimal::from_f32,
    "Failed to convert to Decimal"
);
impl_try_from_float!(
    f64,
    Decimal,
    Decimal::from_f64,
    "Failed to convert to Decimal"
);

impl FromPrimitive for Decimal {
    impl_from_integer!([
        (u8, from_u8),
        (u16, from_u16),
        (u32, from_u32),
        (u64, from_u64),
        (i8, from_i8),
        (i16, from_i16),
        (i32, from_i32),
        (i64, from_i64)
    ]);

    impl_from_float!([(f32, from_f32), (f64, from_f64)]);
}

impl_from!(isize, FromPrimitive::from_isize);
impl_from!(i8, FromPrimitive::from_i8);
impl_from!(i16, FromPrimitive::from_i16);
impl_from!(i32, FromPrimitive::from_i32);
impl_from!(i64, FromPrimitive::from_i64);
impl_from!(usize, FromPrimitive::from_usize);
impl_from!(u8, FromPrimitive::from_u8);
impl_from!(u16, FromPrimitive::from_u16);
impl_from!(u32, FromPrimitive::from_u32);
impl_from!(u64, FromPrimitive::from_u64);

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
    pub fn round_dp(&self, dp: u32) -> Self {
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
            Self::Normalized(d) => Self::Normalized(d.ceil()),
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
    pub fn round(&self) -> Self {
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

    /// TODO: 1. test whether the decimal in rust, any crate, has the same behavior as PG.
    /// 2. support memcomparable encoding for dynamic decimal.
    pub fn mantissa_scale_for_serialization(&self) -> (i128, u8) {
        // Since the largest scale supported by `rust_decimal` is 28,
        // and we first compare scale, we use 29 and 30 to denote +Inf and NaN.
        match self {
            Self::NegativeInf => (0, 29),
            Self::Normalized(d) => {
                // We remark that we do not dynamic numeric, i.e. the scale of all the numeric in
                // the system is fixed. So we don't need to do any rescale, just use
                // the `scale` of `rust_decimal`. However, it is possible that scale
                // may overflow during calculation as `rust_decimal`'s max scale is
                // 28.
                (d.mantissa(), d.scale() as u8)
            }
            Self::PositiveInf => (0, 30),
            Self::NaN => (0, 31),
        }
    }

    pub fn unordered_serialize(&self) -> [u8; 16] {
        // according to https://docs.rs/rust_decimal/1.18.0/src/rust_decimal/decimal.rs.html#665-684
        // the lower 15 bits is not used, so we can use first byte to distinguish nan and inf
        match self {
            Self::Normalized(d) => d.serialize(),
            Self::NaN => [vec![1u8], vec![0u8; 15]].concat().try_into().unwrap(),
            Self::PositiveInf => [vec![2u8], vec![0u8; 15]].concat().try_into().unwrap(),
            Self::NegativeInf => [vec![3u8], vec![0u8; 15]].concat().try_into().unwrap(),
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

    pub fn abs(&self) -> Option<Self> {
        match self {
            Self::Normalized(d) => {
                if d.is_sign_negative() {
                    Some(Self::Normalized(-d))
                } else {
                    Some(Self::Normalized(*d))
                }
            }
            Self::NaN => Some(Self::NaN),
            Self::PositiveInf => Some(Self::PositiveInf),
            Self::NegativeInf => Some(Self::PositiveInf),
        }
    }
}

impl Default for Decimal {
    fn default() -> Self {
        Self::Normalized(RustDecimal::default())
    }
}

impl ToPrimitive for Decimal {
    impl_to_integer!([
        (i64, to_i64),
        (i32, to_i32),
        (i16, to_i16),
        (i8, to_i8),
        (u64, to_u64),
        (u32, to_u32),
        (u16, to_u16),
        (u8, to_u8)
    ]);

    impl_to_float!([(f64, to_f64), (f32, to_f32)]);
}

impl FromStr for Decimal {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "nan" | "NaN" | "NAN" => Ok(Decimal::NaN),
            "inf" | "INF" | "+inf" | "+INF" | "+Inf" => Ok(Decimal::PositiveInf),
            "-inf" | "-INF" | "-Inf" => Ok(Decimal::NegativeInf),
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

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

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
            Decimal::from_f32(1.0).unwrap(),
            Decimal::from_f32(-1.0).unwrap(),
            Decimal::from_f32(0.0).unwrap(),
        ];
        let floats = [
            f32::NAN,
            f32::INFINITY,
            f32::NEG_INFINITY,
            1.0f32,
            -1.0f32,
            0.0f32,
        ];
        for (d_lhs, f_lhs) in decimals.iter().zip_eq(floats.iter()) {
            for (d_rhs, f_rhs) in decimals.iter().zip_eq(floats.iter()) {
                assert!(check((*d_lhs + *d_rhs).to_f32().unwrap(), f_lhs + f_rhs));
                assert!(check((*d_lhs - *d_rhs).to_f32().unwrap(), f_lhs - f_rhs));
                assert!(check((*d_lhs * *d_rhs).to_f32().unwrap(), f_lhs * f_rhs));
                assert!(check((*d_lhs / *d_rhs).to_f32().unwrap(), f_lhs / f_rhs));
                assert!(check((*d_lhs % *d_rhs).to_f32().unwrap(), f_lhs % f_rhs));
            }
        }
    }

    #[test]
    fn basic_test() {
        assert_eq!(Decimal::from_str("nan").unwrap(), Decimal::NaN,);
        assert_eq!(Decimal::from_str("NaN").unwrap(), Decimal::NaN,);
        assert_eq!(Decimal::from_str("NAN").unwrap(), Decimal::NaN,);

        assert_eq!(Decimal::from_str("inf").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("INF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+inf").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+INF").unwrap(), Decimal::PositiveInf,);
        assert_eq!(Decimal::from_str("+Inf").unwrap(), Decimal::PositiveInf,);

        assert_eq!(Decimal::from_str("-inf").unwrap(), Decimal::NegativeInf,);
        assert_eq!(Decimal::from_str("-INF").unwrap(), Decimal::NegativeInf,);
        assert_eq!(Decimal::from_str("-Inf").unwrap(), Decimal::NegativeInf,);

        assert!(Decimal::from_str("nAn").is_err());
        assert!(Decimal::from_str("nAN").is_err());
        assert!(Decimal::from_str("Nan").is_err());
        assert!(Decimal::from_str("NAn").is_err());

        assert!(Decimal::from_str("iNF").is_err());
        assert!(Decimal::from_str("inF").is_err());
        assert!(Decimal::from_str("InF").is_err());
        assert!(Decimal::from_str("INf").is_err());

        assert!(Decimal::from_str("+iNF").is_err());
        assert!(Decimal::from_str("+inF").is_err());
        assert!(Decimal::from_str("+InF").is_err());
        assert!(Decimal::from_str("+INf").is_err());

        assert!(Decimal::from_str("-iNF").is_err());
        assert!(Decimal::from_str("-inF").is_err());
        assert!(Decimal::from_str("-InF").is_err());
        assert!(Decimal::from_str("-INf").is_err());

        assert_eq!(
            Decimal::from_f32(10.0).unwrap() / Decimal::PositiveInf,
            Decimal::from_f32(0.0).unwrap(),
        );
        assert_eq!(
            Decimal::from_f32(f32::INFINITY).unwrap(),
            Decimal::PositiveInf
        );
        assert_eq!(Decimal::from_f64(f64::NAN).unwrap(), Decimal::NaN);
        assert_eq!(
            Decimal::from_f64(f64::INFINITY).unwrap(),
            Decimal::PositiveInf
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from_f64(1.234).unwrap().unordered_serialize()),
            Decimal::from_f64(1.234).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from_u8(1).unwrap().unordered_serialize()),
            Decimal::from_u8(1).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from_i8(1).unwrap().unordered_serialize()),
            Decimal::from_i8(1).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from_u16(1).unwrap().unordered_serialize()),
            Decimal::from_u16(1).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from_i16(1).unwrap().unordered_serialize()),
            Decimal::from_i16(1).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from_u32(1).unwrap().unordered_serialize()),
            Decimal::from_u32(1).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(Decimal::from_i32(1).unwrap().unordered_serialize()),
            Decimal::from_i32(1).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::from_f64(f64::NAN).unwrap().unordered_serialize()
            ),
            Decimal::from_f64(f64::NAN).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::from_f64(f64::INFINITY)
                    .unwrap()
                    .unordered_serialize()
            ),
            Decimal::from_f64(f64::INFINITY).unwrap(),
        );
        assert_eq!(Decimal::to_u8(&Decimal::from_u8(1).unwrap()).unwrap(), 1,);
        assert_eq!(Decimal::to_i8(&Decimal::from_i8(1).unwrap()).unwrap(), 1,);
        assert_eq!(Decimal::to_u16(&Decimal::from_u16(1).unwrap()).unwrap(), 1,);
        assert_eq!(Decimal::to_i16(&Decimal::from_i16(1).unwrap()).unwrap(), 1,);
        assert_eq!(Decimal::to_u32(&Decimal::from_u32(1).unwrap()).unwrap(), 1,);
        assert_eq!(Decimal::to_i32(&Decimal::from_i32(1).unwrap()).unwrap(), 1,);
        assert_eq!(Decimal::to_u64(&Decimal::from_u64(1).unwrap()).unwrap(), 1,);
        assert_eq!(Decimal::to_i64(&Decimal::from_i64(1).unwrap()).unwrap(), 1,);
    }
}
