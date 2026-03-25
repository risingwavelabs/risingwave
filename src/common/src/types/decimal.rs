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

use bigdecimal::BigDecimal;
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use num_traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedRem, CheckedSub, One, Zero};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use risingwave_common_estimate_size::ZeroHeapSize;
use rust_decimal::prelude::FromStr;
use rust_decimal::{Decimal as RustDecimal, Error, MathematicalOps as _, RoundingStrategy};

use super::to_text::ToText;
use super::{CheckedNeg, DataType};
use crate::array::ArrayResult;
use crate::types::Decimal::Normalized;
use crate::types::ordered_float::OrderedFloat;
use crate::types::{IsNegative, Scalar as _};

#[derive(Clone, PartialEq, Hash, Eq, Ord, PartialOrd)]
pub enum Decimal {
    NegativeInf,
    Normalized(Box<BigDecimal>),
    PositiveInf,
    NaN,
}

impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Decimal::NegativeInf => write!(f, "-Infinity"),
            Decimal::Normalized(d) => d.write_plain_string(f),
            Decimal::PositiveInf => write!(f, "Infinity"),
            Decimal::NaN => write!(f, "NaN"),
        }
    }
}

impl std::fmt::Debug for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Decimal::NegativeInf => write!(f, "-Infinity"),
            Decimal::Normalized(d) => {
                write!(f, "Normalized(")?;
                d.write_plain_string(f)?;
                write!(f, ")")
            }
            Decimal::PositiveInf => write!(f, "Infinity"),
            Decimal::NaN => write!(f, "NaN"),
        }
    }
}

#[derive(Copy, parse_display::Display, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
#[display("{0}")]
pub struct DeciRef<'a>(&'a Decimal);

impl Debug for DeciRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<DeciRef<'_>> for Decimal {
    fn from(d: DeciRef<'_>) -> Self {
        d.xxd()
    }
}

impl DeciRef<'_> {
    pub fn xxd(self) -> Decimal {
        self.0.clone()
    }
}
impl Decimal {
    pub fn dxx(&self) -> DeciRef<'_> {
        DeciRef(self)
    }
}
#[easy_ext::ext]
impl RustDecimal {
    fn zq(&self) -> BigDecimal {
        BigDecimal::from_str(&self.to_string()).unwrap()
    }
}
#[easy_ext::ext]
impl BigDecimal {
    fn qz(&self) -> RustDecimal {
        RustDecimal::from_str(&self.to_plain_string()).unwrap()
    }
}
impl Decimal {
    pub fn normalized(d: BigDecimal) -> Self {
        Self::Normalized(Box::new(d))
    }

    pub fn normalizeq(d: RustDecimal) -> Self {
        Self::Normalized(Box::new(d.zq()))
    }

    pub fn qz(d: &BigDecimal) -> RustDecimal {
        d.qz()
    }
}

impl ZeroHeapSize for Decimal {}

impl ToText for DeciRef<'_> {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.xxd())
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
    pub fn to_protobuf(&self, output: &mut impl Write) -> ArrayResult<usize> {
        let buf = self.dxx().unordered_serialize();
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
        Some(Decimal::normalizeq(decimal))
    }

    pub fn from_str_radix(s: &str, radix: u32) -> rust_decimal::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "nan" => Ok(Decimal::NaN),
            "inf" | "+inf" | "infinity" | "+infinity" => Ok(Decimal::PositiveInf),
            "-inf" | "-infinity" => Ok(Decimal::NegativeInf),
            s => RustDecimal::from_str_radix(s, radix).map(Decimal::normalizeq),
        }
    }
}

impl ToSql for DeciRef<'_> {
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
        match self.0 {
            Decimal::Normalized(d) => {
                return d.qz().to_sql(ty, out);
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
            _ => RustDecimal::from_sql(ty, raw).map(Self::normalizeq),
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
                Self::normalizeq(t.into())
            }
        }

        impl core::convert::TryFrom<Decimal> for $T {
            type Error = Error;

            #[inline]
            fn try_from(d: Decimal) -> Result<Self, Self::Error> {
                match d.dxx().round_dp_ties_away(0) {
                    Decimal::Normalized(d) => d.qz().try_into(),
                    _ => Err(Error::ConversionTo(std::any::type_name::<$T>().into())),
                }
            }
        }
        impl core::convert::TryFrom<DeciRef<'_>> for $T {
            type Error = Error;

            #[inline]
            fn try_from(d: DeciRef<'_>) -> Result<Self, Self::Error> {
                match d.round_dp_ties_away(0) {
                    Decimal::Normalized(d) => d.qz().try_into(),
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
                    num => num.try_into().map(Decimal::normalizeq),
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
                    Decimal::Normalized(d) => d.qz().try_into(),
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
        impl core::convert::TryFrom<DeciRef<'_>> for OrderedFloat<$T> {
            type Error = Error;

            fn try_from(d: DeciRef<'_>) -> Result<Self, Self::Error> {
                d.xxd().try_into().map(Self)
            }
        }
    };
}

macro_rules! checked_proxy {
    ($trait:ty, $func:ident, $op: tt, $t2:ty, $f2:ident) => {
        impl $trait for Decimal {
            fn $func(&self, other: &Self) -> Option<Self> {
                match (self, other) {
                    (Self::Normalized(lhs), Self::Normalized(rhs)) => {
                        lhs.qz().$func(rhs.qz()).map(Decimal::normalizeq)
                    }
                    (lhs, rhs) => Some(lhs.as_scalar_ref() $op rhs.as_scalar_ref()),
                }
            }
        }
        impl $t2 for Decimal {
            type Output = Self;

            fn $f2(self, other: Self) -> Self {
                self.as_scalar_ref() $op other.as_scalar_ref()
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

checked_proxy!(CheckedRem, checked_rem, %, Rem, rem);
checked_proxy!(CheckedSub, checked_sub, -, Sub, sub);
checked_proxy!(CheckedAdd, checked_add, +, Add, add);
checked_proxy!(CheckedDiv, checked_div, /, Div, div);
checked_proxy!(CheckedMul, checked_mul, *, Mul, mul);

impl Add for DeciRef<'_> {
    type Output = Decimal;

    fn add(self, other: Self) -> Self::Output {
        match (self.0, other.0) {
            (Decimal::Normalized(lhs), Decimal::Normalized(rhs)) => {
                Decimal::normalized(lhs.as_ref() + rhs.as_ref())
            }
            (Decimal::NaN, _) => Decimal::NaN,
            (_, Decimal::NaN) => Decimal::NaN,
            (Decimal::PositiveInf, Decimal::NegativeInf) => Decimal::NaN,
            (Decimal::NegativeInf, Decimal::PositiveInf) => Decimal::NaN,
            (Decimal::PositiveInf, _) => Decimal::PositiveInf,
            (_, Decimal::PositiveInf) => Decimal::PositiveInf,
            (Decimal::NegativeInf, _) => Decimal::NegativeInf,
            (_, Decimal::NegativeInf) => Decimal::NegativeInf,
        }
    }
}

impl Neg for Decimal {
    type Output = Self;

    fn neg(self) -> Self {
        match self {
            Self::Normalized(d) => Self::normalized(-d.as_ref()),
            Self::NaN => Self::NaN,
            Self::PositiveInf => Self::NegativeInf,
            Self::NegativeInf => Self::PositiveInf,
        }
    }
}

impl CheckedNeg for DeciRef<'_> {
    type Output = Decimal;

    fn checked_neg(self) -> Option<Self::Output> {
        match self.xxd() {
            Decimal::Normalized(d) => Some(Decimal::normalized(-d.as_ref())),
            Decimal::NaN => Some(Decimal::NaN),
            Decimal::PositiveInf => Some(Decimal::NegativeInf),
            Decimal::NegativeInf => Some(Decimal::PositiveInf),
        }
    }
}

impl Rem for DeciRef<'_> {
    type Output = Decimal;

    fn rem(self, other: Self) -> Self::Output {
        match (self.0, other.0) {
            (Decimal::Normalized(lhs), Decimal::Normalized(rhs)) if !rhs.is_zero() => {
                Decimal::normalized(lhs.as_ref() % rhs.as_ref())
            }
            (Decimal::Normalized(_), Decimal::Normalized(_)) => Decimal::NaN,
            (Decimal::Normalized(lhs), Decimal::PositiveInf)
                if lhs.qz().is_sign_positive() || lhs.is_zero() =>
            {
                Decimal::Normalized(lhs.clone())
            }
            (Decimal::Normalized(d), Decimal::PositiveInf) => Decimal::Normalized(d.clone()),
            (Decimal::Normalized(lhs), Decimal::NegativeInf)
                if lhs.qz().is_sign_negative() || lhs.is_zero() =>
            {
                Decimal::Normalized(lhs.clone())
            }
            (Decimal::Normalized(d), Decimal::NegativeInf) => Decimal::Normalized(d.clone()),
            _ => Decimal::NaN,
        }
    }
}

impl Div for DeciRef<'_> {
    type Output = Decimal;

    fn div(self, other: Self) -> Self::Output {
        match (self.0, other.0) {
            // nan
            (Decimal::NaN, _) => Decimal::NaN,
            (_, Decimal::NaN) => Decimal::NaN,
            // div by zero
            (lhs, Decimal::Normalized(rhs)) if rhs.is_zero() => match lhs {
                Decimal::Normalized(lhs) => {
                    if lhs.qz().is_sign_positive() && !lhs.is_zero() {
                        Decimal::PositiveInf
                    } else if lhs.qz().is_sign_negative() && !lhs.is_zero() {
                        Decimal::NegativeInf
                    } else {
                        Decimal::NaN
                    }
                }
                Decimal::PositiveInf => Decimal::PositiveInf,
                Decimal::NegativeInf => Decimal::NegativeInf,
                _ => unreachable!(),
            },
            // div by +/-inf
            (Decimal::Normalized(_), Decimal::PositiveInf) => {
                Decimal::normalized(BigDecimal::from(0))
            }
            (_, Decimal::PositiveInf) => Decimal::NaN,
            (Decimal::Normalized(_), Decimal::NegativeInf) => {
                Decimal::normalized(BigDecimal::from(0))
            }
            (_, Decimal::NegativeInf) => Decimal::NaN,
            // div inf
            (Decimal::PositiveInf, Decimal::Normalized(d)) if d.qz().is_sign_positive() => {
                Decimal::PositiveInf
            }
            (Decimal::PositiveInf, Decimal::Normalized(d)) if d.qz().is_sign_negative() => {
                Decimal::NegativeInf
            }
            (Decimal::NegativeInf, Decimal::Normalized(d)) if d.qz().is_sign_positive() => {
                Decimal::NegativeInf
            }
            (Decimal::NegativeInf, Decimal::Normalized(d)) if d.qz().is_sign_negative() => {
                Decimal::PositiveInf
            }
            // normal case
            (Decimal::Normalized(lhs), Decimal::Normalized(rhs)) => {
                Decimal::normalized(lhs.as_ref() / rhs.as_ref())
            }
            _ => unreachable!(),
        }
    }
}

impl Mul for DeciRef<'_> {
    type Output = Decimal;

    fn mul(self, other: Self) -> Self::Output {
        match (self.0, other.0) {
            (Decimal::Normalized(lhs), Decimal::Normalized(rhs)) => {
                Decimal::normalized(lhs.as_ref() * rhs.as_ref())
            }
            (Decimal::NaN, _) => Decimal::NaN,
            (_, Decimal::NaN) => Decimal::NaN,
            (Decimal::PositiveInf, Decimal::Normalized(rhs))
                if !rhs.is_zero() && rhs.qz().is_sign_negative() =>
            {
                Decimal::NegativeInf
            }
            (Decimal::PositiveInf, Decimal::Normalized(rhs))
                if !rhs.is_zero() && rhs.qz().is_sign_positive() =>
            {
                Decimal::PositiveInf
            }
            (Decimal::PositiveInf, Decimal::PositiveInf) => Decimal::PositiveInf,
            (Decimal::PositiveInf, Decimal::NegativeInf) => Decimal::NegativeInf,
            (Decimal::Normalized(lhs), Decimal::PositiveInf)
                if !lhs.is_zero() && lhs.qz().is_sign_negative() =>
            {
                Decimal::NegativeInf
            }
            (Decimal::Normalized(lhs), Decimal::PositiveInf)
                if !lhs.is_zero() && lhs.qz().is_sign_positive() =>
            {
                Decimal::PositiveInf
            }
            (Decimal::NegativeInf, Decimal::PositiveInf) => Decimal::NegativeInf,
            (Decimal::NegativeInf, Decimal::Normalized(rhs))
                if !rhs.is_zero() && rhs.qz().is_sign_negative() =>
            {
                Decimal::PositiveInf
            }
            (Decimal::NegativeInf, Decimal::Normalized(rhs))
                if !rhs.is_zero() && rhs.qz().is_sign_positive() =>
            {
                Decimal::NegativeInf
            }
            (Decimal::NegativeInf, Decimal::NegativeInf) => Decimal::PositiveInf,
            (Decimal::Normalized(lhs), Decimal::NegativeInf)
                if !lhs.is_zero() && lhs.qz().is_sign_negative() =>
            {
                Decimal::PositiveInf
            }
            (Decimal::Normalized(lhs), Decimal::NegativeInf)
                if !lhs.is_zero() && lhs.qz().is_sign_positive() =>
            {
                Decimal::NegativeInf
            }
            // 0 * {inf, nan} => nan
            _ => Decimal::NaN,
        }
    }
}

impl Sub for DeciRef<'_> {
    type Output = Decimal;

    fn sub(self, other: Self) -> Self::Output {
        match (self.0, other.0) {
            (Decimal::Normalized(lhs), Decimal::Normalized(rhs)) => {
                Decimal::normalized(lhs.as_ref() - rhs.as_ref())
            }
            (Decimal::NaN, _) => Decimal::NaN,
            (_, Decimal::NaN) => Decimal::NaN,
            (Decimal::PositiveInf, Decimal::PositiveInf) => Decimal::NaN,
            (Decimal::NegativeInf, Decimal::NegativeInf) => Decimal::NaN,
            (Decimal::PositiveInf, _) => Decimal::PositiveInf,
            (_, Decimal::PositiveInf) => Decimal::NegativeInf,
            (Decimal::NegativeInf, _) => Decimal::NegativeInf,
            (_, Decimal::NegativeInf) => Decimal::PositiveInf,
        }
    }
}

impl Decimal {
    const MAX_I128_REPR: i128 = 0x0000_0000_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF;
    pub const MAX_PRECISION: u8 = 28;
}

impl DeciRef<'_> {
    pub fn scale(self) -> Option<i32> {
        let Decimal::Normalized(d) = self.0 else {
            return None;
        };
        Some(d.qz().scale() as _)
    }

    pub fn rescale(self, scale: u32) -> Decimal {
        if let Normalized(a) = self.0 {
            let mut a = a.qz();
            a.rescale(scale);
            Decimal::normalizeq(a)
        } else {
            self.xxd()
        }
    }

    #[must_use]
    pub fn round_dp_ties_away(self, dp: u32) -> Decimal {
        match self.0 {
            Decimal::Normalized(d) => {
                let new_d = d
                    .qz()
                    .round_dp_with_strategy(dp, RoundingStrategy::MidpointAwayFromZero);
                Decimal::normalizeq(new_d)
            }
            d => d.clone(),
        }
    }

    /// Round to the left of the decimal point, for example `31.5` -> `30`.
    #[must_use]
    pub fn round_left_ties_away(self, left: u32) -> Option<Decimal> {
        let Decimal::Normalized(d) = self.0 else {
            return Some(self.xxd());
        };

        // First, move the decimal point to the left so that we can reuse `round`. This is more
        // efficient than division.
        let old_scale = d.qz().scale();
        let new_scale = old_scale.saturating_add(left);
        const MANTISSA_UP: i128 = 5 * 10i128.pow(Decimal::MAX_PRECISION as _);
        let d = match new_scale.cmp(&Decimal::MAX_PRECISION.add(1).into()) {
            // trivial within 28 digits
            std::cmp::Ordering::Less => {
                let mut d = d.qz();
                d.set_scale(new_scale).unwrap();
                d.round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero)
            }
            // Special case: scale cannot be 29, but it may or may not be >= 0.5e+29
            std::cmp::Ordering::Equal => (d.qz().mantissa() / MANTISSA_UP).signum().into(),
            // always 0 for >= 30 digits
            std::cmp::Ordering::Greater => 0.into(),
        };

        // Then multiply back. Note that we cannot move decimal point to the right in order to get
        // more zeros.
        match left > Decimal::MAX_PRECISION.into() {
            true => d.is_zero().then(|| 0.into()),
            false => d
                .checked_mul(RustDecimal::from_i128_with_scale(10i128.pow(left), 0))
                .map(Decimal::normalizeq),
        }
    }

    #[must_use]
    pub fn ceil(self) -> Decimal {
        match self.0 {
            Decimal::Normalized(d) => {
                let mut d = d.qz().ceil();
                if d.is_zero() {
                    d.set_sign_positive(true);
                }
                Decimal::normalizeq(d)
            }
            d => d.clone(),
        }
    }

    #[must_use]
    pub fn floor(self) -> Decimal {
        match self.0 {
            Decimal::Normalized(d) => Decimal::normalizeq(d.qz().floor()),
            d => d.clone(),
        }
    }

    #[must_use]
    pub fn trunc(self) -> Decimal {
        match self.0 {
            Decimal::Normalized(d) => {
                let mut d = d.qz().trunc();
                if d.is_zero() {
                    d.set_sign_positive(true);
                }
                Decimal::normalizeq(d)
            }
            d => d.clone(),
        }
    }

    #[must_use]
    pub fn round_ties_even(self) -> Decimal {
        match self.0 {
            Decimal::Normalized(d) => Decimal::normalizeq(d.qz().round()),
            d => d.clone(),
        }
    }
}

impl Decimal {
    pub fn from_i128_with_scale(num: i128, scale: u32) -> Self {
        Decimal::normalizeq(RustDecimal::from_i128_with_scale(num, scale))
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
        Some(Decimal::normalizeq(
            RustDecimal::try_from_i128_with_scale(num, scale).ok()?,
        ))
    }

    pub fn unordered_deserialize(bytes: [u8; 16]) -> Self {
        match bytes[0] {
            0u8 => Self::normalizeq(RustDecimal::deserialize(bytes)),
            1u8 => Self::NaN,
            2u8 => Self::PositiveInf,
            3u8 => Self::NegativeInf,
            _ => unreachable!(),
        }
    }
}

impl DeciRef<'_> {
    pub fn unordered_serialize(self) -> [u8; 16] {
        // according to https://docs.rs/rust_decimal/1.18.0/src/rust_decimal/decimal.rs.html#665-684
        // the lower 15 bits is not used, so we can use first byte to distinguish nan and inf
        match self.0 {
            Decimal::Normalized(d) => d.qz().serialize(),
            Decimal::NaN => [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            Decimal::PositiveInf => [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            Decimal::NegativeInf => [3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        }
    }

    #[must_use]
    pub fn normalize(self) -> Decimal {
        match self.0 {
            Decimal::Normalized(d) => Decimal::normalizeq(d.qz().normalize()),
            d => d.clone(),
        }
    }

    pub fn abs(self) -> Decimal {
        match self.0 {
            Decimal::Normalized(d) => {
                if d.qz().is_sign_negative() {
                    Decimal::normalized(-d.as_ref())
                } else {
                    Decimal::Normalized(d.clone())
                }
            }
            Decimal::NaN => Decimal::NaN,
            Decimal::PositiveInf => Decimal::PositiveInf,
            Decimal::NegativeInf => Decimal::PositiveInf,
        }
    }

    pub fn sign(self) -> Decimal {
        match self.0 {
            Decimal::NaN => Decimal::NaN,
            _ => match self.0.cmp(&0.into()) {
                std::cmp::Ordering::Less => (-1).into(),
                std::cmp::Ordering::Equal => 0.into(),
                std::cmp::Ordering::Greater => 1.into(),
            },
        }
    }

    pub fn checked_exp(self) -> Option<Decimal> {
        match self.0 {
            Decimal::Normalized(d) => d.qz().checked_exp().map(Decimal::normalizeq),
            Decimal::NaN => Some(Decimal::NaN),
            Decimal::PositiveInf => Some(Decimal::PositiveInf),
            Decimal::NegativeInf => Some(Decimal::zero()),
        }
    }

    pub fn checked_ln(self) -> Option<Decimal> {
        match self.0 {
            Decimal::Normalized(d) => d.qz().checked_ln().map(Decimal::normalizeq),
            Decimal::NaN => Some(Decimal::NaN),
            Decimal::PositiveInf => Some(Decimal::PositiveInf),
            Decimal::NegativeInf => None,
        }
    }

    pub fn checked_log10(self) -> Option<Decimal> {
        match self.0 {
            Decimal::Normalized(d) => d.qz().checked_log10().map(Decimal::normalizeq),
            Decimal::NaN => Some(Decimal::NaN),
            Decimal::PositiveInf => Some(Decimal::PositiveInf),
            Decimal::NegativeInf => None,
        }
    }

    pub fn checked_powd(self, rhs: Self) -> Result<Decimal, PowError> {
        use std::cmp::Ordering;

        match (self.0, rhs.0) {
            // A. Handle `nan`, where `1 ^ nan == 1` and `nan ^ 0 == 1`
            (Decimal::NaN, Decimal::NaN)
            | (Decimal::PositiveInf, Decimal::NaN)
            | (Decimal::NegativeInf, Decimal::NaN)
            | (Decimal::NaN, Decimal::PositiveInf)
            | (Decimal::NaN, Decimal::NegativeInf) => Ok(Decimal::NaN),
            (Normalized(lhs), Decimal::NaN) => match lhs.is_one() {
                true => Ok(1.into()),
                false => Ok(Decimal::NaN),
            },
            (Decimal::NaN, Normalized(rhs)) => match rhs.is_zero() {
                true => Ok(1.into()),
                false => Ok(Decimal::NaN),
            },

            // B. Handle `b ^ inf`
            (Normalized(lhs), Decimal::PositiveInf) => match lhs.abs().cmp(&1.into()) {
                Ordering::Greater => Ok(Decimal::PositiveInf),
                Ordering::Equal => Ok(1.into()),
                Ordering::Less => Ok(0.into()),
            },
            // Simply special case of `abs(b) > 1`.
            // Also consistent with `inf ^ p` and `-inf ^ p` below where p is not fractional or odd.
            (Decimal::PositiveInf, Decimal::PositiveInf)
            | (Decimal::NegativeInf, Decimal::PositiveInf) => Ok(Decimal::PositiveInf),

            // C. Handle `b ^ -inf`, which is `(1/b) ^ inf`
            (Normalized(lhs), Decimal::NegativeInf) => match lhs.abs().cmp(&1.into()) {
                Ordering::Greater => Ok(0.into()),
                Ordering::Equal => Ok(1.into()),
                Ordering::Less => match lhs.is_zero() {
                    // Fun fact: ISO 9899 is removing this error to follow IEEE 754 2008.
                    true => Err(PowError::ZeroNegative),
                    false => Ok(Decimal::PositiveInf),
                },
            },
            (Decimal::PositiveInf, Decimal::NegativeInf)
            | (Decimal::NegativeInf, Decimal::NegativeInf) => Ok(0.into()),

            // D. Handle `inf ^ p`
            (Decimal::PositiveInf, Normalized(rhs)) => match rhs.as_ref().cmp(&0.into()) {
                Ordering::Greater => Ok(Decimal::PositiveInf),
                Ordering::Equal => Ok(1.into()),
                Ordering::Less => Ok(0.into()),
            },

            // E. Handle `-inf ^ p`. Finite `p` can be fractional, odd, or even.
            (Decimal::NegativeInf, Normalized(rhs)) => match !rhs.qz().fract().is_zero() {
                // Err in PostgreSQL. No err in ISO 9899 which treats fractional as non-odd below.
                true => Err(PowError::NegativeFract),
                false => match (
                    rhs.as_ref().cmp(&0.into()),
                    rhs.as_ref().rem(&2.into()).abs().is_one(),
                ) {
                    (Ordering::Greater, true) => Ok(Decimal::NegativeInf),
                    (Ordering::Greater, false) => Ok(Decimal::PositiveInf),
                    (Ordering::Equal, true) => unreachable!(),
                    (Ordering::Equal, false) => Ok(1.into()),
                    (Ordering::Less, true) => Ok(0.into()), // no `-0` in PostgreSQL decimal
                    (Ordering::Less, false) => Ok(0.into()),
                },
            },

            // F. Finite numbers
            (Normalized(lhs), Normalized(rhs)) => {
                if lhs.is_zero() && rhs.as_ref() < &BigDecimal::zero() {
                    return Err(PowError::ZeroNegative);
                }
                if lhs.as_ref() < &BigDecimal::zero() && !rhs.qz().fract().is_zero() {
                    return Err(PowError::NegativeFract);
                }
                match lhs.qz().checked_powd(rhs.qz()) {
                    Some(d) => Ok(Decimal::normalizeq(d)),
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

impl From<DeciRef<'_>> for memcomparable::Decimal {
    fn from(d: DeciRef<'_>) -> Self {
        match d.0 {
            Decimal::Normalized(d) => Self::Normalized(d.qz()),
            Decimal::PositiveInf => Self::Inf,
            Decimal::NegativeInf => Self::NegInf,
            Decimal::NaN => Self::NaN,
        }
    }
}

impl From<memcomparable::Decimal> for Decimal {
    fn from(d: memcomparable::Decimal) -> Self {
        match d {
            memcomparable::Decimal::Normalized(d) => Self::normalizeq(d),
            memcomparable::Decimal::Inf => Self::PositiveInf,
            memcomparable::Decimal::NegInf => Self::NegativeInf,
            memcomparable::Decimal::NaN => Self::NaN,
        }
    }
}

impl Default for Decimal {
    fn default() -> Self {
        Self::normalizeq(RustDecimal::default())
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
                .map(Decimal::normalizeq),
        }
    }
}

impl IsNegative for DeciRef<'_> {
    fn is_negative(&self) -> bool {
        match self.0 {
            Decimal::Normalized(d) => d.as_ref() < &BigDecimal::zero(),
            Decimal::NaN => false,
            Decimal::PositiveInf => false,
            Decimal::NegativeInf => true,
        }
    }
}

impl Zero for Decimal {
    fn zero() -> Self {
        Self::normalized(BigDecimal::zero())
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
        Self::normalized(BigDecimal::one())
    }
}

impl From<RustDecimal> for Decimal {
    fn from(d: RustDecimal) -> Self {
        Self::normalizeq(d)
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
                assert!(check(
                    (d_lhs.as_scalar_ref() + d_rhs.as_scalar_ref())
                        .try_into()
                        .unwrap(),
                    f_lhs + f_rhs
                ));
                assert!(check(
                    (d_lhs.as_scalar_ref() - d_rhs.as_scalar_ref())
                        .try_into()
                        .unwrap(),
                    f_lhs - f_rhs
                ));
                assert!(check(
                    (d_lhs.as_scalar_ref() * d_rhs.as_scalar_ref())
                        .try_into()
                        .unwrap(),
                    f_lhs * f_rhs
                ));
                assert!(check(
                    (d_lhs.as_scalar_ref() / d_rhs.as_scalar_ref())
                        .try_into()
                        .unwrap(),
                    f_lhs / f_rhs
                ));
                assert!(check(
                    (d_lhs.as_scalar_ref() % d_rhs.as_scalar_ref())
                        .try_into()
                        .unwrap(),
                    f_lhs % f_rhs
                ));
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
            Decimal::try_from(10.0).unwrap().as_scalar_ref() / Decimal::PositiveInf.as_scalar_ref(),
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
            Decimal::unordered_deserialize(
                Decimal::try_from(1.234)
                    .unwrap()
                    .as_scalar_ref()
                    .unordered_serialize()
            ),
            Decimal::try_from(1.234).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::from(1u8).as_scalar_ref().unordered_serialize()
            ),
            Decimal::from(1u8),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::from(1i8).as_scalar_ref().unordered_serialize()
            ),
            Decimal::from(1i8),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::from(1u16).as_scalar_ref().unordered_serialize()
            ),
            Decimal::from(1u16),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::from(1i16).as_scalar_ref().unordered_serialize()
            ),
            Decimal::from(1i16),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::from(1u32).as_scalar_ref().unordered_serialize()
            ),
            Decimal::from(1u32),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::from(1i32).as_scalar_ref().unordered_serialize()
            ),
            Decimal::from(1i32),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::try_from(f64::NAN)
                    .unwrap()
                    .as_scalar_ref()
                    .unordered_serialize()
            ),
            Decimal::try_from(f64::NAN).unwrap(),
        );
        assert_eq!(
            Decimal::unordered_deserialize(
                Decimal::try_from(f64::INFINITY)
                    .unwrap()
                    .as_scalar_ref()
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
                memcomparable::Decimal::from(ordered[i - 1].as_scalar_ref())
                    < memcomparable::Decimal::from(ordered[i].as_scalar_ref())
            );
        }
    }

    #[test]
    fn test_decimal_estimate_size() {
        let decimal = Decimal::NegativeInf;
        assert_eq!(decimal.estimated_size(), 16);

        let decimal = Decimal::normalized(BigDecimal::try_from(1.0).unwrap());
        assert_eq!(decimal.estimated_size(), 16);
    }
}
