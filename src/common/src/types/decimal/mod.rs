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

use std::fmt;
use std::hash::{Hash, Hasher};
use std::io::{Read, Write};
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;

use bytes::BytesMut;
use num_traits::{
    CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Num, One, Zero,
};
use postgres_types::{FromSql, IsNull, ToSql, Type, accepts, to_sql_checked};
use risingwave_common_estimate_size::ZeroHeapSize;
use rust_decimal::{Decimal as RustDecimal, Error};

use self::legacy::LegacyDecimal;
pub use self::legacy::PowError;
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

/// A decimal number, or one of `NaN`, `Infinity` and `-Infinity`.
///
/// The layout has room for a 128-bit coefficient, while the values themselves are still limited
/// to the range of `rust_decimal`: a 96-bit coefficient and a scale of at most 28. All
/// operations are carried out by [`LegacyDecimal`].
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

impl Decimal {
    pub const MAX_PRECISION: u8 = LegacyDecimal::MAX_PRECISION;
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

    fn to_legacy(self) -> LegacyDecimal {
        match self.kind() {
            KIND_FINITE => {
                let [lo, mid, hi, top] = self.coefficient;
                debug_assert_eq!(top, 0, "coefficient out of the legacy range");
                let scale = (self.flags & SCALE_MASK) >> SCALE_SHIFT;
                let mut d = RustDecimal::from_parts(lo, mid, hi, false, scale);
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

    pub fn is_finite(&self) -> bool {
        self.kind() == KIND_FINITE
    }

    pub fn is_nan(&self) -> bool {
        self.kind() == KIND_NAN
    }

    pub fn to_parts(self) -> DecimalParts {
        match self.to_legacy() {
            LegacyDecimal::Normalized(d) => DecimalParts::Finite {
                mantissa: d.mantissa(),
                scale: d.scale(),
            },
            LegacyDecimal::NaN => DecimalParts::NaN,
            LegacyDecimal::PositiveInf => DecimalParts::PositiveInf,
            LegacyDecimal::NegativeInf => DecimalParts::NegativeInf,
        }
    }

    /// The 16-byte encoding used by the value encoding and protobuf arrays: the flags word
    /// followed by the low three words of the coefficient, all little-endian.
    pub fn unordered_serialize(&self) -> [u8; 16] {
        let [lo, mid, hi, _] = self.coefficient;
        let mut bytes = [0; 16];
        bytes[0..4].copy_from_slice(&self.flags.to_le_bytes());
        bytes[4..8].copy_from_slice(&lo.to_le_bytes());
        bytes[8..12].copy_from_slice(&mid.to_le_bytes());
        bytes[12..16].copy_from_slice(&hi.to_le_bytes());
        bytes
    }

    pub fn unordered_deserialize(bytes: [u8; 16]) -> Self {
        Self::from_legacy(LegacyDecimal::unordered_deserialize(bytes))
    }

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
        LegacyDecimal::from_scientific(value).map(Self::from_legacy)
    }

    pub fn from_str_radix(s: &str, radix: u32) -> rust_decimal::Result<Self> {
        LegacyDecimal::from_str_radix(s, radix).map(Self::from_legacy)
    }

    pub fn from_i128_with_scale(num: i128, scale: u32) -> Self {
        Self::from_legacy(LegacyDecimal::from_i128_with_scale(num, scale))
    }

    /// Truncate the given `num` and `scale` to fit into `Decimal`, return `None` if it cannot be
    /// represented even after truncation.
    pub fn truncated_i128_and_scale(num: i128, scale: u32) -> Option<Self> {
        LegacyDecimal::truncated_i128_and_scale(num, scale).map(Self::from_legacy)
    }

    pub fn scale(&self) -> Option<i32> {
        self.to_legacy().scale()
    }

    pub fn rescale(&mut self, scale: u32) {
        let mut decimal = self.to_legacy();
        decimal.rescale(scale);
        *self = Self::from_legacy(decimal);
    }

    #[must_use]
    pub fn round_dp_ties_away(&self, dp: u32) -> Self {
        Self::from_legacy(self.to_legacy().round_dp_ties_away(dp))
    }

    /// Round to the left of the decimal point, for example `31.5` -> `30`.
    #[must_use]
    pub fn round_left_ties_away(&self, left: u32) -> Option<Self> {
        self.to_legacy()
            .round_left_ties_away(left)
            .map(Self::from_legacy)
    }

    #[must_use]
    pub fn ceil(&self) -> Self {
        Self::from_legacy(self.to_legacy().ceil())
    }

    #[must_use]
    pub fn floor(&self) -> Self {
        Self::from_legacy(self.to_legacy().floor())
    }

    #[must_use]
    pub fn trunc(&self) -> Self {
        Self::from_legacy(self.to_legacy().trunc())
    }

    #[must_use]
    pub fn round_ties_even(&self) -> Self {
        Self::from_legacy(self.to_legacy().round_ties_even())
    }

    #[must_use]
    pub fn normalize(&self) -> Self {
        Self::from_legacy(self.to_legacy().normalize())
    }

    pub fn abs(&self) -> Self {
        Self::from_legacy(self.to_legacy().abs())
    }

    pub fn sign(&self) -> Self {
        Self::from_legacy(self.to_legacy().sign())
    }

    pub fn checked_exp(&self) -> Option<Self> {
        self.to_legacy().checked_exp().map(Self::from_legacy)
    }

    pub fn checked_ln(&self) -> Option<Self> {
        self.to_legacy().checked_ln().map(Self::from_legacy)
    }

    pub fn checked_log10(&self) -> Option<Self> {
        self.to_legacy().checked_log10().map(Self::from_legacy)
    }

    /// Returns `None` for negative values, including `-Infinity`.
    pub fn checked_sqrt(&self) -> Option<Self> {
        self.to_legacy().checked_sqrt().map(Self::from_legacy)
    }

    pub fn checked_powd(&self, rhs: &Self) -> Result<Self, PowError> {
        self.to_legacy()
            .checked_powd(&rhs.to_legacy())
            .map(Self::from_legacy)
    }
}

/// The hash decides the vnode of rows distributed by a decimal column and is folded into
/// persisted aggregation states, so it must stay byte-for-byte stable across versions,
/// independent of the internal representation and of the `rust_decimal` version.
///
/// It feeds the hasher exactly what the former `#[derive(Hash)]` did: the variant index as
/// `isize`; for finite values, also the low, middle and high 32-bit words of the normalized
/// coefficient, then a flags word with the sign at bit 31 and the scale at bits 16..24.
/// Normalization strips trailing zeros and turns negative zero into zero, so equal values hash
/// equally.
impl Hash for Decimal {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.to_legacy() {
            LegacyDecimal::NegativeInf => state.write_isize(0),
            LegacyDecimal::Normalized(d) => {
                state.write_isize(1);
                let d = d.normalize();
                let coefficient = d.mantissa().unsigned_abs();
                state.write_u32(coefficient as u32);
                state.write_u32((coefficient >> 32) as u32);
                state.write_u32((coefficient >> 64) as u32);
                let sign = if d.is_sign_negative() { 1 << 31 } else { 0 };
                state.write_u32(sign | (d.scale() << 16));
            }
            LegacyDecimal::PositiveInf => state.write_isize(2),
            LegacyDecimal::NaN => state.write_isize(3),
        }
    }
}

impl PartialEq for Decimal {
    fn eq(&self, other: &Self) -> bool {
        self.to_legacy() == other.to_legacy()
    }
}

impl Eq for Decimal {}

impl PartialOrd for Decimal {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Decimal {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.to_legacy().cmp(&other.to_legacy())
    }
}

impl fmt::Debug for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.to_legacy(), f)
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.to_legacy(), f)
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
        self.to_legacy().to_sql(ty, out)
    }
}

impl<'a> FromSql<'a> for Decimal {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + 'static + Sync + Send>> {
        LegacyDecimal::from_sql(ty, raw).map(Self::from_legacy)
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
                    d.to_legacy().try_into()
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
                    LegacyDecimal::try_from(num).map(Self::from_legacy)
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
                    d.to_legacy().try_into()
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

macro_rules! impl_binary_op {
    ($({ $op:ident, $func:ident, $checked_op:ident, $checked_func:ident }),*) => {
        $(
            impl $op for Decimal {
                type Output = Self;

                fn $func(self, other: Self) -> Self {
                    Self::from_legacy(self.to_legacy().$func(other.to_legacy()))
                }
            }

            impl $checked_op for Decimal {
                fn $checked_func(&self, other: &Self) -> Option<Self> {
                    self.to_legacy()
                        .$checked_func(&other.to_legacy())
                        .map(Self::from_legacy)
                }
            }
        )*
    };
}

impl_binary_op! {
    { Add, add, CheckedAdd, checked_add },
    { Sub, sub, CheckedSub, checked_sub },
    { Mul, mul, CheckedMul, checked_mul },
    { Div, div, CheckedDiv, checked_div },
    { Rem, rem, CheckedRem, checked_rem }
}

impl Neg for Decimal {
    type Output = Self;

    fn neg(self) -> Self {
        Self::from_legacy(-self.to_legacy())
    }
}

impl CheckedNeg for Decimal {
    fn checked_neg(&self) -> Option<Self> {
        self.to_legacy().checked_neg().map(Self::from_legacy)
    }
}

impl From<Decimal> for memcomparable::Decimal {
    fn from(d: Decimal) -> Self {
        d.to_legacy().into()
    }
}

impl From<memcomparable::Decimal> for Decimal {
    fn from(d: memcomparable::Decimal) -> Self {
        Self::from_legacy(d.into())
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
        LegacyDecimal::from_str(s).map(Self::from_legacy)
    }
}

impl Zero for Decimal {
    fn zero() -> Self {
        Self::from_legacy(LegacyDecimal::zero())
    }

    fn is_zero(&self) -> bool {
        self.to_legacy().is_zero()
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
        <LegacyDecimal as Num>::from_str_radix(str, radix).map(Self::from_legacy)
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
        let decimal = Decimal::NEGATIVE_INF;
        assert_eq!(decimal.estimated_size(), 20);

        let decimal = Decimal::try_from(1.0).unwrap();
        assert_eq!(decimal.estimated_size(), 20);
    }
}
