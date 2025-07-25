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

use std::fmt::{Display, Formatter, Write};
use std::hash::Hasher;
use std::io::Read;
use std::mem;
use std::num::ParseIntError;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;

use bytes::{BufMut, Bytes};
use ethnum::{AsI256, i256, u256};
use num_traits::{
    CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Num, One, Zero,
};
use risingwave_common_estimate_size::EstimateSize;
use risingwave_pb::data::ArrayType;
use serde::{Deserialize, Serialize};
use to_text::ToText;

use crate::array::ArrayResult;
use crate::types::to_binary::ToBinary;
use crate::types::{Buf, DataType, F64, Scalar, ScalarRef, to_text};

/// A 256-bit signed integer.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Int256(pub(crate) Box<i256>);

/// A reference to an `Int256` value.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Int256Ref<'a>(pub &'a i256);

/// A 256-bit unsigned integer.
#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct UInt256(pub(crate) Box<u256>);

/// A reference to an `UInt256` value.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct UInt256Ref<'a>(pub &'a u256);

impl Display for Int256Ref<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.write(f)
    }
}

impl Display for UInt256Ref<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.write(f)
    }
}

macro_rules! impl_common_for_num256 {
    ($scalar:ident, $scalar_ref:ident < $gen:tt > , $inner:ty, $array_type:ident) => {
        impl Scalar for $scalar {
            type ScalarRefType<$gen> = $scalar_ref<$gen>;

            fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
                $scalar_ref(self.0.as_ref())
            }
        }

        impl<$gen> ScalarRef<$gen> for $scalar_ref<$gen> {
            type ScalarType = $scalar;

            fn to_owned_scalar(&self) -> Self::ScalarType {
                $scalar((*self.0).into())
            }

            fn hash_scalar<H: Hasher>(&self, state: &mut H) {
                use std::hash::Hash as _;
                self.0.hash(state)
            }
        }

        impl FromStr for $scalar {
            type Err = ParseIntError;

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                <$inner>::from_str(s).map(Into::into)
            }
        }

        impl $scalar {
            #[inline]
            pub fn min_value() -> Self {
                Self::from(<$inner>::MIN)
            }

            #[inline]
            pub fn max_value() -> Self {
                Self::from(<$inner>::MAX)
            }

            #[inline]
            pub fn into_inner(self) -> $inner {
                *self.0
            }

            #[inline]
            pub const fn size() -> usize {
                mem::size_of::<$inner>()
            }

            #[inline]
            pub fn array_type() -> ArrayType {
                ArrayType::$array_type
            }

            #[inline]
            pub fn from_ne_bytes(bytes: [u8; mem::size_of::<$inner>()]) -> Self {
                Self(Box::new(<$inner>::from_ne_bytes(bytes)))
            }

            #[inline]
            pub fn from_le_bytes(bytes: [u8; mem::size_of::<$inner>()]) -> Self {
                Self(Box::new(<$inner>::from_le_bytes(bytes)))
            }

            #[inline]
            pub fn from_be_bytes(bytes: [u8; mem::size_of::<$inner>()]) -> Self {
                Self(Box::new(<$inner>::from_be_bytes(bytes)))
            }

            pub fn from_protobuf(input: &mut impl Read) -> ArrayResult<Self> {
                let mut buf = [0u8; mem::size_of::<$inner>()];
                input.read_exact(&mut buf)?;
                Ok(Self::from_be_bytes(buf))
            }
        }

        impl From<$inner> for $scalar {
            fn from(value: $inner) -> Self {
                Self(Box::new(value))
            }
        }

        impl $scalar_ref<'_> {
            #[inline]
            pub fn to_le_bytes(self) -> [u8; mem::size_of::<$inner>()] {
                self.0.to_le_bytes()
            }

            #[inline]
            pub fn to_be_bytes(self) -> [u8; mem::size_of::<$inner>()] {
                self.0.to_be_bytes()
            }

            #[inline]
            pub fn to_ne_bytes(self) -> [u8; mem::size_of::<$inner>()] {
                self.0.to_ne_bytes()
            }

            pub fn to_protobuf<T: std::io::Write>(self, output: &mut T) -> ArrayResult<usize> {
                output.write(&self.to_be_bytes()).map_err(Into::into)
            }
        }

        impl ToText for $scalar_ref<'_> {
            fn write<W: Write>(&self, f: &mut W) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }

            fn write_with_type<W: Write>(&self, _ty: &DataType, f: &mut W) -> std::fmt::Result {
                self.write(f)
            }
        }

        impl ToBinary for $scalar_ref<'_> {
            fn to_binary_with_type(&self, _ty: &DataType) -> super::to_binary::Result<Bytes> {
                let mut output = bytes::BytesMut::new();
                let buffer = self.to_be_bytes();
                output.put_slice(&buffer);
                Ok(output.freeze())
            }
        }

        impl $scalar {
            pub fn from_binary(mut input: &[u8]) -> ArrayResult<Self> {
                let mut buf = [0; Self::size()];
                input.read_exact(&mut buf)?;
                Ok(Self::from_be_bytes(buf))
            }
        }
    };
}

impl_common_for_num256!(Int256, Int256Ref<'a>, i256, Int256);
impl_common_for_num256!(UInt256, UInt256Ref<'a>, u256, Uint256);

impl Int256 {
    // `i256::str_from_hex` and `i256::str_from_prefixed` doesn't support uppercase "0X", so when it
    // fails it will try to parse the lowercase version of the `src`

    // `from_str_prefixed` function accepts string inputs that start with "0x". If the parsing
    // fails, it will attempt to parse the input as a decimal value.
    pub fn from_str_prefixed(src: &str) -> Result<Self, ParseIntError> {
        u256::from_str_prefixed(src)
            .or_else(|_| u256::from_str_prefixed(&src.to_lowercase()))
            .map(|u| u.as_i256().into())
    }

    // `from_str_hex` function only accepts string inputs that start with "0x".
    pub fn from_str_hex(src: &str) -> Result<Self, ParseIntError> {
        u256::from_str_hex(src)
            .or_else(|_| u256::from_str_hex(&src.to_lowercase()))
            .map(|u| u.as_i256().into())
    }
}

impl UInt256 {
    // `from_str_prefixed` function accepts string inputs that start with "0x". If the parsing
    // fails, it will attempt to parse the input as a decimal value.
    pub fn from_str_prefixed(src: &str) -> Result<Self, ParseIntError> {
        u256::from_str_prefixed(src)
            .or_else(|_| u256::from_str_prefixed(&src.to_lowercase()))
            .map(Into::into)
    }

    // `from_str_hex` function only accepts string inputs that start with "0x".
    pub fn from_str_hex(src: &str) -> Result<Self, ParseIntError> {
        u256::from_str_hex(src)
            .or_else(|_| u256::from_str_hex(&src.to_lowercase()))
            .map(Into::into)
    }
}

impl Int256Ref<'_> {
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl bytes::BufMut>,
    ) -> memcomparable::Result<()> {
        let (hi, lo) = self.0.into_words();
        (hi, lo as u128).serialize(serializer)
    }
}

impl Int256 {
    pub const MEMCMP_ENCODED_SIZE: usize = 32;

    pub fn memcmp_deserialize(
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let (hi, lo) = <(i128, u128)>::deserialize(deserializer)?;
        let signed = i256::from_words(hi, lo as i128);
        Ok(Int256::from(signed))
    }
}

impl UInt256Ref<'_> {
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl bytes::BufMut>,
    ) -> memcomparable::Result<()> {
        let (hi, lo) = self.0.into_words();
        (hi, lo).serialize(serializer)
    }
}

impl UInt256 {
    pub const MEMCMP_ENCODED_SIZE: usize = 32;

    pub fn memcmp_deserialize(
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let (hi, lo) = <(u128, u128)>::deserialize(deserializer)?;
        let unsigned = u256::from_words(hi, lo);
        Ok(UInt256::from(unsigned))
    }
}

macro_rules! impl_convert_from {
    ($($t:ty),* $(,)?) => {$(
        impl From<$t> for Int256 {
            #[inline]
            fn from(value: $t) -> Self {
                Self(Box::new(value.as_i256()))
            }
        }
    )*};
}

impl_convert_from!(i16, i32, i64);

macro_rules! impl_convert_from_unsigned {
    ($($t:ty),* $(,)?) => {$(
        impl From<$t> for UInt256 {
            #[inline]
            fn from(value: $t) -> Self {
                Self(Box::new(value.into()))
            }
        }
    )*};
}

impl_convert_from_unsigned!(u16, u32, u64);

// Support converting signed integers to unsigned 256-bit, checking for negative values
impl TryFrom<i16> for UInt256 {
    type Error = ();
    fn try_from(value: i16) -> Result<Self, Self::Error> {
        if value < 0 {
            Err(())
        } else {
            Ok(Self(Box::new((value as u16).into())))
        }
    }
}

impl TryFrom<i32> for UInt256 {
    type Error = ();
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value < 0 {
            Err(())
        } else {
            Ok(Self(Box::new((value as u32).into())))
        }
    }
}

impl TryFrom<i64> for UInt256 {
    type Error = ();
    fn try_from(value: i64) -> Result<Self, Self::Error> {
        if value < 0 {
            Err(())
        } else {
            Ok(Self(Box::new((value as u64).into())))
        }
    }
}

impl<'a> From<Int256Ref<'a>> for F64 {
    fn from(value: Int256Ref<'a>) -> Self {
        Self::from(value.0.as_f64())
    }
}

impl<'a> From<UInt256Ref<'a>> for F64 {
    fn from(value: UInt256Ref<'a>) -> Self {
        Self::from(value.0.as_f64())
    }
}

// Conversions for mathematical operations.
impl From<Int256Ref<'_>> for Int256 {
    fn from(value: Int256Ref<'_>) -> Self {
        value.to_owned_scalar()
    }
}

impl From<UInt256Ref<'_>> for UInt256 {
    fn from(value: UInt256Ref<'_>) -> Self {
        value.to_owned_scalar()
    }
}

macro_rules! impl_checked_op {
    ($trait:ty, $func:ident, $op:tt, $proxied_trait:tt, $proxied_func:ident, $scalar:ident, $scalar_ref:ident < $gen:tt >) => {
        impl $proxied_trait<Self> for $scalar {
            type Output = Self;

            fn $proxied_func(self, rhs: Self) -> Self::Output {
                $scalar::from(self.0.as_ref() $op rhs.0.as_ref())
            }
        }

        impl<$gen> $proxied_trait<Self> for $scalar_ref<$gen> {
            type Output = $scalar;

            fn $proxied_func(self, rhs: Self) -> Self::Output {
                $scalar::from(self.0 $op rhs.0)
            }
        }

        impl $trait for $scalar {
            fn $func(&self, other: &Self) -> Option<Self> {
                self.0.$func(*other.0).map(Into::into)
            }
        }
    };
}

impl_checked_op!(CheckedAdd, checked_add, +, Add, add, Int256, Int256Ref<'a>);
impl_checked_op!(CheckedSub, checked_sub, -, Sub, sub, Int256, Int256Ref<'a>);
impl_checked_op!(CheckedMul, checked_mul, *, Mul, mul, Int256, Int256Ref<'a>);
impl_checked_op!(CheckedDiv, checked_div, /, Div, div, Int256, Int256Ref<'a>);
impl_checked_op!(CheckedRem, checked_rem, %, Rem, rem, Int256, Int256Ref<'a>);

impl_checked_op!(CheckedAdd, checked_add, +, Add, add, UInt256, UInt256Ref<'a>);
impl_checked_op!(CheckedSub, checked_sub, -, Sub, sub, UInt256, UInt256Ref<'a>);
impl_checked_op!(CheckedMul, checked_mul, *, Mul, mul, UInt256, UInt256Ref<'a>);
impl_checked_op!(CheckedDiv, checked_div, /, Div, div, UInt256, UInt256Ref<'a>);
impl_checked_op!(CheckedRem, checked_rem, %, Rem, rem, UInt256, UInt256Ref<'a>);

impl Neg for Int256 {
    type Output = Int256;

    fn neg(self) -> Self::Output {
        Int256::from(self.0.neg())
    }
}

impl CheckedNeg for Int256 {
    fn checked_neg(&self) -> Option<Self> {
        self.0.checked_neg().map(Into::into)
    }
}

impl Zero for Int256 {
    fn zero() -> Self {
        Int256::from(i256::ZERO)
    }

    fn is_zero(&self) -> bool {
        !self.0.is_negative() && !self.0.is_positive()
    }
}

impl One for Int256 {
    fn one() -> Self {
        Self::from(i256::ONE)
    }
}

impl Num for Int256 {
    type FromStrRadixErr = ParseIntError;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        i256::from_str_radix(str, radix).map(Into::into)
    }
}

impl EstimateSize for Int256 {
    fn estimated_heap_size(&self) -> usize {
        mem::size_of::<i128>() * 2
    }
}

impl Zero for UInt256 {
    fn zero() -> Self {
        UInt256::from(u256::ZERO)
    }

    fn is_zero(&self) -> bool {
        *self.0 == u256::ZERO
    }
}

impl One for UInt256 {
    fn one() -> Self {
        Self::from(u256::ONE)
    }
}

impl Num for UInt256 {
    type FromStrRadixErr = ParseIntError;

    fn from_str_radix(str: &str, radix: u32) -> Result<Self, Self::FromStrRadixErr> {
        u256::from_str_radix(str, radix).map(Into::into)
    }
}

impl EstimateSize for UInt256 {
    fn estimated_heap_size(&self) -> usize {
        mem::size_of::<u128>() * 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    macro_rules! check_op {
        ($t:ty, $lhs:expr, $rhs:expr, [$($op:tt),+]) => {
            $(assert_eq!(
                Int256::from($lhs as $t) $op Int256::from($rhs as $t),
                Int256::from(($lhs as $t) $op ($rhs as $t))
            );)+
        };
    }

    macro_rules! check_checked_op {
        ($t:ty, $lhs:expr, $rhs:expr, [$($f:ident),+]) => {
            $(assert_eq!(
                Int256::from($lhs as $t).$f(&Int256::from($rhs as $t)),
                ($lhs as $t).$f(($rhs as $t)).map(Int256::from)
            );)+
        };
    }

    macro_rules! generate_op_test {
        ($($t:ty),* $(,)?) => {$(
            check_op!($t, 1, 1, [+, -, *, /, %]);
            check_op!($t, -1, 1, [+, -, *, /, %]);
            check_op!($t, 0, 1, [+, -, *, /, %]);
            check_op!($t, -12, 34, [+, -, *, /, %]);
            check_op!($t, 12, -34, [+, -, *, /, %]);
            check_op!($t, -12, -34, [+, -, *, /, %]);
            check_op!($t, 12, 34, [+, -, *, /, %]);
        )*};
    }

    macro_rules! generate_checked_op_test {
        ($($t:ty),* $(,)?) => {$(
            check_checked_op!($t, 1, 1, [checked_add, checked_sub, checked_mul, checked_div, checked_rem]);
            check_checked_op!($t, 1, 0, [checked_add, checked_sub, checked_mul, checked_div, checked_rem]);
            check_checked_op!($t, -1, 1, [checked_add, checked_sub, checked_mul, checked_div, checked_rem]);
            check_checked_op!($t, -1, 0, [checked_add, checked_sub, checked_mul, checked_div, checked_rem]);
            check_checked_op!($t, 0, 1, [checked_add, checked_sub, checked_mul, checked_div, checked_rem]);
            check_checked_op!($t, -12, 34, [checked_add, checked_sub, checked_mul, checked_div, checked_rem]);
            check_checked_op!($t, 12, -34, [checked_add, checked_sub, checked_mul, checked_div, checked_rem]);
            check_checked_op!($t, -12, -34, [checked_add, checked_sub, checked_mul, checked_div, checked_rem]);
            check_checked_op!($t, 12, 34, [checked_add, checked_sub, checked_mul, checked_div, checked_rem]);
        )*};
    }

    #[test]
    fn test_checked_op() {
        generate_checked_op_test!(i16, i32, i64);
    }

    #[test]
    fn test_op() {
        generate_op_test!(i16, i32, i64);
    }

    #[test]
    fn test_zero() {
        let zero = Int256::zero();
        assert_eq!(zero, Int256::from(0));
        assert!(zero.is_zero());
    }

    #[test]
    fn test_neg() {
        assert_eq!(Int256::from(1).neg(), Int256::from(-1));
        assert_eq!(-Int256::from(1), Int256::from(-1));
        assert_eq!(Int256::from(0).neg(), Int256::from(0));
        assert_eq!(-Int256::from(0), Int256::from(0));
    }

    #[test]
    fn test_float64() {
        let vs: Vec<i64> = vec![-9007199254740990, -100, -1, 0, 1, 100, 9007199254740991];

        for v in vs {
            let i = Int256::from(v);
            assert_eq!(F64::from(i.as_scalar_ref()), F64::from(v));
        }
    }

    #[test]
    fn hex_to_int256() {
        assert_eq!(Int256::from_str_hex("0x0").unwrap(), Int256::from(0));
        assert_eq!(Int256::from_str_hex("0x1").unwrap(), Int256::from(1));
        assert_eq!(
            Int256::from_str_hex(
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
            )
            .unwrap(),
            Int256::from(-1)
        );
        assert_eq!(Int256::from_str_hex("0xa").unwrap(), Int256::from(10));
        assert_eq!(Int256::from_str_hex("0xA").unwrap(), Int256::from(10));
        assert_eq!(Int256::from_str_hex("0Xff").unwrap(), Int256::from(255));

        assert_eq!(
            Int256::from_str_hex(
                "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff01"
            )
            .unwrap(),
            Int256::from(-255)
        );

        assert_eq!(
            Int256::from_str_hex("0xf").unwrap(),
            Int256::from_str("15").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffff").unwrap(),
            Int256::from_str("1048575").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffff").unwrap(),
            Int256::from_str("68719476735").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffff").unwrap(),
            Int256::from_str("4503599627370495").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffff").unwrap(),
            Int256::from_str("295147905179352825855").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffff").unwrap(),
            Int256::from_str("19342813113834066795298815").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffff").unwrap(),
            Int256::from_str("1267650600228229401496703205375").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffffffff").unwrap(),
            Int256::from_str("83076749736557242056487941267521535").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffffffffffff").unwrap(),
            Int256::from_str("5444517870735015415413993718908291383295").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffffffffffffffff").unwrap(),
            Int256::from_str("356811923176489970264571492362373784095686655").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffffffffffffffffffff").unwrap(),
            Int256::from_str("23384026197294446691258957323460528314494920687615").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffffffffffffffffffffffff").unwrap(),
            Int256::from_str("1532495540865888858358347027150309183618739122183602175").unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffffffffffffffffffffffffffff").unwrap(),
            Int256::from_str("100433627766186892221372630771322662657637687111424552206335")
                .unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .unwrap(),
            Int256::from_str("6582018229284824168619876730229402019930943462534319453394436095")
                .unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .unwrap(),
            Int256::from_str(
                "431359146674410236714672241392314090778194310760649159697657763987455"
            )
            .unwrap()
        );
        assert_eq!(
            Int256::from_str_hex("0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .unwrap(),
            Int256::from_str(
                "28269553036454149273332760011886696253239742350009903329945699220681916415"
            )
            .unwrap()
        );

        // int256 max
        assert_eq!(
            Int256::from_str_hex(
                "0x7fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
            )
            .unwrap(),
            Int256::max_value(),
        );

        // int256 min
        assert_eq!(
            Int256::from_str_hex(
                "0x8000000000000000000000000000000000000000000000000000000000000000"
            )
            .unwrap(),
            Int256::min_value(),
        );
    }

    #[test]
    fn test_num256_estimate_size() {
        let num256 = Int256::min_value();
        assert_eq!(num256.estimated_size(), 40);
    }
}
