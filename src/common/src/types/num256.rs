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

use std::fmt::{Display, Formatter, Write};
use std::hash::Hasher;
use std::io::Read;
use std::mem;
use std::num::ParseIntError;
use std::ops::{Add, Div, Mul, Neg, Rem, Sub};
use std::str::FromStr;

use bytes::{BufMut, Bytes};
use ethnum::i256;
use num_traits::{
    CheckedAdd, CheckedDiv, CheckedMul, CheckedNeg, CheckedRem, CheckedSub, Num, One, Signed, Zero,
};
use risingwave_pb::data::ArrayType;
use serde::de::{Error, Visitor};
use serde::{Deserializer, Serializer};
use to_text::ToText;

use crate::array::ArrayResult;
use crate::types::to_binary::ToBinary;
use crate::types::{to_text, Buf, DataType, Scalar, ScalarRef};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Int256(Box<i256>);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Int256Ref<'a>(pub &'a i256);

impl<'a> Display for Int256Ref<'a> {
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
            pub fn min() -> Self {
                Self::from(<$inner>::MIN)
            }

            #[inline]
            pub fn max() -> Self {
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
            fn to_binary_with_type(&self, _ty: &DataType) -> crate::error::Result<Option<Bytes>> {
                let mut output = bytes::BytesMut::new();
                let buffer = self.to_be_bytes();
                output.put_slice(&buffer);
                Ok(Some(output.freeze()))
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

impl<'a> Int256Ref<'a> {
    pub fn memcmp_serialize(
        &self,
        serializer: &mut memcomparable::Serializer<impl bytes::BufMut>,
    ) -> memcomparable::Result<()> {
        let unsigned: i256 = self.0 ^ (i256::from(1) << (i256::BITS - 1));
        serializer.serialize_bytes(&unsigned.to_be_bytes())
    }
}

struct FormattedInt256Visitor;

impl<'de> Visitor<'de> for FormattedInt256Visitor {
    type Value = [u8; 32];

    fn expecting(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("a formatted 256-bit integer")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        if v.len() != 32 {
            return Err(Error::invalid_length(v.len(), &self));
        }

        let mut buf = [0u8; 32];
        buf.copy_from_slice(v);
        Ok(buf)
    }
}

impl Int256 {
    pub const MEMCMP_ENCODED_SIZE: usize = 32;

    pub fn memcmp_deserialize(
        deserializer: &mut memcomparable::Deserializer<impl Buf>,
    ) -> memcomparable::Result<Self> {
        let buf = deserializer.deserialize_bytes(FormattedInt256Visitor)?;
        let unsigned = i256::from_be_bytes(buf);
        let signed = unsigned ^ (i256::from(1) << (i256::BITS - 1));
        Ok(Int256::from(signed))
    }
}

macro_rules! impl_convert_from {
    ($($t:ty),* $(,)?) => {$(
        impl From<$t> for Int256 {
            #[inline]
            fn from(value: $t) -> Self {
                Self(Box::new(i256::from(value)))
            }
        }
    )*};
}

impl_convert_from!(i16, i32, i64);

// Conversions for mathematical operations.
impl From<Int256Ref<'_>> for Int256 {
    fn from(value: Int256Ref<'_>) -> Self {
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
                Int256::from(self.0 $op rhs.0)
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

impl Signed for Int256 {
    fn abs(&self) -> Self {
        self.0.abs().into()
    }

    fn abs_sub(&self, other: &Self) -> Self {
        if self <= other {
            Self::zero()
        } else {
            self.abs()
        }
    }

    fn signum(&self) -> Self {
        self.0.signum().into()
    }

    fn is_positive(&self) -> bool {
        self.0.is_positive()
    }

    fn is_negative(&self) -> bool {
        self.0.is_negative()
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
    fn test_abs() {
        assert_eq!(Int256::from(-1).abs(), Int256::from(1));
        assert_eq!(Int256::from(1).abs(), Int256::from(1));
        assert_eq!(Int256::from(0).abs(), Int256::from(0));
    }
}
