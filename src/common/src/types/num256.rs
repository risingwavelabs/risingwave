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

use std::fmt::Write;
use std::hash::Hasher;
use std::mem;
use std::num::ParseIntError;
use std::ops::{BitAnd, BitOr, BitXor, Not};
use std::str::FromStr;

use bytes::Bytes;
use ethnum::I256;
use num_traits::{FromPrimitive, ToPrimitive, Zero};
use postgres_types::{ToSql, Type};
use risingwave_pb::data::ArrayType;
use serde::{Serialize, Serializer};
use to_text::ToText;

use crate::array::ArrayResult;
use crate::error::RwError;
use crate::types::to_binary::ToBinary;
use crate::types::{to_text, DataType, Interval, Scalar, ScalarRef};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Int256(Box<I256>);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Int256Ref<'a>(pub &'a I256);

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

        impl Serialize for $scalar_ref<'_> {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                self.0.serialize(serializer)
            }
        }

        impl $scalar {
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
            pub fn from_be_bytes(bytes: [u8; mem::size_of::<$inner>()]) -> Self {
                Self(Box::new(<$inner>::from_be_bytes(bytes)))
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
                self.0
                    .to_be_bytes()
                    .as_ref()
                    .to_sql(&Type::ANY, &mut output)
                    .unwrap();
                Ok(Some(output.freeze()))
            }
        }
    };
}

impl_common_for_num256!(Int256, Int256Ref<'a>, I256, Int256);

impl ToPrimitive for Int256 {
    fn to_i64(&self) -> Option<i64> {
        Some(self.0.as_i64())
    }

    fn to_u64(&self) -> Option<u64> {
        Some(self.0.as_u64())
    }
}

impl ToPrimitive for Int256Ref<'_> {
    fn to_i64(&self) -> Option<i64> {
        Some(self.0.as_i64())
    }

    fn to_u64(&self) -> Option<u64> {
        Some(self.0.as_u64())
    }
}

impl FromPrimitive for Int256 {
    fn from_i64(n: i64) -> Option<Self> {
        Some(I256::from(n).into())
    }

    fn from_u64(n: u64) -> Option<Self> {
        Some(I256::from(n).into())
    }
}

impl From<I256> for Int256 {
    fn from(value: I256) -> Self {
        Self(Box::new(value))
    }
}

impl FromStr for Int256 {
    type Err = ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        I256::from_str(s).map(Into::into)
    }
}

// todo , dup code
macro_rules! impl_from {
    ($T:ty, $from_ty:path) => {
        impl core::convert::From<$T> for Int256 {
            #[inline]
            fn from(t: $T) -> Self {
                $from_ty(t).unwrap()
            }
        }
    };
}

macro_rules! impl_try_from_int256 {
    ($from_ty:ty, $to_ty:ty, $convert:path, $err:expr) => {
        impl core::convert::TryFrom<$from_ty> for $to_ty {
            type Error = anyhow::Error;

            fn try_from(value: $from_ty) -> Result<Self, Self::Error> {
                // todo
                Ok($convert(&value).unwrap())
                //$convert(&value).ok_or_else(|| Self::Error::from($err))
            }
        }
    };
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

impl_try_from_int256!(
    Int256Ref<'_>,
    i32,
    Int256Ref::to_i32,
    "Failed to convert to i32"
);

// impl<'a> From<Int256> for Int256Ref<'a> {
//     fn from(value: Int256) -> Self {
//         value.as_scalar_ref()
//     }
// }

impl From<Int256Ref<'_>> for Int256 {
    fn from(value: Int256Ref<'_>) -> Self {
        Self(Box::new(value.0.clone()))
    }
}

impl BitAnd for Int256 {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        Self(Box::new(self.0.as_ref() & rhs.0.as_ref()))
    }
}

impl BitOr for Int256 {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(Box::new(self.0.as_ref() | rhs.0.as_ref()))
    }
}

impl BitXor for Int256 {
    type Output = Self;

    fn bitxor(self, rhs: Self) -> Self::Output {
        Self(Box::new(self.0.as_ref() ^ rhs.0.as_ref()))
    }
}

impl Not for Int256 {
    type Output = Self;

    fn not(self) -> Self::Output {
        Self(Box::new(self.0.as_ref().not()))
    }
}

impl Not for Int256Ref<'_> {
    type Output = Self;

    fn not(self) -> Self::Output {
        todo!()
    }
}