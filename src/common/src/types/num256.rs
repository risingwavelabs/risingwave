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
use std::mem;

use bytes::Bytes;
use ethnum::{I256, U256};
use postgres_types::{ToSql, Type};
use serde::{Serialize, Serializer};
use to_text::ToText;

use crate::types::to_binary::ToBinary;
use crate::types::{to_text, DataType, Scalar, ScalarRef};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Int256(I256);

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Uint256(Box<U256>);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Uint256Ref<'a>(&'a U256);

impl Scalar for Uint256 {
    type ScalarRefType<'a> = Uint256Ref<'a>;

    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
        Uint256Ref(self.0.as_ref())
    }
}

impl<'a> ScalarRef<'a> for Uint256Ref<'a> {
    type ScalarType = Uint256;

    fn to_owned_scalar(&self) -> Self::ScalarType {
        Uint256((*self.0).into())
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
        use std::hash::Hash as _;
        self.0.hash(state)
    }
}

impl Serialize for Uint256Ref<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl Uint256Ref<'_> {
    #[inline]
    pub fn to_le_bytes(self) -> [u8; mem::size_of::<U256>()] {
        self.0.to_le_bytes()
    }
}

impl Int256 {
    #[inline]
    pub fn to_le_bytes(self) -> [u8; mem::size_of::<I256>()] {
        self.0.to_le_bytes()
    }
}

/// Implement `Scalar` for `Int256`.
impl Scalar for Box<Int256> {
    type ScalarRefType<'a> = &'a Int256;

    fn as_scalar_ref(&self) -> Self::ScalarRefType<'_> {
        self.as_ref()
    }
}

/// Implement `ScalarRef` for `Int256`.
impl<'a> ScalarRef<'a> for &'a Int256 {
    type ScalarType = Box<Int256>;

    fn to_owned_scalar(&self) -> Box<Int256> {
        Box::new(Int256(self.0))
    }

    fn hash_scalar<H: std::hash::Hasher>(&self, state: &mut H) {
        use std::hash::Hash as _;
        self.0.hash(state)
    }
}

impl Serialize for Int256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl ToText for Uint256Ref<'_> {
    fn write<W: Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: Write>(&self, _ty: &DataType, f: &mut W) -> std::fmt::Result {
        self.write(f)
    }
}

impl ToBinary for Uint256Ref<'_> {
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

impl ToText for Int256 {
    fn write<W: std::fmt::Write>(&self, f: &mut W) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }

    fn write_with_type<W: std::fmt::Write>(
        &self,
        _ty: &crate::types::DataType,
        f: &mut W,
    ) -> std::fmt::Result {
        self.write(f)
    }
}

impl ToBinary for Int256 {
    fn to_binary_with_type(
        &self,
        _ty: &crate::types::DataType,
    ) -> crate::error::Result<Option<bytes::Bytes>> {
        let mut output = bytes::BytesMut::new();
        self.0
            .to_be_bytes()
            .as_ref()
            .to_sql(&Type::ANY, &mut output)
            .unwrap();
        Ok(Some(output.freeze()))
    }
}
