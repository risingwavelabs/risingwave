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

use bytes::Bytes;
use ethnum::{I256, U256};
use postgres_types::{ToSql, Type};
use serde::{Serialize, Serializer};
use to_text::ToText;

use crate::types::to_binary::ToBinary;
use crate::types::{to_text, DataType, Scalar, ScalarRef};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Uint256(Box<U256>);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Uint256Ref<'a>(&'a U256);

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Default, Hash)]
pub struct Int256(Box<I256>);
#[derive(Debug, Copy, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub struct Int256Ref<'a>(&'a I256);

macro_rules! impl_common_for_num256 {
    ($scalar:ident, $scalar_ref:ident < $gen:tt > , $inner:ty) => {
        impl Scalar for $scalar {
            type ScalarRefType<'a> = $scalar_ref<$gen>;

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

        impl $scalar_ref<'_> {
            #[inline]
            pub fn to_le_bytes(self) -> [u8; mem::size_of::<$inner>()] {
                self.0.to_le_bytes()
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

impl_common_for_num256!(Uint256, Uint256Ref<'a>, U256);
impl_common_for_num256!(Int256, Int256Ref<'a>, I256);
