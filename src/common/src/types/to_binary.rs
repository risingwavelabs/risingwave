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

use bytes::{Bytes, BytesMut};
use postgres_types::{ToSql, Type};

use super::{DataType, DatumRef, ScalarRefImpl, F32, F64};
use crate::error::Result;
// Used to convert ScalarRef to text format
pub trait ToBinary {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>>;
}

// implement use to_sql
macro_rules! implement_using_to_sql {
    ($({ $scalar_type:ty, $data_type:ident, $accessor:expr } ),*) => {
        $(
            impl ToBinary for $scalar_type {
                fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>> {
                    match ty {
                        DataType::$data_type => {
                            let mut output = BytesMut::new();
                            $accessor(self).to_sql(&Type::ANY, &mut output).unwrap();
                            Ok(Some(output.freeze()))
                        },
                        _ => unreachable!(),
                    }
                }
            }
        )*
    };
}

implement_using_to_sql! {
    { i16, Int16, |x| x },
    { i32, Int32, |x| x },
    { i64, Int64, |x| x },
    { &str, Varchar, |x| x },
    { F32, Float32, |x: &F32| x.0 },
    { F64, Float64, |x: &F64| x.0 },
    { bool, Boolean, |x| x },
    { &[u8], Bytea, |x| x }
}

impl ToBinary for ScalarRefImpl<'_> {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>> {
        match self {
            ScalarRefImpl::Int16(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Int32(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Int64(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Int256(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Serial(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Float32(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Float64(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Utf8(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Bool(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Decimal(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Interval(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Date(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Timestamp(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Timestamptz(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Time(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Bytea(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Jsonb(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Struct(_) => todo!(),
            ScalarRefImpl::List(_) => todo!(),
        }
    }
}

impl ToBinary for DatumRef<'_> {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>> {
        match self {
            Some(scalar) => scalar.to_binary_with_type(ty),
            None => Ok(None),
        }
    }
}
