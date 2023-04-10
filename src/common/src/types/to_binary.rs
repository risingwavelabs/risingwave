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
use chrono::{TimeZone, Utc};
use postgres_types::{ToSql, Type};

use super::{DataType, DatumRef, ScalarRefImpl};
use crate::error::Result;
// Used to convert ScalarRef to text format
pub trait ToBinary {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>>;
}

// implement use to_sql
macro_rules! implement_using_to_sql {
    ($({ $scalar_type:ty , $data_type:ident} ),*) => {
        $(
            impl ToBinary for $scalar_type {
                fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>> {
                    match ty {
                        DataType::$data_type => {
                            let mut output = BytesMut::new();
                            self.to_sql(&Type::ANY, &mut output).unwrap();
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
    { i16,Int16  },
    { i32,Int32  },
    { &str,Varchar },
    { crate::types::F32,Float32 },
    { crate::types::F64,Float64 },
    { bool,Boolean },
    { &[u8],Bytea }
}

impl ToBinary for i64 {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>> {
        match ty {
            DataType::Int64 => {
                let mut output = BytesMut::new();
                self.to_sql(&Type::ANY, &mut output).unwrap();
                Ok(Some(output.freeze()))
            }
            DataType::Timestamptz => {
                let secs = self.div_euclid(1_000_000);
                let nsecs = self.rem_euclid(1_000_000) * 1000;
                let instant = Utc.timestamp_opt(secs, nsecs as u32).unwrap();
                let mut out = BytesMut::new();
                // postgres_types::Type::ANY is only used as a placeholder.
                instant
                    .to_sql(&postgres_types::Type::ANY, &mut out)
                    .unwrap();
                Ok(Some(out.freeze()))
            }
            _ => unreachable!(),
        }
    }
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
