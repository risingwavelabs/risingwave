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

use bytes::{Bytes, BytesMut};
use postgres_types::{ToSql, Type};

use super::{DatumRef, OrderedF32, OrderedF64, ScalarRefImpl};
use crate::error::Result;
// Used to convert ScalarRef to text format
pub trait ToBinary {
    fn to_binary(&self) -> Result<Option<Bytes>>;
}

// implement use to_sql
macro_rules! implement_using_to_sql {
    ($($scalar_type:ty),*) => {
        $(
            impl ToBinary for $scalar_type {
                fn to_binary(&self) -> Result<Option<Bytes>> {
                    let mut output = BytesMut::new();
                    self.to_sql(&Type::ANY, &mut output).unwrap();
                    Ok(Some(output.freeze()))
                }
            }
        )*
    };
}

implement_using_to_sql! {
    i16,
    i32,
    i64,
    &str,
    OrderedF32,
    OrderedF64,
    bool,
    &[u8]
}

impl ToBinary for ScalarRefImpl<'_> {
    fn to_binary(&self) -> Result<Option<Bytes>> {
        match self {
            ScalarRefImpl::Int16(v) => v.to_binary(),
            ScalarRefImpl::Int32(v) => v.to_binary(),
            ScalarRefImpl::Int64(v) => v.to_binary(),
            ScalarRefImpl::Float32(v) => v.to_binary(),
            ScalarRefImpl::Float64(v) => v.to_binary(),
            ScalarRefImpl::Utf8(v) => v.to_binary(),
            ScalarRefImpl::Bool(v) => v.to_binary(),
            ScalarRefImpl::Decimal(v) => v.to_binary(),
            ScalarRefImpl::Interval(v) => v.to_binary(),
            ScalarRefImpl::NaiveDate(v) => v.to_binary(),
            ScalarRefImpl::NaiveTime(v) => v.to_binary(),
            ScalarRefImpl::NaiveDateTime(v) => v.to_binary(),
            ScalarRefImpl::Timestampz(v) => v.to_binary(),
            ScalarRefImpl::Bytea(v) => v.to_binary(),
            ScalarRefImpl::Struct(_) => todo!(),
            ScalarRefImpl::List(_) => todo!(),
        }
    }
}

impl ToBinary for DatumRef<'_> {
    fn to_binary(&self) -> Result<Option<Bytes>> {
        match self {
            Some(scalar) => scalar.to_binary(),
            None => Ok(None),
        }
    }
}
