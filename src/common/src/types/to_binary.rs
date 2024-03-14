// Copyright 2024 RisingWave Labs
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

use super::{DataType, DatumRef, ListRef, ScalarRefImpl, F32, F64};
use crate::error::NotImplemented;

/// Error type for [`ToBinary`] trait.
#[derive(thiserror::Error, Debug)]
pub enum ToBinaryError {
    #[error(transparent)]
    ToSql(Box<dyn std::error::Error + Send + Sync>),

    #[error(transparent)]
    NotImplemented(#[from] NotImplemented),
}

pub type Result<T> = std::result::Result<T, ToBinaryError>;

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
                            #[allow(clippy::redundant_closure_call)]
                            $accessor(self).to_sql(&Type::ANY, &mut output).map_err(ToBinaryError::ToSql)?;
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
            ScalarRefImpl::List(v) => v.to_binary_with_type(ty),
            ScalarRefImpl::Struct(_) => bail_not_implemented!(
                issue = 7949,
                "the pgwire extended-mode encoding for {ty} is unsupported"
            ),
        }
    }
}

impl<'a> ToBinary for ListRef<'a> {
    fn to_binary_with_type(&self, ty: &DataType) -> Result<Option<Bytes>> {
        // safe since ListRef
        let elem_ty = ty.as_list();

        let array_ty = match elem_ty {
            DataType::Boolean => Type::BOOL_ARRAY,
            DataType::Int16 => Type::INT2_ARRAY,
            DataType::Int32 => Type::INT4_ARRAY,
            DataType::Int64 => Type::INT8_ARRAY,
            DataType::Float32 => Type::FLOAT4_ARRAY,
            DataType::Float64 => Type::FLOAT8_ARRAY,
            DataType::Decimal => Type::NUMERIC_ARRAY,
            DataType::Date => Type::DATE_ARRAY,
            DataType::Varchar => Type::VARCHAR_ARRAY,
            DataType::Time => Type::TIME_ARRAY,
            DataType::Timestamp => Type::TIMESTAMP_ARRAY,
            DataType::Timestamptz => Type::TIMESTAMPTZ_ARRAY,
            DataType::Interval => Type::INTERVAL_ARRAY,
            DataType::Bytea => Type::BYTEA_ARRAY,
            DataType::Jsonb => Type::JSONB_ARRAY,
            DataType::Serial => Type::INT8_ARRAY,
            // INFO: `Int256` not support in `ScalarRefImpl::to_sql`
            // Just let `Array[Int256]` continue `to_sql`, and the `ScalarRefImpl::to_sql` will handle the error.
            DataType::Int256 => Type::NUMERIC_ARRAY,
            DataType::Struct(_) | DataType::List(_)  => bail_not_implemented!(
                issue = 7949,
                "the pgwire extended-mode encoding for lists with more than one dimension ({ty}) is unsupported"
            ),
        };

        let mut buf = BytesMut::new();
        self.to_sql(&array_ty, &mut buf)
            .map_err(ToBinaryError::ToSql)?;

        Ok(Some(buf.freeze()))
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
