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

use super::{unnested_list_type, DataType};
use crate::error::ErrorCode;

/// Get type information compatible with Postgres type, such as oid, type length.
impl DataType {
    pub fn type_len(&self) -> i16 {
        match self {
            DataType::Boolean => 1,
            DataType::Int16 => 2,
            DataType::Int32 | DataType::Float32 | DataType::Date => 4,
            DataType::Int64
            | DataType::Serial
            | DataType::Float64
            | DataType::Timestamp
            | DataType::Timestamptz
            | DataType::Time => 8,
            DataType::Decimal
            | DataType::Varchar
            | DataType::Bytea
            | DataType::Interval
            | DataType::Jsonb
            | DataType::Struct(_)
            | DataType::List { .. } => -1,
            _ => todo!(),
        }
    }

    // NOTE:
    // Refer https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat when add new TypeOid.
    // Be careful to distinguish oid from array_type_oid.
    // Such as:
    //  https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat#L347
    //  For Numeric(aka Decimal): oid = 1700, array_type_oid = 1231
    pub fn from_oid(oid: i32) -> crate::error::Result<Self> {
        match oid {
            16 => Ok(DataType::Boolean),
            21 => Ok(DataType::Int16),
            23 => Ok(DataType::Int32),
            20 => Ok(DataType::Int64),
            700 => Ok(DataType::Float32),
            701 => Ok(DataType::Float64),
            1700 => Ok(DataType::Decimal),
            1082 => Ok(DataType::Date),
            1043 => Ok(DataType::Varchar),
            1083 => Ok(DataType::Time),
            1114 => Ok(DataType::Timestamp),
            1184 => Ok(DataType::Timestamptz),
            1186 => Ok(DataType::Interval),
            3802 => Ok(DataType::Jsonb),
            1000 => Ok(DataType::List {
                datatype: Box::new(DataType::Boolean),
            }),
            1005 => Ok(DataType::List {
                datatype: Box::new(DataType::Int16),
            }),
            1007 => Ok(DataType::List {
                datatype: Box::new(DataType::Int32),
            }),
            1016 => Ok(DataType::List {
                datatype: Box::new(DataType::Int64),
            }),
            1021 => Ok(DataType::List {
                datatype: Box::new(DataType::Float32),
            }),
            1022 => Ok(DataType::List {
                datatype: Box::new(DataType::Float64),
            }),
            1231 => Ok(DataType::List {
                datatype: Box::new(DataType::Decimal),
            }),
            1182 => Ok(DataType::List {
                datatype: Box::new(DataType::Date),
            }),
            1015 => Ok(DataType::List {
                datatype: Box::new(DataType::Varchar),
            }),
            1266 => Ok(DataType::List {
                datatype: Box::new(DataType::Time),
            }),
            1115 => Ok(DataType::List {
                datatype: Box::new(DataType::Timestamp),
            }),
            1185 => Ok(DataType::List {
                datatype: Box::new(DataType::Timestamptz),
            }),
            1001 => Ok(DataType::List {
                datatype: Box::new(DataType::Bytea),
            }),
            1187 => Ok(DataType::List {
                datatype: Box::new(DataType::Interval),
            }),
            3807 => Ok(DataType::List {
                datatype: Box::new(DataType::Jsonb),
            }),
            _ => Err(ErrorCode::InternalError(format!("Unsupported oid {}", oid)).into()),
        }
    }

    pub fn to_oid(&self) -> i32 {
        match self {
            DataType::Boolean => 16,
            DataType::Int16 => 21,
            DataType::Int32 => 23,
            DataType::Int64 => 20,
            DataType::Int256 => todo!(),
            DataType::Serial => 20,
            DataType::Float32 => 700,
            DataType::Float64 => 701,
            DataType::Decimal => 1700,
            DataType::Date => 1082,
            DataType::Varchar => 1043,
            DataType::Time => 1083,
            DataType::Timestamp => 1114,
            DataType::Timestamptz => 1184,
            DataType::Interval => 1186,
            // NOTE: Struct type don't have oid in postgres, here we use varchar oid so that struct
            // will be considered as a varchar.
            DataType::Struct(_) => 1043,
            DataType::Jsonb => 3802,
            DataType::Bytea => 17,
            DataType::List { datatype } => match unnested_list_type(datatype.as_ref().clone()) {
                DataType::Boolean => 1000,
                DataType::Int16 => 1005,
                DataType::Int32 => 1007,
                DataType::Int64 => 1016,
                DataType::Int256 => todo!(),
                DataType::Serial => 1016,
                DataType::Float32 => 1021,
                DataType::Float64 => 1022,
                DataType::Decimal => 1231,
                DataType::Date => 1182,
                DataType::Varchar => 1015,
                DataType::Bytea => 1001,
                DataType::Time => 1183,
                DataType::Timestamp => 1115,
                DataType::Timestamptz => 1185,
                DataType::Interval => 1187,
                DataType::Jsonb => 3807,
                DataType::Struct(_) => 1015,
                DataType::List { .. } => unreachable!("Never reach here!"),
            },
        }
    }
}
