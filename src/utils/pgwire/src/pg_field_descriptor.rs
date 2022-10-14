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

use std::str::FromStr;

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct PgFieldDescriptor {
    name: String,
    table_oid: i32,
    col_attr_num: i16,

    // NOTE: Static code for data type. To see the oid of a specific type in Postgres,
    // use the following command:
    //   SELECT oid FROM pg_type WHERE typname = 'int4';
    type_oid: TypeOid,

    type_len: i16,
    type_modifier: i32,
    format_code: i16,
}

impl PgFieldDescriptor {
    pub fn new(name: String, type_oid: TypeOid) -> Self {
        let type_modifier = -1;
        let format_code = 0;
        let table_oid = 0;
        let col_attr_num = 0;
        let type_len = match type_oid {
            TypeOid::Boolean => 1,
            TypeOid::Int | TypeOid::Float4 | TypeOid::Date => 4,
            TypeOid::BigInt
            | TypeOid::Float8
            | TypeOid::Timestamp
            | TypeOid::Time
            | TypeOid::Timestamptz => 8,
            TypeOid::SmallInt => 2,
            TypeOid::Varchar | TypeOid::Decimal | TypeOid::Interval => -1,
        };

        Self {
            name,
            table_oid,
            col_attr_num,
            type_oid,
            type_len,
            type_modifier,
            format_code,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn get_table_oid(&self) -> i32 {
        self.table_oid
    }

    pub fn get_col_attr_num(&self) -> i16 {
        self.col_attr_num
    }

    pub fn get_type_oid(&self) -> TypeOid {
        self.type_oid
    }

    pub fn get_type_len(&self) -> i16 {
        self.type_len
    }

    pub fn get_type_modifier(&self) -> i32 {
        self.type_modifier
    }

    pub fn get_format_code(&self) -> i16 {
        self.format_code
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TypeOid {
    Boolean,
    BigInt,
    SmallInt,
    Int,
    Float4,
    Float8,
    Varchar,
    Date,
    Time,
    Timestamp,
    Timestamptz,
    Decimal,
    Interval,
}

#[derive(Clone, Debug, Error)]
#[error("oid:{0} can't be supported")]
pub struct TypeOidError(i32);

impl TypeOid {
    // TypeOid can refer from
    // https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
    pub fn as_type(oid: i32) -> Result<TypeOid, TypeOidError> {
        match oid {
            1043 => Ok(TypeOid::Varchar),
            16 => Ok(TypeOid::Boolean),
            20 => Ok(TypeOid::BigInt),
            21 => Ok(TypeOid::SmallInt),
            23 => Ok(TypeOid::Int),
            700 => Ok(TypeOid::Float4),
            701 => Ok(TypeOid::Float8),
            1082 => Ok(TypeOid::Date),
            1083 => Ok(TypeOid::Time),
            1114 => Ok(TypeOid::Timestamp),
            1184 => Ok(TypeOid::Timestamptz),
            1700 => Ok(TypeOid::Decimal),
            1186 => Ok(TypeOid::Interval),
            v => Err(TypeOidError(v)),
        }
    }

    // NOTE:
    // Refer https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat when add new TypeOid.
    // Be careful to distinguish oid from array_type_oid.
    // Such as:
    //  https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat#L347
    //  For Numeric(aka Decimal): oid = 1700, array_type_oid = 1231
    pub fn as_number(&self) -> i32 {
        match self {
            TypeOid::Boolean => 16,
            TypeOid::BigInt => 20,
            TypeOid::SmallInt => 21,
            TypeOid::Int => 23,
            TypeOid::Float4 => 700,
            TypeOid::Float8 => 701,
            TypeOid::Varchar => 1043,
            TypeOid::Date => 1082,
            TypeOid::Time => 1083,
            TypeOid::Timestamp => 1114,
            TypeOid::Timestamptz => 1184,
            TypeOid::Decimal => 1700,
            TypeOid::Interval => 1186,
        }
    }
}

impl FromStr for TypeOid {
    type Err = TypeOidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.to_ascii_lowercase();
        match s.as_str() {
            "bool" | "boolean" => Ok(TypeOid::Boolean),
            "bigint" | "int8" => Ok(TypeOid::BigInt),
            "smallint" | "int2" => Ok(TypeOid::SmallInt),
            "int" | "int4" => Ok(TypeOid::Int),
            "float4" => Ok(TypeOid::Float4),
            "float8" => Ok(TypeOid::Float8),
            "varchar" => Ok(TypeOid::Varchar),
            "date" => Ok(TypeOid::Date),
            "time" => Ok(TypeOid::Time),
            "timestamp" => Ok(TypeOid::Timestamp),
            "timestamptz" => Ok(TypeOid::Timestamptz),
            "decimal" => Ok(TypeOid::Decimal),
            "interval" => Ok(TypeOid::Interval),
            _ => Err(TypeOidError(0)),
        }
    }
}
