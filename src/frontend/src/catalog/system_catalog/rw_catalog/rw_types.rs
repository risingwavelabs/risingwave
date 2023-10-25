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

use std::sync::LazyLock;

use itertools::Itertools;
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::for_all_base_types;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

macro_rules! impl_pg_type_data {
    ($( { $enum:ident | $oid:literal | $oid_array:literal | $name:ident | $input:ident | $len:literal | $elem:literal} )*) => {
        &[
            $(
            ($oid, stringify!($name), stringify!($input),$elem),
            )*
            // Note: rw doesn't support `text` type, returning it is just a workaround to be compatible
            // with PostgreSQL.
            (25, "text", "textin",0),
            (1301, "rw_int256", "rw_int256_in",0),
            // Note: Here is only to avoid some components of psql from not being able to find relevant results, causing errors. We will not use it in the RW.
            (1000, "_bool", "array_in",16),
            (1001, "_bytea", "array_in",17),
            (1016, "_int8", "array_in",20),
            (1005, "_int2", "array_in",21),
            (1021, "_float4", "array_in",700),
            (1022, "_float8", "array_in",701),
            (1015, "_varchar", "array_in",1043),
            (1182, "_date", "array_in",1082),
            (1183, "_time", "array_in",2083),
            (1115, "_timestamp", "array_in",1114),
            (1185, "_timestamptz", "array_in",1184),
            (1187, "_interval", "array_in",1186),
            (1231, "_numeric", "array_in",1700),
            (3807, "_jsonb", "array_in",3802),
        ]
    }
}
pub const RW_TYPE_DATA: &[(i32, &str, &str, i32)] = for_all_base_types! { impl_pg_type_data };

/// `rw_types` stores all supported types in the database.
pub static RW_TYPES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "rw_types",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "id"),
        (DataType::Varchar, "name"),
        (DataType::Varchar, "input_oid"),
        (DataType::Int32, "elem"),
    ],
    pk: &[0],
});

impl SysCatalogReaderImpl {
    pub fn read_rw_types(&self) -> Result<Vec<OwnedRow>> {
        Ok(RW_TYPE_DATA
            .iter()
            .map(|(id, name, input, elem)| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(*id)),
                    Some(ScalarImpl::Utf8(name.to_string().into())),
                    Some(ScalarImpl::Utf8(input.to_string().into())),
                    Some(ScalarImpl::Int32(*elem)),
                ])
            })
            .collect_vec())
    }
}
