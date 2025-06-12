// Copyright 2025 RisingWave Labs
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

use risingwave_common::for_all_base_types;
use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

/// `rw_types` stores all supported types in the database.
#[derive(Fields)]
pub struct RwType {
    #[primary_key]
    pub id: i32,
    pub name: String,
    pub input_oid: String,
    pub typelem: i32,
    pub typarray: i32,
}

#[system_catalog(table, "rw_catalog.rw_types")]
pub fn read_rw_types(_: &SysCatalogReaderImpl) -> Result<Vec<RwType>> {
    let mut rows = vec![];
    for (id, name, input_oid, typelem, typarray) in RW_TYPE_DATA {
        rows.push(RwType {
            id: *id,
            name: name.to_string(),
            input_oid: input_oid.to_string(),
            typelem: *typelem,
            typarray: *typarray,
        });
    }
    Ok(rows)
}

macro_rules! impl_pg_type_data {
    ($( { $enum:ident | $oid:literal | $oid_array:literal | $name:ident | $input:ident | $len:literal } )*) => {
        &[
            $(
            ($oid, stringify!($name), stringify!($input), 0, $oid_array),
            )*
            // Note: rw doesn't support `text` type, returning it is just a workaround to be compatible
            // with PostgreSQL.
            (25, "text", "textin",0,1009),
            (1301, "rw_int256", "rw_int256_in",0,1302),
            (1305, "rw_uint256", "rw_uint256_in",0,1306),
            // Note: Here is only to avoid some components of psql from not being able to find relevant results, causing errors. We will not use it in the RW.
            $(
            ($oid_array, concat!("_", stringify!($name)), "array_in", $oid, 0),
            )*
            (1302, "_rw_int256", "array_in", 1301, 0),
            (1306, "_rw_uint256", "array_in", 1305, 0),
        ]
    }
}
const RW_TYPE_DATA: &[(i32, &str, &str, i32, i32)] = for_all_base_types! { impl_pg_type_data };
