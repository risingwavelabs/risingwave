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
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

// TODO: uniform the default data with `TypeOid` under `pg_field_descriptor`.
pub const RW_TYPE_DATA: &[(i32, &str)] = &[
    (16, "bool"),
    (17, "bytea"),
    (20, "int8"),
    (21, "int2"),
    (23, "int4"),
    // Note: rw doesn't support `text` type, returning it is just a workaround to be compatible
    // with PostgreSQL.
    (25, "text"),
    (700, "float4"),
    (701, "float8"),
    (1043, "varchar"),
    (1082, "date"),
    (1083, "time"),
    (1114, "timestamp"),
    (1184, "timestamptz"),
    (1186, "interval"),
    (1301, "rw_int256"),
    (1700, "numeric"),
    (3802, "jsonb"),
];

/// `rw_types` stores all supported types in the database.
pub static RW_TYPES: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "rw_types",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[(DataType::Int32, "id"), (DataType::Varchar, "name")],
    pk: &[0],
});

impl SysCatalogReaderImpl {
    pub fn read_rw_types(&self) -> Result<Vec<OwnedRow>> {
        Ok(RW_TYPE_DATA
            .iter()
            .map(|(id, name)| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(*id)),
                    Some(ScalarImpl::Utf8(name.to_string().into())),
                ])
            })
            .collect_vec())
    }
}
