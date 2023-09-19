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

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::BuiltinView;

/// The catalog `pg_type` stores information about data types.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-type.html`]
pub static PG_TYPE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_type",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "oid"),
        (DataType::Varchar, "typname"),
        // 0
        (DataType::Int32, "typelem"),
        // 0
        (DataType::Int32, "typarray"),
        // FIXME: Should be regproc type
        (DataType::Varchar, "typinput"),
        // false
        (DataType::Boolean, "typnotnull"),
        // 0
        (DataType::Int32, "typbasetype"),
        // -1
        (DataType::Int32, "typtypmod"),
        // 0
        (DataType::Int32, "typcollation"),
        // 0
        (DataType::Int32, "typlen"),
        // should be pg_catalog oid.
        (DataType::Int32, "typnamespace"),
        // 'b'
        (DataType::Varchar, "typtype"),
        // 0
        (DataType::Int32, "typrelid"),
        // None
        (DataType::Varchar, "typdefault"),
        // None
        (DataType::Varchar, "typcategory"),
        // None
        (DataType::Int32, "typreceive"),
    ],
    sql: "SELECT t.id AS oid, \
                t.name AS typname, \
                0 AS typelem, \
                0 AS typarray, \
                t.input_oid AS typinput, \
                false AS typnotnull, \
                0 AS typbasetype, \
                -1 AS typtypmod, \
                0 AS typcollation, \
                0 AS typlen, \
                s.id AS typnamespace, \
                'b' AS typtype, \
                0 AS typrelid, \
                NULL AS typdefault, \
                NULL AS typcategory, \
                NULL::integer AS typreceive \
            FROM rw_catalog.rw_types t \
            JOIN rw_catalog.rw_schemas s \
            ON s.name = 'pg_catalog'"
        .to_string(),
});
