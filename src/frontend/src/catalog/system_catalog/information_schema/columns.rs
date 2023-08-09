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

use risingwave_common::catalog::INFORMATION_SCHEMA_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::BuiltinView;

/// The view `columns` contains information about all table columns (or view columns) in the
/// database. System columns (ctid, etc.) are not included. Only those columns are shown that the
/// current user has access to (by way of being the owner or having some privilege).
/// Ref: [`https://www.postgresql.org/docs/current/infoschema-columns.html`]
///
/// In RisingWave, `columns` also contains all materialized views' columns.
pub static INFORMATION_SCHEMA_COLUMNS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "columns",
    schema: INFORMATION_SCHEMA_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "table_catalog"),
        (DataType::Varchar, "table_schema"),
        (DataType::Varchar, "table_name"),
        (DataType::Varchar, "column_name"),
        (DataType::Varchar, "column_default"),
        (DataType::Int32, "character_maximum_length"),
        (DataType::Int32, "ordinal_position"),
        (DataType::Varchar, "is_nullable"),
        (DataType::Varchar, "collation_name"),
        (DataType::Varchar, "udt_schema"),
        (DataType::Varchar, "data_type"),
        (DataType::Varchar, "udt_name"),
    ],
    sql: "SELECT CURRENT_DATABASE() AS table_catalog, \
                s.name AS table_schema, \
                r.name AS table_name, \
                c.name AS column_name, \
                NULL AS column_default, \
                NULL::integer AS character_maximum_length, \
                c.position AS ordinal_position, \
                'YES' AS is_nullable, \
                NULL AS collation_name, \
                'pg_catalog' AS udt_schema, \
                c.data_type AS data_type, \
                c.udt_type AS udt_name \
            FROM rw_catalog.rw_columns c \
            LEFT JOIN rw_catalog.rw_relations r ON c.relation_id = r.id \
            JOIN rw_catalog.rw_schemas s ON s.id = r.schema_id\
    "
    .to_string(),
});
