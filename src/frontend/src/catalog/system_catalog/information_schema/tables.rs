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

use std::string::ToString;
use std::sync::LazyLock;

use risingwave_common::catalog::INFORMATION_SCHEMA_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::BuiltinView;

/// The view tables contains all tables and views defined in the current database. Only those tables
/// and views are shown that the current user has access to (by way of being the owner or having
/// some privilege).
/// Ref: [`https://www.postgresql.org/docs/current/infoschema-tables.html`]
///
/// In RisingWave, `tables` contains all relations.
pub static INFORMATION_SCHEMA_TABLES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "tables",
    schema: INFORMATION_SCHEMA_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "table_catalog"),
        (DataType::Varchar, "table_schema"),
        (DataType::Varchar, "table_name"),
        (DataType::Varchar, "table_type"),
        (DataType::Varchar, "is_insertable_into"),
    ],
    sql: "SELECT CURRENT_DATABASE() AS table_catalog, \
                s.name AS table_schema, \
                r.name AS table_name, \
                CASE r.relation_type \
                    WHEN 'materialized view' THEN 'MATERIALIZED VIEW' \
                    WHEN 'table' THEN 'BASE TABLE' \
                    WHEN 'system table' THEN 'SYSTEM TABLE' \
                    WHEN 'view' THEN 'VIEW' \
                ELSE UPPER(r.relation_type) \
                END AS table_type, \
                CASE \
                WHEN r.relation_type = 'table' \
                THEN 'YES' \
                ELSE 'NO' \
                END AS is_insertable_into \
            FROM rw_catalog.rw_relations r \
            JOIN rw_catalog.rw_schemas s ON r.schema_id = s.id \
        ORDER BY table_schema, table_name"
        .to_string(),
});
