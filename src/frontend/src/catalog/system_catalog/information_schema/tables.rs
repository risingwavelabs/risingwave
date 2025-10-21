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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The view tables contains all tables and views defined in the current database. Only those tables
/// and views are shown that the current user has access to (by way of being the owner or having
/// some privilege).
/// Ref: `https://www.postgresql.org/docs/current/infoschema-tables.html`
///
/// In RisingWave, `tables` contains all relations.
#[system_catalog(
    view,
    "information_schema.tables",
    "SELECT CURRENT_DATABASE() AS table_catalog,
            s.name AS table_schema,
            r.name AS table_name,
            CASE r.relation_type
                WHEN 'materialized view' THEN 'MATERIALIZED VIEW'
                WHEN 'table' THEN 'BASE TABLE'
                WHEN 'system table' THEN 'SYSTEM TABLE'
                WHEN 'view' THEN 'VIEW'
            ELSE UPPER(r.relation_type)
            END AS table_type,
            CASE
            WHEN r.relation_type = 'table'
            THEN 'YES'
            ELSE 'NO'
            END AS is_insertable_into
        FROM rw_catalog.rw_relations r
        JOIN rw_catalog.rw_schemas s ON r.schema_id = s.id
        ORDER BY table_schema, table_name"
)]
#[derive(Fields)]
struct Table {
    table_catalog: String,
    table_schema: String,
    table_name: String,
    table_type: String,
    is_insertable_into: String,
}
