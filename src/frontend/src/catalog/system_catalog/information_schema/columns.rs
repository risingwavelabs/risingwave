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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The view `columns` contains information about all table columns (or view columns) in the
/// database. System columns (ctid, etc.) are not included. Only those columns are shown that the
/// current user has access to (by way of being the owner or having some privilege).
/// Ref: [`https://www.postgresql.org/docs/current/infoschema-columns.html`]
///
/// In RisingWave, `columns` also contains all materialized views' columns.
#[system_catalog(
    view,
    "information_schema.columns",
    "SELECT CURRENT_DATABASE() AS table_catalog,
        s.name AS table_schema,
        r.name AS table_name,
        c.name AS column_name,
        NULL AS column_default,
        NULL::integer AS character_maximum_length,
        NULL::integer AS numeric_precision,
        NULL::integer AS numeric_scale,
        c.position AS ordinal_position,
        'YES' AS is_nullable,
        NULL AS collation_name,
        'pg_catalog' AS udt_schema,
        CASE
            WHEN c.data_type = 'varchar' THEN 'character varying'
            ELSE c.data_type
        END AS data_type,
        c.udt_type AS udt_name
    FROM rw_catalog.rw_columns c
    LEFT JOIN rw_catalog.rw_relations r ON c.relation_id = r.id
    JOIN rw_catalog.rw_schemas s ON s.id = r.schema_id
    WHERE c.is_hidden = false"
)]
#[derive(Fields)]
struct Column {
    table_catalog: String,
    table_schema: String,
    table_name: String,
    column_name: String,
    column_default: String,
    character_maximum_length: i32,
    numeric_precision: i32,
    numeric_scale: i32,
    ordinal_position: i32,
    is_nullable: String,
    collation_name: String,
    udt_schema: String,
    data_type: String,
    udt_name: String,
}
