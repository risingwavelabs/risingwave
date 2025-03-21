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
        NULL::integer AS numeric_precision_radix,
        NULL::integer AS numeric_scale,
        NULL::integer AS datetime_precision,
        c.position AS ordinal_position,
        CASE
            WHEN c.is_nullable THEN 'YES'
            ELSE 'NO'
        END AS is_nullable,
        CASE
            WHEN c.data_type = 'varchar' THEN 'character varying'
            ELSE c.data_type
        END AS data_type,
        CURRENT_DATABASE() AS udt_catalog,
        'pg_catalog' AS udt_schema,
        c.udt_type AS udt_name,
        NULL AS character_set_catalog,
        NULL AS character_set_schema,
        NULL AS character_set_name,
        NULL AS collation_catalog,
        NULL AS collation_schema,
        NULL AS collation_name,
        NULL AS domain_catalog,
        NULL AS domain_schema,
        NULL AS domain_name,
        NULL AS scope_catalog,
        NULL AS scope_schema,
        NULL AS scope_name,
        'NO' AS is_identity,
        NULL AS identity_generation,
        NULL AS identity_start,
        NULL AS identity_increment,
        NULL AS identity_maximum,
        NULL AS identity_minimum,
        NULL AS identity_cycle,
        CASE
            WHEN c.is_generated THEN 'ALWAYS'
            ELSE 'NEVER'
        END AS is_generated,
        c.generation_expression,
        NULL AS interval_type
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
    numeric_precision_radix: i32,
    numeric_scale: i32,
    datetime_precision: i32,
    ordinal_position: i32,
    is_nullable: String,
    data_type: String,
    udt_catalog: String,
    udt_schema: String,
    udt_name: String,
    character_set_catalog: String,
    character_set_schema: String,
    character_set_name: String,
    collation_catalog: String,
    collation_schema: String,
    collation_name: String,
    domain_catalog: String,
    domain_schema: String,
    domain_name: String,
    scope_catalog: String,
    scope_schema: String,
    scope_name: String,
    is_identity: String,
    identity_generation: String,
    identity_start: String,
    identity_increment: String,
    identity_maximum: String,
    identity_minimum: String,
    identity_cycle: String,
    is_generated: String,
    generation_expression: String,
    interval_type: String,
}
