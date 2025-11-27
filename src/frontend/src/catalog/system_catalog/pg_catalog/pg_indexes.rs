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

/// The view `pg_indexes` provides access to useful information about each index in the database.
/// Ref: `https://www.postgresql.org/docs/current/view-pg-indexes.html`
#[system_catalog(
    view,
    "pg_catalog.pg_indexes",
    "SELECT s.name AS schemaname,
            t.name AS tablename,
            i.name AS indexname,
            NULL AS tablespace,
            i.definition AS indexdef
        FROM rw_catalog.rw_indexes i
        JOIN rw_catalog.rw_tables t ON i.primary_table_id = t.id
        JOIN rw_catalog.rw_schemas s ON i.schema_id = s.id
    UNION ALL
    SELECT s.name AS schemaname,
            t.name AS tablename,
            i.name AS indexname,
            NULL AS tablespace,
            i.definition AS indexdef
        FROM rw_catalog.rw_indexes i
        JOIN rw_catalog.rw_materialized_views t ON i.primary_table_id = t.id
        JOIN rw_catalog.rw_schemas s ON i.schema_id = s.id
    UNION ALL
    SELECT s.name AS schemaname,
            t.name AS tablename,
            concat(t.name, '_pkey') AS indexname,
            NULL AS tablespace,
            '' AS indexdef
        FROM rw_catalog.rw_tables t
        JOIN rw_catalog.rw_schemas s ON t.schema_id = s.id
        WHERE t.id IN (
            SELECT DISTINCT relation_id
            FROM rw_catalog.rw_columns
            WHERE is_primary_key = true AND is_hidden = false
        )
    "
)]
#[derive(Fields)]
struct PgIndexes {
    schemaname: String,
    tablename: String,
    indexname: String,
    tablespace: String,
    indexdef: String,
}
