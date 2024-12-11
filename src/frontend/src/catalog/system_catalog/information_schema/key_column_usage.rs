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

/// The view `key_column_usage` contains all constraints belonging to tables that the current user owns or has some privilege other than SELECT on.
/// Ref: [`https://www.postgresql.org/docs/current/infoschema-key-column-usage.html`]
/// Limitation:
/// This view assume the constraint schema is the same as the table schema, since `pg_clatalog`.`pg_constraint` only support primrary key.
#[system_catalog(
    view,
    "information_schema.key_column_usage",
    "WITH key_column_usage_without_name AS (
        SELECT CURRENT_DATABASE() AS constraint_catalog,
            pg_namespace.nspname AS constraint_schema,
            pg_constraint.conname AS constraint_name,
            CURRENT_DATABASE() AS table_catalog,
            pg_namespace.nspname AS table_schema,
            pg_class.relname AS table_name,
            unnest(conkey) as col_id,
            conrelid as table_id
        FROM pg_catalog.pg_constraint
        JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid
        JOIN rw_catalog.rw_relations ON rw_relations.id = pg_class.oid
        JOIN pg_catalog.pg_namespace ON pg_class.relnamespace = pg_namespace.oid
        WHERE rw_relations.relation_type != 'table' or (rw_relations.relation_type = 'table' and has_table_privilege(pg_constraint.conrelid, 'INSERT, UPDATE, DELETE'))
        ORDER BY constraint_catalog, constraint_schema, constraint_name
    )
    SELECT constraint_catalog, constraint_schema, constraint_name, table_catalog, table_schema, table_name, 
           name as column_name, rw_columns.position as ordinal_position, NULL::int as position_in_unique_constraint
    FROM key_column_usage_without_name
    JOIN rw_catalog.rw_columns ON 
        rw_columns.position = key_column_usage_without_name.col_id AND 
        rw_columns.relation_id = key_column_usage_without_name.table_id"
)]
#[derive(Fields)]
struct KeyColumnUsage {
    constraint_catalog: String,
    constraint_schema: String,
    constraint_name: String,
    table_catalog: String,
    table_schema: String,
    table_name: String,
    column_name: String,
    ordinal_position: i32,
    position_in_unique_constraint: i32,
}
