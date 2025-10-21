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

/// The view `table_constraints` contains all constraints belonging to tables that the current user owns or has some privilege other than SELECT on.
/// Ref: `https://www.postgresql.org/docs/current/infoschema-table-constraints.html`
/// Limitation:
/// This view assume the constraint schema is the same as the table schema, since `pg_catalog`.`pg_constraint` only support primary key.
#[system_catalog(
    view,
    "information_schema.table_constraints",
    "SELECT CURRENT_DATABASE() AS constraint_catalog,
            pg_namespace.nspname AS constraint_schema,
            pg_constraint.conname AS constraint_name,
            CURRENT_DATABASE() AS table_catalog,
            pg_namespace.nspname AS table_schema,
            pg_class.relname AS table_name,
            CASE
                WHEN contype = 'p' THEN 'PRIMARY KEY'
                WHEN contype = 'u' THEN 'UNIQUE'
                WHEN contype = 'c' THEN 'CHECK'
                WHEN contype = 'x' THEN 'EXCLUDE'
                ELSE contype
            END AS constraint_type,
            CASE
                WHEN condeferrable THEN 'YES'
                ELSE 'NO'
            END AS is_deferrable,
            'NO' AS initially_deferred,
            CASE
                WHEN convalidated THEN 'YES'
                ELSE 'NO'
            END AS enforced
        FROM pg_catalog.pg_constraint
        JOIN pg_catalog.pg_class ON pg_constraint.conrelid = pg_class.oid
        JOIN rw_catalog.rw_relations ON rw_relations.id = pg_class.oid
        JOIN pg_catalog.pg_namespace ON pg_class.relnamespace = pg_namespace.oid
        WHERE rw_relations.relation_type != 'table' or (rw_relations.relation_type = 'table' and has_table_privilege(pg_constraint.conrelid, 'INSERT, UPDATE, DELETE'))
        ORDER BY constraint_catalog, constraint_schema, constraint_name"
)]
#[derive(Fields)]
struct TableConstraints {
    constraint_catalog: String,
    constraint_schema: String,
    constraint_name: String,
    table_catalog: String,
    table_schema: String,
    table_name: String,
    constraint_type: String,
    is_deferrable: String,
    initially_deferred: String,
    enforced: String,
}
