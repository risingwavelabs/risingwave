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

/// The catalog `pg_class` catalogs tables and most everything else that has columns or is otherwise
/// similar to a table. Ref: [`https://www.postgresql.org/docs/current/catalog-pg-class.html`]
/// todo: should we add internal tables as well?
#[system_catalog(view, "pg_catalog.pg_class",
    "SELECT id AS oid, name AS relname, schema_id AS relnamespace, owner AS relowner, 'p' as relpersistence,
    CASE
        WHEN relation_type = 'table' THEN 'r'
        WHEN relation_type = 'system table' THEN 'r'
        WHEN relation_type = 'index' THEN 'i'
        WHEN relation_type = 'view' THEN 'v'
        WHEN relation_type = 'materialized view' THEN 'm'
    END relkind,
    0 AS relam,
    0 AS reltablespace,
    ARRAY[]::varchar[] AS reloptions,
    FALSE AS relispartition,
    null AS relpartbound
    FROM nim_catalog.nim_relations
")]
#[derive(Fields)]
struct PgClass {
    oid: i32,
    relname: String,
    relnamespace: i32,
    relowner: i32,
    // p = permanent table, u = unlogged table, t = temporary table
    relpersistence: String,
    // r = ordinary table, i = index, S = sequence, t = TOAST table, v = view, m = materialized view,
    // c = composite type, f = foreign table, p = partitioned table, I = partitioned index
    relkind: String,
    relam: i32,
    reltablespace: i32,
    reloptions: Vec<String>,
    relispartition: bool,
    // PG uses pg_node_tree type but RW doesn't support it
    relpartbound: Option<String>,
}
