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

/// The catalog `pg_index` contains part of the information about indexes.
/// Ref: `https://www.postgresql.org/docs/current/catalog-pg-index.html`
#[system_catalog(
    view,
    "pg_catalog.pg_index",
    "SELECT id AS indexrelid,
        primary_table_id AS indrelid,
        ARRAY_LENGTH(key_columns || include_columns)::smallint AS indnatts,
        ARRAY_LENGTH(key_columns)::smallint AS indnkeyatts,
        false AS indisunique,
        key_columns || include_columns AS indkey,
        ARRAY[]::smallint[] as indoption,
        NULL AS indexprs,
        NULL AS indpred,
        FALSE AS indisprimary,
        ARRAY[]::int[] AS indclass,
        false AS indisexclusion,
        true AS indimmediate,
        false AS indisclustered,
        true AS indisvalid,
        false AS indcheckxmin,
        true AS indisready,
        true AS indislive,
        false AS indisreplident
    FROM rw_catalog.rw_indexes
    UNION ALL
    SELECT c.relation_id AS indexrelid,
        c.relation_id AS indrelid,
        COUNT(*)::smallint AS indnatts,
        COUNT(*)::smallint AS indnkeyatts,
        true AS indisunique,
        ARRAY_AGG(c.position)::smallint[] AS indkey,
        ARRAY[]::smallint[] as indoption,
        NULL AS indexprs,
        NULL AS indpred,
        TRUE AS indisprimary,
        ARRAY[]::int[] AS indclass,
        false AS indisexclusion,
        true AS indimmediate,
        false AS indisclustered,
        true AS indisvalid,
        false AS indcheckxmin,
        true AS indisready,
        true AS indislive,
        false AS indisreplident
    FROM rw_catalog.rw_columns c
    WHERE c.is_primary_key = true AND c.is_hidden = false
    AND c.relation_id IN (
        SELECT id
        FROM rw_catalog.rw_tables
    )
    GROUP BY c.relation_id"
)]
#[derive(Fields)]
struct PgIndex {
    indexrelid: i32,
    indrelid: i32,
    indnatts: i16,
    indnkeyatts: i16,
    // We return false as default to indicate that this is NOT a unique index
    indisunique: bool,
    indkey: Vec<i16>,
    indoption: Vec<i16>,
    // None. We don't have `pg_node_tree` type yet, so we use `text` instead.
    indexprs: Option<String>,
    // None. We don't have `pg_node_tree` type yet, so we use `text` instead.
    indpred: Option<String>,
    // TODO: we return false as the default value.
    indisprimary: bool,
    // Empty array. We only have a dummy implementation of `pg_opclass` yet.
    indclass: Vec<i32>,

    // Unused columns. Kept for compatibility with PG.
    indisexclusion: bool,
    indimmediate: bool,
    indisclustered: bool,
    indisvalid: bool,
    indcheckxmin: bool,
    indisready: bool,
    indislive: bool,
    indisreplident: bool,
}
