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

/// The catalog `pg_index` contains part of the information about indexes.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-index.html`]
#[system_catalog(
    view,
    "pg_catalog.pg_index",
    "SELECT id AS indexrelid,
        primary_table_id AS indrelid,
        ARRAY_LENGTH(indkey)::smallint AS indnatts,
        false AS indisunique,
        indkey,
        ARRAY[]::smallint[] as indoption,
        NULL AS indexprs,
        NULL AS indpred,
        FALSE AS indisprimary
    FROM rw_catalog.rw_indexes"
)]
#[derive(Fields)]
struct PgIndex {
    indexrelid: i32,
    indrelid: i32,
    indnatts: i16,
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
}
