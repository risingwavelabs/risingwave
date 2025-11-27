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

/// The view `pg_matviews` provides access to useful information about each materialized view in the
/// database.
/// Ref: `https://www.postgresql.org/docs/current/view-pg-matviews.html`
#[system_catalog(
    view,
    "pg_catalog.pg_matviews",
    "SELECT
       s.name as schemaname,
       mv.name as matviewname,
       mv.owner as matviewowner,
       NULL AS tablespace,
       false AS hasindexes,
       true AS ispopulated,
       mv.definition as definition
     FROM rw_materialized_views mv
     JOIN rw_schemas s ON mv.schema_id = s.id"
)]
#[derive(Fields)]
struct PgMatview {
    schemaname: String,
    matviewname: String,
    matviewowner: i32,
    tablespace: Option<String>,
    hasindexes: bool,
    ispopulated: bool,
    definition: String,
}
