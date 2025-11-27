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

/// The catalog `pg_range` stores information about range types. This is in addition to the types' entries in `pg_type`.
/// Ref: `https://www.postgresql.org/docs/current/catalog-pg-range.html`
#[system_catalog(view, "pg_catalog.pg_range")]
#[derive(Fields)]
struct PgRange {
    rngtypid: i32,
    rngsubtype: i32,
    rngmultitypid: i32,
    rngcollation: i32,
    rngsubopc: i32,
    rngcanonical: String,
    rngsubdiff: String,
}
