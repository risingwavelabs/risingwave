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

/// The catalog `pg_tablespace` stores information about the available tablespaces.
/// Ref: `https://www.postgresql.org/docs/current/catalog-pg-tablespace.html`
/// This is introduced only for pg compatibility and is not used in our system.
#[system_catalog(view, "pg_catalog.pg_tablespace")]
#[derive(Fields)]
struct PgTablespace {
    oid: i32,
    spcname: String,
    spcowner: i32,
    spcacl: Vec<String>,
    spcoptions: String,
}
