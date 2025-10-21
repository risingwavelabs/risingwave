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

/// Mapping from sql name to system locale groups.
/// Reference: `https://www.postgresql.org/docs/current/catalog-pg-collation.html`.
#[system_catalog(view, "pg_catalog.pg_collation")]
#[derive(Fields)]
struct PgCollation {
    oid: i32,
    collname: String,
    collnamespace: i32,
    collowner: i32,
    collprovider: i32,
    collisdeterministic: bool,
    collencoding: i32,
    collcollate: String,
    collctype: String,
    colliculocale: String,
    collversion: String,
}
