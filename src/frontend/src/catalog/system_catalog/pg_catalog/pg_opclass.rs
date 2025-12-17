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

/// The catalog `pg_opclass` defines index access method operator classes.
/// Reference: `https://www.postgresql.org/docs/current/catalog-pg-opclass.html`.
#[system_catalog(view, "pg_catalog.pg_opclass")]
#[derive(Fields)]
struct PgOpclass {
    oid: i32,
    opcmethod: i32,
    opcname: String,
    opcnamespace: i32,
    opcowner: i32,
    opcfamily: i32,
    opcintype: i32,
    opcdefault: bool,
    opckeytype: i32,
}
