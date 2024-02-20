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

/// The catalog `pg_type` stores information about data types.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-type.html`]
#[system_catalog(
    view,
    "pg_catalog.pg_type",
    "SELECT t.id AS oid,
        t.name AS typname,
        t.typelem AS typelem,
        t.typarray AS typarray,
        t.input_oid AS typinput,
        false AS typnotnull,
        0 AS typbasetype,
        -1 AS typtypmod,
        0 AS typcollation,
        0 AS typlen,
        s.id AS typnamespace,
        'b' AS typtype,
        0 AS typrelid,
        NULL AS typdefault,
        NULL AS typcategory,
        NULL::integer AS typreceive
    FROM rw_catalog.rw_types t
    JOIN rw_catalog.rw_schemas s
    ON s.name = 'pg_catalog'"
)]
#[derive(Fields)]
struct PgType {
    oid: i32,
    typname: String,
    typelem: i32,
    typarray: i32,
    typinput: String,
    typnotnull: bool,
    typbasetype: i32,
    typtypmod: i32,
    typcollation: i32,
    typlen: i32,
    typnamespace: i32,
    typtype: String,
    typrelid: i32,
    typdefault: String,
    typcategory: String,
    typreceive: i32,
}
