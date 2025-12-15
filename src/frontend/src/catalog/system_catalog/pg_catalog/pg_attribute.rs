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

/// The catalog `pg_attribute` stores information about table columns. There will be exactly one
/// `pg_attribute` row for every column in every table in the database. (There will also be
/// attribute entries for indexes, and indeed all objects that have `pg_class` entries.)
/// Ref: `https://www.postgresql.org/docs/current/catalog-pg-attribute.html`
///
/// In RisingWave, we simply make it contain the columns of the view and all the columns of the
/// tables that are not internal tables.
#[system_catalog(
    view,
    "pg_catalog.pg_attribute",
    "SELECT c.relation_id AS attrelid,
            c.name AS attname,
            c.type_oid AS atttypid,
            CASE
              WHEN c.udt_type = 'list' THEN 1::int2
              ELSE 0::int2
            END AS attndims,
            c.type_len AS attlen,
            c.position::smallint AS attnum,
            false AS attnotnull,
            false AS atthasdef,
            false AS attisdropped,
            ''::varchar AS attidentity,
            CASE
              WHEN c.is_generated THEN 's'::varchar
              ELSE ''::varchar
            END AS attgenerated,
            -1 AS atttypmod,
            NULL::text[] AS attoptions,
            0 AS attcollation
        FROM rw_catalog.rw_columns c
        WHERE c.is_hidden = false"
)]
#[derive(Fields)]
struct PgAttribute {
    attrelid: i32,
    attname: String,
    atttypid: i32,
    attndims: i16,
    attlen: i16,
    attnum: i16,
    attnotnull: bool,
    atthasdef: bool,
    attisdropped: bool,
    attidentity: String,
    attgenerated: String,
    atttypmod: i32,
    attoptions: Vec<String>,
    attcollation: i32,
}
