// Copyright 2022 RisingWave Labs
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

/// The catalog `pg_database` stores database.
///
/// Example from Postgres:
///
/// ```text
/// dev=# select * from pg_catalog.pg_database;
///   oid  |  datname  | datdba | encoding | datcollate | datctype | datistemplate | datallowconn | datconnlimit | datlastsysoid | datfrozenxid | datminmxid | dattablespace |         datacl
/// -------+-----------+--------+----------+------------+----------+---------------+--------------+--------------+---------------+--------------+------------+---------------+-------------------------
///  14021 | postgres  |     10 |        6 | C          | C        | f             | t            |           -1 |         14020 |          726 |          1 |          1663 |
///  16384 | dev       |     10 |        6 | C          | C        | f             | t            |           -1 |         14020 |          726 |          1 |          1663 |
///      1 | template1 |     10 |        6 | C          | C        | t             | t            |           -1 |         14020 |          726 |          1 |          1663 | {=c/eric,eric=CTc/eric}
///  14020 | template0 |     10 |        6 | C          | C        | t             | f            |           -1 |         14020 |          726 |          1 |          1663 | {=c/eric,eric=CTc/eric}
/// (4 rows)
/// ```
///
/// Ref: [`pg_database`](https://www.postgresql.org/docs/current/catalog-pg-database.html)
#[system_catalog(
    view,
    "pg_catalog.pg_database",
    "SELECT id AS oid,
        name AS datname,
        owner AS datdba,
        6 AS encoding,
        'C' AS datcollate,
        'C' AS datctype,
        false AS datistemplate,
        true AS datallowconn,
        -1 AS datconnlimit,
        1663 AS dattablespace,
        acl AS datacl FROM rw_catalog.rw_databases"
)]
#[derive(Fields)]
struct PgDatabase {
    oid: i32,
    datname: String,
    datdba: i32,
    encoding: i32,
    datcollate: String,
    datctype: String,
    datistemplate: bool,
    datallowconn: bool,
    datconnlimit: i32,
    dattablespace: i32,
    datacl: Vec<String>,
}
