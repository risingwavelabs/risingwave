// Copyright 2023 RisingWave Labs
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

use std::convert::Into;
use std::sync::LazyLock;

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::BuiltinView;

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
pub static PG_DATABASE: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_database",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "oid"),
        (DataType::Varchar, "datname"),
        (DataType::Int32, "datdba"),
        (DataType::Int32, "encoding"),
        (DataType::Varchar, "datcollate"),
        (DataType::Varchar, "datctype"),
        (DataType::Boolean, "datistemplate"),
        (DataType::Boolean, "datallowconn"),
        (DataType::Int32, "datconnlimit"),
        (DataType::Int32, "dattablespace"),
        (DataType::Varchar, "datacl"),
    ],
    sql: "SELECT id AS oid, \
                 name AS datname, \
                 owner AS datdba, \
                 6 AS encoding, \
                 'C' AS datcollate, \
                 'C' AS datctype, \
                 false AS datistemplate, \
                 true AS datallowconn, \
                 -1 AS datconnlimit, \
                 1663 AS dattablespace, \
                 acl AS datacl FROM rw_catalog.rw_databases"
        .into(),
});
