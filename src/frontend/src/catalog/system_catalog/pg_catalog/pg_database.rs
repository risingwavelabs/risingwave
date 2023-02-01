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

use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

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

pub const PG_DATABASE_TABLE_NAME: &str = "pg_database";

pub const PG_DATABASE_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Varchar, "datname"),
    // None
    (DataType::Int32, "datdba"),
    // 6
    (DataType::Int32, "encoding"),
    // 'C'
    (DataType::Varchar, "datcollate"),
    // 'C'
    (DataType::Varchar, "datctype"),
    // false
    (DataType::Boolean, "datistemplate"),
    // true
    (DataType::Boolean, "datallowconn"),
    // -1
    (DataType::Int32, "datconnlimit"),
    // null
    (DataType::Varchar, "datacl"),
];

pub fn new_pg_database_row(id: u32, name: &str) -> OwnedRow {
    OwnedRow::new(vec![
        Some(ScalarImpl::Int32(id as i32)),
        Some(ScalarImpl::Utf8(name.into())),
        None,
        Some(ScalarImpl::Int32(6)),
        Some(ScalarImpl::Utf8("C".into())),
        Some(ScalarImpl::Utf8("C".into())),
        Some(ScalarImpl::Bool(false)),
        Some(ScalarImpl::Bool(true)),
        Some(ScalarImpl::Int32(-1)),
        None,
    ])
}
