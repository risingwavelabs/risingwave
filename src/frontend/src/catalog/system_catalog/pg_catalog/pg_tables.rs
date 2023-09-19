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

use std::sync::LazyLock;

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::{BuiltinView, SystemCatalogColumnsDef};

pub const PG_TABLES_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Varchar, "schemaname"),
    (DataType::Varchar, "tablename"),
    (DataType::Varchar, "tableowner"),
    (DataType::Varchar, "tablespace"), /* Since we don't have any concept of tablespace, we will
                                        * set this to null. */
];

/// The view `pg_tables` provides access to useful information about each table in the database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-tables.html`]
pub static PG_TABLES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_tables",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: PG_TABLES_COLUMNS,
    sql: "SELECT s.name AS schemaname, \
                 t.tablename, \
                 pg_catalog.pg_get_userbyid(t.owner) AS tableowner, \
                 NULL AS tablespace \
            FROM \
                (SELECT name AS tablename, \
                       schema_id, \
                       owner \
                       FROM rw_catalog.rw_tables \
                 UNION \
                 SELECT name AS tablename, \
                       schema_id, \
                       owner \
                       FROM rw_catalog.rw_system_tables) AS t \
                JOIN rw_catalog.rw_schemas s ON t.schema_id = s.id"
        .into(),
});
