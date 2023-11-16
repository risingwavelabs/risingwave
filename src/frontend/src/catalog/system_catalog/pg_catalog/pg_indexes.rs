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

use crate::catalog::system_catalog::BuiltinView;

/// The view `pg_indexes` provides access to useful information about each index in the database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-indexes.html`]
pub static PG_INDEXES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_indexes",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "schemaname"),
        (DataType::Varchar, "tablename"),
        (DataType::Varchar, "indexname"),
        (DataType::Varchar, "tablespace"),
        (DataType::Varchar, "indexdef"),
    ],
    sql: "SELECT s.name AS schemaname, \
                t.name AS tablename, \
                i.name AS indexname, \
                NULL AS tablespace, \
                i.definition AS indexdef \
            FROM rw_catalog.rw_indexes i \
            JOIN rw_catalog.rw_tables t ON i.primary_table_id = t.id \
            JOIN rw_catalog.rw_schemas s ON i.schema_id = s.id\
    "
    .to_string(),
});
