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

pub static PG_CLASS_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
        (DataType::Int32, "oid"),
        (DataType::Varchar, "relname"),
        (DataType::Int32, "relnamespace"),
        (DataType::Int32, "relowner"),
        (DataType::Varchar, "relkind"), /* r = ordinary table, i = index, S = sequence, t =
                                         * TOAST table, v = view, m = materialized view, c =
                                         * composite type, f = foreign table, p = partitioned
                                         * table, I = partitioned index */
        (DataType::Int32, "relam"),
        (DataType::Int32, "reltablespace"),
        (DataType::List(Box::new(DataType::Varchar)), "reloptions"),
    ]
});

/// The catalog `pg_class` catalogs tables and most everything else that has columns or is otherwise
/// similar to a table. Ref: [`https://www.postgresql.org/docs/current/catalog-pg-class.html`]
/// todo: should we add internal tables as well?
pub static PG_CLASS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_class",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &PG_CLASS_COLUMNS,
    sql: "SELECT id AS oid, name AS relname, schema_id AS relnamespace, owner AS relowner, \
        CASE \
            WHEN relation_type = 'table' THEN 'r' \
            WHEN relation_type = 'system table' THEN 'r' \
            WHEN relation_type = 'index' THEN 'i' \
            WHEN relation_type = 'view' THEN 'v' \
            WHEN relation_type = 'materialized view' THEN 'm' \
        END relkind, \
        0 AS relam, \
        0 AS reltablespace, \
        ARRAY[]::varchar[] AS reloptions \
        FROM rw_catalog.rw_relations\
    "
    .to_string(),
});
