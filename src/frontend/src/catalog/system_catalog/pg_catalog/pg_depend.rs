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

use crate::catalog::system_catalog::{infer_dummy_view_sql, BuiltinView, SystemCatalogColumnsDef};

pub const PG_DEPEND_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "classid"),
    (DataType::Int32, "objid"),
    (DataType::Int16, "objsubid"),
    (DataType::Int32, "refclassid"),
    (DataType::Int32, "refobjid"),
    (DataType::Int16, "refobjsubid"),
    (DataType::Varchar, "deptype"),
];

/// The catalog `pg_depend` records the dependency relationships between database objects.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-depend.html`]
pub static PG_DEPEND: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_depend",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: PG_DEPEND_COLUMNS,
    sql: infer_dummy_view_sql(PG_DEPEND_COLUMNS),
});
