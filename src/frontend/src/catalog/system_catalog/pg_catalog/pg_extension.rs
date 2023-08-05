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

pub static PG_EXTENSION_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
        (DataType::Int32, "oid"), // oid
        (DataType::Varchar, "extname"),
        (DataType::Int32, "extowner"),     // oid
        (DataType::Int32, "extnamespace"), // oid
        (DataType::Boolean, "extrelocatable"),
        (DataType::Varchar, "extversion"),
        (DataType::List(Box::new(DataType::Int32)), "extconfig"), // []oid
        (DataType::List(Box::new(DataType::Varchar)), "extcondition"),
    ]
});

/// The catalog `pg_extension` stores information about the installed extensions. See Section 38.17
/// for details about extensions.
///
/// Reference: <https://www.postgresql.org/docs/current/catalog-pg-extension.html>.
/// Currently, we don't have any type of extension.
pub static PG_EXTENSION: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_extension",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &PG_EXTENSION_COLUMNS,
    sql: infer_dummy_view_sql(&PG_EXTENSION_COLUMNS),
});
