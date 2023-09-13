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

pub const PG_KEYWORDS_TABLE_NAME: &str = "pg_keywords";
pub const PG_GET_KEYWORDS_FUNC_NAME: &str = "pg_get_keywords";
pub const PG_KEYWORDS_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Varchar, "word"),
    (DataType::Varchar, "catcode"),
    (DataType::Varchar, "catdesc"),
];

/// The catalog `pg_keywords` stores keywords. `pg_get_keywords` returns the content of this table.
/// Ref: [`https://www.postgresql.org/docs/15/functions-info.html`]
// TODO: change to read reserved keywords here
pub static PG_KEYWORDS: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: PG_KEYWORDS_TABLE_NAME,
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: PG_KEYWORDS_COLUMNS,
    sql: infer_dummy_view_sql(PG_KEYWORDS_COLUMNS),
});
