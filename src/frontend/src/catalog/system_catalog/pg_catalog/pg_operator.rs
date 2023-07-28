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

pub const PG_OPERATOR_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Varchar, "oprname"),
    (DataType::Int32, "oprnamespace"),
    (DataType::Int32, "oprowner"),
    (DataType::Varchar, "oprkind"),
    (DataType::Boolean, "oprcanmerge"),
    (DataType::Boolean, "oprcanhash"),
    (DataType::Int32, "oprleft"),
    (DataType::Int32, "oprright"),
    (DataType::Int32, "oprresult"),
    (DataType::Int32, "oprcom"),
    (DataType::Int32, "oprnegate"),
    (DataType::Int32, "oprcode"),
    (DataType::Int32, "oprrest"),
    (DataType::Int32, "oprjoin"),
];

/// The catalog `pg_operator` stores operator info.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-operator.html`]
pub static PG_OPERATOR: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_operator",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: PG_OPERATOR_COLUMNS,
    sql: infer_dummy_view_sql(PG_OPERATOR_COLUMNS),
});
