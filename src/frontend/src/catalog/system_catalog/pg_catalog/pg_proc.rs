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

pub const PG_PROC_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Varchar, "proname"),
    (DataType::Int32, "pronamespace"),
    (DataType::Int32, "proowner"),
    (DataType::Int32, "proargdefaults"),
    (DataType::Int32, "prorettype"), // Data type of the return value, refer to pg_type.
];

/// The catalog `pg_proc` stores information about functions, procedures, aggregate functions, and
/// window functions (collectively also known as routines).
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-proc.html`]
// TODO: read real data including oid etc in rw, currently there are no such data in rw.
// more details can be found here: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_proc.dat
pub static PG_PROC: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_proc",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: PG_PROC_COLUMNS,
    sql: infer_dummy_view_sql(PG_PROC_COLUMNS),
});
