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

pub static PG_CONSTRAINT_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> =
    LazyLock::new(|| {
        vec![
            (DataType::Int32, "oid"),
            (DataType::Varchar, "conname"),
            (DataType::Int32, "connamespace"),
            (DataType::Varchar, "contype"),
            (DataType::Boolean, "condeferrable"),
            (DataType::Boolean, "convalidated"),
            (DataType::Int32, "conrelid"),
            (DataType::Int32, "contypid"),
            (DataType::Int32, "conindid"),
            (DataType::Int32, "conparentid"),
            (DataType::Int32, "confrelid"),
            (DataType::Varchar, "confupdtype"),
            (DataType::Varchar, "confdeltype"),
            (DataType::Varchar, "confmatchtype"),
            (DataType::Boolean, "conislocal"),
            (DataType::Int32, "coninhcount"),
            (DataType::Boolean, "connoinherit"),
            (DataType::List(Box::new(DataType::Int16)), "conkey"),
            (DataType::List(Box::new(DataType::Int16)), "confkey"),
            (DataType::List(Box::new(DataType::Int32)), "conpfeqop"),
            (DataType::List(Box::new(DataType::Int32)), "conppeqop"),
            (DataType::List(Box::new(DataType::Int32)), "conffeqop"),
            (DataType::List(Box::new(DataType::Int16)), "confdelsetcols"),
            (DataType::List(Box::new(DataType::Int32)), "conexclop"),
            (DataType::Varchar, "conbin"),
        ]
    });

/// The catalog `pg_constraint` records information about table and index inheritance hierarchies.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-constraint.html`]
/// This is introduced only for pg compatibility and is not used in our system.
pub static PG_CONSTRAINT: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "pg_constraint",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &PG_CONSTRAINT_COLUMNS,
    sql: infer_dummy_view_sql(&PG_CONSTRAINT_COLUMNS),
});
