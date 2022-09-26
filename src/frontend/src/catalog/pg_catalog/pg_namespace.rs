// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use risingwave_common::types::DataType;

use crate::catalog::pg_catalog::PgCatalogColumnsDef;

/// The catalog `pg_namespace` stores namespaces. A namespace is the structure underlying SQL
/// schemas: each namespace can have a separate collection of relations, types, etc. without name
/// conflicts. Ref: [`https://www.postgresql.org/docs/current/catalog-pg-namespace.html`]
pub const PG_NAMESPACE_TABLE_NAME: &str = "pg_namespace";
pub const PG_NAMESPACE_COLUMNS: &[PgCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Varchar, "nspname"),
    (DataType::Int32, "nspowner"),
    (DataType::Varchar, "nspacl"),
];
