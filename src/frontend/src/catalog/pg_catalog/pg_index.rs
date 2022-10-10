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

/// The catalog `pg_index` contains part of the information about indexes.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-class.html`]
pub const PG_INDEX_TABLE_NAME: &str = "pg_index";
pub const PG_INDEX_COLUMNS: &[PgCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "indexrelid"),
    (DataType::Int32, "indrelid"),
    (DataType::Int32, "indnatts"),
];
