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

use risingwave_common::types::DataType;

use crate::catalog::system_catalog::SystemCatalogColumnsDef;

/// The view `pg_indexes` provides access to useful information about each index in the database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-indexes.html`]
pub const PG_INDEXES_TABLE_NAME: &str = "pg_indexes";
pub const PG_INDEXES_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Varchar, "schemaname"),
    (DataType::Varchar, "tablename"),
    (DataType::Varchar, "indexname"),
    (DataType::Varchar, "tablespace"),
    (DataType::Varchar, "indexdef"),
];
