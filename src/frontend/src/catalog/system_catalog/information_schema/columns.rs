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

/// The view `columns` contains information about all table columns (or view columns) in the
/// database. System columns (ctid, etc.) are not included. Only those columns are shown that the
/// current user has access to (by way of being the owner or having some privilege).
/// Ref: [`https://www.postgresql.org/docs/current/infoschema-columns.html`]
///
/// In RisingWave, `columns` also contains all materialized views' columns.
pub const COLUMNS_TABLE_NAME: &str = "columns";
pub const COLUMNS_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Varchar, "table_catalog"),
    (DataType::Varchar, "table_schema"),
    (DataType::Varchar, "table_name"),
    (DataType::Varchar, "column_name"),
    (DataType::Varchar, "column_default"),
    (DataType::Int32, "character_maximum_length"),
    (DataType::Int32, "ordinal_position"),
    (DataType::Varchar, "is_nullable"),
    (DataType::Varchar, "data_type"),
    (DataType::Varchar, "udt_name"),
];
