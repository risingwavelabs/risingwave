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

/// Mapping from sql name to system locale groups.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-collation.html`].
pub const PG_COLLATION_TABLE_NAME: &str = "pg_collation";
pub const PG_COLLATION_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Varchar, "collname"),
    (DataType::Int32, "collnamespace"),
    (DataType::Int32, "collowner"),
    (DataType::Int32, "collprovider"),
    (DataType::Boolean, "collisdeterministic"),
    (DataType::Int32, "collencoding"),
    (DataType::Varchar, "collcollate"),
    (DataType::Varchar, "collctype"),
    (DataType::Varchar, "colliculocale"),
    (DataType::Varchar, "collversion"),
];
