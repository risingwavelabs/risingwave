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

/// The `pg_enum` catalog contains entries showing the values and labels for each enum type.
/// The internal representation of a given enum value is actually the OID of its associated row in
/// `pg_enum`. Reference: [`https://www.postgresql.org/docs/current/catalog-pg-enum.html`]
pub const PG_ENUM_TABLE_NAME: &str = "pg_enum";
pub const PG_ENUM_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Int32, "enumtypid"),
    (DataType::Float32, "enumsortorder"),
    (DataType::Varchar, "enumlabel"),
];
