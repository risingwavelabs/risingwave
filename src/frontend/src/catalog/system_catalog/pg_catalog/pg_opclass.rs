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

/// The catalog `pg_opclass` defines index access method operator classes.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-opclass.html`].
pub const PG_OPCLASS_TABLE_NAME: &str = "pg_opclass";
pub const PG_OPCLASS_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Int32, "oid"),
    (DataType::Int32, "opcmethod"),
    (DataType::Varchar, "opcname"),
    (DataType::Int32, "opcnamespace"),
    (DataType::Int32, "opcowner"),
    (DataType::Int32, "opcfamily"),
    (DataType::Int32, "opcintype"),
    (DataType::Int32, "opcdefault"),
    (DataType::Int32, "opckeytype"),
];
