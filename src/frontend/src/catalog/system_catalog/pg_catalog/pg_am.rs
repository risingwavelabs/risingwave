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

use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::DataType;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

/// Stores information about relation access methods.
/// Reference: [`https://www.postgresql.org/docs/current/catalog-pg-am.html`]
pub const PG_AM: BuiltinTable = BuiltinTable {
    name: "pg_am",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "oid"),
        (DataType::Varchar, "amname"),
        (DataType::Int32, "amhandler"),
        (DataType::Varchar, "amtype"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_am_info(&self) -> Result<Vec<OwnedRow>> {
        Ok(vec![])
    }
}
