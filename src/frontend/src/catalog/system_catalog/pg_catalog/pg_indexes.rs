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

use itertools::Itertools;
use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

/// The view `pg_indexes` provides access to useful information about each index in the database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-indexes.html`]
pub const PG_INDEXES: BuiltinTable = BuiltinTable {
    name: "pg_indexes",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "schemaname"),
        (DataType::Varchar, "tablename"),
        (DataType::Varchar, "indexname"),
        (DataType::Varchar, "tablespace"),
        (DataType::Varchar, "indexdef"),
    ],
    pk: &[0, 2],
};

impl SysCatalogReaderImpl {
    pub fn read_indexes_info(&self) -> Result<Vec<OwnedRow>> {
        let catalog_reader = self.catalog_reader.read_guard();
        let schemas = catalog_reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema: &SchemaCatalog| {
                schema.iter_index().map(|index| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.name().into())),
                        Some(ScalarImpl::Utf8(index.primary_table.name.clone().into())),
                        Some(ScalarImpl::Utf8(index.index_table.name.clone().into())),
                        None,
                        Some(ScalarImpl::Utf8(index.index_table.create_sql().into())),
                    ])
                })
            })
            .collect_vec())
    }
}
