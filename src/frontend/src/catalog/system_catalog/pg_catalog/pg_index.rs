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

use itertools::Itertools;
use risingwave_common::array::ListValue;
use risingwave_common::catalog::PG_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl, SystemCatalogColumnsDef};

pub const PG_INDEX_TABLE_NAME: &str = "pg_index";
pub static PG_INDEX_COLUMNS: LazyLock<Vec<SystemCatalogColumnsDef<'_>>> = LazyLock::new(|| {
    vec![
        (DataType::Int32, "indexrelid"),
        (DataType::Int32, "indrelid"),
        (DataType::Int16, "indnatts"),
        (DataType::List(Box::new(DataType::Int16)), "indkey"),
        // None. We don't have `pg_node_tree` type yet, so we use `text` instead.
        (DataType::Varchar, "indexprs"),
        // None. We don't have `pg_node_tree` type yet, so we use `text` instead.
        (DataType::Varchar, "indpred"),
    ]
});

/// The catalog `pg_index` contains part of the information about indexes.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-index.html`]
pub static PG_INDEX: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: PG_INDEX_TABLE_NAME,
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &PG_INDEX_COLUMNS,
    pk: &[0],
});

impl SysCatalogReaderImpl {
    pub fn read_index_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_index().map(|index| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Int32(index.id.index_id() as i32)),
                        Some(ScalarImpl::Int32(index.primary_table.id.table_id() as i32)),
                        Some(ScalarImpl::Int16(index.original_columns.len() as i16)),
                        Some(ScalarImpl::List(ListValue::new(
                            index
                                .original_columns
                                .iter()
                                .map(|index| Some(ScalarImpl::Int16(index.get_id() as i16 + 1)))
                                .collect_vec(),
                        ))),
                        None,
                        None,
                    ])
                })
            })
            .collect_vec())
    }
}
