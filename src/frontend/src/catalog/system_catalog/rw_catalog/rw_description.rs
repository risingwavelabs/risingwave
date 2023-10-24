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

use std::iter;

use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub const RW_DESCRIPTION: BuiltinTable = BuiltinTable {
    name: "rw_description",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        // table_id, view_id, function_id, etc.
        (DataType::Int32, "objoid"),
        // rw_tables, rw_views, rw_functions, etc.
        (DataType::Int32, "classoid"),
        // If `objoid` is `table_id`, then non-null `objsubid` is column number.
        (DataType::Int32, "objsubid"),
        (DataType::Varchar, "description"),
    ],
    pk: &[0, 1, 2],
};

impl SysCatalogReaderImpl {
    pub fn read_rw_description(&self) -> Result<Vec<OwnedRow>> {
        let build_row =
            |table_id, catalog_id, index: Option<i32>, description: Option<Box<str>>| {
                OwnedRow::new(vec![
                    Some(ScalarImpl::Int32(table_id)),
                    Some(ScalarImpl::Int32(catalog_id)),
                    index.map(ScalarImpl::Int32),
                    description.map(ScalarImpl::Utf8),
                ])
            };

        let reader = self.catalog_reader.read_guard();
        let rw_catalog =
            reader.get_schema_by_name(&self.auth_context.database, RW_CATALOG_SCHEMA_NAME)?;
        let schemas = reader
            .iter_schemas(&self.auth_context.database)?
            .filter(|schema| schema.id() != rw_catalog.id());

        let rw_tables_id: i32 = rw_catalog
            .get_system_table_by_name("rw_tables")
            .map(|st| st.id.table_id)
            .unwrap_or_default() as _;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_table().flat_map(|table| {
                    iter::once(build_row(
                        table.id.table_id as _,
                        rw_tables_id,
                        None,
                        table.description.as_deref().map(Into::into),
                    ))
                    .chain(table.columns.iter().map(|col| {
                        build_row(
                            table.id.table_id as _,
                            rw_tables_id,
                            Some(col.column_id().get_id() as _),
                            col.column_desc.description.as_deref().map(Into::into),
                        )
                    }))
                })
            })
            .collect())
    }
}
