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

pub mod columns;

pub use columns::*;
use itertools::Itertools;
use risingwave_common::array::Row;
use risingwave_common::error::Result;
use risingwave_common::types::ScalarImpl;

use super::SysCatalogReaderImpl;

impl SysCatalogReaderImpl {
    pub(super) fn read_columns_info(&self) -> Result<Vec<Row>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                let table_columns = schema
                    .iter_table()
                    .map(|table| (table.name(), table.columns()));
                let sys_table_columns = schema
                    .iter_system_tables()
                    .map(|table| (table.name(), table.columns()));
                let mv_columns = schema.iter_mv().map(|mv| (mv.name(), mv.columns()));

                table_columns
                    .chain(sys_table_columns)
                    .chain(mv_columns)
                    .map(|(table_name, columns)| {
                        columns
                            .iter()
                            .enumerate()
                            .filter(|(_, column)| !column.is_hidden())
                            .map(|(index, column)| {
                                Row::new(vec![
                                    Some(ScalarImpl::Utf8(self.auth_context.database.clone())),
                                    Some(ScalarImpl::Utf8(schema.name())),
                                    Some(ScalarImpl::Utf8(table_name.to_string())),
                                    Some(ScalarImpl::Utf8(column.name().to_string())),
                                    Some(ScalarImpl::Int32(index as i32 + 1)),
                                    // TODO: refactor when we support "NOT NULL".
                                    Some(ScalarImpl::Utf8("YES".to_string())),
                                    Some(ScalarImpl::Utf8(column.data_type().to_string())),
                                ])
                            })
                    })
            })
            .flatten()
            .collect_vec())
    }
}
