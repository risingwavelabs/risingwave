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

pub mod columns;
pub mod tables;

pub use columns::*;
use itertools::Itertools;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::ScalarImpl;
pub use tables::*;

use super::SysCatalogReaderImpl;

impl SysCatalogReaderImpl {
    pub(super) fn read_columns_info(&self) -> Result<Vec<OwnedRow>> {
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

                let view_rows = schema.iter_view().flat_map(|view| {
                    view.columns.iter().enumerate().map(|(index, column)| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Utf8(self.auth_context.database.clone().into())),
                            Some(ScalarImpl::Utf8(schema.name().into())),
                            Some(ScalarImpl::Utf8(view.name().into())),
                            Some(ScalarImpl::Utf8(column.name.clone().into())),
                            None,
                            None,
                            Some(ScalarImpl::Int32(index as i32 + 1)),
                            // TODO: refactor when we support "NOT NULL".
                            Some(ScalarImpl::Utf8("YES".into())),
                            Some(ScalarImpl::Utf8(column.data_type().to_string().into())),
                            Some(ScalarImpl::Utf8(column.data_type().pg_name().into())),
                        ])
                    })
                });

                table_columns
                    .chain(sys_table_columns)
                    .chain(mv_columns)
                    .flat_map(|(table_name, columns)| {
                        columns
                            .iter()
                            .enumerate()
                            .filter(|(_, column)| !column.is_hidden())
                            .map(|(index, column)| {
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Utf8(
                                        self.auth_context.database.clone().into(),
                                    )),
                                    Some(ScalarImpl::Utf8(schema.name().into())),
                                    Some(ScalarImpl::Utf8(table_name.into())),
                                    Some(ScalarImpl::Utf8(column.name().into())),
                                    None,
                                    None,
                                    Some(ScalarImpl::Int32(index as i32 + 1)),
                                    // TODO: refactor when we support "NOT NULL".
                                    Some(ScalarImpl::Utf8("YES".into())),
                                    Some(ScalarImpl::Utf8(column.data_type().to_string().into())),
                                    Some(ScalarImpl::Utf8(column.data_type().pg_name().into())),
                                ])
                            })
                    })
                    .chain(view_rows)
            })
            .collect_vec())
    }

    pub(super) fn read_tables_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                let table_rows = schema.iter_table().map(|table| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(self.auth_context.database.clone().into())),
                        Some(ScalarImpl::Utf8(schema.name().into())),
                        Some(ScalarImpl::Utf8(table.name().into())),
                        Some(ScalarImpl::Utf8("BASE TABLE".into())),
                        Some(ScalarImpl::Utf8("YES".into())),
                    ])
                });
                let sys_table_rows = schema.iter_system_tables().map(|table| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(self.auth_context.database.clone().into())),
                        Some(ScalarImpl::Utf8(schema.name().into())),
                        Some(ScalarImpl::Utf8(table.name().into())),
                        Some(ScalarImpl::Utf8("SYSTEM TABLE".into())),
                        Some(ScalarImpl::Utf8("NO".into())),
                    ])
                });
                let mv_rows = schema.iter_mv().map(|mv| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(self.auth_context.database.clone().into())),
                        Some(ScalarImpl::Utf8(schema.name().into())),
                        Some(ScalarImpl::Utf8(mv.name().into())),
                        Some(ScalarImpl::Utf8("MATERIALIZED VIEW".into())),
                        Some(ScalarImpl::Utf8("NO".into())),
                    ])
                });
                let view_rows = schema.iter_view().map(|view| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(self.auth_context.database.clone().into())),
                        Some(ScalarImpl::Utf8(schema.name().into())),
                        Some(ScalarImpl::Utf8(view.name().into())),
                        Some(ScalarImpl::Utf8("VIEW".into())),
                        Some(ScalarImpl::Utf8("NO".into())),
                    ])
                });

                table_rows
                    .chain(sys_table_rows)
                    .chain(mv_rows)
                    .chain(view_rows)
            })
            .collect_vec())
    }
}
