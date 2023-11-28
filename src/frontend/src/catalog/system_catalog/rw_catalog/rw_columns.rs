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
use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

pub static RW_COLUMNS: LazyLock<BuiltinTable> = LazyLock::new(|| BuiltinTable {
    name: "rw_columns",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "relation_id"), // belonged relation id
        (DataType::Varchar, "name"),      // column name
        (DataType::Int32, "position"),    // 1-indexed position
        (DataType::Boolean, "is_hidden"),
        (DataType::Boolean, "is_primary_key"),
        (DataType::Boolean, "is_distribution_key"),
        (DataType::Varchar, "data_type"),
        (DataType::Int32, "type_oid"),
        (DataType::Int16, "type_len"),
        (DataType::Varchar, "udt_type"),
    ],
    pk: &[0, 1],
});

impl SysCatalogReaderImpl {
    pub fn read_rw_columns_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                let view_rows = schema.iter_view().flat_map(|view| {
                    view.columns.iter().enumerate().map(|(index, column)| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(view.id as i32)),
                            Some(ScalarImpl::Utf8(column.name.clone().into())),
                            Some(ScalarImpl::Int32(index as i32 + 1)),
                            Some(ScalarImpl::Bool(false)),
                            Some(ScalarImpl::Bool(false)),
                            Some(ScalarImpl::Bool(false)),
                            Some(ScalarImpl::Utf8(column.data_type().to_string().into())),
                            Some(ScalarImpl::Int32(column.data_type().to_oid())),
                            Some(ScalarImpl::Int16(column.data_type().type_len())),
                            Some(ScalarImpl::Utf8(column.data_type().pg_name().into())),
                        ])
                    })
                });

                let sink_rows = schema
                    .iter_sink()
                    .flat_map(|sink| {
                        sink.full_columns()
                            .iter()
                            .enumerate()
                            .map(|(index, column)| {
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Int32(sink.id.sink_id as i32)),
                                    Some(ScalarImpl::Utf8(column.name().into())),
                                    Some(ScalarImpl::Int32(index as i32 + 1)),
                                    Some(ScalarImpl::Bool(column.is_hidden)),
                                    Some(ScalarImpl::Bool(sink.downstream_pk.contains(&index))),
                                    Some(ScalarImpl::Bool(sink.distribution_key.contains(&index))),
                                    Some(ScalarImpl::Utf8(column.data_type().to_string().into())),
                                    Some(ScalarImpl::Int32(column.data_type().to_oid())),
                                    Some(ScalarImpl::Int16(column.data_type().type_len())),
                                    Some(ScalarImpl::Utf8(column.data_type().pg_name().into())),
                                ])
                            })
                    })
                    .chain(view_rows);

                let rows = schema
                    .iter_system_tables()
                    .flat_map(|table| {
                        table
                            .columns
                            .iter()
                            .enumerate()
                            .map(move |(index, column)| {
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Int32(table.id.table_id as i32)),
                                    Some(ScalarImpl::Utf8(column.name().into())),
                                    Some(ScalarImpl::Int32(index as i32 + 1)),
                                    Some(ScalarImpl::Bool(column.is_hidden)),
                                    Some(ScalarImpl::Bool(table.pk.contains(&index))),
                                    Some(ScalarImpl::Bool(false)),
                                    Some(ScalarImpl::Utf8(column.data_type().to_string().into())),
                                    Some(ScalarImpl::Int32(column.data_type().to_oid())),
                                    Some(ScalarImpl::Int16(column.data_type().type_len())),
                                    Some(ScalarImpl::Utf8(column.data_type().pg_name().into())),
                                ])
                            })
                    })
                    .chain(sink_rows);

                schema
                    .iter_valid_table()
                    .flat_map(|table| {
                        table
                            .columns
                            .iter()
                            .enumerate()
                            .map(move |(index, column)| {
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Int32(table.id.table_id as i32)),
                                    Some(ScalarImpl::Utf8(column.name().into())),
                                    Some(ScalarImpl::Int32(index as i32 + 1)),
                                    Some(ScalarImpl::Bool(column.is_hidden)),
                                    Some(ScalarImpl::Bool(
                                        table.pk().iter().any(|idx| idx.column_index == index),
                                    )),
                                    Some(ScalarImpl::Bool(
                                        table.distribution_key().contains(&index),
                                    )),
                                    Some(ScalarImpl::Utf8(column.data_type().to_string().into())),
                                    Some(ScalarImpl::Int32(column.data_type().to_oid())),
                                    Some(ScalarImpl::Int16(column.data_type().type_len())),
                                    Some(ScalarImpl::Utf8(column.data_type().pg_name().into())),
                                ])
                            })
                    })
                    .chain(rows)
            })
            .collect_vec())
    }
}
