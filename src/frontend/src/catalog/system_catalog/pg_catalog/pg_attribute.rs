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

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

/// The catalog `pg_attribute` stores information about table columns. There will be exactly one
/// `pg_attribute` row for every column in every table in the database. (There will also be
/// attribute entries for indexes, and indeed all objects that have `pg_class` entries.)
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-attribute.html`]
///
/// In RisingWave, we simply make it contain the columns of the view and all the columns of the
/// tables that are not internal tables.
pub const PG_ATTRIBUTE: BuiltinTable = BuiltinTable {
    name: "pg_attribute",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "attrelid"),
        (DataType::Varchar, "attname"),
        (DataType::Int32, "atttypid"),
        (DataType::Int16, "attlen"),
        (DataType::Int16, "attnum"),
        (DataType::Boolean, "attnotnull"),
        (DataType::Boolean, "attisdropped"),
        (DataType::Varchar, "attidentity"),
        (DataType::Varchar, "attgenerated"),
        (DataType::Int32, "atttypmod"),
    ],
    pk: &[0, 4],
};

impl SysCatalogReaderImpl {
    pub fn read_pg_attribute(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                let view_rows = schema.iter_view().flat_map(|view| {
                    view.columns.iter().enumerate().map(|(index, column)| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(view.id as i32)),
                            Some(ScalarImpl::Utf8(column.name.clone().into())),
                            Some(ScalarImpl::Int32(column.data_type().to_oid())),
                            Some(ScalarImpl::Int16(column.data_type().type_len())),
                            Some(ScalarImpl::Int16(index as i16 + 1)),
                            Some(ScalarImpl::Bool(false)),
                            Some(ScalarImpl::Bool(false)),
                            Some(ScalarImpl::Utf8("".into())),
                            Some(ScalarImpl::Utf8("".into())),
                            // From https://www.postgresql.org/docs/current/catalog-pg-attribute.html
                            // The value will generally be -1 for types that do not need
                            // `atttypmod`.
                            Some(ScalarImpl::Int32(-1)),
                        ])
                    })
                });

                schema
                    .iter_valid_table()
                    .flat_map(|table| {
                        table
                            .columns()
                            .iter()
                            .enumerate()
                            .filter(|(_, column)| !column.is_hidden())
                            .map(|(index, column)| {
                                OwnedRow::new(vec![
                                    Some(ScalarImpl::Int32(table.id.table_id() as i32)),
                                    Some(ScalarImpl::Utf8(column.name().into())),
                                    Some(ScalarImpl::Int32(column.data_type().to_oid())),
                                    Some(ScalarImpl::Int16(column.data_type().type_len())),
                                    Some(ScalarImpl::Int16(index as i16 + 1)),
                                    Some(ScalarImpl::Bool(false)),
                                    Some(ScalarImpl::Bool(false)),
                                    Some(ScalarImpl::Utf8("".into())),
                                    Some(ScalarImpl::Utf8("".into())),
                                    // From https://www.postgresql.org/docs/current/catalog-pg-attribute.html
                                    // The value will generally be -1 for types that do not need
                                    // `atttypmod`.
                                    Some(ScalarImpl::Int32(-1)),
                                ])
                            })
                    })
                    .chain(view_rows)
            })
            .collect_vec())
    }
}
