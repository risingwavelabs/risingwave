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
use risingwave_common::util::iter_util::ZipEqDebug;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

/// The catalog `pg_class` catalogs tables and most everything else that has columns or is otherwise
/// similar to a table. Ref: [`https://www.postgresql.org/docs/current/catalog-pg-class.html`]
pub const PG_CLASS: BuiltinTable = BuiltinTable {
    name: "pg_class",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "oid"),
        (DataType::Varchar, "relname"),
        (DataType::Int32, "relnamespace"),
        (DataType::Int32, "relowner"),
        (DataType::Varchar, "relkind"),
        // 0
        (DataType::Int32, "relam"),
        // 0
        (DataType::Int32, "reltablespace"),
    ],
    pk: &[0],
};

impl SysCatalogReaderImpl {
    pub fn read_class_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;
        let schema_infos = reader.get_all_schema_info(&self.auth_context.database)?;

        Ok(schemas
            .zip_eq_debug(schema_infos.iter())
            .flat_map(|(schema, schema_info)| {
                // !!! If we need to add more class types, remember to update
                // Catalog::get_id_by_class_name_inner accordingly.

                let rows = schema
                    .iter_table()
                    .map(|table| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(table.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(table.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(table.owner as i32)),
                            Some(ScalarImpl::Utf8("r".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let mvs = schema
                    .iter_mv()
                    .map(|mv| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(mv.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(mv.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(mv.owner as i32)),
                            Some(ScalarImpl::Utf8("m".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let indexes = schema
                    .iter_index()
                    .map(|index| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(index.index_table.id.table_id as i32)),
                            Some(ScalarImpl::Utf8(index.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(index.index_table.owner as i32)),
                            Some(ScalarImpl::Utf8("i".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let sources = schema
                    .iter_source()
                    .map(|source| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(source.id as i32)),
                            Some(ScalarImpl::Utf8(source.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(source.owner as i32)),
                            Some(ScalarImpl::Utf8("x".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let sys_tables = schema
                    .iter_system_tables()
                    .map(|table| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(table.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(table.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(table.owner as i32)),
                            Some(ScalarImpl::Utf8("r".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let views = schema
                    .iter_view()
                    .map(|view| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(view.id as i32)),
                            Some(ScalarImpl::Utf8(view.name().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(view.owner as i32)),
                            Some(ScalarImpl::Utf8("v".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                let internal_tables = schema
                    .iter_internal_table()
                    .map(|table| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Int32(table.id.table_id() as i32)),
                            Some(ScalarImpl::Utf8(table.name.clone().into())),
                            Some(ScalarImpl::Int32(schema_info.id as i32)),
                            Some(ScalarImpl::Int32(table.owner as i32)),
                            Some(ScalarImpl::Utf8("n".into())),
                            Some(ScalarImpl::Int32(0)),
                            Some(ScalarImpl::Int32(0)),
                        ])
                    })
                    .collect_vec();

                rows.into_iter()
                    .chain(mvs.into_iter())
                    .chain(indexes.into_iter())
                    .chain(sources.into_iter())
                    .chain(sys_tables.into_iter())
                    .chain(views.into_iter())
                    .chain(internal_tables.into_iter())
                    .collect_vec()
            })
            .collect_vec())
    }
}
