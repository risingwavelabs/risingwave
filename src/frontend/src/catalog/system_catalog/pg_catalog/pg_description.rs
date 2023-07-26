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

/// The catalog `pg_description` stores description.
/// Ref: [`https://www.postgresql.org/docs/current/catalog-pg-description.html`]
pub const PG_DESCRIPTION: BuiltinTable = BuiltinTable {
    name: "pg_description",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Int32, "objoid"),
        // None
        (DataType::Int32, "classoid"),
        // 0
        (DataType::Int32, "objsubid"),
        // None
        (DataType::Varchar, "description"),
    ],
    pk: &[0],
};

pub fn new_pg_description_row(id: u32) -> OwnedRow {
    OwnedRow::new(vec![
        Some(ScalarImpl::Int32(id as i32)),
        None,
        Some(ScalarImpl::Int32(0)),
        None,
    ])
}

impl SysCatalogReaderImpl {
    pub fn read_description_info(&self) -> Result<Vec<OwnedRow>> {
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                let rows = schema
                    .iter_table()
                    .map(|table| new_pg_description_row(table.id().table_id))
                    .collect_vec();

                let mvs = schema
                    .iter_mv()
                    .map(|mv| new_pg_description_row(mv.id().table_id))
                    .collect_vec();

                let indexes = schema
                    .iter_index()
                    .map(|index| new_pg_description_row(index.id.index_id()))
                    .collect_vec();

                let sources = schema
                    .iter_source()
                    .map(|source| new_pg_description_row(source.id))
                    .collect_vec();

                let sys_tables = schema
                    .iter_system_tables()
                    .map(|table| new_pg_description_row(table.id().table_id))
                    .collect_vec();

                let views = schema
                    .iter_view()
                    .map(|view| new_pg_description_row(view.id))
                    .collect_vec();

                rows.into_iter()
                    .chain(mvs.into_iter())
                    .chain(indexes.into_iter())
                    .chain(sources.into_iter())
                    .chain(sys_tables.into_iter())
                    .chain(views.into_iter())
                    .collect_vec()
            })
            .collect_vec())
    }
}
