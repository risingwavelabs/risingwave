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

/// The view `pg_tables` provides access to useful information about each table in the database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-tables.html`]
pub const PG_TABLES: BuiltinTable = BuiltinTable {
    name: "pg_tables",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "schemaname"),
        (DataType::Varchar, "tablename"),
        (DataType::Varchar, "tableowner"),
        (DataType::Varchar, "tablespace"), /* Since we don't have any concept of tablespace, we
                                            * will
                                            * set this to null. */
    ],
    pk: &[],
};

impl SysCatalogReaderImpl {
    pub fn read_pg_tables_info(&self) -> Result<Vec<OwnedRow>> {
        // TODO: avoid acquire two read locks here. The order is the same as in `read_views_info`.
        let reader = self.catalog_reader.read_guard();
        let user_info_reader = self.user_info_reader.read_guard();
        let schemas = reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema
                    .iter_table()
                    .map(|table| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Utf8(schema.name().into())),
                            Some(ScalarImpl::Utf8(table.name().into())),
                            Some(ScalarImpl::Utf8(
                                user_info_reader
                                    .get_user_name_by_id(table.owner)
                                    .unwrap()
                                    .into(),
                            )),
                            None,
                        ])
                    })
                    .chain(schema.iter_system_tables().map(|table| {
                        OwnedRow::new(vec![
                            Some(ScalarImpl::Utf8(schema.name().into())),
                            Some(ScalarImpl::Utf8(table.name().into())),
                            Some(ScalarImpl::Utf8(
                                user_info_reader
                                    .get_user_name_by_id(table.owner)
                                    .unwrap()
                                    .into(),
                            )),
                            None,
                        ])
                    }))
            })
            .collect_vec())
    }
}
