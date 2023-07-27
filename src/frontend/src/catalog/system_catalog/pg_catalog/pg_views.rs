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

/// The view `pg_views` provides access to useful information about each view in the database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-views.html`]
///
/// `pg_views` in RisingWave doesn't contain system catalog.
pub const PG_VIEWS: BuiltinTable = BuiltinTable {
    name: "pg_views",
    schema: PG_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "schemaname"),
        (DataType::Varchar, "viewname"),
        (DataType::Varchar, "viewowner"),
        (DataType::Varchar, "definition"),
    ],
    pk: &[0, 1],
};

impl SysCatalogReaderImpl {
    pub fn read_views_info(&self) -> Result<Vec<OwnedRow>> {
        // TODO(zehua): solve the deadlock problem.
        // Get two read locks. The order must be the same as
        // `FrontendObserverNode::handle_initialization_notification`.
        let catalog_reader = self.catalog_reader.read_guard();
        let user_info_reader = self.user_info_reader.read_guard();
        let schemas = catalog_reader.iter_schemas(&self.auth_context.database)?;

        Ok(schemas
            .flat_map(|schema| {
                schema.iter_view().map(|view| {
                    OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.name().into())),
                        Some(ScalarImpl::Utf8(view.name().into())),
                        Some(ScalarImpl::Utf8(
                            user_info_reader
                                .get_user_name_by_id(view.owner)
                                .unwrap()
                                .into(),
                        )),
                        // TODO(zehua): may be not same as postgresql's "definition" column.
                        Some(ScalarImpl::Utf8(view.sql.clone().into())),
                    ])
                })
            })
            .collect_vec())
    }
}
