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

use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use serde_json::json;

use crate::catalog::system_catalog::{SysCatalogReaderImpl, SystemCatalogColumnsDef};

/// The view `pg_matviews` provides access to useful information about each materialized view in the
/// database.
/// Ref: [`https://www.postgresql.org/docs/current/view-pg-matviews.html`]
pub const PG_MATVIEWS_TABLE_NAME: &str = "pg_matviews";
pub const PG_MATVIEWS_COLUMNS: &[SystemCatalogColumnsDef<'_>] = &[
    (DataType::Varchar, "schemaname"),
    (DataType::Varchar, "matviewname"),
    (DataType::Int32, "matviewowner"),
    // TODO: add index back when open create index doc again.
    // (DataType::Boolean, "hasindexes"),
    (DataType::Varchar, "definition"),
    // Below are some columns that PostgreSQL doesn't have.
    (DataType::Int32, "matviewid"),
    (DataType::Varchar, "matviewtimezone"), /* The timezone used to interpret ambiguous
                                             * dates/timestamps as tstz */
    (DataType::Varchar, "matviewgraph"), // materialized view graph is json encoded fragment infos.
];

impl SysCatalogReaderImpl {
    pub async fn read_mviews_info(&self) -> Result<Vec<OwnedRow>> {
        let mut table_ids = Vec::new();
        {
            let reader = self.catalog_reader.read_guard();
            let schemas = reader.get_all_schema_names(&self.auth_context.database)?;
            for schema in &schemas {
                reader
                    .get_schema_by_name(&self.auth_context.database, schema)?
                    .iter_mv()
                    .for_each(|t| {
                        table_ids.push(t.id.table_id);
                    });
            }
        }

        let table_fragments = self.meta_client.list_table_fragments(&table_ids).await?;
        let mut rows = Vec::new();
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.get_all_schema_names(&self.auth_context.database)?;
        for schema in &schemas {
            reader
                .get_schema_by_name(&self.auth_context.database, schema)?
                .iter_mv()
                .for_each(|t| {
                    if let Some(fragments) = table_fragments.get(&t.id.table_id) {
                        rows.push(OwnedRow::new(vec![
                            Some(ScalarImpl::Utf8(schema.clone().into())),
                            Some(ScalarImpl::Utf8(t.name.clone().into())),
                            Some(ScalarImpl::Int32(t.owner as i32)),
                            Some(ScalarImpl::Utf8(t.definition.clone().into())),
                            Some(ScalarImpl::Int32(t.id.table_id as i32)),
                            Some(ScalarImpl::Utf8(
                                fragments.get_env().unwrap().get_timezone().clone().into(),
                            )),
                            Some(ScalarImpl::Utf8(
                                json!(fragments.get_fragments()).to_string().into(),
                            )),
                        ]));
                    }
                });
        }

        Ok(rows)
    }
}
