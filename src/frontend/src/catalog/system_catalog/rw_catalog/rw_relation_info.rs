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

use risingwave_common::catalog::RW_CATALOG_SCHEMA_NAME;
use risingwave_common::error::Result;
use risingwave_common::row::OwnedRow;
use risingwave_common::types::{DataType, ScalarImpl};
use serde_json::json;

use crate::catalog::system_catalog::{BuiltinTable, SysCatalogReaderImpl};

// TODO: `rw_relation_info` contains some extra streaming meta info that's only meaningful for
// streaming jobs, we'd better query relation infos from `rw_relations` and move these streaming
// infos into anther system table.
pub const RW_RELATION_INFO: BuiltinTable = BuiltinTable {
    name: "rw_relation_info",
    schema: RW_CATALOG_SCHEMA_NAME,
    columns: &[
        (DataType::Varchar, "schemaname"),
        (DataType::Varchar, "relationname"),
        (DataType::Int32, "relationowner"),
        (DataType::Varchar, "definition"),
        (DataType::Varchar, "relationtype"),
        (DataType::Int32, "relationid"),
        (DataType::Varchar, "relationtimezone"), /* The timezone used to interpret ambiguous
                                                  * dates/timestamps as tstz */
        (DataType::Varchar, "fragments"), // fragments is json encoded fragment infos.
        (DataType::Timestamptz, "initialized_at"),
        (DataType::Timestamptz, "created_at"),
    ],
    pk: &[0, 1],
};

impl SysCatalogReaderImpl {
    pub async fn read_relation_info(&self) -> Result<Vec<OwnedRow>> {
        let mut table_ids = Vec::new();
        {
            let reader = self.catalog_reader.read_guard();
            let schemas = reader.get_all_schema_names(&self.auth_context.database)?;
            for schema in &schemas {
                let schema_catalog =
                    reader.get_schema_by_name(&self.auth_context.database, schema)?;

                schema_catalog.iter_mv().for_each(|t| {
                    table_ids.push(t.id.table_id);
                });

                schema_catalog.iter_table().for_each(|t| {
                    table_ids.push(t.id.table_id);
                });

                schema_catalog.iter_sink().for_each(|t| {
                    table_ids.push(t.id.sink_id);
                });

                schema_catalog.iter_index().for_each(|t| {
                    table_ids.push(t.index_table.id.table_id);
                });
            }
        }

        let table_fragments = self.meta_client.list_table_fragments(&table_ids).await?;
        let mut rows = Vec::new();
        let reader = self.catalog_reader.read_guard();
        let schemas = reader.get_all_schema_names(&self.auth_context.database)?;
        for schema in &schemas {
            let schema_catalog = reader.get_schema_by_name(&self.auth_context.database, schema)?;
            schema_catalog.iter_mv().for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.id.table_id) {
                    rows.push(OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.clone().into())),
                        Some(ScalarImpl::Utf8(t.name.clone().into())),
                        Some(ScalarImpl::Int32(t.owner as i32)),
                        Some(ScalarImpl::Utf8(t.definition.clone().into())),
                        Some(ScalarImpl::Utf8("MATERIALIZED VIEW".into())),
                        Some(ScalarImpl::Int32(t.id.table_id as i32)),
                        Some(ScalarImpl::Utf8(
                            fragments.get_env().unwrap().get_timezone().clone().into(),
                        )),
                        Some(ScalarImpl::Utf8(
                            json!(fragments.get_fragments()).to_string().into(),
                        )),
                        t.initialized_at_epoch.map(|e| e.as_scalar()),
                        t.created_at_epoch.map(|e| e.as_scalar()),
                    ]));
                }
            });

            schema_catalog.iter_table().for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.id.table_id) {
                    rows.push(OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.clone().into())),
                        Some(ScalarImpl::Utf8(t.name.clone().into())),
                        Some(ScalarImpl::Int32(t.owner as i32)),
                        Some(ScalarImpl::Utf8(t.definition.clone().into())),
                        Some(ScalarImpl::Utf8("TABLE".into())),
                        Some(ScalarImpl::Int32(t.id.table_id as i32)),
                        Some(ScalarImpl::Utf8(
                            fragments.get_env().unwrap().get_timezone().clone().into(),
                        )),
                        Some(ScalarImpl::Utf8(
                            json!(fragments.get_fragments()).to_string().into(),
                        )),
                        t.initialized_at_epoch.map(|e| e.as_scalar()),
                        t.created_at_epoch.map(|e| e.as_scalar()),
                    ]));
                }
            });

            schema_catalog.iter_sink().for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.id.sink_id) {
                    rows.push(OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.clone().into())),
                        Some(ScalarImpl::Utf8(t.name.clone().into())),
                        Some(ScalarImpl::Int32(t.owner.user_id as i32)),
                        Some(ScalarImpl::Utf8(t.definition.clone().into())),
                        Some(ScalarImpl::Utf8("SINK".into())),
                        Some(ScalarImpl::Int32(t.id.sink_id as i32)),
                        Some(ScalarImpl::Utf8(
                            fragments.get_env().unwrap().get_timezone().clone().into(),
                        )),
                        Some(ScalarImpl::Utf8(
                            json!(fragments.get_fragments()).to_string().into(),
                        )),
                        t.initialized_at_epoch.map(|e| e.as_scalar()),
                        t.created_at_epoch.map(|e| e.as_scalar()),
                    ]));
                }
            });

            schema_catalog.iter_index().for_each(|t| {
                if let Some(fragments) = table_fragments.get(&t.index_table.id.table_id) {
                    rows.push(OwnedRow::new(vec![
                        Some(ScalarImpl::Utf8(schema.clone().into())),
                        Some(ScalarImpl::Utf8(t.name.clone().into())),
                        Some(ScalarImpl::Int32(t.index_table.owner as i32)),
                        Some(ScalarImpl::Utf8(t.index_table.definition.clone().into())),
                        Some(ScalarImpl::Utf8("INDEX".into())),
                        Some(ScalarImpl::Int32(t.index_table.id.table_id as i32)),
                        Some(ScalarImpl::Utf8(
                            fragments.get_env().unwrap().get_timezone().clone().into(),
                        )),
                        Some(ScalarImpl::Utf8(
                            json!(fragments.get_fragments()).to_string().into(),
                        )),
                        t.initialized_at_epoch.map(|e| e.as_scalar()),
                        t.created_at_epoch.map(|e| e.as_scalar()),
                    ]));
                }
            });

            // Sources have no fragments.
            schema_catalog.iter_source().for_each(|t| {
                rows.push(OwnedRow::new(vec![
                    Some(ScalarImpl::Utf8(schema.clone().into())),
                    Some(ScalarImpl::Utf8(t.name.clone().into())),
                    Some(ScalarImpl::Int32(t.owner as i32)),
                    Some(ScalarImpl::Utf8(t.definition.clone().into())),
                    Some(ScalarImpl::Utf8("SOURCE".into())),
                    Some(ScalarImpl::Int32(t.id as i32)),
                    Some(ScalarImpl::Utf8("".into())),
                    None,
                    t.initialized_at_epoch.map(|e| e.as_scalar()),
                    t.created_at_epoch.map(|e| e.as_scalar()),
                ]));
            });
        }

        Ok(rows)
    }
}
