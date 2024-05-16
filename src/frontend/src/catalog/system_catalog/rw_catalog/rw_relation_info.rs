// Copyright 2024 RisingWave Labs
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

use risingwave_common::types::{Fields, Timestamptz};
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;
#[derive(Fields)]
#[primary_key(schemaname, relationname)]
struct RwRelationInfo {
    schemaname: String,
    relationname: String,
    relationowner: i32,
    definition: String,
    relationtype: String,
    relationid: i32,
    initialized_at: Option<Timestamptz>,
    created_at: Option<Timestamptz>,
    initialized_at_cluster_version: Option<String>,
    created_at_cluster_version: Option<String>,
}

#[system_catalog(table, "rw_catalog.rw_relation_info")]
async fn read_relation_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwRelationInfo>> {
    let mut rows = Vec::new();
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.get_all_schema_names(&reader.auth_context.database)?;
    for schema in &schemas {
        let schema_catalog =
            catalog_reader.get_schema_by_name(&reader.auth_context.database, schema)?;
        schema_catalog.iter_mv().for_each(|t| {
            rows.push(RwRelationInfo {
                schemaname: schema.clone(),
                relationname: t.name.clone(),
                relationowner: t.owner as i32,
                definition: t.definition.clone(),
                relationtype: "MATERIALIZED VIEW".into(),
                relationid: t.id.table_id as i32,
                initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                created_at_cluster_version: t.created_at_cluster_version.clone(),
            });
        });

        schema_catalog.iter_table().for_each(|t| {
            rows.push(RwRelationInfo {
                schemaname: schema.clone(),
                relationname: t.name.clone(),
                relationowner: t.owner as i32,
                definition: t.definition.clone(),
                relationtype: "TABLE".into(),
                relationid: t.id.table_id as i32,
                initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                created_at_cluster_version: t.created_at_cluster_version.clone(),
            });
        });

        schema_catalog.iter_sink().for_each(|t| {
            rows.push(RwRelationInfo {
                schemaname: schema.clone(),
                relationname: t.name.clone(),
                relationowner: t.owner.user_id as i32,
                definition: t.definition.clone(),
                relationtype: "SINK".into(),
                relationid: t.id.sink_id as i32,
                initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                created_at_cluster_version: t.created_at_cluster_version.clone(),
            });
        });

        schema_catalog.iter_index().for_each(|t| {
            rows.push(RwRelationInfo {
                schemaname: schema.clone(),
                relationname: t.name.clone(),
                relationowner: t.index_table.owner as i32,
                definition: t.index_table.definition.clone(),
                relationtype: "INDEX".into(),
                relationid: t.index_table.id.table_id as i32,
                initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                created_at_cluster_version: t.created_at_cluster_version.clone(),
            });
        });

        schema_catalog.iter_source().for_each(|t| {
            rows.push(RwRelationInfo {
                schemaname: schema.clone(),
                relationname: t.name.clone(),
                relationowner: t.owner as i32,
                definition: t.definition.clone(),
                relationtype: "SOURCE".into(),
                relationid: t.id as i32,
                initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                created_at_cluster_version: t.created_at_cluster_version.clone(),
            });
        });

        schema_catalog.iter_subscription().for_each(|t| {
            rows.push(RwRelationInfo {
                schemaname: schema.clone(),
                relationname: t.name.clone(),
                relationowner: t.owner.user_id as i32,
                definition: t.definition.clone(),
                relationtype: "SUBSCRIPTION".into(),
                relationid: t.id.subscription_id as i32,
                initialized_at: t.initialized_at_epoch.map(|e| e.as_timestamptz()),
                created_at: t.created_at_epoch.map(|e| e.as_timestamptz()),
                initialized_at_cluster_version: t.initialized_at_cluster_version.clone(),
                created_at_cluster_version: t.created_at_cluster_version.clone(),
            });
        });
    }

    Ok(rows)
}
