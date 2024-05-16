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

use risingwave_common::types::{Fields, JsonbVal};
use risingwave_frontend_macro::system_catalog;
use serde_json::json;

use crate::catalog::system_catalog::SysCatalogReaderImpl;
use crate::error::Result;

#[derive(Fields)]
struct RwStreamingGraph {
    #[primary_key]
    id: i32,
    name: String,
    relation_type: String,
    definition: String,
    timezone: String, // The timezone used to interpret ambiguous dates/timestamps as tstz
    graph: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_streaming_graph")]
async fn read_streaming_graph(reader: &SysCatalogReaderImpl) -> Result<Vec<RwStreamingGraph>> {
    let mut job_ids = vec![];
    {
        let catalog_reader = reader.catalog_reader.read_guard();
        let schemas = catalog_reader.get_all_schema_names(&reader.auth_context.database)?;
        for schema in &schemas {
            let schema_catalog =
                catalog_reader.get_schema_by_name(&reader.auth_context.database, schema)?;
            job_ids.extend(schema_catalog.iter_mv().map(|mv| mv.id.table_id));
            job_ids.extend(schema_catalog.iter_table().map(|t| t.id.table_id));
            job_ids.extend(schema_catalog.iter_sink().map(|s| s.id.sink_id));
            job_ids.extend(
                schema_catalog
                    .iter_source()
                    .filter_map(|s| s.info.is_shared().then(|| s.id)),
            );
            job_ids.extend(
                schema_catalog
                    .iter_index()
                    .map(|idx| idx.index_table.id.table_id),
            );
        }
    }

    let mut table_fragments = reader.meta_client.list_table_fragments(&job_ids).await?;
    let mut rows = Vec::new();
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.get_all_schema_names(&reader.auth_context.database)?;
    for schema in &schemas {
        let schema_catalog =
            catalog_reader.get_schema_by_name(&reader.auth_context.database, schema)?;
        schema_catalog.iter_mv().for_each(|t| {
            if let Some(fragments) = table_fragments.remove(&t.id.table_id) {
                rows.push(RwStreamingGraph {
                    id: t.id.table_id as i32,
                    name: t.name.clone(),
                    relation_type: "MATERIALIZED VIEW".into(),
                    definition: t.definition.clone(),
                    timezone: fragments.ctx.unwrap().timezone,
                    graph: json!(fragments.fragments).into(),
                });
            }
        });

        schema_catalog.iter_table().for_each(|t| {
            if let Some(fragments) = table_fragments.remove(&t.id.table_id) {
                rows.push(RwStreamingGraph {
                    id: t.id.table_id as i32,
                    name: t.name.clone(),
                    relation_type: "TABLE".into(),
                    definition: t.definition.clone(),
                    timezone: fragments.ctx.unwrap().timezone,
                    graph: json!(fragments.fragments).into(),
                });
            }
        });

        schema_catalog.iter_sink().for_each(|t| {
            if let Some(fragments) = table_fragments.remove(&t.id.sink_id) {
                rows.push(RwStreamingGraph {
                    id: t.id.sink_id as i32,
                    name: t.name.clone(),
                    relation_type: "SINK".into(),
                    definition: t.definition.clone(),
                    timezone: fragments.ctx.unwrap().timezone,
                    graph: json!(fragments.fragments).into(),
                });
            }
        });

        schema_catalog.iter_index().for_each(|t| {
            if let Some(fragments) = table_fragments.remove(&t.index_table.id.table_id) {
                rows.push(RwStreamingGraph {
                    id: t.index_table.id.table_id as i32,
                    name: t.name.clone(),
                    relation_type: "INDEX".into(),
                    definition: t.index_table.definition.clone(),
                    timezone: fragments.ctx.unwrap().timezone,
                    graph: json!(fragments.fragments).into(),
                });
            }
        });

        schema_catalog
            .iter_source()
            .filter(|s| s.info.is_shared())
            .for_each(|t| {
                if let Some(fragments) = table_fragments.remove(&t.id) {
                    rows.push(RwStreamingGraph {
                        id: t.id as i32,
                        name: t.name.clone(),
                        relation_type: "SOURCE".into(),
                        definition: t.definition.clone(),
                        timezone: fragments.ctx.unwrap().timezone,
                        graph: json!(fragments.fragments).into(),
                    });
                }
            });
    }

    Ok(rows)
}
