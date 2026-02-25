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

use risingwave_common::catalog::CreateType;
use risingwave_common::id::{ConnectionId, SchemaId, SinkId, UserId};
use risingwave_common::types::{Fields, JsonbVal, Timestamptz};
use risingwave_connector::WithOptionsSecResolved;
use risingwave_frontend_macro::system_catalog;

use crate::catalog::system_catalog::rw_catalog::rw_sources::serialize_props_with_secret;
use crate::catalog::system_catalog::{SysCatalogReaderImpl, get_acl_items};
use crate::error::Result;
use crate::handler::create_source::UPSTREAM_SOURCE_KEY;

#[derive(Fields)]
struct RwSink {
    #[primary_key]
    id: SinkId,
    name: String,
    schema_id: SchemaId,
    owner: UserId,
    connector: String,
    sink_type: String,
    connection_id: Option<ConnectionId>,
    definition: String,
    acl: Vec<String>,
    initialized_at: Option<Timestamptz>,
    created_at: Option<Timestamptz>,
    initialized_at_cluster_version: Option<String>,
    created_at_cluster_version: Option<String>,
    background_ddl: bool,

    // connector properties in json format
    connector_props: JsonbVal,
    // format and encode properties in json format
    format_encode_options: JsonbVal,
}

#[system_catalog(table, "rw_catalog.rw_sinks")]
fn read_rw_sinks_info(reader: &SysCatalogReaderImpl) -> Result<Vec<RwSink>> {
    let catalog_reader = reader.catalog_reader.read_guard();
    let schemas = catalog_reader.iter_schemas(&reader.auth_context.database)?;
    let user_reader = reader.user_info_reader.read_guard();
    let current_user = user_reader
        .get_user_by_name(&reader.auth_context.user_name)
        .expect("user not found");
    let users = user_reader.get_all_users();
    let username_map = user_reader.get_user_name_map();

    Ok(schemas
        .flat_map(|schema| {
            schema.iter_sink_with_acl(current_user).map(|sink| {
                let connector_props = serialize_props_with_secret(
                    &catalog_reader,
                    &reader.auth_context.database,
                    WithOptionsSecResolved::new(sink.properties.clone(), sink.secret_refs.clone()),
                )
                .into();
                let format_encode_options = sink
                    .format_desc
                    .as_ref()
                    .map(|desc| {
                        serialize_props_with_secret(
                            &catalog_reader,
                            &reader.auth_context.database,
                            WithOptionsSecResolved::new(
                                desc.options.clone(),
                                desc.secret_refs.clone(),
                            ),
                        )
                    })
                    .unwrap_or_else(jsonbb::Value::null)
                    .into();
                RwSink {
                    id: sink.id,
                    name: sink.name.clone(),
                    schema_id: schema.id(),
                    owner: sink.owner,
                    connector: sink
                        .properties
                        .get(UPSTREAM_SOURCE_KEY)
                        .cloned()
                        .unwrap_or("".to_owned())
                        .to_uppercase(),
                    sink_type: sink.sink_type.to_proto().as_str_name().into(),
                    connection_id: sink.connection_id,
                    definition: sink.create_sql(),
                    acl: get_acl_items(sink.id, false, &users, username_map),
                    initialized_at: sink.initialized_at_epoch.map(|e| e.as_timestamptz()),
                    created_at: sink.created_at_epoch.map(|e| e.as_timestamptz()),
                    initialized_at_cluster_version: sink.initialized_at_cluster_version.clone(),
                    created_at_cluster_version: sink.created_at_cluster_version.clone(),
                    connector_props,
                    format_encode_options,
                    background_ddl: sink.create_type == CreateType::Background,
                }
            })
        })
        .collect())
}

#[system_catalog(
    view,
    "rw_catalog.rw_sink_decouple",
    "WITH decoupled_sink_internal_table_ids AS (
        SELECT
            sink_id,
            internal_table_id
        FROM rw_catalog.rw_sink_log_store_tables
    ),
    internal_table_vnode_count AS (
        SELECT
            internal_table_id, count(*)::int as watermark_vnode_count
        FROM decoupled_sink_internal_table_ids
            LEFT JOIN
                rw_catalog.rw_hummock_table_watermark
            ON decoupled_sink_internal_table_ids.internal_table_id = rw_catalog.rw_hummock_table_watermark.table_id
        GROUP BY internal_table_id
    )
    SELECT
        rw_catalog.rw_sinks.id as sink_id,
        (watermark_vnode_count is not null) as is_decouple,
        watermark_vnode_count
    FROM rw_catalog.rw_sinks
        LEFT JOIN
            (decoupled_sink_internal_table_ids
                JOIN
                    internal_table_vnode_count
                ON decoupled_sink_internal_table_ids.internal_table_id = internal_table_vnode_count.internal_table_id
            )
        ON sink_id = rw_catalog.rw_sinks.id
    "
)]
#[derive(Fields)]
struct RwSinkDecouple {
    sink_id: i32,
    is_decouple: bool,
    watermark_vnode_count: i32,
}
