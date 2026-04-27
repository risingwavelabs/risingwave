// Copyright 2025 RisingWave Labs
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

use risingwave_common::id::ObjectId;
use risingwave_common::session_config::SearchPath;
use risingwave_expr::{Result, capture_context, function};

use super::context::{CATALOG_READER, DB_NAME, SEARCH_PATH};
use crate::catalog::CatalogReader;
use crate::catalog::schema_catalog::SchemaCatalog;
use crate::catalog::system_catalog::is_system_catalog;

#[function("pg_table_is_visible(int4) -> boolean")]
fn pg_table_is_visible(oid: i32) -> Result<Option<bool>> {
    pg_table_is_visible_impl_captured(ObjectId::new(oid as _))
}

#[capture_context(CATALOG_READER, SEARCH_PATH, DB_NAME)]
fn pg_table_is_visible_impl(
    catalog: &CatalogReader,
    search_path: &SearchPath,
    db_name: &str,
    oid: ObjectId,
) -> Result<Option<bool>> {
    // To maintain consistency with PostgreSQL, we ensure that system catalogs are always visible.
    if is_system_catalog(oid) {
        return Ok(Some(true));
    }

    let catalog_reader = catalog.read_guard();
    let Some(target_name) = catalog_reader
        .iter_schemas(db_name)
        .ok()
        .and_then(|mut schemas| schemas.find_map(|schema| relation_name_by_oid(schema, oid)))
    else {
        return Ok(None);
    };

    for schema in search_path.path() {
        if let Ok(schema) = catalog_reader.get_schema_by_name(db_name, schema)
            && let Some(found_oid) = relation_oid_by_name(schema, &target_name)
        {
            return Ok(Some(found_oid == oid));
        }
    }

    Ok(Some(false))
}

fn relation_name_by_oid(schema: &SchemaCatalog, oid: ObjectId) -> Option<String> {
    if let Some(table) = schema.get_created_table_by_id(oid.as_table_id()) {
        Some(table.name.clone())
    } else if let Some(view) = schema.get_view_by_id(oid.as_view_id()) {
        Some(view.name.clone())
    } else if let Some(source) = schema.get_source_by_id(oid.as_source_id()) {
        Some(source.name.clone())
    } else if let Some(sink) = schema.get_sink_by_id(oid.as_sink_id()) {
        Some(sink.name.clone())
    } else if let Some(index) = schema.get_index_by_id(oid.as_index_id()) {
        Some(index.name.clone())
    } else {
        schema
            .get_subscription_by_id(oid.as_subscription_id())
            .map(|subscription| subscription.name.clone())
    }
}

fn relation_oid_by_name(schema: &SchemaCatalog, name: &str) -> Option<ObjectId> {
    if let Some(table) = schema.get_created_table_by_name(name) {
        Some(table.id.as_object_id())
    } else if let Some(view) = schema.get_view_by_name(name) {
        Some(view.id.into())
    } else if let Some(source) = schema.get_source_by_name(name) {
        Some(source.id.into())
    } else if let Some(sink) = schema.get_created_sink_by_name(name) {
        Some(sink.id.into())
    } else if let Some(index) = schema.get_created_index_by_name(name) {
        Some(index.id.as_object_id())
    } else {
        schema
            .get_subscription_by_name(name)
            .map(|subscription| subscription.id.into())
    }
}
