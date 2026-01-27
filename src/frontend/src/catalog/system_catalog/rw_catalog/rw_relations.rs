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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// `rw_relations` is a view that shows all relations in the database.
#[system_catalog(
    view,
    "rw_catalog.rw_relations",
    "SELECT id, name, 'table' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_tables
    UNION ALL SELECT id, name, 'system table' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_system_tables
    UNION ALL SELECT id, name, 'source' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_sources
    UNION ALL SELECT id, name, 'index' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_indexes
    UNION ALL SELECT id, name, 'sink' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_sinks
    UNION ALL SELECT id, name, 'subscription' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_subscriptions
    UNION ALL SELECT id, name, 'materialized view' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_materialized_views
    UNION ALL SELECT id, name, 'view' AS relation_type, schema_id, owner, definition, acl FROM rw_catalog.rw_views
    "
)]
#[derive(Fields)]
struct RwRelation {
    id: i32,
    name: String,
    relation_type: String,
    schema_id: i32,
    owner: i32,
    definition: String,
    acl: Vec<String>,
}
