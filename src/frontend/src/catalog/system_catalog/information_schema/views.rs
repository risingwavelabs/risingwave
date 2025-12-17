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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

/// The view `views` contains all views defined in the current database. Only those views
/// are shown that the current user has access to (by way of being the owner or having
/// some privilege).
/// Ref: `https://www.postgresql.org/docs/current/infoschema-views.html`
///
/// In RisingWave, `views` contains information about defined views.
#[system_catalog(
    view,
    "information_schema.views",
    "SELECT CURRENT_DATABASE() AS table_catalog,
            s.name AS table_schema,
            v.name AS table_name,
            v.definition AS view_definition
        FROM rw_catalog.rw_views v
        JOIN rw_catalog.rw_schemas s ON v.schema_id = s.id
        ORDER BY table_schema, table_name"
)]
#[derive(Fields)]
struct View {
    table_catalog: String,
    table_schema: String,
    table_name: String,
    view_definition: String,
}
