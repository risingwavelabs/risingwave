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

use risingwave_common::types::Fields;
use risingwave_frontend_macro::system_catalog;

#[system_catalog(
    view,
    "rw_catalog.rw_internal_table_info",
    "WITH all_streaming_jobs AS (
        SELECT id, name, 'table' as job_type, schema_id, owner FROM rw_tables
        UNION ALL
        SELECT id, name, 'materialized view' as job_type, schema_id, owner FROM rw_materialized_views
        UNION ALL
        SELECT id, name, 'sink' as job_type, schema_id, owner FROM rw_sinks
        UNION ALL
        SELECT id, name, 'index' as job_type, schema_id, owner FROM rw_indexes
        UNION ALL
        SELECT id, name, 'source' as job_type, schema_id, owner FROM rw_sources WHERE is_shared = true
    )

    SELECT i.id,
            i.name,
            j.id as job_id,
            j.name as job_name,
            j.job_type,
            s.name as schema_name,
            u.name as owner
    FROM rw_catalog.rw_internal_tables i
    JOIN all_streaming_jobs j ON i.job_id = j.id
    JOIN rw_catalog.rw_schemas s ON j.schema_id = s.id
    JOIN rw_catalog.rw_users u ON j.owner = u.id"
)]
#[derive(Fields)]
struct RwInternalTableInfo {
    #[primary_key]
    id: i32,
    name: String,
    job_id: i32,
    job_name: String,
    job_type: String,
    schema_name: String,
    owner: String,
}
