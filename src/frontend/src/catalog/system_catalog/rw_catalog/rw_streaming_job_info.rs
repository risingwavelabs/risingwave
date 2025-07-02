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

/// `rw_streaming_job_info` is a view that provides information about streaming jobs in the system.
#[system_catalog(
    view,
    "rw_catalog.rw_streaming_job_info",
    "SELECT
        j.id,
        j.name,
        d.name as database_name,
        j.status,
        j.parallelism,
        j.max_parallelism,
        j.resource_group
    FROM
        rw_streaming_jobs AS j
    JOIN
        rw_databases AS d ON j.database_id = d.id"
)]
#[derive(Fields)]
struct RwStreamingJobInfo {
    #[primary_key]
    id: i32,
    name: String,
    database_name: String,
    status: String,
    parallelism: String,
    max_parallelism: i32,
    resource_group: String,
}
