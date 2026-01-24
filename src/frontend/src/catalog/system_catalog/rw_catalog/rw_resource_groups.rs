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

/// `rw_resource_groups` is a view that shows all resource groups in all databases.
#[system_catalog(
    view,
    "rw_catalog.rw_resource_groups",
    "WITH all_groups AS (SELECT DISTINCT resource_group
                    FROM (SELECT resource_group
                          FROM rw_worker_nodes
                          WHERE is_streaming
                          UNION ALL
                          SELECT resource_group
                          FROM rw_databases
                          UNION ALL
                          SELECT resource_group
                          FROM rw_streaming_jobs) t),
     worker_node_stats AS (SELECT resource_group,
                                  COUNT(*)         AS streaming_workers,
                                  SUM(parallelism) AS parallelism
                           FROM rw_worker_nodes
                           WHERE is_streaming
                           GROUP BY resource_group),
     database_stats AS (SELECT resource_group,
                               COUNT(*) AS databases
                        FROM rw_databases
                        GROUP BY resource_group),
     streaming_job_stats AS (SELECT resource_group,
                                    COUNT(*) AS streaming_jobs
                             FROM rw_streaming_jobs
                             GROUP BY resource_group)
    SELECT ag.resource_group                AS name,
           COALESCE(w.streaming_workers, 0) AS streaming_workers,
           COALESCE(w.parallelism, 0)       AS parallelism,
           COALESCE(d.databases, 0)         AS databases,
           COALESCE(j.streaming_jobs, 0)    AS streaming_jobs
    FROM all_groups ag
             LEFT JOIN worker_node_stats w ON ag.resource_group = w.resource_group
             LEFT JOIN database_stats d ON ag.resource_group = d.resource_group
             LEFT JOIN streaming_job_stats j ON ag.resource_group = j.resource_group"
)]
#[derive(Fields)]
struct RwResourceGroup {
    name: String,
    streaming_workers: i64,
    parallelism: i64,
    databases: i64,
    streaming_jobs: i64,
}
