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

#[system_catalog(
    view,
    "rw_catalog.rw_recovery_info",
    "WITH events AS (
        SELECT event_type,
               timestamp,
               COALESCE(
                   info -> 'recovery' -> 'databaseStart'   ->> 'databaseId',
                   info -> 'recovery' -> 'databaseSuccess' ->> 'databaseId',
                   info -> 'recovery' -> 'databaseFailure' ->> 'databaseId'
               ) AS database_id
        FROM rw_catalog.rw_event_logs
        WHERE event_type LIKE 'DATABASE_RECOVERY_%'
    ),
    ranked AS (
        SELECT event_type,
               timestamp AS event_timestamp,
               database_id::integer AS database_id,
               row_number() OVER (PARTITION BY database_id ORDER BY timestamp DESC) AS rn
        FROM events
        WHERE database_id IS NOT NULL
    ),
last_global_event AS (
    SELECT
        event_type,
        timestamp AS event_timestamp,
        info
    FROM rw_catalog.rw_event_logs
    WHERE event_type IN ('GLOBAL_RECOVERY_SUCCESS', 'GLOBAL_RECOVERY_FAILURE')
    ORDER BY timestamp DESCa
    LIMIT 1
),
global_flag AS (
    SELECT
        (SELECT event_type FROM last_global_event) AS event_type,
        (SELECT event_timestamp FROM last_global_event) AS event_timestamp
),
running_ids AS (
    SELECT jsonb_array_elements_text(
               info -> 'recovery' -> 'globalSuccess' -> 'runningDatabaseIds'
           )::integer AS database_id
    FROM last_global_event
),
recovering_ids AS (
    SELECT jsonb_array_elements_text(
               info -> 'recovery' -> 'globalSuccess' -> 'recoveringDatabaseIds'
           )::integer AS database_id
    FROM last_global_event
),
    combined AS (
        SELECT
            d.id AS database_id,
            d.name AS database_name,
            regexp_replace(
                COALESCE(r.event_type, 'DATABASE_RECOVERY_UNKNOWN'),
                '^DATABASE_RECOVERY_',
                ''
            ) AS last_database_event,
            CASE
                WHEN global_flag.event_type = 'GLOBAL_RECOVERY_SUCCESS'
                     AND global_flag.event_timestamp >= COALESCE(r.event_timestamp, global_flag.event_timestamp) THEN
                    CASE
                        WHEN running.database_id IS NOT NULL THEN 'RUNNING'
                        WHEN recovering.database_id IS NOT NULL THEN 'RECOVERING'
                        ELSE 'UNKNOWN'
                    END
                WHEN global_flag.event_type = 'GLOBAL_RECOVERY_FAILURE' THEN 'RECOVERING'
                ELSE 'UNKNOWN'
            END AS last_global_event,
            (
                global_flag.event_type = 'GLOBAL_RECOVERY_SUCCESS'
                AND global_flag.event_timestamp >= COALESCE(r.event_timestamp, global_flag.event_timestamp)
                AND running.database_id IS NOT NULL
            ) AS in_global_running,
            (
                global_flag.event_type = 'GLOBAL_RECOVERY_SUCCESS'
                AND global_flag.event_timestamp >= COALESCE(r.event_timestamp, global_flag.event_timestamp)
                AND recovering.database_id IS NOT NULL
            ) AS in_global_recovering
        FROM rw_catalog.rw_databases d
        LEFT JOIN ranked r
               ON d.id = r.database_id AND r.rn = 1
        LEFT JOIN running_ids AS running
               ON d.id = running.database_id
        LEFT JOIN recovering_ids AS recovering
               ON d.id = recovering.database_id
        CROSS JOIN global_flag
    )
    SELECT
        database_id,
        database_name,
        CASE
            WHEN last_database_event = 'SUCCESS' THEN 'RUNNING'
            WHEN last_global_event = 'RUNNING' AND in_global_running THEN 'RUNNING'
            WHEN last_global_event = 'RUNNING' AND in_global_recovering THEN 'RECOVERING'
            WHEN last_global_event = 'RECOVERING' AND in_global_recovering THEN 'RECOVERING'
            WHEN last_database_event = 'START' THEN 'RECOVERING'
            WHEN last_global_event = 'RECOVERING' THEN 'RECOVERING'
            ELSE 'UNKNOWN'
        END AS recovery_state,
        last_database_event,
        last_global_event,
        in_global_running,
        in_global_recovering
    FROM combined
    ORDER BY database_id"
)]
#[derive(Fields)]
struct RwRecoveryInfo {
    database_id: i32,
    database_name: String,
    recovery_state: String,
    last_database_event: String,
    last_global_event: String,
    in_global_running: bool,
    in_global_recovering: bool,
}
