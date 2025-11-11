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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, KillOpts};
use serde::Deserialize;
use tokio::time::{Instant, sleep};

pub(crate) async fn kill_cn_and_wait_recover(cluster: &mut Cluster) {
    cluster
        .kill_nodes(["compute-1", "compute-2", "compute-3"], 0)
        .await;
    wait_all_database_recovered(cluster).await;
}

pub(crate) async fn kill_cn_and_meta_and_wait_recover(cluster: &mut Cluster) {
    cluster
        .kill_nodes(["compute-1", "compute-2", "compute-3", "meta-1"], 0)
        .await;
    wait_all_database_recovered(cluster).await;
}

pub(crate) async fn kill_random_and_wait_recover(cluster: &mut Cluster) {
    // Kill it again
    for _ in 0..3 {
        sleep(Duration::from_secs(2)).await;
        cluster.kill_node(&KillOpts::ALL_FAST).await;
    }
    wait_all_database_recovered(cluster).await;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseRecoveryState {
    Running,
    Recovering,
    Failed,
    Unknown,
}

impl DatabaseRecoveryState {
    fn from_event(event: &str) -> Self {
        match event.trim() {
            "DATABASE_RECOVERY_SUCCESS" => Self::Running,
            "DATABASE_RECOVERY_START" => Self::Recovering,
            "DATABASE_RECOVERY_FAILURE" => Self::Failed,
            _ => Self::Unknown,
        }
    }

    fn is_running(self) -> bool {
        matches!(self, Self::Running)
    }
}

#[derive(Debug)]
struct DatabaseRecoveryInfo {
    id: u32,
    name: String,
    state: DatabaseRecoveryState,
}

#[derive(Deserialize)]
struct DatabaseRecoveryRow {
    id: u32,
    name: String,
    #[serde(rename = "event_type")]
    event_type: String,
}

#[derive(Debug)]
struct GlobalRecoveryInfo {
    running_ids: HashSet<u32>,
    recovering_ids: HashSet<u32>,
}

#[derive(Deserialize)]
struct GlobalRecoveryRow {
    has_event: bool,
    #[serde(default)]
    running_ids: Vec<u32>,
    #[serde(default)]
    recovering_ids: Vec<u32>,
}

impl GlobalRecoveryInfo {
    fn new(running_ids: Vec<u32>, recovering_ids: Vec<u32>) -> Self {
        let running_ids: HashSet<u32> = running_ids.into_iter().collect();
        let recovering_ids: HashSet<u32> = recovering_ids.into_iter().collect();
        if let Some(duplicated) = running_ids
            .iter()
            .find(|id| recovering_ids.contains(id))
            .copied()
        {
            panic!(
                "Database {duplicated} appears in both running and recovering sets of global recovery event"
            );
        }
        Self {
            running_ids,
            recovering_ids,
        }
    }
}

const ALL_DATABASE_RECOVERY_STATUS_SQL: &str = r#"
WITH events AS (
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
           database_id::integer AS database_id,
           row_number() OVER (PARTITION BY database_id ORDER BY timestamp DESC) AS rn
    FROM events
    WHERE database_id IS NOT NULL
)
SELECT row_to_json(row) AS json_line
FROM (
    SELECT d.id,
           d.name,
           COALESCE(r.event_type, 'DATABASE_RECOVERY_UNKNOWN') AS event_type
    FROM rw_catalog.rw_databases d
    LEFT JOIN ranked r
           ON d.id = r.database_id AND r.rn = 1
    ORDER BY d.name
) AS row;
"#;

const LATEST_GLOBAL_RECOVERY_STATUS_SQL: &str = r#"
WITH last_global_success AS (
    SELECT info
    FROM rw_catalog.rw_event_logs
    WHERE event_type = 'GLOBAL_RECOVERY_SUCCESS'
    ORDER BY timestamp DESC
    LIMIT 1
),
running_ids AS (
    SELECT jsonb_array_elements_text(
               info -> 'recovery' -> 'globalSuccess' -> 'runningDatabaseIds'
           )::integer AS database_id
    FROM last_global_success
),
recovering_ids AS (
    SELECT jsonb_array_elements_text(
               info -> 'recovery' -> 'globalSuccess' -> 'recoveringDatabaseIds'
           )::integer AS database_id
    FROM last_global_success
)
SELECT row_to_json(row) AS json_line
FROM (
    SELECT
        EXISTS (SELECT 1 FROM last_global_success) AS has_event,
        COALESCE(
            (
                SELECT array_agg(database_id ORDER BY database_id)
                FROM running_ids
            ),
            ARRAY[]::integer[]
        ) AS running_ids,
        COALESCE(
            (
                SELECT array_agg(database_id ORDER BY database_id)
                FROM recovering_ids
            ),
            ARRAY[]::integer[]
        ) AS recovering_ids
) AS row;
"#;

pub(crate) async fn query_all_database_recovery_state(
    cluster: &mut Cluster,
) -> Result<Vec<DatabaseRecoveryInfo>> {
    let mut session = cluster.start_session();
    let output = session.run(ALL_DATABASE_RECOVERY_STATUS_SQL).await?;
    drop(session);

    let mut result = Vec::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let row: DatabaseRecoveryRow = serde_json::from_str(line)?;
        result.push(DatabaseRecoveryInfo {
            id: row.id,
            name: row.name,
            state: DatabaseRecoveryState::from_event(&row.event_type),
        });
    }
    Ok(result)
}

async fn query_latest_global_recovery_state(
    cluster: &mut Cluster,
) -> Result<Option<GlobalRecoveryInfo>> {
    let mut session = cluster.start_session();
    let output = session.run(LATEST_GLOBAL_RECOVERY_STATUS_SQL).await?;
    drop(session);

    let line = output.lines().find(|line| !line.trim().is_empty());
    let Some(line) = line else {
        return Ok(None);
    };

    let row: GlobalRecoveryRow = serde_json::from_str(line)?;
    if !row.has_event {
        return Ok(None);
    }

    Ok(Some(GlobalRecoveryInfo::new(
        row.running_ids,
        row.recovering_ids,
    )))
}

fn validate_global_consistency(databases: &[DatabaseRecoveryInfo], global: &GlobalRecoveryInfo) {
    let db_map: HashMap<u32, &DatabaseRecoveryInfo> =
        databases.iter().map(|info| (info.id, info)).collect();

    for &running_id in &global.running_ids {
        let info = db_map.get(&running_id).unwrap_or_else(|| {
            panic!("Global recovery lists unknown running database id {running_id}")
        });
        if !info.state.is_running() {
            panic!(
                "Global recovery marks database {}({}) running but latest database event is {:?}",
                info.name, info.id, info.state
            );
        }
    }

    for &recovering_id in &global.recovering_ids {
        let info = db_map.get(&recovering_id).unwrap_or_else(|| {
            panic!("Global recovery lists unknown recovering database id {recovering_id}")
        });
        if info.state.is_running() {
            panic!(
                "Global recovery marks database {}({}) recovering but latest database event is {:?}",
                info.name, info.id, info.state
            );
        }
    }

    for info in databases {
        if !info.state.is_running() && !global.recovering_ids.contains(&info.id) {
            panic!(
                "Database {}({}) is not running but absent from global recovering ids",
                info.name, info.id
            );
        }
    }
}

pub(crate) async fn cluster_fully_running(cluster: &mut Cluster) -> Result<bool> {
    let db_statuses = match query_all_database_recovery_state(cluster).await {
        Ok(statuses) => statuses,
        Err(_) => return Ok(false),
    };

    if db_statuses.is_empty() {
        return Ok(false);
    }

    let global_status = match query_latest_global_recovery_state(cluster).await {
        Ok(status) => status,
        Err(_) => return Ok(false),
    };

    if let Some(global) = &global_status {
        validate_global_consistency(&db_statuses, global);
    }

    let fully_running = db_statuses.iter().all(|info| {
        info.state.is_running()
            || global_status
                .as_ref()
                .map_or(false, |global| global.running_ids.contains(&info.id))
    });

    Ok(fully_running)
}

const WAIT_ALL_DB_TIMEOUT: Duration = Duration::from_secs(100);
const WAIT_ALL_DB_INTERVAL: Duration = Duration::from_secs(10);

pub(crate) async fn wait_all_database_recovered(cluster: &mut Cluster) {
    let start = Instant::now();
    while start.elapsed() < WAIT_ALL_DB_TIMEOUT {
        if matches!(cluster_fully_running(cluster).await, Ok(true)) {
            return;
        }
        sleep(WAIT_ALL_DB_INTERVAL).await;
    }
}
