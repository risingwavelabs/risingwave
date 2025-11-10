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

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, KillOpts};
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
           database_id::bigint AS database_id,
           row_number() OVER (PARTITION BY database_id ORDER BY timestamp DESC) AS rn
    FROM events
    WHERE database_id IS NOT NULL
)
SELECT d.name,
       COALESCE(r.event_type, 'DATABASE_RECOVERY_UNKNOWN') AS event_type
FROM rw_catalog.rw_databases d
LEFT JOIN ranked r
       ON d.id = r.database_id AND r.rn = 1
ORDER BY d.name;
"#;

pub(crate) async fn query_all_database_recovery_state(
    cluster: &mut Cluster,
) -> Result<HashMap<String, DatabaseRecoveryState>> {
    let mut session = cluster.start_session();
    let output = session.run(ALL_DATABASE_RECOVERY_STATUS_SQL).await?;
    drop(session);

    let mut result = HashMap::new();
    for line in output.lines().filter(|line| !line.trim().is_empty()) {
        let (name, event) = line
            .rsplit_once(' ')
            .unwrap_or((line, "DATABASE_RECOVERY_UNKNOWN"));
        result.insert(
            name.trim().to_owned(),
            DatabaseRecoveryState::from_event(event),
        );
    }
    Ok(result)
}

pub(crate) async fn cluster_fully_running(cluster: &mut Cluster) -> Result<bool> {
    let global_running = {
        let mut session = cluster.start_session();
        match session.run("select rw_recovery_status()").await {
            Ok(status) => status.trim() == "RUNNING",
            Err(_) => return Ok(false),
        }
    };

    let db_statuses = match query_all_database_recovery_state(cluster).await {
        Ok(statuses) => statuses,
        Err(_) => return Ok(false),
    };
    Ok(global_running && db_statuses.values().all(|s| s.is_running()))
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
