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
    Unknown,
}

impl DatabaseRecoveryState {
    fn from_str(state: &str) -> Self {
        match state.trim() {
            "RUNNING" => Self::Running,
            "RECOVERING" => Self::Recovering,
            "UNKNOWN" => Self::Unknown,
            other => panic!("Unexpected recovery state: {other}"),
        }
    }

    fn is_running(self) -> bool {
        matches!(self, Self::Running)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DatabaseRecoveryEvent {
    Success,
    Start,
    Failure,
    Unknown,
}

impl DatabaseRecoveryEvent {
    fn from_str(event: &str) -> Self {
        match event.trim() {
            "SUCCESS" => Self::Success,
            "START" => Self::Start,
            "FAILURE" => Self::Failure,
            "UNKNOWN" => Self::Unknown,
            other => panic!("Unexpected last database event: {other}"),
        }
    }

    fn is_success(self) -> bool {
        matches!(self, Self::Success)
    }
}

#[derive(Debug)]
struct DatabaseRecoveryInfo {
    id: u32,
    name: String,
    last_database_event: DatabaseRecoveryEvent,
    last_global_event: GlobalRecoveryEvent,
    in_global_running: bool,
    in_global_recovering: bool,
    state: DatabaseRecoveryState,
}

#[derive(Deserialize)]
struct DatabaseRecoveryRow {
    #[serde(rename = "database_id")]
    database_id: u32,
    #[serde(rename = "database_name")]
    database_name: String,
    #[serde(rename = "last_database_event")]
    last_database_event: String,
    #[serde(rename = "last_global_event")]
    last_global_event: String,
    #[serde(rename = "recovery_state")]
    recovery_state: String,
    #[serde(rename = "in_global_running")]
    in_global_running: bool,
    #[serde(rename = "in_global_recovering")]
    in_global_recovering: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GlobalRecoveryEvent {
    Running,
    Recovering,
    Unknown,
}

impl GlobalRecoveryEvent {
    fn from_str(event: &str) -> Self {
        match event.trim() {
            "RUNNING" => Self::Running,
            "RECOVERING" => Self::Recovering,
            "UNKNOWN" => Self::Unknown,
            other => panic!("Unexpected global recovery event: {other}"),
        }
    }
}

const ALL_DATABASE_RECOVERY_STATUS_SQL: &str = r#"
SELECT row_to_json(row) AS json_line
FROM rw_catalog.rw_recovery_info AS row;
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
            id: row.database_id,
            name: row.database_name,
            last_database_event: DatabaseRecoveryEvent::from_str(&row.last_database_event),
            last_global_event: GlobalRecoveryEvent::from_str(&row.last_global_event),
            in_global_running: row.in_global_running,
            in_global_recovering: row.in_global_recovering,
            state: DatabaseRecoveryState::from_str(&row.recovery_state),
        });
    }
    Ok(result)
}

fn validate_global_consistency(databases: &[DatabaseRecoveryInfo]) {
    for info in databases {
        let expected = derive_state(
            info.last_database_event,
            info.last_global_event,
            info.in_global_running,
            info.in_global_recovering,
        );
        if info.state != expected {
            panic!(
                "Derived recovery state mismatch for database {}({}): expected {:?}, got {:?}",
                info.name, info.id, expected, info.state
            );
        }

        if info.in_global_running && info.in_global_recovering {
            panic!(
                "Database {}({}) marked as both running and recovering in global event",
                info.name, info.id
            );
        }

        if info.in_global_running && !info.last_database_event.is_success() {
            panic!(
                "Global recovery marks database {}({}) running but latest database event is {:?}",
                info.name, info.id, info.last_database_event
            );
        }

        if info.in_global_recovering && info.last_database_event.is_success() {
            panic!(
                "Global recovery marks database {}({}) recovering but latest database event is SUCCESS",
                info.name, info.id
            );
        }

        if info.last_global_event == GlobalRecoveryEvent::Running
            && !info.in_global_running
            && !info.in_global_recovering
        {
            panic!(
                "Global recovery event indicates RUNNING but database {}({}) absent from both running and recovering ids",
                info.name, info.id
            );
        }
    }
}

fn derive_state(
    database_event: DatabaseRecoveryEvent,
    global_event: GlobalRecoveryEvent,
    in_global_running: bool,
    in_global_recovering: bool,
) -> DatabaseRecoveryState {
    if database_event.is_success() {
        DatabaseRecoveryState::Running
    } else if matches!(global_event, GlobalRecoveryEvent::Running) && in_global_running {
        DatabaseRecoveryState::Running
    } else if matches!(global_event, GlobalRecoveryEvent::Running) && in_global_recovering {
        DatabaseRecoveryState::Recovering
    } else if matches!(database_event, DatabaseRecoveryEvent::Start) {
        DatabaseRecoveryState::Recovering
    } else {
        DatabaseRecoveryState::Unknown
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

    validate_global_consistency(&db_statuses);

    let fully_running = db_statuses.iter().all(|info| info.state.is_running());

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
