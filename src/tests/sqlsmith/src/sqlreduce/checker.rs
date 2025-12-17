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

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use risingwave_sqlparser::ast::Statement;
use tokio_postgres::{Client, NoTls};

/// Information about a detected panic in component logs.
#[derive(Debug, Clone)]
struct PanicInfo {
    component: String,
    #[allow(dead_code)]
    message: String,
}

/// Checker evaluates whether a transformed SQL still preserves the original failure.
pub struct Checker {
    pub client: Client,
    pub setup_stmts: Vec<Statement>,
    restore_cmd: String,
}

impl Checker {
    pub fn new(client: Client, setup_stmts: Vec<Statement>, restore_cmd: String) -> Self {
        Self {
            client,
            setup_stmts,
            restore_cmd,
        }
    }

    /// Prepares the schema namespace for testing.
    ///
    /// This creates the `sqlsmith_reducer` schema and sets the search path.
    /// Should be called once before any reduction begins.
    pub async fn prepare_schema(&self) {
        let _ = self
            .client
            .simple_query("CREATE SCHEMA IF NOT EXISTS sqlsmith_reducer;")
            .await;
        let _ = self
            .client
            .simple_query("SET search_path TO sqlsmith_reducer;")
            .await;
    }

    /// Drop the schema.
    ///
    /// Should be called after the reduction is complete.
    pub async fn drop_schema(&self) {
        let _ = self
            .client
            .simple_query("DROP SCHEMA IF EXISTS sqlsmith_reducer CASCADE;")
            .await;
    }

    /// Determines if the transformation preserved the original failure behavior.
    ///
    /// Each test run resets the schema, replays setup, and runs the query.
    pub async fn is_failure_preserved(&mut self, old: &str, new: &str) -> bool {
        self.reset_schema().await;
        self.replay_setup().await;
        let old_result = run_query(&mut self.client, old, &self.restore_cmd).await;

        self.reset_schema().await;
        self.replay_setup().await;
        let new_result = run_query(&mut self.client, new, &self.restore_cmd).await;

        tracing::debug!("old_result: {:?}", old_result);
        tracing::debug!("new_result: {:?}", new_result);

        old_result == new_result
    }

    /// Drops the entire schema to reset database state.
    async fn reset_schema(&self) {
        let _ = self
            .client
            .simple_query("DROP SCHEMA IF EXISTS sqlsmith_reducer CASCADE;")
            .await;
        let _ = self
            .client
            .simple_query("CREATE SCHEMA sqlsmith_reducer;")
            .await;
        let _ = self
            .client
            .simple_query("SET search_path TO sqlsmith_reducer;")
            .await;
    }

    /// Replays the setup statements (DDL, inserts, etc.)
    async fn replay_setup(&self) {
        for stmt in &self.setup_stmts {
            let _ = self.client.simple_query(&stmt.to_string()).await;
        }
    }
}

/// Executes a single SQL query string and returns (`is_ok`, `error_message_if_any`)
pub async fn run_query(client: &mut Client, query: &str, restore_cmd: &str) -> (bool, String) {
    let query_start_time = SystemTime::now();

    match client.simple_query(query).await {
        Ok(_) => (true, String::new()),
        Err(e) => {
            let error_msg = e.to_string();
            tracing::debug!("Query failed with error: {}", error_msg);

            // Step 1: Check if this is a frontend-only error (SQL syntax, type errors, etc.)
            // These are safe and can be handled by auto-recovery
            if is_frontend_error(&error_msg) {
                tracing::debug!("Detected frontend error, attempting auto-recovery");

                // Try reconnecting to frontend
                if let Ok((new_client, connection)) = reconnect_frontend().await {
                    tokio::spawn(async move {
                        if let Err(e) = connection.await {
                            tracing::error!("connection error: {}", e);
                        }
                    });
                    *client = new_client;
                    tracing::info!("Reconnected to frontend after error");

                    // Wait for auto-recovery with 30s timeout
                    match wait_for_auto_recovery(client, 30).await {
                        Ok(_) => {
                            tracing::info!("Frontend auto-recovery successful");
                        }
                        Err(_) => {
                            // Auto-recovery stuck, fallback to bootstrap
                            tracing::warn!(
                                "Frontend auto-recovery timeout, falling back to bootstrap"
                            );
                            if let Err(err) = bootstrap_recovery(restore_cmd) {
                                tracing::error!("Bootstrap recovery failed: {}", err);
                            }
                        }
                    }
                } else {
                    tracing::error!("Failed to reconnect to frontend");
                }

                return (false, error_msg);
            }

            // Step 2: Check if connection is lost (possible panic)
            if client.is_closed() {
                tracing::warn!("Connection lost, checking for component panics");

                // Step 3: Check logs for panic in compute/meta/compactor
                // Look for panics that occurred around the query time (within 10s window)
                let time_window = query_start_time
                    .checked_sub(Duration::from_secs(10))
                    .unwrap_or(query_start_time);

                if let Some(panic_info) = check_component_panic(time_window) {
                    // Critical component panic detected - need bootstrap recovery
                    tracing::error!(
                        "Panic detected in {} component, executing bootstrap recovery",
                        panic_info.component
                    );

                    if let Err(err) = bootstrap_recovery(restore_cmd) {
                        tracing::error!("Bootstrap recovery failed: {}", err);
                    }

                    // Reconnect after bootstrap recovery
                    if let Ok((new_client, connection)) = reconnect_frontend().await {
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                tracing::error!("connection error: {}", e);
                            }
                        });
                        *client = new_client;
                        tracing::info!("Reconnected after bootstrap recovery");

                        // Wait for recovery to complete
                        if let Err(err) = wait_for_recovery(client).await {
                            tracing::error!("Failed to complete recovery: {}", err);
                        }
                    }
                } else {
                    // No panic found in logs, but connection is lost - this is uncertain
                    // Use bootstrap recovery to be safe (avoid getting stuck in auto-recovery)
                    tracing::warn!(
                        "No component panic detected, but connection lost - using bootstrap recovery"
                    );

                    if let Err(err) = bootstrap_recovery(restore_cmd) {
                        tracing::error!("Bootstrap recovery failed: {}", err);
                    }

                    if let Ok((new_client, connection)) = reconnect_frontend().await {
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                tracing::error!("connection error: {}", e);
                            }
                        });
                        *client = new_client;
                        tracing::info!("Reconnected after bootstrap recovery");

                        // Wait for recovery to complete
                        if let Err(err) = wait_for_recovery(client).await {
                            tracing::error!("Failed to complete recovery: {}", err);
                        }
                    }
                }
            } else {
                // Connection still alive but query failed - likely a normal query error
                tracing::debug!("Connection alive, treating as normal query error");
            }

            (false, error_msg)
        }
    }
}

/// Reconnect to the frontend `PostgreSQL` interface.
async fn reconnect_frontend() -> Result<
    (
        Client,
        tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>,
    ),
    tokio_postgres::Error,
> {
    tokio_postgres::Config::new()
        .host("localhost")
        .port(4566)
        .dbname("dev")
        .user("root")
        .password("")
        .connect_timeout(Duration::from_secs(60))
        .connect(NoTls)
        .await
}

/// Wait until RW recovery finishes (`rw_recovery_status() = 'RUNNING'`)
pub async fn wait_for_recovery(client: &Client) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(300);
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    let res: Result<(), anyhow::Error> = tokio::time::timeout(timeout, async {
        loop {
            let query_res = client.simple_query("select rw_recovery_status();").await;
            if let Ok(messages) = query_res {
                for msg in messages {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg
                        && let Some(status) = row.get(0)
                        && status == "RUNNING"
                    {
                        return Ok(());
                    }
                }
            }
            interval.tick().await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for recovery"))?;

    res
}

/// Check if the error message indicates a frontend-only error (SQL syntax, type mismatch, etc.)
/// These errors are typically safe and can be handled by auto-recovery.
fn is_frontend_error(error_msg: &str) -> bool {
    error_msg.contains("syntax error")
        || error_msg.contains("parse error")
        || error_msg.contains("Bind error")
        || error_msg.contains("Planner error")
        || error_msg.contains("Catalog error")
        || error_msg.contains("type mismatch")
        || error_msg.contains("not found")
        || error_msg.contains("already exists")
        || error_msg.contains("permission denied")
}

/// Get the default `RisingWave` log directory path.
fn get_log_dir() -> PathBuf {
    PathBuf::from(".risingwave/log")
}

/// Check if any critical component (compute/meta/compactor) has panicked recently.
/// Only checks logs modified within the given time window to avoid false positives.
///
/// Returns Some(PanicInfo) if a panic is detected, None otherwise.
fn check_component_panic(since: SystemTime) -> Option<PanicInfo> {
    let log_dir = get_log_dir();
    if !log_dir.exists() {
        tracing::warn!("Log directory not found: {}", log_dir.display());
        return None;
    }

    // Read all log files in the directory
    let entries = match fs::read_dir(&log_dir) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!("Failed to read log directory: {}", e);
            return None;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        let filename = match path.file_name().and_then(|n| n.to_str()) {
            Some(name) => name,
            None => continue,
        };

        // Identify critical components (skip frontend logs)
        let component = if filename.starts_with("compute-node-") {
            "compute"
        } else if filename.starts_with("meta-node-") {
            "meta"
        } else if filename.starts_with("compactor-") {
            "compactor"
        } else {
            continue; // Skip frontend and other logs
        };

        // Check if the log file was modified recently (within time window)
        if let Ok(metadata) = fs::metadata(&path)
            && let Ok(modified) = metadata.modified()
            && modified < since
        {
            continue; // Skip old log files
        }

        // Search for panic patterns in the log file
        if let Some(panic_msg) = search_panic_in_log(&path) {
            tracing::error!("Detected panic in {} component: {}", component, panic_msg);
            return Some(PanicInfo {
                component: component.to_owned(),
                message: panic_msg,
            });
        }
    }

    None
}

/// Search for panic patterns in a log file.
/// Uses tail to read only the last N lines for efficiency.
///
/// Panic patterns include:
/// - "thread .* panicked"
/// - Lines containing "PANIC" at ERROR/FATAL level
fn search_panic_in_log(log_path: &Path) -> Option<String> {
    // Use tail to read last 500 lines for efficiency
    let output = Command::new("tail")
        .arg("-n")
        .arg("500")
        .arg(log_path)
        .output()
        .ok()?;

    let content = String::from_utf8_lossy(&output.stdout);

    // Search for panic patterns
    for line in content.lines() {
        let line_lower = line.to_lowercase();
        if line_lower.contains("thread") && line_lower.contains("panicked") {
            return Some(line.to_owned());
        }
        if line_lower.contains("panic")
            && (line_lower.contains("error") || line_lower.contains("fatal"))
        {
            return Some(line.to_owned());
        }
    }

    None
}

/// Execute bootstrap recovery command to fully restart `RisingWave`.
/// This is needed when critical components (compute/meta/compactor) panic.
fn bootstrap_recovery(restore_cmd: &str) -> anyhow::Result<()> {
    tracing::warn!("Executing bootstrap recovery due to component panic");

    let status = Command::new("sh").arg("-c").arg(restore_cmd).status()?;

    if status.success() {
        tracing::info!("Bootstrap recovery completed successfully");
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Bootstrap recovery failed with status: {}",
            status
        ))
    }
}

/// Wait for frontend to auto-recover with a timeout.
/// Returns Ok if recovery succeeds, Err if timeout expires.
async fn wait_for_auto_recovery(client: &Client, timeout_secs: u64) -> anyhow::Result<()> {
    let timeout = Duration::from_secs(timeout_secs);
    let mut interval = tokio::time::interval(Duration::from_millis(100));

    tokio::time::timeout(timeout, async {
        loop {
            let query_res = client.simple_query("select rw_recovery_status();").await;
            if let Ok(messages) = query_res {
                for msg in messages {
                    if let tokio_postgres::SimpleQueryMessage::Row(row) = msg
                        && let Some(status) = row.get(0)
                        && status == "RUNNING"
                    {
                        return Ok(());
                    }
                }
            }
            interval.tick().await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("Frontend auto-recovery timeout after {}s", timeout_secs))?
}
