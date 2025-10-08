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

use std::process::Command;
use std::time::Duration;

use risingwave_sqlparser::ast::Statement;
use tokio_postgres::{Client, NoTls};

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
    match client.simple_query(query).await {
        Ok(_) => (true, String::new()),
        Err(e) => {
            if e.is_closed() {
                tracing::error!("Frontend panic detected, restoring with `{restore_cmd}`");

                let status = Command::new("sh").arg("-c").arg(restore_cmd).status();
                match status {
                    Ok(s) if s.success() => tracing::info!("restore cmd executed successfully"),
                    Ok(s) => tracing::error!("restore cmd failed with status: {s}"),
                    Err(err) => tracing::error!("failed to execute restore cmd: {err}"),
                }

                // The connection to the frontend is lost when the frontend process panics,
                // so the old `Client` instance becomes unusable (is_closed() = true).
                // We must rebuild a brand new client connection here, otherwise all queries
                // will keep failing. After reconnection, we still need to wait until RW
                // finishes recovery before continuing.
                match tokio_postgres::Config::new()
                    .host("localhost")
                    .port(4566)
                    .dbname("dev")
                    .user("root")
                    .password("")
                    .connect_timeout(Duration::from_secs(5))
                    .connect(NoTls)
                    .await
                {
                    Ok((new_client, connection)) => {
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                tracing::error!("connection error: {}", e);
                            }
                        });
                        *client = new_client;
                        tracing::info!("Reconnected to Frontend after panic");

                        if let Err(err) = wait_for_recovery(client).await {
                            tracing::error!("RW failed to recover after frontend panic: {:?}", err);
                        } else {
                            tracing::info!("RW recovery complete (frontend case)");
                        }
                    }
                    Err(err) => {
                        tracing::error!("Failed to reconnect frontend: {}", err);
                    }
                }
            } else if e.as_db_error().is_some() {
                tracing::error!("Compute panic detected, waiting for recovery");
                if let Err(err) = wait_for_recovery(client).await {
                    tracing::error!("RW failed to recover after compute panic: {:?}", err);
                } else {
                    tracing::info!("RW recovery complete (compute case)");
                }
            } else {
                tracing::error!("Other panics detected");
            }

            (false, e.to_string())
        }
    }
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
