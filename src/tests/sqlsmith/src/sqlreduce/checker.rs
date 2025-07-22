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

use std::collections::HashSet;

use risingwave_sqlparser::ast::Statement;
use tokio_postgres::Client;

/// Checker evaluates whether a transformed SQL still preserves the original failure.
pub struct Checker<'a> {
    pub client: &'a Client,
    pub setup_stmts: Vec<Statement>,
    pub objects_to_cleanup: HashSet<String>,
}

impl<'a> Checker<'a> {
    pub fn new(
        client: &'a Client,
        setup_stmts: Vec<Statement>,
        objects_to_cleanup: HashSet<String>,
    ) -> Self {
        Self {
            client,
            setup_stmts,
            objects_to_cleanup,
        }
    }

    /// Determines if the transformation preserved the original failure behavior.
    pub async fn is_failure_preserved(&self, old: &str, new: &str) -> bool {
        // Clean environment and replay setup before testing original query
        self.reset_environment().await;
        self.replay_setup().await;
        let old_result = run_query(self.client, old).await;

        // Clean environment and replay setup before testing transformed query
        self.reset_environment().await;
        self.replay_setup().await;
        let new_result = run_query(self.client, new).await;

        tracing::info!("old_result: {:?}", old_result);
        tracing::info!("new_result: {:?}", new_result);

        // Check if both status and error message are the same
        old_result == new_result
    }

    /// Drops all known objects that may interfere with testing.
    async fn reset_environment(&self) {
        for name in &self.objects_to_cleanup {
            let drop_mv = format!("DROP MATERIALIZED VIEW IF EXISTS {} CASCADE", name);
            let drop_view = format!("DROP VIEW IF EXISTS {} CASCADE", name);
            let drop_table = format!("DROP TABLE IF EXISTS {} CASCADE", name);

            let _ = self.client.simple_query(&drop_mv).await;
            let _ = self.client.simple_query(&drop_view).await;
            let _ = self.client.simple_query(&drop_table).await;
        }
    }

    /// Replays the setup statements (DDL, inserts, etc.)
    async fn replay_setup(&self) {
        for stmt in &self.setup_stmts {
            let _ = self.client.simple_query(&stmt.to_string()).await;
        }
    }
}

/// Executes a single SQL query string and returns (`is_ok`, `error_message_if_any`)
pub async fn run_query(client: &Client, query: &str) -> (bool, String) {
    match client.simple_query(query).await {
        Ok(_) => (true, String::new()),
        Err(e) => (false, e.to_string()),
    }
}
