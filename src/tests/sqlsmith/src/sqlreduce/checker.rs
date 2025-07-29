use risingwave_sqlparser::ast::Statement;
use tokio_postgres::Client;

/// Checker evaluates whether a transformed SQL still preserves the original failure.
pub struct Checker<'a> {
    pub client: &'a Client,
    pub setup_stmts: Vec<Statement>,
}

impl<'a> Checker<'a> {
    pub fn new(client: &'a Client, setup_stmts: Vec<Statement>) -> Self {
        Self {
            client,
            setup_stmts,
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
    pub async fn is_failure_preserved(&self, old: &str, new: &str) -> bool {
        self.reset_schema().await;
        self.replay_setup().await;
        let old_result = run_query(self.client, old).await;

        self.reset_schema().await;
        self.replay_setup().await;
        let new_result = run_query(self.client, new).await;

        tracing::info!("old_result: {:?}", old_result);
        tracing::info!("new_result: {:?}", new_result);

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
pub async fn run_query(client: &Client, query: &str) -> (bool, String) {
    match client.simple_query(query).await {
        Ok(_) => (true, String::new()),
        Err(e) => (false, e.to_string()),
    }
}
