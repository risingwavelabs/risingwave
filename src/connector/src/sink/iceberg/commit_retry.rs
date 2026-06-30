// Copyright 2026 RisingWave Labs
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

//! Shared iceberg commit-with-retry primitive used by both the V1/V2 sink
//! committer and the V3 sink coordinator worker. Wraps the standard pattern
//! of "reload the table, build an action against the freshly-loaded snapshot,
//! commit, retry on transient errors only".

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use thiserror_ext::AsReport;
use tokio_retry::RetryIf;
use tokio_retry::strategy::{ExponentialBackoff, jitter};

/// Distinguishes retriable from non-retriable errors inside [`run_with_retry`].
pub enum CommitError {
    /// `reload_table` failed (table not found, schema mismatch, partition
    /// evolution). Non-retriable — the call site's invariants no longer hold.
    ReloadTable(anyhow::Error),
    /// `Transaction::commit` (or its `apply`) failed. Retriable — likely a
    /// commit conflict or transient network error.
    Commit(anyhow::Error),
}

/// Reload the iceberg table from the catalog and assert that its current
/// `schema_id` and `default_partition_spec_id` still match the values the
/// caller computed against. Schema or partition evolution mid-commit is
/// surfaced as a non-retriable error by the call sites.
pub async fn reload_table(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    schema_id: i32,
    partition_spec_id: i32,
) -> Result<Table> {
    let table = catalog
        .load_table(table_ident)
        .await
        .map_err(|e| anyhow!(e).context("reload iceberg table"))?;
    if table.metadata().current_schema_id() != schema_id {
        bail!(
            "iceberg sink: schema evolution not supported; expect schema id {}, got {}",
            schema_id,
            table.metadata().current_schema_id(),
        );
    }
    if table.metadata().default_partition_spec_id() != partition_spec_id {
        bail!(
            "iceberg sink: partition evolution not supported; expect partition spec id {}, got {}",
            partition_spec_id,
            table.metadata().default_partition_spec_id(),
        );
    }
    Ok(table)
}

/// Run a commit-action against the given iceberg table with retry.
/// 1. Calls `reload_table` before each commit attempt to get the latest metadata
/// 2. If `reload_table` fails (table not exists/schema/partition mismatch), stops retrying immediately
/// 3. If commit fails, retries with backoff up to `retry_num` times.
///
/// Strategy: exponential backoff 10ms→60s with jitter, up to `retry_num` retries.
pub async fn run_with_retry<F, Fut, Out>(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema_id: i32,
    partition_spec_id: i32,
    retry_num: usize,
    commit_action: F,
) -> Result<Out>
where
    F: Fn(Table) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Out, CommitError>> + Send,
{
    let retry_strategy = ExponentialBackoff::from_millis(10)
        .max_delay(Duration::from_secs(60))
        .map(jitter)
        .take(retry_num);

    RetryIf::spawn(
        retry_strategy,
        || {
            let catalog = catalog.clone();
            let table_ident = table_ident.clone();
            let commit_action = &commit_action;
            async move {
                let table =
                    reload_table(catalog.as_ref(), &table_ident, schema_id, partition_spec_id)
                        .await
                        .map_err(CommitError::ReloadTable)?;
                commit_action(table).await
            }
        },
        |err: &CommitError| match err {
            CommitError::Commit(e) => {
                tracing::warn!(
                    error = %e.as_report(),
                    "iceberg commit failed; will retry",
                );
                true
            }
            CommitError::ReloadTable(e) => {
                tracing::error!(
                    error = %e.as_report(),
                    "iceberg reload_table failed; will not retry",
                );
                false
            }
        },
    )
    .await
    .map_err(|e| match e {
        CommitError::ReloadTable(e) | CommitError::Commit(e) => e,
    })
}
