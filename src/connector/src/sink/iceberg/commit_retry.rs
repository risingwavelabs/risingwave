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
use iceberg::spec::FormatVersion;
use iceberg::table::Table;
use iceberg::{Catalog, TableIdent};
use risingwave_common::util::retry::exponential_backoff;
use thiserror_ext::AsReport;
use tokio_retry::RetryIf;
use tokio_retry::strategy::jitter;

#[derive(Clone, Debug)]
pub struct CommitRetryLogContext {
    pub iceberg_component: &'static str,
    pub iceberg_operation: &'static str,
    pub table: String,
    pub branch: String,
    pub sink_id: Option<String>,
    pub epoch: Option<u64>,
    pub snapshot_id: Option<i64>,
}

impl CommitRetryLogContext {
    pub fn new(
        iceberg_component: &'static str,
        iceberg_operation: &'static str,
        table: impl Into<String>,
        branch: impl Into<String>,
    ) -> Self {
        Self {
            iceberg_component,
            iceberg_operation,
            table: table.into(),
            branch: branch.into(),
            sink_id: None,
            epoch: None,
            snapshot_id: None,
        }
    }

    pub fn with_sink_id(mut self, sink_id: impl ToString) -> Self {
        self.sink_id = Some(sink_id.to_string());
        self
    }

    pub fn with_epoch(mut self, epoch: u64) -> Self {
        self.epoch = Some(epoch);
        self
    }

    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }
}

/// Distinguishes retriable from non-retriable errors inside [`run_with_retry`].
pub enum CommitError {
    /// `reload_table` failed (table not found, schema mismatch, partition or requested format
    /// evolution). Non-retriable — the call site's invariants no longer hold.
    ReloadTable(anyhow::Error),
    /// `Transaction::commit` (or its `apply`) failed. Retriable — likely a
    /// commit conflict or transient network error.
    Commit(anyhow::Error),
}

/// Reload the iceberg table from the catalog and assert that its current
/// `schema_id` and `default_partition_spec_id` still match the values the
/// caller computed against. Schema, partition, or requested format evolution mid-commit is
/// surfaced as a non-retriable error by the call sites.
pub async fn reload_table(
    catalog: &dyn Catalog,
    table_ident: &TableIdent,
    schema_id: i32,
    partition_spec_id: i32,
    format_version: Option<FormatVersion>,
) -> Result<Table> {
    let table = catalog
        .load_table(table_ident)
        .await
        .map_err(|e| anyhow!(e).context("reload iceberg table"))?;
    validate_table_metadata(&table, schema_id, partition_spec_id, format_version)?;
    Ok(table)
}

fn validate_table_metadata(
    table: &Table,
    schema_id: i32,
    partition_spec_id: i32,
    format_version: Option<FormatVersion>,
) -> Result<()> {
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
    if let Some(format_version) = format_version
        && table.metadata().format_version() != format_version
    {
        bail!(
            "iceberg sink: format evolution not supported; expect format version {}, got {}",
            format_version,
            table.metadata().format_version(),
        );
    }
    Ok(())
}

/// Run a commit-action against the given iceberg table with retry.
/// 1. Calls `reload_table` before each commit attempt to get the latest metadata
/// 2. If `reload_table` fails (table not exists/schema/partition/requested format mismatch), stops retrying immediately
/// 3. If commit fails, retries with backoff up to `retry_num` times.
///
/// Strategy: exponential backoff 10ms→60s with jitter, up to `retry_num` retries.
pub async fn run_with_retry<F, Fut, Out>(
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema_id: i32,
    partition_spec_id: i32,
    format_version: Option<FormatVersion>,
    retry_num: usize,
    log_context: CommitRetryLogContext,
    commit_action: F,
) -> Result<Out>
where
    F: Fn(Table) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Out, CommitError>> + Send,
{
    run_with_retry_from_table(
        None,
        catalog,
        table_ident,
        schema_id,
        partition_spec_id,
        format_version,
        retry_num,
        log_context,
        commit_action,
    )
    .await
}

/// Same as [`run_with_retry`], but when `initial_table` is set the first attempt runs against it
/// (after the same metadata validation) instead of reloading it from the catalog. Use this when
/// the caller loaded the table right before the commit: a stale table only costs a commit
/// conflict, and every retry reloads. `initial_table` must be the table named by `table_ident`.
pub async fn run_with_retry_from_table<F, Fut, Out>(
    initial_table: Option<Table>,
    catalog: Arc<dyn Catalog>,
    table_ident: TableIdent,
    schema_id: i32,
    partition_spec_id: i32,
    format_version: Option<FormatVersion>,
    retry_num: usize,
    log_context: CommitRetryLogContext,
    commit_action: F,
) -> Result<Out>
where
    F: Fn(Table) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Out, CommitError>> + Send,
{
    if let Some(table) = &initial_table
        && table.identifier() != &table_ident
    {
        bail!(
            "iceberg commit: initial table {} does not match commit target {}",
            table.identifier(),
            table_ident
        );
    }
    let retry_strategy =
        exponential_backoff(Duration::from_millis(10), 10, Duration::from_secs(60))
            .map(jitter)
            .take(retry_num);
    let mut initial_table = initial_table;

    RetryIf::spawn(
        retry_strategy,
        || {
            let catalog = catalog.clone();
            let table_ident = table_ident.clone();
            let commit_action = &commit_action;
            let initial_table = initial_table.take();
            async move {
                let table = match initial_table {
                    Some(table) => {
                        validate_table_metadata(
                            &table,
                            schema_id,
                            partition_spec_id,
                            format_version,
                        )
                        .map_err(CommitError::ReloadTable)?;
                        table
                    }
                    None => reload_table(
                        catalog.as_ref(),
                        &table_ident,
                        schema_id,
                        partition_spec_id,
                        format_version,
                    )
                    .await
                    .map_err(CommitError::ReloadTable)?,
                };
                commit_action(table).await
            }
        },
        |err: &CommitError| match err {
            CommitError::Commit(e) => {
                tracing::warn!(
                    iceberg_component = log_context.iceberg_component,
                    iceberg_operation = log_context.iceberg_operation,
                    sink_id = log_context.sink_id.as_deref().unwrap_or("unknown"),
                    epoch = ?log_context.epoch,
                    snapshot_id = ?log_context.snapshot_id,
                    table = %log_context.table,
                    branch = %log_context.branch,
                    schema_id,
                    partition_spec_id,
                    error = %e.as_report(),
                    "iceberg_commit_retryable_error",
                );
                true
            }
            CommitError::ReloadTable(e) => {
                tracing::error!(
                    iceberg_component = log_context.iceberg_component,
                    iceberg_operation = log_context.iceberg_operation,
                    sink_id = log_context.sink_id.as_deref().unwrap_or("unknown"),
                    epoch = ?log_context.epoch,
                    snapshot_id = ?log_context.snapshot_id,
                    table = %log_context.table,
                    branch = %log_context.branch,
                    schema_id,
                    partition_spec_id,
                    error = %e.as_report(),
                    "iceberg_commit_reload_table_non_retryable_error",
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

#[cfg(all(test, not(madsim)))]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use iceberg::io::FileIO;
    use iceberg::spec::{
        NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };
    use iceberg::{Namespace, NamespaceIdent, Runtime, TableCommit, TableCreation};

    use super::*;

    fn test_table() -> Table {
        let schema = Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .unwrap();
        let metadata = TableMetadataBuilder::new(
            schema,
            UnboundPartitionSpec::builder().build(),
            SortOrder::unsorted_order(),
            "memory://commit-retry".to_owned(),
            FormatVersion::V2,
            HashMap::new(),
        )
        .unwrap()
        .build()
        .unwrap()
        .metadata;
        Table::builder()
            .identifier(TableIdent::new(
                NamespaceIdent::new("db".to_owned()),
                "table".to_owned(),
            ))
            .file_io(FileIO::new_with_fs())
            .runtime(Runtime::try_current().unwrap())
            .metadata(metadata)
            .build()
            .unwrap()
    }

    #[derive(Debug)]
    struct CountingCatalog {
        table: Table,
        loads: AtomicUsize,
    }

    #[async_trait]
    impl Catalog for CountingCatalog {
        async fn load_table(&self, _table: &TableIdent) -> iceberg::Result<Table> {
            self.loads.fetch_add(1, Ordering::SeqCst);
            Ok(self.table.clone())
        }

        async fn list_namespaces(
            &self,
            _parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            unreachable!()
        }

        async fn create_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<Namespace> {
            unreachable!()
        }

        async fn get_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
            unreachable!()
        }

        async fn namespace_exists(&self, _namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            unreachable!()
        }

        async fn update_namespace(
            &self,
            _namespace: &NamespaceIdent,
            _properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            unreachable!()
        }

        async fn drop_namespace(&self, _namespace: &NamespaceIdent) -> iceberg::Result<()> {
            unreachable!()
        }

        async fn list_tables(
            &self,
            _namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            unreachable!()
        }

        async fn create_table(
            &self,
            _namespace: &NamespaceIdent,
            _creation: TableCreation,
        ) -> iceberg::Result<Table> {
            unreachable!()
        }

        async fn drop_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            unreachable!()
        }

        async fn purge_table(&self, _table: &TableIdent) -> iceberg::Result<()> {
            unreachable!()
        }

        async fn table_exists(&self, _table: &TableIdent) -> iceberg::Result<bool> {
            unreachable!()
        }

        async fn rename_table(&self, _src: &TableIdent, _dest: &TableIdent) -> iceberg::Result<()> {
            unreachable!()
        }

        async fn register_table(
            &self,
            _table: &TableIdent,
            _metadata_location: String,
        ) -> iceberg::Result<Table> {
            unreachable!()
        }

        async fn update_table(&self, _commit: TableCommit) -> iceberg::Result<Table> {
            unreachable!()
        }
    }

    fn log_context() -> CommitRetryLogContext {
        CommitRetryLogContext::new("test", "commit", "db.table", "main")
    }

    #[tokio::test]
    async fn first_attempt_uses_initial_table_and_retries_reload() {
        let table = test_table();
        let table_ident = table.identifier().clone();
        let catalog = Arc::new(CountingCatalog {
            table: table.clone(),
            loads: AtomicUsize::new(0),
        });
        let attempts = AtomicUsize::new(0);

        run_with_retry_from_table(
            Some(table),
            catalog.clone(),
            table_ident,
            0,
            0,
            Some(FormatVersion::V2),
            3,
            log_context(),
            |_table| {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        Err(CommitError::Commit(anyhow!("injected conflict")))
                    } else {
                        Ok(())
                    }
                }
            },
        )
        .await
        .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            catalog.loads.load(Ordering::SeqCst),
            1,
            "only the retry reloads the table"
        );
    }

    #[tokio::test]
    async fn initial_table_is_validated_without_retry() {
        let table = test_table();
        let table_ident = table.identifier().clone();
        let catalog = Arc::new(CountingCatalog {
            table: table.clone(),
            loads: AtomicUsize::new(0),
        });

        let error = run_with_retry_from_table(
            Some(table),
            catalog.clone(),
            table_ident,
            7,
            0,
            None,
            3,
            log_context(),
            |_table| async { Ok::<_, CommitError>(()) },
        )
        .await
        .expect_err("schema mismatch must fail");

        assert!(error.to_string().contains("schema evolution not supported"));
        assert_eq!(catalog.loads.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn initial_table_must_match_commit_target() {
        let table = test_table();
        let catalog = Arc::new(CountingCatalog {
            table: table.clone(),
            loads: AtomicUsize::new(0),
        });
        let attempts = AtomicUsize::new(0);

        let error = run_with_retry_from_table(
            Some(table),
            catalog.clone(),
            TableIdent::new(NamespaceIdent::new("db".to_owned()), "other".to_owned()),
            0,
            0,
            None,
            3,
            log_context(),
            |_table| {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Ok::<_, CommitError>(()) }
            },
        )
        .await
        .expect_err("a table with another identifier must be rejected");

        assert!(error.to_string().contains("does not match commit target"));
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
        assert_eq!(catalog.loads.load(Ordering::SeqCst), 0);
    }
}
