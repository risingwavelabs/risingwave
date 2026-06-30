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

//! Cluster setup and V3 sink registration helpers for V3 2PC fault injection tests.
//!
//! The test plumbing mirrors the production V3 sink topology in
//! `e2e_test/iceberg/test_case/pure_slt/iceberg_sink_no_partition_v3_with_pk_index.slt`:
//!
//!   TABLE  --->  MATERIALIZED VIEW  --->  V3 ICEBERG SINK
//!     ^                                          |
//!     | INSERT INTO from background task         v
//!  (driver)                          MockIcebergV3Catalog
//!
//! The TABLE serves as the upstream of the streaming graph; the MV materialises
//! with the primary key as the stream key (so it matches the V3 sink's
//! `downstream_pk`), satisfying the V3 writer's current assumption that
//! upstream stream key == downstream PK.

use std::sync::Arc;

use anyhow::{Context, Result};
use iceberg::spec::{NestedField, PrimitiveType, Schema, TableMetadataBuilder, Type};
use iceberg::{TableCreation, TableIdent};
use risingwave_common::id::SinkId;
use risingwave_connector::sink::iceberg::mock_v3_catalog_registry::{MockCatalogGuard, register};
use risingwave_meta_model::pending_sink_state;
use risingwave_simulation::cluster::Cluster;
use sea_orm::{ColumnTrait, Database, EntityTrait, QueryFilter, QueryOrder};

use super::mock_catalog::MockIcebergV3Catalog;

/// All state needed by a V3 fault-injection test.
pub struct V3TestHandle {
    pub cluster: Cluster,
    pub mock: MockIcebergV3Catalog,
    /// Keeps the mock catalog registered for the duration of the test.
    pub _mock_guard: MockCatalogGuard,
}

/// Start a V3 sink test cluster with a wired-up mock catalog plus an
/// upstream `TABLE -> MV -> SINK` chain. Once this returns, the streaming
/// graph is live and ready to accept `INSERT INTO test_v3_table` writes.
pub async fn start_v3_test_cluster_with_sink(parallelism: usize) -> Result<V3TestHandle> {
    // 1. Start the simulation cluster.
    let mut cluster = crate::sink::utils::start_sink_test_cluster().await?;

    // 2. Build a minimal iceberg TableMetadata for schema (id int, name varchar).
    let schema = Schema::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "name", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    let creation = TableCreation::builder()
        .name("t".to_string())
        .schema(schema)
        .location("memory://test".to_string())
        .properties(std::collections::HashMap::<String, String>::new())
        .build();

    let metadata = TableMetadataBuilder::from_table_creation(creation)?
        .build()?
        .metadata;

    // 3. Construct the mock catalog and register it globally.
    let table_ident = TableIdent::from_strs(["ns", "t"])?;
    let mock = MockIcebergV3Catalog::new(table_ident, metadata);
    let _mock_guard = register(Arc::new(mock.clone()));

    // 4. Create the TABLE -> MV -> V3 SINK streaming graph.
    let mut session = cluster.start_session();

    session
        .run(&format!("SET streaming_parallelism = {}", parallelism))
        .await?;

    session
        .run("CREATE TABLE test_v3_table (id int PRIMARY KEY, name varchar)")
        .await?;

    // MV in the middle matches the production V3 topology and gives a
    // streaming layer between the TABLE and the SINK.
    session
        .run(
            "CREATE MATERIALIZED VIEW test_v3_mv AS \
             SELECT id, name FROM test_v3_table",
        )
        .await?;

    session
        .run(
            "CREATE SINK test_v3_sink FROM test_v3_mv \
             WITH ( \
               connector = 'iceberg', \
               catalog.type = 'mock_v3', \
               warehouse.path = 'memory://test', \
               database.name = 'ns', \
               table.name = 't', \
               is_exactly_once = 'true', \
               enable_pk_index = 'true', \
               format_version = '3', \
               type = 'upsert' \
             )",
        )
        .await?;

    Ok(V3TestHandle {
        cluster,
        mock,
        _mock_guard,
    })
}

/// Spawn a background task that streams a continuous workload into the V3
/// test TABLE.
///
/// Workload pattern (deterministic given `seed`):
///   - Every batch issues an `INSERT INTO test_v3_table VALUES (...)` with
///     `batch_size` rows whose ids cycle through `0..pk_count`. Subsequent
///     batches naturally produce upserts by re-inserting the same PKs with
///     new values (which V3 sink converts to delete + insert on the PK).
///   - With probability `delete_ratio` per batch, an additional
///     `DELETE FROM test_v3_table WHERE id IN (...)` removes a small slice of
///     PKs. This exercises the V3 writer's delete-vector merge path beyond
///     what upsert-via-reinsert already covers.
///   - Optional `tokio::time::sleep(batch_interval)` between batches.
///
/// The task is `abort`-cancelled when [`WorkloadDriver::shutdown`] is called,
/// so callers can keep it alive for the duration of a fault injection test
/// and stop it at teardown.
pub struct WorkloadConfig {
    pub pk_count: i32,
    pub batch_size: usize,
    pub batch_interval: std::time::Duration,
    /// Probability per batch of running a DELETE alongside the INSERT. Set to
    /// 0.0 to disable deletes (insert/upsert only).
    pub delete_ratio: f64,
    /// Approximate number of PKs to delete per delete batch.
    pub delete_chunk: usize,
    /// RNG seed for deterministic delete-PK choices.
    pub seed: u64,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        Self {
            pk_count: 50,
            batch_size: 5,
            batch_interval: std::time::Duration::from_millis(200),
            delete_ratio: 0.2,
            delete_chunk: 2,
            seed: 42,
        }
    }
}

pub struct WorkloadDriver {
    task: tokio::task::JoinHandle<()>,
}

impl WorkloadDriver {
    pub async fn shutdown(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

pub fn spawn_continuous_workload(cluster: &mut Cluster, cfg: WorkloadConfig) -> WorkloadDriver {
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    let mut session = cluster.start_session();
    let task = tokio::spawn(async move {
        let mut rng = StdRng::seed_from_u64(cfg.seed);
        let mut batch_idx: u64 = 0;
        // Tracks consecutive SQL failures; we only bail out if the cluster
        // has clearly gone down (many failures in a row). Transient errors
        // during fault injection are expected and should not stop the loop.
        let mut consecutive_failures: u32 = 0;
        const MAX_CONSECUTIVE_FAILURES: u32 = 100;
        loop {
            // INSERT batch.
            let mut values = String::new();
            for i in 0..cfg.batch_size {
                if i > 0 {
                    values.push_str(", ");
                }
                // Use i64 arithmetic and rem_euclid to avoid overflow on long-running
                // tests where batch_idx * batch_size can exceed i32::MAX.
                let pk = (((batch_idx as i64) * (cfg.batch_size as i64) + (i as i64))
                    .rem_euclid(cfg.pk_count as i64)) as i32;
                values.push_str(&format!("({}, 'v{}-{}')", pk, batch_idx, pk));
            }
            let insert_sql = format!("INSERT INTO test_v3_table VALUES {}", values);
            let insert_ok = session.run(&insert_sql).await.is_ok();

            // Maybe DELETE.
            let delete_ok = if cfg.delete_ratio > 0.0
                && cfg.delete_chunk > 0
                && rng.random_bool(cfg.delete_ratio.clamp(0.0, 1.0))
            {
                let mut ids: Vec<i32> = Vec::with_capacity(cfg.delete_chunk);
                for _ in 0..cfg.delete_chunk {
                    ids.push(rng.random_range(0..cfg.pk_count));
                }
                let id_list = ids
                    .iter()
                    .map(|id| id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                let delete_sql = format!("DELETE FROM test_v3_table WHERE id IN ({})", id_list);
                session.run(&delete_sql).await.is_ok()
            } else {
                true
            };

            if insert_ok && delete_ok {
                consecutive_failures = 0;
            } else {
                consecutive_failures += 1;
                if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                    // Cluster appears down; stop driving so the test can
                    // observe and assert instead of looping forever.
                    break;
                }
            }

            batch_idx = batch_idx.wrapping_add(1);
            if !cfg.batch_interval.is_zero() {
                tokio::time::sleep(cfg.batch_interval).await;
            }
        }
    });
    WorkloadDriver { task }
}

/// Assert basic invariants after the sink has drained.
///
/// - Waits for effective file count to stabilise (no-failure path: any count),
///   with a 30s timeout so hangs surface diagnostically.
/// - Verifies no duplicate file_path appears across snapshots.
/// - Requires at least one committed snapshot so a silently-idle sink never
///   passes (`wait_for_data_files_stable(0, ..)` would otherwise accept 0).
/// - Bounds the retry budget: every `'r'` event (duplicate snapshot_id retry)
///   must follow at least one prior `'I'` for that snapshot_id, so retries
///   can never exceed successful commits.
/// - Confirms `pending_sink_state` has fully drained: no `Pending` or
///   `Aborted` rows linger for this sink (only the most recent `Committed`
///   marker is permitted to remain).
pub async fn assert_invariants(handle: &mut V3TestHandle) -> Result<()> {
    // Wait for stable file count with a 30s cap so hangs surface diagnostically.
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        handle.mock.wait_for_data_files_stable(0, usize::MAX),
    )
    .await
    .context("wait_for_data_files_stable timed out after 30s in assert_v3_invariants")?
    .context("wait_for_data_files_stable returned error")?;

    // No duplicate file paths across snapshot history.
    handle.mock.check_no_duplicate_file_path_in_history()?;

    // At least one snapshot must have landed — otherwise the test silently
    // passed without actually exercising the sink commit path.
    let snapshot_count = handle.mock.committed_snapshot_count();
    anyhow::ensure!(
        snapshot_count >= 1,
        "expected at least 1 committed snapshot, got {}; event trace = {}",
        snapshot_count,
        handle.mock.get_event_trace(),
    );

    // Retry budget: each 'r' requires a prior 'I' for the same snapshot_id,
    // so retries can never exceed successful commits.
    let r_count = handle.mock.count_events('r');
    let i_count = handle.mock.count_events('I');
    anyhow::ensure!(
        r_count <= i_count,
        "retry count 'r'={} exceeds successful commit count 'I'={}; event trace = {}",
        r_count,
        i_count,
        handle.mock.get_event_trace(),
    );

    assert_pending_sink_state_drained(handle).await?;

    Ok(())
}

/// Open a read-only sea-orm connection against the cluster's SQLite metastore
/// and verify the V3 sink's `pending_sink_state` rows have fully drained.
///
/// After the sink is idle, every `persist_pre_commit_metadata` row must have
/// been advanced through `commit_and_prune_epoch` (Committed + prev epoch
/// deleted) — at most the latest `Committed` row should remain. Lingering
/// `Pending` rows mean a 2PC commit was abandoned mid-flight; `Aborted` rows
/// mean recovery flagged a row that `clean_aborted_records` then failed to
/// reap. Either signals a bug in the V3 state machine that the existing mock
/// assertions cannot detect.
async fn assert_pending_sink_state_drained(handle: &mut V3TestHandle) -> Result<()> {
    let sink_id_str = handle
        .cluster
        .run("SELECT id FROM rw_catalog.rw_sinks WHERE name = 'test_v3_sink'")
        .await
        .context("failed to look up test_v3_sink id from rw_sinks")?;
    let sink_id_u32: u32 = sink_id_str
        .trim()
        .parse()
        .with_context(|| format!("unexpected rw_sinks.id result: {:?}", sink_id_str))?;
    let sink_id = SinkId::from(sink_id_u32);

    let url = format!(
        "sqlite://{}?mode=ro",
        handle.cluster.meta_sqlite_path().display()
    );
    let db = Database::connect(&url)
        .await
        .with_context(|| format!("failed to open meta sqlite at {}", url))?;

    // The Committed marker is written by `commit_and_prune_epoch` after iceberg
    // accepts the snapshot, so it may briefly lag the mock's 'I' event. Poll
    // for up to 30s before failing.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let rows = pending_sink_state::Entity::find()
            .filter(pending_sink_state::Column::SinkId.eq(sink_id))
            .order_by_asc(pending_sink_state::Column::Epoch)
            .all(&db)
            .await
            .context("failed to query pending_sink_state")?;

        let bad: Vec<_> = rows
            .iter()
            .filter(|r| !matches!(r.sink_state, pending_sink_state::SinkState::Committed))
            .collect();

        if bad.is_empty() {
            return Ok(());
        }

        if std::time::Instant::now() >= deadline {
            let detail: Vec<_> = rows
                .iter()
                .map(|r| format!("(epoch={}, state={:?})", r.epoch, r.sink_state))
                .collect();
            anyhow::bail!(
                "pending_sink_state did not drain for sink_id={}: rows = [{}]; event trace = {}",
                sink_id_u32,
                detail.join(", "),
                handle.mock.get_event_trace(),
            );
        }

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}
