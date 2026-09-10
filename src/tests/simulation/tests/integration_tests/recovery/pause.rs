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

use std::time::Duration;

use anyhow::{Result, bail};
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use tokio::time::sleep;

use crate::utils::wait_all_database_recovered;

const SEED_ROWS: u64 = 10000;
const CREATE_TABLE: &str = "CREATE TABLE t (v int);";
const SEED_TABLE: &str = "INSERT INTO t SELECT * FROM generate_series(1, 10000);";
const CREATE_MV: &str = "CREATE MATERIALIZED VIEW mv AS SELECT * FROM t;";
const ALTER_RATE_LIMIT_DEFAULT: &str =
    "ALTER MATERIALIZED VIEW mv SET BACKFILL_RATE_LIMIT = DEFAULT;";

async fn seed_table(session: &mut Session) -> Result<()> {
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE).await?;
    session.flush().await
}

/// Starts a rate-limited snapshot backfill in the background.
async fn start_slow_snapshot_backfill(cluster: &mut Cluster) -> Result<Session> {
    let mut session = cluster.start_session();
    session
        .run("SET streaming_use_snapshot_backfill = true;")
        .await?;
    session.run("SET background_ddl = true;").await?;
    session.run("SET backfill_rate_limit = 10;").await?;
    seed_table(&mut session).await?;
    session.run(CREATE_MV).await?;
    Ok(session)
}

/// Rows consumed by the only creating job, parsed from a progress like
/// `Snapshot [1.23% (123/10000)]`.
async fn consumed_rows(session: &mut Session) -> Result<u64> {
    let progress = session
        .run("SELECT progress FROM rw_catalog.rw_ddl_progress;")
        .await?;
    let Some((_, rest)) = progress.split_once('(') else {
        bail!("unexpected progress: {progress}");
    };
    let Some((consumed, _)) = rest.split_once('/') else {
        bail!("unexpected progress: {progress}");
    };
    Ok(consumed.parse()?)
}

async fn wait_backfill_started(session: &mut Session) -> Result<()> {
    for _ in 0..60 {
        if consumed_rows(session).await.unwrap_or(0) > 0 {
            return Ok(());
        }
        sleep(Duration::from_secs(1)).await;
    }
    bail!("backfill did not start");
}

/// Asserts the backfill makes no progress once the pause barrier has settled and returns the
/// frozen count.
async fn assert_backfill_frozen(session: &mut Session) -> Result<u64> {
    sleep(Duration::from_secs(3)).await;
    let before = consumed_rows(session).await?;
    sleep(Duration::from_secs(10)).await;
    let after = consumed_rows(session).await?;
    assert_eq!(before, after, "backfill progressed while paused");
    Ok(after)
}

async fn resume_and_finish(
    cluster: &mut Cluster,
    session: &mut Session,
    frozen: u64,
) -> Result<()> {
    cluster.resume().await?;
    sleep(Duration::from_secs(5)).await;
    assert!(
        consumed_rows(session).await? > frozen,
        "backfill did not resume"
    );

    session.run(ALTER_RATE_LIMIT_DEFAULT).await?;
    session.run("WAIT;").await?;
    let count = session.run("SELECT count(*) FROM mv;").await?;
    assert_eq!(count, SEED_ROWS.to_string());
    Ok(())
}

#[tokio::test]
async fn test_pause_freezes_snapshot_backfill() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = start_slow_snapshot_backfill(&mut cluster).await?;
    wait_backfill_started(&mut session).await?;

    cluster.pause().await?;
    let frozen = assert_backfill_frozen(&mut session).await?;

    resume_and_finish(&mut cluster, &mut session, frozen).await
}

#[tokio::test]
async fn test_pause_on_next_bootstrap_with_snapshot_backfill_job() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = start_slow_snapshot_backfill(&mut cluster).await?;
    wait_backfill_started(&mut session).await?;

    session
        .run("ALTER SYSTEM SET pause_on_next_bootstrap = true;")
        .await?;
    cluster.kill_nodes_and_restart(["meta-1"], 5).await;
    cluster.wait_for_recovery().await?;
    wait_all_database_recovered(&mut cluster).await;

    let mut session = cluster.start_session();
    let frozen = assert_backfill_frozen(&mut session).await?;

    resume_and_finish(&mut cluster, &mut session, frozen).await
}

/// The compute nodes come back after meta, so the database fails the global recovery attempt and
/// is recovered on its own. The paused bootstrap must survive that path.
#[tokio::test]
async fn test_pause_on_next_bootstrap_with_late_compute_nodes() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = start_slow_snapshot_backfill(&mut cluster).await?;
    wait_backfill_started(&mut session).await?;

    session
        .run("ALTER SYSTEM SET pause_on_next_bootstrap = true;")
        .await?;
    tokio::join!(
        cluster.kill_nodes_and_restart(["meta-1"], 2),
        cluster.kill_nodes_and_restart(["compute-1", "compute-2", "compute-3"], 8),
    );
    cluster.wait_for_recovery().await?;
    wait_all_database_recovered(&mut cluster).await;

    let mut session = cluster.start_session();
    let frozen = assert_backfill_frozen(&mut session).await?;

    resume_and_finish(&mut cluster, &mut session, frozen).await
}

/// A paused database stays paused after it recovers from a compute node failure.
#[tokio::test]
async fn test_pause_survives_database_recovery() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = start_slow_snapshot_backfill(&mut cluster).await?;
    wait_backfill_started(&mut session).await?;

    cluster.pause().await?;
    assert_backfill_frozen(&mut session).await?;

    cluster.kill_nodes_and_restart(["compute-1"], 3).await;
    wait_all_database_recovered(&mut cluster).await;

    let mut session = cluster.start_session();
    let frozen = assert_backfill_frozen(&mut session).await?;

    resume_and_finish(&mut cluster, &mut session, frozen).await
}

/// Creates `mv` with `create_mv` while the cluster is paused, checks that it consumes nothing
/// until `resume`, then lets it finish.
async fn assert_create_while_paused(
    cluster: &mut Cluster,
    session: &mut Session,
    create_mv: &str,
) -> Result<()> {
    cluster.pause().await?;
    session.run("SET background_ddl = true;").await?;
    session.run("SET backfill_rate_limit = 10;").await?;
    session.run(create_mv).await?;
    sleep(Duration::from_secs(10)).await;
    assert_eq!(consumed_rows(session).await?, 0, "backfill ran while paused");

    cluster.resume().await?;
    wait_backfill_started(session).await?;
    session.run(ALTER_RATE_LIMIT_DEFAULT).await?;
    session.run("WAIT;").await?;
    let count = session.run("SELECT count(*) FROM mv;").await?;
    assert_eq!(count, SEED_ROWS.to_string());
    Ok(())
}

/// A job created while the cluster is paused starts paused and only runs after `resume`.
#[tokio::test]
async fn test_create_while_paused() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    seed_table(&mut session).await?;
    session
        .run("SET streaming_use_snapshot_backfill = true;")
        .await?;
    assert_create_while_paused(&mut cluster, &mut session, CREATE_MV).await
}

/// Same for a batch refresh job, whose partial graph is created by its own path in meta.
#[tokio::test]
async fn test_create_batch_refresh_while_paused() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    seed_table(&mut session).await?;
    session
        .run("CREATE MATERIALIZED VIEW mv_up AS SELECT * FROM t;")
        .await?;
    assert_create_while_paused(
        &mut cluster,
        &mut session,
        "CREATE MATERIALIZED VIEW mv WITH (refresh.interval.sec = 600) AS SELECT * FROM mv_up;",
    )
    .await
}
