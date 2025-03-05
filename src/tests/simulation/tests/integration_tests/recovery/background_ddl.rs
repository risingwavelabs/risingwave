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

use anyhow::{Result, anyhow};
use risingwave_common::error::AsReport;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use tokio::time::sleep;

use crate::utils::{
    kill_cn_and_meta_and_wait_recover, kill_cn_and_wait_recover, kill_random_and_wait_recover,
};

const CREATE_TABLE: &str = "CREATE TABLE t(v1 int);";
const DROP_TABLE: &str = "DROP TABLE t;";
const SEED_TABLE_500: &str = "INSERT INTO t SELECT generate_series FROM generate_series(1, 500);";
const SEED_TABLE_100: &str = "INSERT INTO t SELECT generate_series FROM generate_series(1, 100);";
const SET_BACKGROUND_DDL: &str = "SET BACKGROUND_DDL=true;";
const SET_RATE_LIMIT_2: &str = "SET BACKFILL_RATE_LIMIT=2;";
const SET_RATE_LIMIT_1: &str = "SET BACKFILL_RATE_LIMIT=1;";
const RESET_RATE_LIMIT: &str = "SET BACKFILL_RATE_LIMIT=DEFAULT;";
const CREATE_MV1: &str = "CREATE MATERIALIZED VIEW mv1 as SELECT * FROM t;";
const DROP_MV1: &str = "DROP MATERIALIZED VIEW mv1;";
const WAIT: &str = "WAIT;";

async fn cancel_stream_jobs(session: &mut Session) -> Result<Vec<u32>> {
    tracing::info!("finding streaming jobs to cancel");
    let ids = session
        .run("select ddl_id from rw_catalog.rw_ddl_progress;")
        .await?;
    tracing::info!("selected streaming jobs to cancel {:?}", ids);
    tracing::info!("cancelling streaming jobs");
    let ids = ids.split('\n').collect::<Vec<_>>().join(",");
    let result = session.run(&format!("cancel jobs {};", ids)).await?;
    tracing::info!("cancelled streaming jobs, {}", result);
    let ids = result
        .split('\n')
        .map(|s| {
            s.parse::<u32>()
                .map_err(|_e| anyhow!("failed to parse {}", s))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ids)
}

fn init_logger() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .try_init();
}

async fn create_mv(session: &mut Session) -> Result<()> {
    session.run(CREATE_MV1).await?;
    sleep(Duration::from_secs(2)).await;
    Ok(())
}

#[tokio::test]
async fn test_background_mv_barrier_recovery() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    session.run(CREATE_TABLE).await?;
    session
        .run("INSERT INTO t SELECT generate_series FROM generate_series(1, 200);")
        .await?;
    session.flush().await?;
    session.run(SET_RATE_LIMIT_2).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    create_mv(&mut session).await?;

    // If the CN is killed before first barrier pass for the MV, the MV will be dropped.
    // This is because it's table fragments will NOT be committed until first barrier pass.
    kill_cn_and_wait_recover(&cluster).await;

    // Send some upstream updates.
    session
        .run("INSERT INTO t SELECT generate_series FROM generate_series(201, 400);")
        .await?;
    session.flush().await?;

    kill_random_and_wait_recover(&cluster).await;

    // Now just wait for it to complete.
    session.run(WAIT).await?;

    let t_count = session.run("SELECT COUNT(v1) FROM t").await?;
    let mv1_count = session.run("SELECT COUNT(v1) FROM mv1").await?;
    if t_count != mv1_count {
        let missing_rows = session
            .run("SELECT v1 FROM t WHERE v1 NOT IN (SELECT v1 FROM mv1)")
            .await?;
        tracing::debug!(missing_rows);
        let missing_rows_with_row_id = session
            .run("SELECT _row_id, v1 FROM t WHERE v1 NOT IN (SELECT v1 FROM mv1)")
            .await?;
        tracing::debug!(missing_rows_with_row_id);
    }
    assert_eq!(t_count, mv1_count);

    // Make sure that if MV killed and restarted
    // it will not be dropped.
    session.run(DROP_MV1).await?;
    session.run(DROP_TABLE).await?;

    Ok(())
}

#[tokio::test]
async fn test_background_join_mv_recovery() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    session.run("CREATE TABLE t1 (v1 int)").await?;
    session.run("CREATE TABLE t2 (v1 int)").await?;
    session
        .run("INSERT INTO t1 SELECT generate_series FROM generate_series(1, 200);")
        .await?;
    session
        .run("INSERT INTO t2 SELECT generate_series FROM generate_series(1, 200);")
        .await?;
    session.flush().await?;
    session.run(SET_RATE_LIMIT_2).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    session
        .run("CREATE MATERIALIZED VIEW mv1 as select t1.v1 from t1 join t2 on t1.v1 = t2.v1;")
        .await?;
    sleep(Duration::from_secs(2)).await;

    kill_cn_and_meta_and_wait_recover(&cluster).await;

    // Now just wait for it to complete.
    session.run(WAIT).await?;

    let t_count = session.run("SELECT COUNT(v1) FROM t1").await?;
    let mv1_count = session.run("SELECT COUNT(v1) FROM mv1").await?;
    assert_eq!(t_count, mv1_count);

    // Make sure that if MV killed and restarted
    // it will not be dropped.
    session.run("DROP MATERIALIZED VIEW mv1;").await?;
    session.run("DROP TABLE t1;").await?;
    session.run("DROP TABLE t2;").await?;

    Ok(())
}

/// Test cancel for background ddl, foreground ddl.
#[tokio::test]
async fn test_ddl_cancel() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_500).await?;
    session.flush().await?;
    session.run(SET_RATE_LIMIT_1).await?;
    session.run(SET_BACKGROUND_DDL).await?;

    for _ in 0..5 {
        create_mv(&mut session).await?;
        let ids = cancel_stream_jobs(&mut session).await?;
        assert_eq!(ids.len(), 1);
    }

    session.run(SET_RATE_LIMIT_1).await?;
    create_mv(&mut session).await?;

    // Test cancel after kill cn
    kill_cn_and_wait_recover(&cluster).await;

    let ids = cancel_stream_jobs(&mut session).await?;
    assert_eq!(ids.len(), 1);

    sleep(Duration::from_secs(2)).await;

    create_mv(&mut session).await?;

    // Test cancel after kill meta
    kill_random_and_wait_recover(&cluster).await;

    let ids = cancel_stream_jobs(&mut session).await?;
    assert_eq!(ids.len(), 1);

    // Test cancel by sigkill

    let mut session2 = cluster.start_session();
    tokio::spawn(async move {
        session2.run(SET_RATE_LIMIT_1).await.unwrap();
        let _ = create_mv(&mut session2).await;
    });

    // Keep searching for the process in process list
    loop {
        let processlist = session.run("SHOW PROCESSLIST;").await?;
        if let Some(line) = processlist
            .lines()
            .find(|line| line.to_lowercase().contains("mv1"))
        {
            let pid = line.split_whitespace().next().unwrap();
            let pid = pid.parse::<usize>().unwrap();
            session.run(format!("kill {};", pid)).await?;
            sleep(Duration::from_secs(10)).await;
            break;
        }
        sleep(Duration::from_secs(2)).await;
    }

    session.run(RESET_RATE_LIMIT).await?;
    session.run(SET_BACKGROUND_DDL).await?;

    // Make sure MV can be created after all these cancels
    // Keep retrying since cancel happens async.
    loop {
        let result = create_mv(&mut session).await;
        match result {
            Ok(_) => break,
            Err(e) if e.to_string().contains("The table is being created") => {
                tracing::info!("create mv failed, retrying: {}", e);
            }
            Err(e) => {
                return Err(e);
            }
        }
        sleep(Duration::from_secs(2)).await;
        tracing::info!("create mv failed, retrying");
    }

    // Wait for job to finish
    session.run(WAIT).await?;

    session.run("DROP MATERIALIZED VIEW mv1").await?;
    session.run("DROP TABLE t").await?;

    Ok(())
}

/// When cancelling a stream job under high latency,
/// the cancel should take a long time to take effect.
/// If we trigger a recovery however, the cancel should take effect immediately,
/// since cancel will immediately drop the table fragment.
async fn test_high_barrier_latency_cancel(config: Configuration) -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    // Join 2 fact tables together to create a high barrier latency scenario.

    session.run("CREATE TABLE fact1 (v1 int)").await?;
    session
        .run("INSERT INTO fact1 select 1 from generate_series(1, 10000)")
        .await?;

    session.run("CREATE TABLE fact2 (v1 int)").await?;
    session
        .run("INSERT INTO fact2 select 1 from generate_series(1, 10000)")
        .await?;
    session.flush().await?;

    tracing::info!("seeded base tables");

    // Create high barrier latency scenario
    // Keep creating mv1, if it's not created.
    loop {
        session.run(SET_BACKGROUND_DDL).await?;
        session.run(SET_RATE_LIMIT_2).await?;
        session.run("CREATE MATERIALIZED VIEW mv1 as select fact1.v1 from fact1 join fact2 on fact1.v1 = fact2.v1").await?;
        tracing::info!("created mv in background");
        sleep(Duration::from_secs(1)).await;

        cluster
            .kill_nodes_and_restart(["compute-1", "compute-2", "compute-3"], 2)
            .await;
        sleep(Duration::from_secs(2)).await;

        tracing::debug!("killed cn, waiting recovery");

        // Check if mv stream job is created in the background
        match session
            .run("select * from rw_catalog.rw_ddl_progress;")
            .await
        {
            Ok(s) if s.is_empty() => {
                // MV was dropped
                continue;
            }
            Err(e) => {
                return Err(e);
            }
            Ok(s) => {
                tracing::info!("created mv stream job with status: {}", s);
                break;
            }
        }
    }

    tracing::info!("restarted cn: trigger stream job recovery");

    // Make sure there's some progress first.
    loop {
        // Wait until at least 10% of records are ingested.
        let progress = session
            .run("select progress from rw_catalog.rw_ddl_progress;")
            .await
            .unwrap();
        tracing::info!(progress, "get progress before cancel stream job");
        let progress = progress.split_once("%").unwrap().0;
        let progress = progress.parse::<f64>().unwrap();
        if progress >= 0.01 {
            break;
        } else {
            sleep(Duration::from_micros(1)).await;
        }
    }
    // Loop in case the cancel gets dropped after
    // cn kill, before it drops the table fragment.
    for iteration in 0..5 {
        tracing::info!(iteration, "cancelling stream job");
        let mut session2 = cluster.start_session();
        let handle = tokio::spawn(async move {
            let result = cancel_stream_jobs(&mut session2).await;
            tracing::info!(?result, "cancel stream jobs");
        });

        sleep(Duration::from_millis(500)).await;
        kill_cn_and_wait_recover(&cluster).await;
        tracing::info!("restarted cn: cancel should take effect");

        handle.await.unwrap();

        // Create MV with same relation name should succeed,
        // since the previous job should be cancelled.
        tracing::info!("recreating mv");
        session.run("SET BACKGROUND_DDL=false").await?;
        if let Err(e) = session
            .run("CREATE MATERIALIZED VIEW mv1 as values(1)")
            .await
        {
            tracing::info!(error = %e.as_report(), "Recreate mv failed");
            continue;
        } else {
            tracing::info!("recreated mv");
            break;
        }
    }

    session.run(DROP_MV1).await?;
    session.run("DROP TABLE fact1").await?;
    session.run("DROP TABLE fact2").await?;

    Ok(())
}

#[tokio::test]
async fn test_high_barrier_latency_cancel_for_arrangement_backfill() -> Result<()> {
    test_high_barrier_latency_cancel(Configuration::for_arrangement_backfill()).await
}

#[tokio::test]
async fn test_high_barrier_latency_cancel_for_no_shuffle() -> Result<()> {
    test_high_barrier_latency_cancel(Configuration::for_scale_no_shuffle()).await
}

// When cluster stop, foreground ddl job must be cancelled.
#[tokio::test]
async fn test_foreground_ddl_no_recovery() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_100).await?;
    session.flush().await?;

    let mut session2 = cluster.start_session();
    tokio::spawn(async move {
        session2.run(SET_RATE_LIMIT_2).await.unwrap();
        let result = create_mv(&mut session2).await;
        assert!(result.is_err());
    });

    // Wait for job to start
    sleep(Duration::from_secs(2)).await;

    // Kill CN should stop the job
    kill_cn_and_wait_recover(&cluster).await;

    // Create MV should succeed, since the previous foreground job should be cancelled.
    session.run(SET_RATE_LIMIT_2).await?;
    create_mv(&mut session).await?;

    session.run(DROP_MV1).await?;
    session.run(DROP_TABLE).await?;

    Ok(())
}

#[tokio::test]
async fn test_foreground_index_cancel() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_100).await?;
    session.flush().await?;

    let mut session2 = cluster.start_session();
    tokio::spawn(async move {
        session2.run(SET_RATE_LIMIT_2).await.unwrap();
        let result = session2.run("CREATE INDEX i ON t (v1);").await;
        assert!(result.is_err());
    });

    // Wait for job to start
    sleep(Duration::from_secs(2)).await;

    // Kill CN should stop the job
    cancel_stream_jobs(&mut session).await?;

    // Create MV should succeed, since the previous foreground job should be cancelled.
    session.run(SET_RATE_LIMIT_2).await?;
    session.run("CREATE INDEX i ON t (v1);").await?;

    session.run("DROP INDEX i;").await?;
    session.run(DROP_TABLE).await?;

    Ok(())
}

#[tokio::test]
async fn test_background_sink_create() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_100).await?;
    session.flush().await?;

    let mut session2 = cluster.start_session();
    tokio::spawn(async move {
        session2.run(SET_BACKGROUND_DDL).await.unwrap();
        session2.run(SET_RATE_LIMIT_2).await.unwrap();
        session2
            .run("CREATE SINK s FROM t WITH (connector='blackhole');")
            .await
            .expect("create sink should succeed");
    });

    // Wait for job to start
    sleep(Duration::from_secs(2)).await;

    kill_cn_and_meta_and_wait_recover(&cluster).await;

    // Sink job should still be present, and we can drop it.
    session.run("DROP SINK s;").await?;
    session.run(DROP_TABLE).await?;

    Ok(())
}

#[tokio::test]
async fn test_background_agg_mv_recovery() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();

    session.run("CREATE TABLE t1 (v1 int)").await?;
    session
        .run("INSERT INTO t1 SELECT generate_series FROM generate_series(1, 200);")
        .await?;
    session.flush().await?;
    session.run(SET_RATE_LIMIT_1).await?;
    session.run(SET_BACKGROUND_DDL).await?;
    session
        .run("CREATE MATERIALIZED VIEW mv1 as select v1, count(*) from t1 group by v1;")
        .await?;
    sleep(Duration::from_secs(2)).await;

    kill_cn_and_meta_and_wait_recover(&cluster).await;

    // Now just wait for it to complete.
    session.run(WAIT).await?;

    let t_count = session.run("SELECT COUNT(v1) FROM t1").await?;
    let mv1_count = session.run("SELECT COUNT(v1) FROM mv1").await?;
    assert_eq!(t_count, mv1_count);

    // Make sure that if MV killed and restarted
    // it will not be dropped.
    session.run("DROP MATERIALIZED VIEW mv1;").await?;
    session.run("DROP TABLE t1;").await?;

    Ok(())
}
