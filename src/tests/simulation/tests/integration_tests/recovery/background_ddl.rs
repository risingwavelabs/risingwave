// Copyright 2023 RisingWave Labs
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
use itertools::Itertools;
use risingwave_simulation::cluster::{Cluster, Configuration, KillOpts, Session};
use tokio::time::sleep;

const CREATE_TABLE: &str = "CREATE TABLE t(v1 int);";
const DROP_TABLE: &str = "DROP TABLE t;";
const SEED_TABLE_500: &str = "INSERT INTO t SELECT generate_series FROM generate_series(1, 500);";
const SEED_TABLE_100: &str = "INSERT INTO t SELECT generate_series FROM generate_series(1, 100);";
const SET_BACKGROUND_DDL: &str = "SET BACKGROUND_DDL=true;";
const SET_RATE_LIMIT_2: &str = "SET STREAMING_RATE_LIMIT=2;";
const SET_RATE_LIMIT_1: &str = "SET STREAMING_RATE_LIMIT=1;";
const RESET_RATE_LIMIT: &str = "SET STREAMING_RATE_LIMIT=0;";
const CREATE_MV1: &str = "CREATE MATERIALIZED VIEW mv1 as SELECT * FROM t;";
const DROP_MV1: &str = "DROP MATERIALIZED VIEW mv1;";
const WAIT: &str = "WAIT;";

async fn kill_cn_and_wait_recover(cluster: &Cluster) {
    cluster
        .kill_nodes(["compute-1", "compute-2", "compute-3"], 0)
        .await;
    sleep(Duration::from_secs(10)).await;
}

async fn kill_and_wait_recover(cluster: &Cluster) {
    // Kill it again
    for _ in 0..3 {
        sleep(Duration::from_secs(2)).await;
        cluster.kill_node(&KillOpts::ALL_FAST).await;
    }
    sleep(Duration::from_secs(10)).await;
}

async fn cancel_stream_jobs(session: &mut Session) -> Result<Vec<u32>> {
    tracing::info!("finding streaming jobs to cancel");
    let ids = session
        .run("select ddl_id from rw_catalog.rw_ddl_progress;")
        .await?;
    tracing::info!("selected streaming jobs to cancel {:?}", ids);
    tracing::info!("cancelling streaming jobs");
    let ids = ids.split('\n').collect::<Vec<_>>().join(",");
    let result = session.run(&format!("cancel jobs {};", ids)).await?;
    tracing::info!("cancelled streaming jobs, {:#?}", result);
    let ids = result
        .split('\n')
        .map(|s| s.parse::<u32>().unwrap())
        .collect_vec();
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

    kill_and_wait_recover(&cluster).await;

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
async fn test_background_ddl_cancel() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_500).await?;
    session.flush().await?;
    session.run(SET_RATE_LIMIT_2).await?;
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
    kill_and_wait_recover(&cluster).await;

    let ids = cancel_stream_jobs(&mut session).await?;
    assert_eq!(ids.len(), 1);

    // Make sure MV can be created after all these cancels
    session.run(RESET_RATE_LIMIT).await?;
    create_mv(&mut session).await?;

    // Wait for job to finish
    session.run(WAIT).await?;

    session.run("DROP MATERIALIZED VIEW mv1").await?;
    session.run("DROP TABLE t").await?;

    Ok(())
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
async fn test_foreground_sink_cancel() -> Result<()> {
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE_100).await?;
    session.flush().await?;

    let mut session2 = cluster.start_session();
    tokio::spawn(async move {
        session2.run(SET_RATE_LIMIT_2).await.unwrap();
        let result = session2
            .run("CREATE SINK s FROM t WITH (connector='blackhole');")
            .await;
        assert!(result.is_err());
    });

    // Wait for job to start
    sleep(Duration::from_secs(2)).await;

    // Kill CN should stop the job
    cancel_stream_jobs(&mut session).await?;

    // Create MV should succeed, since the previous foreground job should be cancelled.
    session.run(SET_RATE_LIMIT_2).await?;
    session
        .run("CREATE SINK s FROM t WITH (connector='blackhole');")
        .await?;

    session.run("DROP SINK s;").await?;
    session.run(DROP_TABLE).await?;

    Ok(())
}
