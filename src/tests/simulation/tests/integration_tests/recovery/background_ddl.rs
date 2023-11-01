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
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

const CREATE_TABLE: &str = "CREATE TABLE t(v1 int);";
const SEED_TABLE: &str = "INSERT INTO t SELECT generate_series FROM generate_series(1, 500);";
const SET_BACKGROUND_DDL: &str = "SET BACKGROUND_DDL=true;";
const SET_RATE_LIMIT_2: &str = "SET STREAMING_RATE_LIMIT=2;";
const SET_RATE_LIMIT_1: &str = "SET STREAMING_RATE_LIMIT=1;";
const RESET_RATE_LIMIT: &str = "SET STREAMING_RATE_LIMIT=0;";
const CREATE_MV1: &str = "CREATE MATERIALIZED VIEW mv1 as SELECT * FROM t;";
const WAIT: &str = "WAIT;";

async fn kill_cn_and_wait_recover(cluster: &Cluster) {
    // Kill it again
    for _ in 0..5 {
        cluster
            .kill_node(&KillOpts {
                kill_rate: 1.0,
                kill_meta: false,
                kill_frontend: false,
                kill_compute: true,
                kill_compactor: false,
                restart_delay_secs: 1,
            })
            .await;
        sleep(Duration::from_secs(2)).await;
    }
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

#[tokio::test]
async fn test_background_mv_barrier_recovery() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run("CREATE TABLE t1 (v1 int);").await?;
    session
        .run("INSERT INTO t1 select * from generate_series(1, 400000)")
        .await?;
    session.run("flush").await?;
    session.run("SET BACKGROUND_DDL=true;").await?;
    session
        .run("create materialized view m1 as select * from t1;")
        .await?;

    // If the CN is killed before first barrier pass for the MV, the MV will be dropped.
    // This is because it's table fragments will NOT be committed until first barrier pass.
    sleep(Duration::from_secs(5)).await;
    kill_cn_and_wait_recover(&cluster).await;

    // Send some upstream updates.
    cluster
        .run("INSERT INTO t1 select * from generate_series(1, 100000);")
        .await?;
    cluster.run("flush;").await?;

    kill_cn_and_wait_recover(&cluster).await;
    kill_and_wait_recover(&cluster).await;

    // Send some upstream updates.
    cluster
        .run("INSERT INTO t1 select * from generate_series(1, 100000);")
        .await?;
    cluster.run("flush;").await?;

    kill_and_wait_recover(&cluster).await;
    kill_cn_and_wait_recover(&cluster).await;

    // Send some upstream updates.
    cluster
        .run("INSERT INTO t1 select * from generate_series(1, 100000);")
        .await?;
    cluster.run("flush;").await?;

    // Now just wait for it to complete.

    sleep(Duration::from_secs(10)).await;

    session
        .run("SELECT COUNT(v1) FROM m1")
        .await?
        .assert_result_eq("700000");

    // Make sure that if MV killed and restarted
    // it will not be dropped.

    session.run("DROP MATERIALIZED VIEW m1").await?;
    session.run("DROP TABLE t1").await?;

    Ok(())
}

#[tokio::test]
async fn test_background_ddl_cancel() -> Result<()> {
    async fn create_mv(session: &mut Session) -> Result<()> {
        session.run(CREATE_MV1).await?;
        sleep(Duration::from_secs(2)).await;
        Ok(())
    }
    init_logger();
    let mut cluster = Cluster::start(Configuration::for_background_ddl()).await?;
    let mut session = cluster.start_session();
    session.run(CREATE_TABLE).await?;
    session.run(SEED_TABLE).await?;
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
