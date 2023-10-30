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
use risingwave_simulation::cluster::{Cluster, Configuration, KillOpts};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

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
    for _ in 0..5 {
        sleep(Duration::from_secs(2)).await;
        cluster.kill_node(&KillOpts::ALL).await;
    }
    sleep(Duration::from_secs(20)).await;
}

#[tokio::test]
async fn test_background_mv_barrier_recovery() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_backfill()).await?;
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

    // Now just wait for it to complete.

    sleep(Duration::from_secs(10)).await;

    // Make sure after finished, we should have 5000_000 rows.
    session
        .run("SELECT COUNT(v1) FROM m1")
        .await?
        .assert_result_eq("600000");

    session.run("DROP MATERIALIZED VIEW m1").await?;
    session.run("DROP TABLE t1").await?;

    Ok(())
}
