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

use anyhow::{Result, bail};
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

use crate::scale::auto_parallelism::MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE;

async fn wait_parallelism(
    session: &mut risingwave_simulation::cluster::Session,
    name: &str,
    expected: &str,
) -> Result<()> {
    for _ in 0..(MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 10) {
        let res = session
            .run(format!(
                "select distinct parallelism from rw_fragment_parallelism where name = '{}' order by parallelism;",
                name
            ))
            .await?;
        if res.trim() == expected {
            return Ok(());
        }
        sleep(Duration::from_millis(200)).await;
    }
    bail!(
        "parallelism for {} did not reach expected {}",
        name,
        expected
    );
}

async fn wait_jobs_finished(session: &mut risingwave_simulation::cluster::Session) -> Result<()> {
    for _ in 0..(MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 20) {
        let res = session.run("show jobs;").await?;
        if res.trim().is_empty() {
            return Ok(());
        }
        sleep(Duration::from_millis(500)).await;
    }
    bail!("jobs are still running after waiting");
}

#[tokio::test]
async fn test_backfill_parallelism_switches_to_normal_after_completion() -> Result<()> {
    let config = Configuration::for_background_ddl();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism=3;").await?;
    session
        .run("set streaming_parallelism_for_backfill=2;")
        .await?;
    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 12);")
        .await?;
    session.run("set backfill_rate_limit=1;").await?;
    session.run("set background_ddl=true;").await?;
    session
        .run("create materialized view m as select * from t;")
        .await?;

    wait_parallelism(&mut session, "m", "2").await?;
    wait_jobs_finished(&mut session).await?;
    wait_parallelism(&mut session, "m", "3").await?;

    // also ensure the catalog view reflects the final parallelism
    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 'm' order by parallelism;")
        .await?
        .assert_result_eq("3");

    Ok(())
}

#[tokio::test]
async fn test_backfill_parallelism_optional_and_adaptive() -> Result<()> {
    let config = Configuration::for_background_ddl();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    // Adaptive (no streaming_parallelism) with a custom backfill override.
    session
        .run("set streaming_parallelism_for_backfill=2;")
        .await?;
    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 12);")
        .await?;
    session.run("set backfill_rate_limit=1;").await?;
    session.run("set background_ddl=true;").await?;
    session
        .run("create materialized view m as select * from t;")
        .await?;

    // backfill uses override
    wait_parallelism(&mut session, "m", "2").await?;
    wait_jobs_finished(&mut session).await?;

    // final should be adaptive (full cluster), expect fallback to >2; we just assert it is not 2.
    let res = session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 'm' order by parallelism;")
        .await?;
    if res.trim() == "2" {
        bail!("adaptive + backfill override did not restore to adaptive parallelism");
    }

    // Now create another MV without setting backfill override; it should not lower to 2.
    session
        .run("set streaming_parallelism_for_backfill=default;")
        .await?;
    session
        .run("create materialized view m2 as select * from t;")
        .await?;
    wait_jobs_finished(&mut session).await?;
    let res = session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 'm2' order by parallelism;")
        .await?;
    if res.trim() == "2" {
        bail!("default backfill parallelism should not force parallelism to 2");
    }

    Ok(())
}
