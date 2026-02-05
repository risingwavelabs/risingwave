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
use crate::utils::kill_cn_meta_and_wait_full_recovery;

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

async fn wait_jobs_contains(
    session: &mut risingwave_simulation::cluster::Session,
    expected: &[&str],
) -> Result<()> {
    for _ in 0..(MAX_HEARTBEAT_INTERVAL_SECS_CONFIG_FOR_AUTO_SCALE * 10) {
        let res = session.run("show jobs;").await?;
        if expected.iter().all(|name| res.contains(name)) {
            return Ok(());
        }
        sleep(Duration::from_millis(200)).await;
    }
    bail!("jobs did not contain all expected entries: {:?}", expected);
}

#[tokio::test]
async fn test_backfill_parallelism_switches_to_normal_after_completion() -> Result<()> {
    let config = Configuration::for_background_ddl();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 3;").await?;
    session
        .run("set streaming_parallelism_for_backfill = 2;")
        .await?;
    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 12);")
        .await?;
    session.run("set backfill_rate_limit = 1;").await?;
    session.run("set background_ddl = true;").await?;
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
        .run("set streaming_parallelism_for_backfill = 2;")
        .await?;
    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 12);")
        .await?;
    session.run("set backfill_rate_limit = 1;").await?;
    session.run("set background_ddl = true;").await?;
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
        .run("set streaming_parallelism_for_backfill = default;")
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

#[tokio::test]
async fn test_backfill_parallelism_persists_after_recovery() -> Result<()> {
    let config = Configuration::for_background_ddl();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 4;").await?;
    session
        .run("set streaming_parallelism_for_backfill = 2;")
        .await?;
    session.run("set backfill_rate_limit = 1;").await?;
    session.run("set background_ddl = true;").await?;

    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 500);")
        .await?;
    session
        .run("create materialized view m as select * from t;")
        .await?;

    // Ensure backfill starts with the configured backfill parallelism.
    wait_parallelism(&mut session, "m", "2").await?;

    // Trigger recovery while backfill is in progress.
    kill_cn_meta_and_wait_full_recovery(&mut cluster).await;

    // After recovery, the job should continue using backfill parallelism.
    wait_parallelism(&mut session, "m", "2").await?;

    // Eventually backfill finishes and parallelism restores to the normal value.
    wait_jobs_finished(&mut session).await?;
    wait_parallelism(&mut session, "m", "4").await?;

    Ok(())
}

#[tokio::test]
async fn test_backfill_parallelism_prefers_backfill_override_over_mview_override() -> Result<()> {
    let config = Configuration::for_background_ddl();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 3;").await?;
    session
        .run("set streaming_parallelism_for_materialized_view = 2;")
        .await?;
    session
        .run("set streaming_parallelism_for_backfill = 1;")
        .await?;
    session.run("set backfill_rate_limit = 1;").await?;
    session.run("set background_ddl = true;").await?;

    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 100);")
        .await?;

    // With both MV-specific and backfill-specific overrides, backfill uses the backfill value (1),
    // while normal parallelism falls back to the MV-specific value (2) instead of the global (3).
    session
        .run("create materialized view m1 as select * from t;")
        .await?;
    wait_parallelism(&mut session, "m1", "1").await?;
    wait_jobs_finished(&mut session).await?;
    wait_parallelism(&mut session, "m1", "2").await?;
    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 'm1' order by parallelism;")
        .await?
        .assert_result_eq("2");

    // Clear backfill override; backfill should now use the MV-specific parallelism (2) for the new MV.
    session
        .run("set streaming_parallelism_for_backfill = default;")
        .await?;
    session
        .run("create materialized view m2 as select * from t;")
        .await?;
    wait_parallelism(&mut session, "m2", "2").await?;
    wait_jobs_finished(&mut session).await?;
    wait_parallelism(&mut session, "m2", "2").await?;
    session
        .run("select distinct parallelism from rw_fragment_parallelism where name = 'm2' order by parallelism;")
        .await?
        .assert_result_eq("2");

    Ok(())
}

#[tokio::test]
async fn test_concurrent_backfill_parallelism_restores_after_finish() -> Result<()> {
    // Regression test for issue 24687.
    let config = Configuration::for_background_ddl();
    let mut cluster = Cluster::start(config).await?;
    let mut session = cluster.start_session();

    session.run("set streaming_parallelism = 3;").await?;
    session
        .run("set streaming_parallelism_for_backfill = 2;")
        .await?;
    session.run("create table t(v int);").await?;
    session
        .run("insert into t select * from generate_series(1, 200);")
        .await?;
    session.run("set backfill_rate_limit = 1;").await?;
    session.run("set background_ddl = true;").await?;

    session
        .run("create materialized view m1 as select * from t;")
        .await?;
    session
        .run("create materialized view m2 as select * from t;")
        .await?;

    // Ensure both backfill jobs are running concurrently.
    wait_jobs_contains(&mut session, &["m1", "m2"]).await?;

    // Backfill should use the override parallelism.
    wait_parallelism(&mut session, "m1", "2").await?;
    wait_parallelism(&mut session, "m2", "2").await?;

    // After both jobs finish, parallelism should restore to the normal value.
    wait_jobs_finished(&mut session).await?;
    wait_parallelism(&mut session, "m1", "3").await?;
    wait_parallelism(&mut session, "m2", "3").await?;

    Ok(())
}
