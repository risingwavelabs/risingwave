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

#![cfg(madsim)]

use std::default;
use std::io::Write;
use std::time::Duration;

use anyhow::Result;
use risingwave_simulation::cluster::{Cluster, ConfigPath, Configuration, Session};

fn cluster_config() -> Configuration {
    let config_path = {
        let mut file = tempfile::NamedTempFile::new().expect("failed to create temp config file");
        file.write_all(
            "\
[server]
telemetry_enabled = false
metrics_level = \"Disabled\"
[meta]
event_log_flush_interval_ms = 10\
        "
            .as_bytes(),
        )
        .expect("failed to write config file");
        file.into_temp_path()
    };

    Configuration {
        config_path: ConfigPath::Temp(config_path.into()),
        frontend_nodes: 1,
        compute_nodes: 3,
        meta_nodes: 1,
        compactor_nodes: 0,
        compute_node_cores: 2,
        ..Default::default()
    }
}

async fn assert_event_count(mut session: Session, expected_count: u64) {
    let event_type = "CREATE_STREAM_JOB_FAIL";
    let count = session
        .run(format!(
            "select count(*) from rw_event_logs where event_type='{}'",
            event_type
        ))
        .await
        .unwrap();
    assert_eq!(count, expected_count.to_string());
}

async fn assert_latest_event(
    mut session: Session,
    name: impl ToString,
    definition: impl ToString,
    error: impl ToString,
) {
    let event_type = "CREATE_STREAM_JOB_FAIL";
    let info = session
        .run(format!(
            "select info from rw_event_logs where event_type='{}' order by timestamp desc limit 1",
            event_type
        ))
        .await
        .unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&info).unwrap();
    let inner = json.get("createStreamJobFail").unwrap();
    // the event log shows the detail of the failed creation.
    assert_eq!(
        inner.get("name").unwrap().as_str().unwrap(),
        name.to_string(),
    );
    assert_eq!(
        inner
            .get("definition")
            .unwrap()
            .as_str()
            .unwrap()
            .to_lowercase(),
        definition.to_string().to_lowercase()
    );
    assert!(
        inner
            .get("error")
            .unwrap()
            .as_str()
            .unwrap()
            .to_lowercase()
            .find(&error.to_string().to_lowercase())
            .is_some()
    );
}

async fn test_create_succ(
    session: &mut Session,
    expect_failure_count: &mut u64,
    create_should_succ: &str,
) {
    session.run(create_should_succ).await.unwrap();
    assert_event_count(session.clone(), *expect_failure_count).await;
}

async fn test_create_fail(
    session: &mut Session,
    expect_failure_count: &mut u64,
    create_should_fail: &str,
    name: &str,
    error: &str,
) {
    let fp_start_actors_err = "start_actors_err";
    fail::cfg(fp_start_actors_err, "return").unwrap();
    session.run(create_should_fail).await.unwrap_err();
    fail::remove(fp_start_actors_err);
    *expect_failure_count += 1;
    assert_event_count(session.clone(), *expect_failure_count).await;
    assert_latest_event(session.clone(), name, create_should_fail, error).await;
}

/// Tests event log can record info of stream job creation failure, for CREATE TABLE/MV/INDEX/SINK.
#[tokio::test]
#[ignore]
async fn failpoint_limited_test_create_stream_job_fail() -> Result<()> {
    let mut cluster = Cluster::start(cluster_config()).await.unwrap();
    let mut session = cluster.start_session();
    const WAIT_RECOVERY_SEC: u64 = 5;
    let mut expect_failure_count = 0;

    // create table succeeds.
    test_create_succ(
        &mut session,
        &mut expect_failure_count,
        "create table t1 (c int primary key)",
    )
    .await;
    // create table fails due to injected failure and subsequent recovery.
    test_create_fail(
        &mut session,
        &mut expect_failure_count,
        "create table t2 (c int primary key)",
        "t2",
        "intentional start_actors_err",
    )
    .await;

    // wait for cluster recovery.
    tokio::time::sleep(Duration::from_secs(WAIT_RECOVERY_SEC)).await;
    // create table with source succeeds.
    test_create_succ(
        &mut session,
        &mut expect_failure_count,
        "create table ts1 (c int primary key) with (connector = 'datagen', datagen.rows.per.second = '1') format native encode native",
    ).await;
    test_create_fail(
        &mut session,
        &mut expect_failure_count,
        "create table ts2 (c int primary key) with (connector = 'datagen', datagen.rows.per.second = '1') format native encode native",
        "ts2",
        "intentional start_actors_err",
    ).await;

    tokio::time::sleep(Duration::from_secs(WAIT_RECOVERY_SEC)).await;
    // create mv succeeds.
    test_create_succ(
        &mut session,
        &mut expect_failure_count,
        "create materialized view mv1 as select * from t1",
    )
    .await;
    test_create_fail(
        &mut session,
        &mut expect_failure_count,
        "create materialized view mv2 as select * from t1",
        "mv2",
        "intentional start_actors_err",
    )
    .await;
    tokio::time::sleep(Duration::from_secs(WAIT_RECOVERY_SEC)).await;
    // create sink succeeds succeeds.
    test_create_succ(
        &mut session,
        &mut expect_failure_count,
        "create sink s1 as select * from t1 with (connector = 'blackhole', type = 'append-only', force_append_only = 'true')",
    ).await;
    test_create_fail(
        &mut session,
        &mut expect_failure_count,
        "create sink s2 as select * from t1 with (connector = 'blackhole', type = 'append-only', force_append_only = 'true')",
        "s2",
        "intentional start_actors_err",
    ).await;

    tokio::time::sleep(Duration::from_secs(WAIT_RECOVERY_SEC)).await;
    // create index succeeds.
    test_create_succ(
        &mut session,
        &mut expect_failure_count,
        "create index i1 on t1(c)",
    )
    .await;
    test_create_fail(
        &mut session,
        &mut expect_failure_count,
        "create index i2 on t1(c)",
        "i2",
        "intentional start_actors_err",
    )
    .await;

    Ok(())
}

/// Tests event log can record info of barrier collection failure.
///
/// This test is expected to be flaky, but is not according to my test:
/// Theoretically errors either reported by compute nodes, or chosen by meta node, may not be the root cause.
/// But during my tests with MADSIM_TEST_SEED from 1..1000, this test always succeeded.
#[tokio::test]
#[ignore]
async fn failpoint_limited_test_collect_barrier_failure() -> Result<()> {
    let mut cluster = Cluster::start(cluster_config()).await.unwrap();
    let mut session = cluster.start_session();
    session
        .run("create table t (c int primary key)")
        .await
        .unwrap();
    session
        .run("create materialized view mv as select * from t")
        .await
        .unwrap();

    let fp_collect_actor_err = "collect_actors_err";
    fail::cfg(fp_collect_actor_err, "return").unwrap();
    // Wait until barrier fails.
    tokio::time::sleep(Duration::from_secs(3)).await;
    fail::remove(fp_collect_actor_err);

    let event_type = "COLLECT_BARRIER_FAIL";
    let info = session
        .run(format!(
            "select info from rw_event_logs where event_type='{}' order by timestamp desc limit 1",
            event_type
        ))
        .await
        .unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&info).unwrap();
    let inner = json.get("collectBarrierFail").unwrap();
    assert!(
        inner
            .get("error")
            .unwrap()
            .as_str()
            .unwrap()
            .find("intentional collect_actors_err")
            .is_some()
    );

    Ok(())
}
