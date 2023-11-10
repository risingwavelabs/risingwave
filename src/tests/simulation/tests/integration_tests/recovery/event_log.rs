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

#![cfg(madsim)]

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
        compute_nodes: 1,
        meta_nodes: 1,
        compactor_nodes: 0,
        compute_node_cores: 2,
        etcd_timeout_rate: 0.0,
        etcd_data_path: None,
    }
}

async fn assert_event_count(mut session: Session, expected_count: u64) {
    let event_type = risingwave_pb::meta::event_log::EventType::CreateStreamJobFail.as_str_name();
    let count = session
        .run(format!(
            "select count(*) from rw_event_logs where event_type='{}'",
            event_type
        ))
        .await
        .unwrap();
    assert_eq!(count, expected_count.to_string());
}

async fn assert_latest_event(mut session: Session, name: impl ToString, definition: impl ToString) {
    let event_type = risingwave_pb::meta::event_log::EventType::CreateStreamJobFail.as_str_name();
    let info = session
        .run(format!(
            "select info from rw_event_logs where event_type='{}' order by timestamp desc limit 1",
            event_type
        ))
        .await
        .unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&info).unwrap();
    // the event log shows the detail of the failed creation.
    assert_eq!(
        json.get("name").unwrap().as_str().unwrap(),
        name.to_string()
    );
    assert_eq!(
        json.get("definition")
            .unwrap()
            .as_str()
            .unwrap()
            .to_lowercase(),
        definition.to_string().to_lowercase()
    );
}

/// Tests event log do record info of stream job creation failure, for CREATE TABLE/MV/INDEX/SINK.
#[tokio::test]
async fn test_create_stream_job_fail() -> Result<()> {
    let mut cluster = Cluster::start(cluster_config()).await.unwrap();
    let mut session = cluster.start_session();
    const WAIT_RECOVERY_SEC: u64 = 5;
    let mut expect_failure_count = 0;

    // create table succeeds.
    session
        .run("create table t1 (c int primary key);")
        .await
        .unwrap();
    assert_event_count(session.clone(), expect_failure_count).await;
    // create table fails due to injected failure and subsequent recovery.
    let fp_start_actors_err = "start_actors_err";
    let create_should_fail = "create table t2 (c int primary key)";
    fail::cfg(fp_start_actors_err, "return").unwrap();
    session.run(create_should_fail).await.unwrap_err();
    fail::remove(fp_start_actors_err);
    expect_failure_count += 1;
    assert_event_count(session.clone(), expect_failure_count).await;
    assert_latest_event(session.clone(), "t2", create_should_fail).await;

    tokio::time::sleep(Duration::from_secs(WAIT_RECOVERY_SEC)).await;
    // create table with source succeeds.
    session
        .run("create table ts1 (c int primary key) with (connector = 'datagen', datagen.rows.per.second = '1') format native encode native")
        .await
        .unwrap();
    assert_event_count(session.clone(), expect_failure_count).await;
    // create table with source fails due to injected failure and subsequent recovery.
    let create_should_fail = "create table ts2 (c int primary key) with (connector = 'datagen', datagen.rows.per.second = '1') format native encode native";
    fail::cfg(fp_start_actors_err, "return").unwrap();
    session.run(create_should_fail).await.unwrap_err();
    fail::remove(fp_start_actors_err);
    expect_failure_count += 1;
    assert_event_count(session.clone(), expect_failure_count).await;
    assert_latest_event(session.clone(), "ts2", create_should_fail).await;

    // wait for cluster recovery.
    tokio::time::sleep(Duration::from_secs(WAIT_RECOVERY_SEC)).await;
    // create mv succeeds.
    session
        .run("create materialized view mv1 as select * from t1")
        .await
        .unwrap();
    assert_event_count(session.clone(), expect_failure_count).await;
    // create table fails due to injected failure and subsequent recovery.
    let create_should_fail = "create materialized view mv2 as select * from mv1";
    fail::cfg(fp_start_actors_err, "return").unwrap();
    session.run(create_should_fail).await.unwrap_err();
    fail::remove(fp_start_actors_err);
    expect_failure_count += 1;
    assert_event_count(session.clone(), expect_failure_count).await;
    assert_latest_event(session.clone(), "mv2", create_should_fail).await;

    tokio::time::sleep(Duration::from_secs(WAIT_RECOVERY_SEC)).await;
    // create sink succeeds succeeds.
    session
        .run("CREATE SINK s1 AS select * from t1 WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true')")
        .await
        .unwrap();
    // create sink fails due to injected failure and subsequent recovery.
    assert_event_count(session.clone(), expect_failure_count).await;
    fail::cfg(fp_start_actors_err, "return").unwrap();
    let create_should_fail = "create sink s2 as select * from t1 with (connector = 'blackhole', type = 'append-only', force_append_only = 'true')";
    session.run(create_should_fail).await.unwrap_err();
    fail::remove(fp_start_actors_err);
    expect_failure_count += 1;
    assert_event_count(session.clone(), expect_failure_count).await;
    assert_latest_event(session.clone(), "s2", create_should_fail).await;

    tokio::time::sleep(Duration::from_secs(WAIT_RECOVERY_SEC)).await;
    // create index succeeds.
    session.run("create index i1 on t1(c)").await.unwrap();
    assert_event_count(session.clone(), expect_failure_count).await;
    // create index fails due to injected failure and subsequent recovery.
    let create_should_fail = "create index i2 on t1(c)";
    fail::cfg(fp_start_actors_err, "return").unwrap();
    session.run(create_should_fail).await.unwrap_err();
    fail::remove(fp_start_actors_err);
    expect_failure_count += 1;
    assert_event_count(session.clone(), expect_failure_count).await;
    assert_latest_event(session.clone(), "i2", create_should_fail).await;

    Ok(())
}
