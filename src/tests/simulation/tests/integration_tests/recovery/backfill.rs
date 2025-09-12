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

use std::sync::LazyLock;
use std::time::Duration;

use anyhow::{Context as _, Result};
use itertools::Itertools;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

async fn single_internal_table_name(session: &mut Session) -> Result<String> {
    session
        .show_internal_tables()
        .await?
        .into_iter()
        .exactly_one()
        .map(|(_, table_name)| table_name)
        .context("expected exactly one internal table")
}

const ROOT_TABLE_CREATE: &str = "create table t1 (v1 int);";
const ROOT_TABLE_DROP: &str = "drop table t1;";
const MV1: &str = "create materialized view m1 as select * from t1 where v1 > 5;";
const INDEX: &str = "create index i1 on t1(v1);";

static EXPECTED_NO_BACKFILL: LazyLock<String> = LazyLock::new(|| {
    (0..=255)
        .map(|vnode| format!("{} NULL t 0", vnode))
        .join("\n")
});

fn select_all(table: impl AsRef<str>) -> String {
    format!("SELECT * FROM {} ORDER BY vnode", table.as_ref())
}

async fn test_no_backfill_state(session: &mut Session) -> Result<()> {
    // After startup with no backfill, should be NO_BACKFILL state.
    let table = single_internal_table_name(session).await?;
    let actual = session.run(select_all(table)).await?;
    assert_eq!(&actual, EXPECTED_NO_BACKFILL.as_str());
    Ok(())
}

#[tokio::test]
async fn test_snapshot_mv() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run(ROOT_TABLE_CREATE).await?;
    session.run(MV1).await?;

    test_no_backfill_state(&mut session).await?;

    session
        .run(&format!(
            "insert into t1 values {}",
            (1..=10).map(|x| format!("({x})")).join(",")
        ))
        .await?;

    session.run("flush").await?;

    // v1 > 5, result is [6, 7, 8, 9, 10]
    session
        .run("select count(*) from m1")
        .await?
        .assert_result_eq("5");

    // After startup with no backfill, with data inserted after, should be NO_BACKFILL state.
    test_no_backfill_state(&mut session).await?;

    let fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("StreamTableScan"),
        ])
        .await?;

    let id = fragment.id();

    let workers = fragment.all_worker_count().into_keys().collect_vec();

    // prev cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    cluster
        .reschedule(format!(
            "{}:[{}]",
            id,
            format_args!("{}:-1,{}:-2,{}:-2", workers[0], workers[1], workers[2]),
        ))
        .await?;

    sleep(Duration::from_secs(3)).await;

    // Before complete recovery should be NO_BACKFILL state
    test_no_backfill_state(&mut session).await?;

    // prev cluster.reschedule(format!("{id}+[1,2,3,4,5]")).await?;
    cluster
        .reschedule(format!(
            "{}:[{}]",
            id,
            format_args!("{}:1,{}:2,{}:2", workers[0], workers[1], workers[2]),
        ))
        .await?;

    sleep(Duration::from_secs(3)).await;

    // After recovery should be NO_BACKFILL state
    test_no_backfill_state(&mut session).await?;

    Ok(())
}

#[tokio::test]
async fn test_backfill_mv() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run(ROOT_TABLE_CREATE).await?;

    session
        .run(&format!(
            "insert into t1 values {}",
            (1..=10).map(|x| format!("({x})")).join(",")
        ))
        .await?;
    session.run("flush").await?;

    session.run(MV1).await?;

    let internal_table = single_internal_table_name(&mut session).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    let fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("StreamTableScan"),
        ])
        .await?;

    let id = fragment.id();

    let workers = fragment.all_worker_count().into_keys().collect_vec();

    // prev cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    cluster
        .reschedule(format!(
            "{}:[{}]",
            id,
            format_args!("{}:-1,{}:-2,{}:-2", workers[0], workers[1], workers[2]),
        ))
        .await?;

    sleep(Duration::from_secs(3)).await;

    let internal_table = single_internal_table_name(&mut session).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    // prev cluster.reschedule(format!("{id}+[1,2,3,4,5]")).await?;
    cluster
        .reschedule(format!(
            "{}:[{}]",
            id,
            format_args!("{}:1,{}:2,{}:2", workers[0], workers[1], workers[2]),
        ))
        .await?;

    sleep(Duration::from_secs(3)).await;

    let internal_table = single_internal_table_name(&mut session).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    Ok(())
}

#[tokio::test]
async fn test_index_backfill() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run(ROOT_TABLE_CREATE).await?;

    session
        .run(&format!(
            "insert into t1 values {}",
            (1..=10).map(|x| format!("({x})")).join(",")
        ))
        .await?;
    session.run("flush").await?;

    session.run(INDEX).await?;

    let internal_table = single_internal_table_name(&mut session).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    let fragment = cluster
        .locate_one_fragment([
            identity_contains("index"),
            no_identity_contains("StreamTableScan"),
        ])
        .await?;

    let id = fragment.id();

    let workers = fragment.all_worker_count().into_keys().collect_vec();

    // prev cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    cluster
        .reschedule(format!(
            "{}:[{}]",
            id,
            format_args!("{}:-1,{}:-2,{}:-2", workers[0], workers[1], workers[2]),
        ))
        .await?;
    sleep(Duration::from_secs(3)).await;

    let internal_table = single_internal_table_name(&mut session).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    // prev cluster.reschedule(format!("{id}+[1,2,3,4,5]")).await?;
    cluster
        .reschedule(format!(
            "{}:[{}]",
            id,
            format_args!("{}:1,{}:2,{}:2", workers[0], workers[1], workers[2]),
        ))
        .await?;
    sleep(Duration::from_secs(3)).await;

    let internal_table = single_internal_table_name(&mut session).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    session.run(ROOT_TABLE_DROP).await?;
    let results = session.show_internal_tables().await?;
    assert!(results.is_empty());

    Ok(())
}
