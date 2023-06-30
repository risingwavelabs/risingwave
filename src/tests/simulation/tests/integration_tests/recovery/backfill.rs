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

use std::sync::LazyLock;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use madsim::time::sleep;
use risingwave_simulation::cluster::{Cluster, Configuration, Session};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::utils::AssertResult;

const ROOT_TABLE_CREATE: &str = "create table t1 (v1 int);";
const ROOT_TABLE_DROP: &str = "drop table t1;";
const MV1: &str = "create materialized view m1 as select * from t1 where v1 > 5;";
const INDEX: &str = "create index i1 on t1(v1);";
const SHOW_INTERNAL_TABLES: &str = "SHOW INTERNAL TABLES;";

static EXPECTED_NO_BACKFILL: LazyLock<String> = LazyLock::new(|| {
    (0..=255)
        .map(|vnode| format!("{} NULL t", vnode))
        .join("\n")
});

fn select_all(table: impl AsRef<str>) -> String {
    format!("SELECT * FROM {} ORDER BY vnode", table.as_ref())
}

async fn test_no_backfill_state(session: &mut Session) -> Result<()> {
    // After startup with no backfill, should be NO_BACKFILL state.
    let internal_table = session.run(SHOW_INTERNAL_TABLES).await?;
    let actual = session.run(select_all(internal_table)).await?;
    assert_eq!(&actual, EXPECTED_NO_BACKFILL.as_str());
    Ok(())
}

#[madsim::test]
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
            no_identity_contains("chain"),
        ])
        .await?;

    let id = fragment.id();

    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(3)).await;

    // Before complete recovery should be NO_BACKFILL state
    test_no_backfill_state(&mut session).await?;

    cluster.reschedule(format!("{id}+[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(3)).await;

    // After recovery should be NO_BACKFILL state
    test_no_backfill_state(&mut session).await?;

    Ok(())
}

#[madsim::test]
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

    let internal_table = session.run(SHOW_INTERNAL_TABLES).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    let fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("chain"),
        ])
        .await?;

    let id = fragment.id();

    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(3)).await;

    let internal_table = session.run(SHOW_INTERNAL_TABLES).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    cluster.reschedule(format!("{id}+[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(3)).await;

    let internal_table = session.run(SHOW_INTERNAL_TABLES).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    Ok(())
}

#[madsim::test]
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

    let internal_table = session.run(SHOW_INTERNAL_TABLES).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    let fragment = cluster
        .locate_one_fragment([identity_contains("index"), no_identity_contains("chain")])
        .await?;

    let id = fragment.id();

    cluster.reschedule(format!("{id}-[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(3)).await;

    let internal_table = session.run(SHOW_INTERNAL_TABLES).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    cluster.reschedule(format!("{id}+[1,2,3,4,5]")).await?;
    sleep(Duration::from_secs(3)).await;

    let internal_table = session.run(SHOW_INTERNAL_TABLES).await?;
    let results = session
        .run(format!("SELECT * FROM {}", internal_table))
        .await?;
    assert_eq!(results.lines().collect_vec().len(), 256);

    session.run(ROOT_TABLE_DROP).await?;
    let results = session.run(SHOW_INTERNAL_TABLES).await?;
    assert_eq!(results, "");

    Ok(())
}
