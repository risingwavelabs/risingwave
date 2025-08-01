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

use anyhow::Result;
use itertools::Itertools;
use rand::prelude::SliceRandom;
use rand::rng as thread_rng;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

const ROOT_TABLE_CREATE: &str = "create table t (v1 int);";
const ROOT_MV: &str = "create materialized view m1 as select count(*) as c1 from t;";
const CASCADE_MV: &str = "create materialized view m2 as select * from m1;";

async fn test_singleton_migration_helper(configuration: Configuration) -> Result<()> {
    let mut cluster = Cluster::start(configuration).await?;
    let mut session = cluster.start_session();

    session.run(ROOT_TABLE_CREATE).await?;
    session.run(ROOT_MV).await?;
    session.run(CASCADE_MV).await?;

    let fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            identity_contains("simpleAgg"),
        ])
        .await?;

    let mut all_worker_slots = fragment.all_worker_slots().into_iter().collect_vec();
    let used_worker_slots = fragment.used_worker_slots();

    assert_eq!(used_worker_slots.len(), 1);

    all_worker_slots.shuffle(&mut thread_rng());

    let mut target_worker_slots = all_worker_slots
        .into_iter()
        .filter(|work_slot| !used_worker_slots.contains(work_slot));

    let source_slot = used_worker_slots.iter().exactly_one().cloned().unwrap();
    let target_slot = target_worker_slots.next().unwrap();

    assert_ne!(target_slot, source_slot);

    cluster
        .reschedule(fragment.reschedule([source_slot], [target_slot]))
        .await?;

    sleep(Duration::from_secs(3)).await;

    session
        .run(&format!(
            "insert into t values {}",
            (1..=10).map(|x| format!("({x})")).join(",")
        ))
        .await?;

    session.run("flush").await?;

    session
        .run("select * from m2")
        .await?
        .assert_result_eq("10");

    let source_slot = target_slot;
    let target_slot = target_worker_slots.next().unwrap();

    cluster
        .reschedule(fragment.reschedule([source_slot], [target_slot]))
        .await?;

    sleep(Duration::from_secs(3)).await;

    session
        .run(&format!(
            "insert into t values {}",
            (11..=20).map(|x| format!("({x})")).join(",")
        ))
        .await?;

    session.run("flush").await?;

    session
        .run("select * from m2")
        .await?
        .assert_result_eq("20");

    Ok(())
}

#[tokio::test]
async fn test_singleton_migration() -> Result<()> {
    test_singleton_migration_helper(Configuration::for_scale()).await
}

#[tokio::test]
async fn test_singleton_migration_for_no_shuffle() -> Result<()> {
    test_singleton_migration_helper(Configuration::for_scale_no_shuffle()).await
}
