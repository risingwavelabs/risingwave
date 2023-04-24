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
use madsim::time::sleep;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;
use risingwave_simulation::utils::AssertResult;

const ROOT_TABLE_CREATE: &str = "create table t (v1 int);";
const ROOT_MV: &str = "create materialized view m1 as select count(*) as c1 from t;";
const CASCADE_MV: &str = "create materialized view m2 as select * from m1;";

#[madsim::test]
async fn test_singleton_migration() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run(ROOT_TABLE_CREATE).await?;
    session.run(ROOT_MV).await?;
    session.run(CASCADE_MV).await?;

    let fragment = cluster
        .locate_one_fragment(vec![
            identity_contains("materialize"),
            identity_contains("globalSimpleAgg"),
        ])
        .await?;

    let id = fragment.id();

    let (mut all, used) = fragment.parallel_unit_usage();

    assert_eq!(used.len(), 1);

    all.shuffle(&mut thread_rng());

    let mut target_parallel_units = all
        .into_iter()
        .filter(|parallel_unit_id| !used.contains(parallel_unit_id));

    let source_parallel_unit = used.iter().next().cloned().unwrap();
    let target_parallel_unit = target_parallel_units.next().unwrap();

    assert_ne!(target_parallel_unit, source_parallel_unit);

    cluster
        .reschedule(format!(
            "{id}-[{source_parallel_unit}]+[{target_parallel_unit}]"
        ))
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

    let source_parallel_unit = target_parallel_unit;
    let target_parallel_unit = target_parallel_units.next().unwrap();

    cluster
        .reschedule(format!(
            "{id}-[{source_parallel_unit}]+[{target_parallel_unit}]"
        ))
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
