// Copyright 2024 RisingWave Labs
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

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use risingwave_pb::common::{WorkerNode, WorkerType};
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::{identity_contains, no_identity_contains};
use risingwave_simulation::ctl_ext::Fragment;
use risingwave_simulation::utils::AssertResult;
use tokio::time::sleep;

#[tokio::test]
async fn test_diamond_cascade_materialized_view_alter() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    let mut session = cluster.start_session();

    session.run("create table t1 (v1 int);").await?;
    session
        .run("create materialized view m1 as select * from t1 where v1 > 5;")
        .await?;
    session
        .run("create materialized view m2 as select * from t1 where v1 > 10;")
        .await?;
    session
        .run("create materialized view m3 as select * from m2 where v1 < 15;")
        .await?;
    session.run("create materialized view m4 as select m1.v1 as m1v, m3.v1 as m3v from m1 join m3 on m1.v1 = m3.v1 limit 100;").await?;
    session
        .run("create materialized view m5 as select * from m4;")
        .await?;

    let fragment = cluster
        .locate_one_fragment([
            identity_contains("materialize"),
            no_identity_contains("StreamTableScan"),
            no_identity_contains("topn"),
            no_identity_contains("hashjoin"),
        ])
        .await?;

    let id = fragment.id();

    session.run("alter table t1 set parallelism = 1;").await?;
    sleep(Duration::from_secs(3)).await;
    //
    // let fragment = cluster.locate_fragment_by_id(id).await?;
    // assert_eq!(fragment.inner.actors.len(), 1);
    //
    // session
    //     .run(&format!(
    //         "insert into t1 values {}",
    //         (1..=10).map(|x| format!("({x})")).join(",")
    //     ))
    //     .await?;
    //
    // session.run("flush").await?;
    // session
    //     .run("select count(*) from m5")
    //     .await?
    //     .assert_result_eq("0");
    //
    // session
    //     .run("alter table t1 set parallelism = adaptive;")
    //     .await?;
    // sleep(Duration::from_secs(3)).await;
    //
    // let fragment = cluster.locate_fragment_by_id(id).await?;
    // assert_eq!(fragment.inner.actors.len(), 6);
    //
    // session
    //     .run("select count(*) from m5")
    //     .await?
    //     .assert_result_eq("0");
    //
    // session
    //     .run(&format!(
    //         "insert into t1 values {}",
    //         (11..=20).map(|x| format!("({x})")).join(",")
    //     ))
    //     .await?;
    //
    // session.run("flush").await?;
    // session
    //     .run("select count(*) from m5")
    //     .await?
    //     .assert_result_eq("4");

    Ok(())
}
