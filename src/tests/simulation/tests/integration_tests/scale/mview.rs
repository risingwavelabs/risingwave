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

use std::iter::repeat_with;
use std::time::Duration;

use anyhow::Result;
use itertools::Itertools;
use tokio::time::sleep;
use risingwave_simulation::cluster::{Cluster, Configuration, KillOpts};
use risingwave_simulation::ctl_ext::predicate::identity_contains;

#[tokio::test]
async fn test_scale_in_mv_then_recover() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    cluster.run("create table t (v1 int, v2 int);").await?;
    cluster.run("insert into t select generate_series % 100 as v1, generate_series as v2 from generate_series(1, 300000);").await?;
    cluster.run("flush;").await?;
    cluster.run("create materialized view m1 as select v1, sum(v2) from t group by v1;").await?;

    let fragment = cluster
        .locate_one_fragment([identity_contains("materialize"), identity_contains("sum")])
        .await?;

    cluster
        .reschedule(fragment.reschedule([0, 1, 2, 4], []))
        .await?;

    for _ in 0..5 {
        sleep(Duration::from_secs(2)).await;
        cluster.kill_node(&KillOpts {
            kill_rate: 1.0,
            kill_meta: false,
            kill_frontend: false,
            kill_compute: true,
            kill_compactor: false,
            restart_delay_secs: 10,
        }).await;
    }
    sleep(Duration::from_secs(30)).await;
    let results = cluster.run("select count(*) from m1;").await?;
    assert_eq!(results, "100");
    // panic!("FAIL INTENTIONALLY");

    Ok(())
}
