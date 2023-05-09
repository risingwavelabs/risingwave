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

use std::iter::{repeat, repeat_with};
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
const MV1: &str = "create materialized view m1 as select * from t;";

macro_rules! insert_and_flush {
    ($cluster:ident) => {{
        let mut session = $cluster.start_session();
        let values = repeat_with(|| rand::random::<i32>())
            .take(100)
            .map(|i| format!("({})", i))
            .join(",");
        session
            .run(format!("insert into t values {}", values))
            .await?;
        session.run("flush").await?;
    }};
}

#[madsim::test]
async fn test_table() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    cluster.run(ROOT_TABLE_CREATE).await?;

    let fragment = cluster
        .locate_one_fragment([identity_contains("dml"), identity_contains("source")])
        .await?;

    insert_and_flush!(cluster);

    cluster
        .reschedule(fragment.reschedule([0, 2, 4], []))
        .await?;

    insert_and_flush!(cluster);

    cluster.reschedule(fragment.reschedule([1], [0, 4])).await?;

    insert_and_flush!(cluster);

    Ok(())
}

#[madsim::test]
async fn test_mv_on_scaled_table() -> Result<()> {
    let mut cluster = Cluster::start(Configuration::for_scale()).await?;
    cluster.run(ROOT_TABLE_CREATE).await?;

    let fragment = cluster
        .locate_one_fragment([identity_contains("materialize")])
        .await?;

    cluster.reschedule(fragment.reschedule([0, 2, 4], [])).await?;

    insert_and_flush!(cluster);

    cluster.reschedule(fragment.reschedule([1], [0, 4])).await?;

    insert_and_flush!(cluster);

    cluster.run(MV1).await?;

    insert_and_flush!(cluster);

    Ok(())
}
