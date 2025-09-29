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

use std::iter::repeat_with;

use anyhow::Result;
use itertools::Itertools;
use risingwave_simulation::cluster::{Cluster, Configuration};
use risingwave_simulation::ctl_ext::predicate::identity_contains;

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

#[tokio::test]
async fn test_table() -> Result<()> {
    let configuration = Configuration::for_scale();
    let total_core = configuration.compute_nodes * configuration.compute_node_cores;

    let mut cluster = Cluster::start(configuration).await?;
    cluster.run(ROOT_TABLE_CREATE).await?;

    insert_and_flush!(cluster);

    cluster
        .run(format!(
            "alter table t set parallelism = {}",
            total_core - 3
        ))
        .await?;

    insert_and_flush!(cluster);

    cluster
        .run(format!(
            "alter table t set parallelism = {}",
            total_core - 2
        ))
        .await?;

    insert_and_flush!(cluster);

    Ok(())
}

#[tokio::test]
async fn test_mv_on_scaled_table() -> Result<()> {
    let configuration = Configuration::for_scale();
    let total_core = configuration.compute_nodes * configuration.compute_node_cores;

    let mut cluster = Cluster::start(configuration).await?;
    cluster.run(ROOT_TABLE_CREATE).await?;

    cluster
        .run(format!(
            "alter table t set parallelism = {}",
            total_core - 3
        ))
        .await?;

    insert_and_flush!(cluster);

    cluster
        .run(format!(
            "alter table t set parallelism = {}",
            total_core - 1
        ))
        .await?;

    insert_and_flush!(cluster);

    cluster.run(MV1).await?;

    insert_and_flush!(cluster);

    Ok(())
}

#[tokio::test]
async fn test_scale_on_schema_change() -> Result<()> {
    let configuration = Configuration::for_scale_no_shuffle();
    let total_core = configuration.compute_nodes * configuration.compute_node_cores;
    let mut cluster = Cluster::start(configuration).await?;

    cluster.run(ROOT_TABLE_CREATE).await?;

    cluster.run(MV1).await?;

    cluster
        .run(format!(
            "alter table t set parallelism = {}",
            total_core - 3
        ))
        .await?;

    insert_and_flush!(cluster);

    cluster.run("alter table t add column v2 int").await?;

    cluster
        .run(format!(
            "alter table t set parallelism = {}",
            total_core - 2
        ))
        .await?;

    let fragment = cluster
        .locate_one_fragment([identity_contains("materialize"), identity_contains("union")])
        .await?;
    let used = fragment.used_worker_slots();
    assert_eq!(used.len(), 4);

    insert_and_flush!(cluster);

    Ok(())
}
